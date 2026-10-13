use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use anyhow::{anyhow, bail, Result};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, info, warn};

use crate::{
    chat::ChatManager,
    config::{Config, ConfigStore, Priority},
    entities::{
        drops::{Campaign, Drop},
        streamer::{ChatPresence, Source, Streamer, StreamerSettings},
    },
    pubsub::{PubSubMessage, PubSubPool},
    twitch::{PointsContext, StreamInfo, Twitch},
    utils::{millify, now, random_between},
};

const MAX_WATCHING: usize = 2;
const MAX_EVENTS: usize = 300;

#[derive(Debug, Clone, Serialize)]
pub struct LogEvent {
    pub ts: f64,
    pub text: String,
}

pub struct Miner {
    pub twitch: Twitch,
    pub config: Config,
    store: ConfigStore,
    streamers: RwLock<Vec<Arc<Streamer>>>,
    pubsub: PubSubPool,
    chat: ChatManager,
    events: Mutex<VecDeque<LogEvent>>,
    adding: Mutex<HashSet<String>>,
    pub started_at: f64,
}

fn valid_login(name: &str) -> bool {
    (1..=25).contains(&name.len()) && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl Miner {
    pub fn new(
        twitch: Twitch,
        config: Config,
        store: ConfigStore,
        pubsub_messages: mpsc::UnboundedSender<String>,
    ) -> Self {
        let token = twitch.creds.auth_token.clone();
        let chat = ChatManager::new(twitch.creds.login.clone(), token.clone());
        Self {
            twitch,
            config,
            store,
            streamers: RwLock::new(Vec::new()),
            pubsub: PubSubPool::new(token, pubsub_messages),
            chat,
            events: Mutex::new(VecDeque::new()),
            adding: Mutex::new(HashSet::new()),
            started_at: now(),
        }
    }

    /// Logs a notable event and keeps it for the web UI.
    pub fn event(&self, text: impl Into<String>) {
        let text = text.into();
        info!("{text}");
        let mut events = self.events.lock().unwrap();
        events.push_back(LogEvent { ts: now(), text });
        while events.len() > MAX_EVENTS {
            events.pop_front();
        }
    }

    pub fn events(&self) -> Vec<LogEvent> {
        self.events.lock().unwrap().iter().cloned().collect()
    }

    pub fn streamers(&self) -> Vec<Arc<Streamer>> {
        self.streamers.read().unwrap().clone()
    }

    fn find_by_channel(&self, channel_id: &str) -> Option<Arc<Streamer>> {
        self.streamers
            .read()
            .unwrap()
            .iter()
            .find(|s| s.channel_id == channel_id)
            .cloned()
    }

    // === Startup / runtime management === //

    pub async fn run(self: Arc<Self>, pubsub_rx: mpsc::UnboundedReceiver<String>) {
        tokio::spawn(self.clone().dispatch_pubsub(pubsub_rx));
        self.pubsub
            .submit(format!("community-points-user-v1.{}", self.twitch.creds.user_id));

        if self.config.claim_drops_startup {
            self.claim_all_drops_from_inventory().await;
        }

        // (username, settings, source, channel id if already known)
        let mut entries: Vec<(String, StreamerSettings, Source, Option<String>)> = Vec::new();
        for entry in &self.config.streamers {
            match entry.settings(&self.config.streamer_defaults) {
                Ok(settings) => entries.push((entry.username(), settings, Source::Config, None)),
                Err(e) => warn!("{e:#}"),
            }
        }
        if self.config.followers {
            match self.twitch.followers(self.config.followers_order).await {
                Ok(follows) => {
                    self.event(format!("Loaded {} followed channels from your profile", follows.len()));
                    for (name, id) in follows {
                        if !self.config.blacklist.contains(&name) {
                            entries.push((name, self.config.streamer_defaults.clone(), Source::Followers, id));
                        }
                    }
                }
                Err(e) => warn!("Could not load followed channels: {e:#}"),
            }
        }
        let mut seen = HashSet::new();
        entries.retain_mut(|(name, ..)| match normalize_login(name) {
            Ok(normalized) => {
                *name = normalized;
                seen.insert(name.clone())
            }
            Err(e) => {
                warn!("Skipping {name}: {e:#}");
                false
            }
        });
        self.load_streamers(entries).await;

        tokio::spawn(self.clone().watch_loop());
        tokio::spawn(self.clone().campaigns_loop());
        tokio::spawn(self.clone().refresh_context_loop());
    }

    pub async fn add_streamer(
        &self,
        username: &str,
        settings: StreamerSettings,
        source: Source,
        persist: bool,
    ) -> Result<Arc<Streamer>> {
        let username = normalize_login(username)?;
        if self.streamers.read().unwrap().iter().any(|s| s.username == username) {
            bail!("{username} is already being mined");
        }
        if !self.adding.lock().unwrap().insert(username.clone()) {
            bail!("{username} is already being added");
        }
        let result = self.add_streamer_inner(&username, settings, source, persist).await;
        self.adding.lock().unwrap().remove(&username);
        result
    }

    async fn add_streamer_inner(
        &self,
        username: &str,
        settings: StreamerSettings,
        source: Source,
        persist: bool,
    ) -> Result<Arc<Streamer>> {
        let channel_id = self
            .twitch
            .get_channel_id(username)
            .await?
            .ok_or_else(|| anyhow!("streamer {username} does not exist"))?;
        let streamer = Arc::new(Streamer::new(username.to_string(), channel_id, settings, source));
        self.init_streamers(std::slice::from_ref(&streamer)).await;
        self.register(&streamer);

        if persist {
            if let Err(e) = self.store.add_streamer(username) {
                warn!("Could not save {username} to the config file: {e:#}");
            }
            self.event(format!("Added {streamer}"));
        }
        Ok(streamer)
    }

    /// Loads the startup streamers with batched requests: one round for channel ids, one for
    /// points + live status, one for drop campaigns of live channels.
    async fn load_streamers(&self, mut entries: Vec<(String, StreamerSettings, Source, Option<String>)>) {
        // Reserve the names so a web UI add can't load the same streamer meanwhile.
        let reserved: Vec<String> = {
            let existing: HashSet<String> = self.streamers().iter().map(|s| s.username.clone()).collect();
            let mut adding = self.adding.lock().unwrap();
            entries.retain(|(name, ..)| !existing.contains(name) && adding.insert(name.clone()));
            entries.iter().map(|(name, ..)| name.clone()).collect()
        };
        self.event(format!("Loading data for {} streamers. Please wait...", entries.len()));

        let unknown: Vec<String> = entries
            .iter()
            .filter(|(.., id)| id.is_none())
            .map(|(name, ..)| name.clone())
            .collect();
        let mut ids = self.twitch.channel_ids(&unknown).await.into_iter();
        let mut streamers = Vec::with_capacity(entries.len());
        for (name, settings, source, id) in entries {
            let id = match id {
                Some(id) => Ok(Some(id)),
                None => ids.next().unwrap_or_else(|| Err(anyhow!("no channel id returned"))),
            };
            match id {
                Ok(Some(id)) => streamers.push(Arc::new(Streamer::new(name, id, settings, source))),
                Ok(None) => warn!("Skipping {name}: streamer does not exist"),
                Err(e) => warn!("Skipping {name}: {e:#}"),
            }
        }

        self.init_streamers(&streamers).await;
        for streamer in &streamers {
            self.register(streamer);
        }
        let mut adding = self.adding.lock().unwrap();
        for name in &reserved {
            adding.remove(name);
        }
    }

    /// Loads channel points and live status for new streamers in as few requests as possible.
    async fn init_streamers(&self, streamers: &[Arc<Streamer>]) {
        let logins: Vec<String> = streamers.iter().map(|s| s.username.clone()).collect();
        let overview = self.twitch.channels_overview(&logins).await;

        let mut claims = Vec::new();
        let mut live = Vec::new();
        for (streamer, (points, info)) in streamers.iter().zip(overview) {
            match points {
                Ok(Some(ctx)) => {
                    if let Some(claim_id) = self.apply_points(streamer, ctx) {
                        claims.push((streamer, claim_id));
                    }
                }
                Ok(None) => warn!("Could not load channel points for {}: channel not found", streamer.username),
                Err(e) => warn!("Could not load channel points for {}: {e:#}", streamer.username),
            }
            match info {
                Ok(Some(info)) => live.push((streamer, info)),
                Ok(None) => self.set_offline(streamer),
                Err(e) => warn!("Could not update stream info for {}: {e:#}", streamer.username),
            }
        }

        let drops_channels: Vec<String> = live
            .iter()
            .filter(|(s, info)| wants_campaigns(s, info))
            .map(|(s, _)| s.channel_id.clone())
            .collect();
        let mut campaigns = self.twitch.campaign_ids_for_channels(&drops_channels).await.into_iter();
        for (streamer, info) in live {
            let campaigns_ids = match wants_campaigns(streamer, &info) {
                true => campaigns.next().unwrap_or_default(),
                false => Vec::new(),
            };
            self.apply_stream_info(streamer, info, campaigns_ids);
            self.set_online(streamer);
        }

        for (streamer, claim_id) in claims {
            self.claim_bonus(streamer, &claim_id).await;
        }
    }

    /// Starts mining a loaded streamer: PubSub topics and chat.
    fn register(&self, streamer: &Arc<Streamer>) {
        self.streamers.write().unwrap().push(streamer.clone());
        for topic in channel_topics(streamer) {
            self.pubsub.submit(topic);
        }
        self.update_chat(streamer);
    }

    pub fn remove_streamer(&self, username: &str) -> Result<()> {
        let username = username.trim().to_lowercase();
        let streamer = {
            let mut list = self.streamers.write().unwrap();
            let pos = list
                .iter()
                .position(|s| s.username == username)
                .ok_or_else(|| anyhow!("{username} is not being mined"))?;
            list.remove(pos)
        };
        for topic in channel_topics(&streamer) {
            self.pubsub.remove(&topic);
        }
        self.chat.leave(&username);

        // A followed channel may also be in `streamers` if it was prioritized.
        let saved = match streamer.source {
            Source::Config => self.store.remove_streamer(&username),
            Source::Followers => self
                .store
                .remove_streamer(&username)
                .and_then(|_| self.store.add_to_blacklist(&username)),
        };
        if let Err(e) = saved {
            warn!("Could not update the config file: {e:#}");
        }
        self.event(format!("Removed {streamer}"));
        Ok(())
    }

    pub fn set_prioritized(&self, username: &str, prioritized: bool) -> Result<Arc<Streamer>> {
        let username = username.trim().to_lowercase();
        let streamer = self
            .streamers()
            .into_iter()
            .find(|s| s.username == username)
            .ok_or_else(|| anyhow!("{username} is not being mined"))?;
        streamer.state().prioritized = prioritized;
        if let Err(e) = self.store.set_prioritize(&username, prioritized) {
            warn!("Could not save the priority of {username} to the config file: {e:#}");
        }
        self.event(if prioritized {
            format!("Prioritized {streamer}: it gets a watch slot whenever it's live")
        } else {
            format!("{streamer} is no longer prioritized")
        });
        Ok(streamer)
    }

    // === Online state === //

    fn set_online(&self, streamer: &Streamer) {
        let changed = {
            let mut st = streamer.state();
            let changed = !st.is_online || !st.checked;
            if !st.is_online {
                st.online_at = now();
                st.is_online = true;
                st.stream.init_watch_streak();
            }
            st.checked = true;
            changed
        };
        if changed {
            self.event(format!("{streamer} is Online!"));
        }
        self.update_chat(streamer);
    }

    fn set_offline(&self, streamer: &Streamer) {
        let changed = {
            let mut st = streamer.state();
            let changed = st.is_online || !st.checked;
            if st.is_online {
                st.offline_at = now();
                st.is_online = false;
            }
            st.watching = false;
            st.checked = true;
            st.stream.invalidate();
            changed
        };
        if changed {
            self.event(format!("{streamer} is Offline!"));
        }
        self.update_chat(streamer);
    }

    fn update_chat(&self, streamer: &Streamer) {
        let online = streamer.state().is_online;
        let join = match streamer.settings.chat {
            ChatPresence::Always => true,
            ChatPresence::Never => false,
            ChatPresence::Online => online,
            ChatPresence::Offline => !online,
        };
        if join {
            self.chat.join(&streamer.username);
        } else {
            self.chat.leave(&streamer.username);
        }
    }

    async fn check_online(&self, streamer: &Streamer) {
        if now() < streamer.state().offline_at + 60.0 {
            return;
        }
        match self.update_stream(streamer).await {
            Ok(true) => self.set_online(streamer),
            Ok(false) => self.set_offline(streamer),
            Err(e) => warn!("Could not update stream info for {}: {e:#}", streamer.username),
        }
    }

    /// Returns whether the streamer is live.
    async fn update_stream(&self, streamer: &Streamer) -> Result<bool> {
        if !streamer.state().stream.update_required() {
            return Ok(true);
        }
        let Some(info) = self.twitch.stream_info(&streamer.username).await? else {
            return Ok(false);
        };
        let campaigns_ids = match wants_campaigns(streamer, &info) {
            true => self.twitch.campaign_ids_for_channel(&streamer.channel_id).await,
            false => Vec::new(),
        };
        self.apply_stream_info(streamer, info, campaigns_ids);
        Ok(true)
    }

    fn apply_stream_info(&self, streamer: &Streamer, info: StreamInfo, campaigns_ids: Vec<String>) {
        let drops_game = info.game.clone().filter(|_| streamer.settings.claim_drops);
        let user_id = &self.twitch.creds.user_id;
        let mut properties = json!({
            "channel_id": streamer.channel_id,
            "broadcast_id": info.broadcast_id,
            "player": "site",
            "user_id": user_id.parse::<u64>().map(Value::from).unwrap_or_else(|_| user_id.clone().into()),
            "live": true,
            "channel": streamer.username,
        });
        if let Some(game) = &drops_game {
            properties["game"] = game.name.clone().into();
            properties["game_id"] = game.id.clone().into();
        }

        let mut st = streamer.state();
        st.stream
            .update(info.broadcast_id, &info.title, info.game, info.viewers);
        st.stream.campaigns_ids = campaigns_ids;
        st.stream.payload = Some(json!([{ "event": "minute-watched", "properties": properties }]));
    }

    async fn load_points_context(&self, streamer: &Streamer) -> Result<()> {
        let Some(ctx) = self.twitch.channel_points_context(&streamer.username).await? else {
            bail!("streamer {} does not exist", streamer.username);
        };
        if let Some(claim_id) = self.apply_points(streamer, ctx) {
            self.claim_bonus(streamer, &claim_id).await;
        }
        Ok(())
    }

    /// Returns the id of a bonus waiting to be claimed.
    fn apply_points(&self, streamer: &Streamer, ctx: PointsContext) -> Option<String> {
        let mut st = streamer.state();
        st.set_points(ctx.balance);
        st.multipliers = ctx.multipliers;
        ctx.claim_id
    }

    async fn claim_bonus(&self, streamer: &Streamer, claim_id: &str) {
        self.event(format!("Claiming the bonus for {streamer}!"));
        if let Err(e) = self.twitch.claim_bonus(&streamer.channel_id, claim_id).await {
            warn!("Bonus claim failed for {}: {e:#}", streamer.username);
        }
    }

    // === Minute watched === //

    fn select_watching(&self, candidates: &[Arc<Streamer>]) -> Vec<Arc<Streamer>> {
        let mut selected: Vec<Arc<Streamer>> = Vec::new();
        let take =|s: &Arc<Streamer>, selected: &mut Vec<Arc<Streamer>>| {
            if selected.len() < MAX_WATCHING && !selected.iter().any(|x| Arc::ptr_eq(x, s)) {
                selected.push(s.clone());
            }
        };
        // Prioritized channels come first, in list order, ahead of the `priority` rules.
        candidates
            .iter()
            .filter(|s| s.state().prioritized)
            .for_each(|s| take(s, &mut selected));
        let now = now();
        for priority in &self.config.priority {
            if selected.len() >= MAX_WATCHING {
                break;
            }
            match priority {
                Priority::Order => candidates.iter().for_each(|s| take(s, &mut selected)),
                Priority::PointsAscending | Priority::PointsDescending => {
                    let mut sorted: Vec<_> = candidates
                        .iter()
                        .map(|s| (s.state().channel_points, s))
                        .collect();
                    sorted.sort_by_key(|(points, _)| *points);
                    if *priority == Priority::PointsDescending {
                        sorted.reverse();
                    }
                    sorted.into_iter().for_each(|(_, s)| take(s, &mut selected));
                }
                // Watch streak: be there for at least ~7 minutes of a stream that started
                // more than 30 minutes after the previous one ended.
                Priority::Streak => candidates
                    .iter()
                    .filter(|s| {
                        let st = s.state();
                        s.settings.watch_streak
                            && st.stream.watch_streak_missing
                            && (st.offline_at == 0.0 || (now - st.offline_at) / 60.0 > 30.0)
                            && st.stream.minute_watched < 7.0
                    })
                    .for_each(|s| take(s, &mut selected)),
                Priority::Drops => candidates
                    .iter()
                    .filter(|s| s.drops_condition())
                    .for_each(|s| take(s, &mut selected)),
                Priority::Subscribed => {
                    let mut with_multiplier: Vec<_> = candidates
                        .iter()
                        .map(|s| (s.state().total_multiplier(), s))
                        .filter(|(m, _)| *m > 0.0)
                        .collect();
                    with_multiplier.sort_by(|a, b| b.0.total_cmp(&a.0));
                    with_multiplier.into_iter().for_each(|(_, s)| take(s, &mut selected));
                }
            }
        }
        selected
    }

    async fn watch_loop(self: Arc<Self>) {
        loop {
            let streamers = self.streamers();
            let now = now();
            let candidates: Vec<_> = streamers
                .iter()
                .filter(|s| {
                    let st = s.state();
                    st.is_online && (st.online_at == 0.0 || now - st.online_at > 30.0)
                })
                .cloned()
                .collect();

            // Online but not refreshed in 10 minutes? Double check it's still live.
            for s in &candidates {
                if s.state().stream.update_elapsed() / 60.0 > 10.0 {
                    self.check_online(s).await;
                }
            }

            let watching = self.select_watching(&candidates);
            for s in &streamers {
                s.state().watching = watching.iter().any(|w| Arc::ptr_eq(w, s));
            }
            if watching.is_empty() {
                tokio::time::sleep(Duration::from_secs(20)).await;
                continue;
            }

            for s in &watching {
                let next = Instant::now() + Duration::from_secs_f64(20.0 / watching.len() as f64);
                if let Err(e) = self.watch_once(s).await {
                    debug!("Minute watched failed for {}: {e:#}", s.username);
                }
                tokio::time::sleep_until(next).await;
            }
        }
    }

    async fn watch_once(&self, streamer: &Streamer) -> Result<()> {
        let payload = streamer
            .state()
            .stream
            .encode_payload()
            .ok_or_else(|| anyhow!("no stream payload yet"))?;
        let spade = self.twitch.spade_url(&streamer.username).await?;
        if !self.twitch.watch(&streamer.username, &spade, &payload).await? {
            // The cached endpoint may have changed; scrape it again next time.
            self.twitch.forget_spade_url();
            return Ok(());
        }

        let lines = {
            let mut st = streamer.state();
            st.stream.update_minute_watched();
            let header = format!("{} is streaming {}", streamer.username, st.stream);
            let mut lines = Vec::new();
            for campaign in &mut st.stream.campaigns {
                let campaign_line = format!("Campaign: {campaign}");
                for drop in &mut campaign.drops {
                    if drop.has_preconditions_met != Some(false) && drop.is_printable {
                        drop.is_printable = false;
                        lines.extend([
                            header.clone(),
                            campaign_line.clone(),
                            format!("Drop: {drop}"),
                            drop.progress_bar(),
                        ]);
                    }
                }
            }
            lines
        };
        for line in lines {
            self.event(line);
        }
        Ok(())
    }

    async fn refresh_context_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(30 * 60)).await;
            for s in self.streamers() {
                if s.state().is_online {
                    if let Err(e) = self.load_points_context(&s).await {
                        warn!("Could not refresh channel points for {}: {e:#}", s.username);
                    }
                }
            }
        }
    }

    // === Drops === //

    async fn claim_drop(&self, drop: &Drop) -> bool {
        let Some(instance_id) = &drop.drop_instance_id else {
            return false;
        };
        self.event(format!("Claim {drop}"));
        self.twitch.claim_drop(instance_id).await
    }

    pub async fn claim_all_drops_from_inventory(&self) {
        let inventory = match self.twitch.inventory().await {
            Ok(inv) => inv,
            Err(e) => return warn!("Could not load the drops inventory: {e:#}"),
        };
        for campaign in inventory["dropCampaignsInProgress"].as_array().into_iter().flatten() {
            for drop_json in campaign["timeBasedDrops"].as_array().into_iter().flatten() {
                let Some(mut drop) = Drop::from_json(drop_json) else { continue };
                drop.update(&drop_json["self"]);
                if drop.is_claimable {
                    drop.is_claimed = self.claim_drop(&drop).await;
                    tokio::time::sleep(random_between(5.0, 10.0)).await;
                }
            }
        }
    }

    async fn fetch_campaigns(&self) -> Result<Vec<Campaign>> {
        let ids: Vec<String> = self
            .twitch
            .active_drop_campaigns()
            .await?
            .iter()
            .filter_map(|c| c["id"].as_str().map(str::to_string))
            .collect();
        let details = self.twitch.campaign_details(&ids).await?;
        Ok(details
            .iter()
            .filter_map(Campaign::from_json)
            .filter(Campaign::dt_match)
            .filter_map(|mut c| {
                c.clear_drops();
                (!c.drops.is_empty()).then_some(c)
            })
            .collect())
    }

    /// Updates drop progress from the inventory and claims whatever is ready.
    async fn sync_campaigns(&self, campaigns: &mut [Campaign], inventory: &Value) {
        let Some(in_progress) = inventory["dropCampaignsInProgress"].as_array() else {
            return;
        };
        for campaign in campaigns.iter_mut() {
            campaign.clear_drops();
            let Some(progress) = in_progress
                .iter()
                .find(|p| p["id"].as_str() == Some(campaign.id.as_str()))
            else {
                continue;
            };
            campaign.in_inventory = true;
            for drop_json in progress["timeBasedDrops"].as_array().into_iter().flatten() {
                let Some(drop) = campaign
                    .drops
                    .iter_mut()
                    .find(|d| drop_json["id"].as_str() == Some(d.id.as_str()))
                else {
                    continue;
                };
                drop.update(&drop_json["self"]);
                if drop.is_claimable {
                    drop.is_claimed = self.claim_drop(drop).await;
                }
            }
            campaign.clear_drops();
        }
    }

    async fn campaigns_loop(self: Arc<Self>) {
        let mut campaigns: Vec<Campaign> = Vec::new();
        let mut last_refresh = 0.0;
        loop {
            let streamers = self.streamers();
            if streamers.iter().any(|s| s.settings.claim_drops) {
                if now() - last_refresh > 30.0 * 60.0 {
                    last_refresh = now();
                    self.claim_all_drops_from_inventory().await;
                    match self.fetch_campaigns().await {
                        Ok(c) => campaigns = c,
                        Err(e) => warn!("Could not load drop campaigns: {e:#}"),
                    }
                }
                match self.twitch.inventory().await {
                    Ok(inventory) => self.sync_campaigns(&mut campaigns, &inventory).await,
                    Err(e) => warn!("Error while syncing inventory: {e:#}"),
                }
                for s in streamers.iter().filter(|s| s.drops_condition()) {
                    let mut st = s.state();
                    let game_id = st.stream.game.as_ref().map(|g| g.id.clone()).unwrap_or_default();
                    let matching = campaigns
                        .iter()
                        .filter(|c| {
                            !c.drops.is_empty()
                                && c.game_id == game_id
                                && st.stream.campaigns_ids.contains(&c.id)
                        })
                        .cloned()
                        .collect();
                    st.stream.campaigns = matching;
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    // === PubSub === //

    async fn dispatch_pubsub(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<String>) {
        let mut last: Option<(String, String)> = None;
        while let Some(raw) = rx.recv().await {
            let Some(msg) = PubSubMessage::parse(&raw) else {
                debug!("Unparseable PubSub message: {raw}");
                continue;
            };
            // With several connections the same message may arrive twice.
            let key = (msg.timestamp_raw.clone(), msg.identifier());
            if last.as_ref() == Some(&key) {
                continue;
            }
            last = Some(key);

            let Some(streamer) = self.find_by_channel(&msg.channel_id) else {
                continue;
            };
            self.handle_message(streamer, msg);
        }
    }

    fn handle_message(self: &Arc<Self>, streamer: Arc<Streamer>, msg: PubSubMessage) {
        let data = &msg.data;
        match (msg.topic.as_str(), msg.kind.as_str()) {
            ("community-points-user-v1", kind @ ("points-earned" | "points-spent")) => {
                if let Some(balance) = data["balance"]["balance"].as_i64() {
                    streamer.state().set_points(balance);
                }
                if kind == "points-earned" {
                    let earned = data["point_gain"]["total_points"].as_i64().unwrap_or(0);
                    let reason = data["point_gain"]["reason_code"].as_str().unwrap_or("UNKNOWN");
                    streamer.state().update_history(reason, earned, 1);
                    self.event(format!("+{earned} → {streamer} - Reason: {reason}."));
                }
            }
            ("community-points-user-v1", "claim-available") => {
                if let Some(claim_id) = data["claim"]["id"].as_str().map(str::to_string) {
                    let miner = self.clone();
                    tokio::spawn(async move { miner.claim_bonus(&streamer, &claim_id).await });
                }
            }
            ("video-playback-by-id", "stream-up") => streamer.state().stream_up = now(),
            ("video-playback-by-id", "stream-down") => {
                if streamer.state().is_online {
                    self.set_offline(&streamer);
                }
            }
            ("video-playback-by-id", "viewcount") => {
                if streamer.state().stream_up_elapsed() {
                    let miner = self.clone();
                    tokio::spawn(async move { miner.check_online(&streamer).await });
                }
            }
            ("raid", "raid_update_v2") => {
                let raid = &msg.message["raid"];
                let (Some(raid_id), Some(target)) = (raid["id"].as_str(), raid["target_login"].as_str()) else {
                    return;
                };
                if !streamer.settings.follow_raid {
                    return;
                }
                {
                    let mut st = streamer.state();
                    if st.raid_id.as_deref() == Some(raid_id) {
                        return;
                    }
                    st.raid_id = Some(raid_id.to_string());
                }
                self.event(format!("Joining raid from {streamer} to {target}!"));
                let (miner, raid_id) = (self.clone(), raid_id.to_string());
                tokio::spawn(async move {
                    if let Err(e) = miner.twitch.join_raid(&raid_id).await {
                        warn!("Joining raid failed: {e:#}");
                    }
                });
            }
            ("community-moments-channel-v1", "active") => {
                if let (true, Some(moment_id)) = (streamer.settings.claim_moments, data["moment_id"].as_str()) {
                    self.event(format!("Claiming the moment for {streamer}!"));
                    let (miner, moment_id) = (self.clone(), moment_id.to_string());
                    tokio::spawn(async move {
                        if let Err(e) = miner.twitch.claim_moment(&moment_id).await {
                            warn!("Moment claim failed: {e:#}");
                        }
                    });
                }
            }
            _ => {}
        }
    }

    /// Printed on shutdown, like the original's session report.
    pub fn report(&self) {
        let minutes = ((now() - self.started_at) / 60.0) as i64;
        info!("Session duration: {}h {}m", minutes / 60, minutes % 60);
        for s in self.streamers() {
            let st = s.state();
            let gained = st.channel_points - st.starting_points.unwrap_or(st.channel_points);
            info!(
                "{} ({} points), total gained: {} {}",
                s.username,
                millify(st.channel_points),
                millify(gained),
                st.print_history()
            );
        }
    }
}

fn channel_topics(streamer: &Streamer) -> Vec<String> {
    let id = &streamer.channel_id;
    let settings = &streamer.settings;
    let mut topics = vec![format!("video-playback-by-id.{id}")];
    if settings.follow_raid {
        topics.push(format!("raid.{id}"));
    }
    if settings.claim_moments {
        topics.push(format!("community-moments-channel-v1.{id}"));
    }
    topics
}

fn normalize_login(name: &str) -> Result<String> {
    let name = name.trim().trim_start_matches('@').to_lowercase();
    if !valid_login(&name) {
        bail!("'{name}' is not a valid Twitch username");
    }
    Ok(name)
}

/// Whether to look up drop campaigns for a live stream.
fn wants_campaigns(streamer: &Streamer, info: &StreamInfo) -> bool {
    streamer.settings.claim_drops && info.game.is_some()
}

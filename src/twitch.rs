//! Twitch GraphQL / HTTP client.

use std::{sync::Mutex, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use regex::Regex;
use reqwest::Client;
use serde_json::{json, Value};
use tracing::debug;

use crate::{
    auth::Credentials,
    config::FollowersOrder,
    constants::*,
    entities::stream::Game,
    utils::{now, random_hex},
};

pub struct PointsContext {
    pub balance: i64,
    pub multipliers: Vec<f64>,
    pub claim_id: Option<String>,
}

pub struct StreamInfo {
    pub broadcast_id: String,
    pub title: String,
    pub game: Option<Game>,
    pub viewers: i64,
}

pub struct Twitch {
    http: Client,
    pub creds: Credentials,
    client_session: String,
    /// (Client-Version, fetched at)
    client_version: Mutex<(String, f64)>,
    build_id_re: Regex,
    settings_re: Regex,
    spade_re: Regex,
}

fn last_url_line(text: &str) -> Option<&str> {
    text.lines()
        .map(str::trim)
        .filter(|l| l.starts_with("http"))
        .next_back()
}

impl Twitch {
    pub fn new(http: Client, creds: Credentials) -> Self {
        Self {
            http,
            creds,
            client_session: random_hex(16),
            client_version: Mutex::new((CLIENT_VERSION.to_string(), 0.0)),
            build_id_re: Regex::new(
                r#"window\.__twilightBuildID\s*=\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})""#,
            )
            .unwrap(),
            settings_re: Regex::new(
                r"(https://static\.twitchcdn\.net/config/settings.*?js|https://assets\.twitch\.tv/config/settings.*?\.js)",
            )
            .unwrap(),
            spade_re: Regex::new(r#""spade_url":"(.*?)""#).unwrap(),
        }
    }

    /// The web client's build id, refreshed hourly.
    async fn client_version(&self) -> String {
        {
            let cached = self.client_version.lock().unwrap();
            if now() - cached.1 < 3600.0 {
                return cached.0.clone();
            }
        }
        let fetched = async {
            let page = self
                .http
                .get(TWITCH_URL)
                .header("User-Agent", CHROME_USER_AGENT)
                .send()
                .await
                .ok()?
                .text()
                .await
                .ok()?;
            Some(self.build_id_re.captures(&page)?[1].to_string())
        }
        .await;
        let mut cached = self.client_version.lock().unwrap();
        if let Some(version) = fetched {
            cached.0 = version;
        }
        cached.1 = now();
        cached.0.clone()
    }

    pub async fn gql(&self, body: &Value) -> Result<Value> {
        let resp = self
            .http
            .post(GQL_URL)
            .json(body)
            .header("Authorization", format!("OAuth {}", self.creds.auth_token))
            .header("Client-Id", CLIENT_ID)
            .header("Client-Session-Id", &self.client_session)
            .header("Client-Version", self.client_version().await)
            .header("User-Agent", CHROME_USER_AGENT)
            .header("X-Device-Id", &self.creds.device_id)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        debug!("GQL {} -> {status}: {text}", body["operationName"]);
        serde_json::from_str(&text).with_context(|| format!("GQL returned {status}: {text}"))
    }

    pub async fn get_channel_id(&self, login: &str) -> Result<Option<String>> {
        let resp = self.gql(&GET_ID_FROM_LOGIN.with(json!({ "login": login }))).await?;
        if resp.get("data").is_none() {
            bail!("unexpected response: {resp}");
        }
        Ok(resp["data"]["user"]["id"].as_str().map(str::to_string))
    }

    /// `Ok(None)` means the channel is offline.
    pub async fn stream_info(&self, login: &str) -> Result<Option<StreamInfo>> {
        let resp = self.gql(&STREAM_INFO.with(json!({ "channel": login }))).await?;
        let user = &resp["data"]["user"];
        if resp.get("data").is_none() {
            bail!("unexpected response: {resp}");
        }
        let stream = &user["stream"];
        if stream.is_null() {
            return Ok(None);
        }
        Ok(Some(StreamInfo {
            broadcast_id: stream["id"].as_str().unwrap_or_default().to_string(),
            title: user["broadcastSettings"]["title"].as_str().unwrap_or_default().to_string(),
            game: Game::from_json(&user["broadcastSettings"]["game"]),
            viewers: stream["viewersCount"].as_i64().unwrap_or(0),
        }))
    }

    /// Scrapes the analytics ("spade") endpoint used for minute-watched events.
    pub async fn spade_url(&self, login: &str) -> Result<String> {
        let page = self
            .http
            .get(format!("{TWITCH_URL}/{login}"))
            .header("User-Agent", FIREFOX_USER_AGENT)
            .send()
            .await?
            .text()
            .await?;
        let settings_url = self
            .settings_re
            .captures(&page)
            .ok_or_else(|| anyhow!("settings.js URL not found on channel page"))?[1]
            .to_string();
        let settings = self
            .http
            .get(settings_url)
            .header("User-Agent", FIREFOX_USER_AGENT)
            .send()
            .await?
            .text()
            .await?;
        Ok(self
            .spade_re
            .captures(&settings)
            .ok_or_else(|| anyhow!("spade_url not found in settings.js"))?[1]
            .to_string())
    }

    /// `Ok(None)` means the channel does not exist.
    pub async fn channel_points_context(&self, login: &str) -> Result<Option<PointsContext>> {
        let resp = self
            .gql(&CHANNEL_POINTS_CONTEXT.with(json!({ "channelLogin": login })))
            .await?;
        let data = &resp["data"];
        if data.is_null() {
            bail!("unexpected response: {resp}");
        }
        if data["community"].is_null() {
            return Ok(None);
        }
        let points = &data["community"]["channel"]["self"]["communityPoints"];
        Ok(Some(PointsContext {
            balance: points["balance"].as_i64().unwrap_or(0),
            multipliers: points["activeMultipliers"]
                .as_array()
                .map(|m| m.iter().filter_map(|x| x["factor"].as_f64()).collect())
                .unwrap_or_default(),
            claim_id: points["availableClaim"]["id"].as_str().map(str::to_string),
        }))
    }

    pub async fn claim_bonus(&self, channel_id: &str, claim_id: &str) -> Result<()> {
        self.gql(&CLAIM_COMMUNITY_POINTS.with(json!({
            "input": { "channelID": channel_id, "claimID": claim_id }
        })))
        .await?;
        Ok(())
    }

    pub async fn claim_moment(&self, moment_id: &str) -> Result<()> {
        self.gql(&CLAIM_MOMENT.with(json!({ "input": { "momentID": moment_id } })))
            .await?;
        Ok(())
    }

    pub async fn join_raid(&self, raid_id: &str) -> Result<()> {
        self.gql(&JOIN_RAID.with(json!({ "input": { "raidID": raid_id } })))
            .await?;
        Ok(())
    }

    pub async fn campaign_ids_for_channel(&self, channel_id: &str) -> Vec<String> {
        let Ok(resp) = self
            .gql(&AVAILABLE_DROPS.with(json!({ "channelID": channel_id })))
            .await
        else {
            return Vec::new();
        };
        resp["data"]["channel"]["viewerDropCampaigns"]
            .as_array()
            .map(|c| c.iter().filter_map(|x| x["id"].as_str().map(str::to_string)).collect())
            .unwrap_or_default()
    }

    pub async fn inventory(&self) -> Result<Value> {
        let resp = self
            .gql(&INVENTORY.with(json!({ "fetchRewardCampaigns": true })))
            .await?;
        Ok(resp["data"]["currentUser"]["inventory"].clone())
    }

    pub async fn active_drop_campaigns(&self) -> Result<Vec<Value>> {
        let resp = self
            .gql(&VIEWER_DROPS_DASHBOARD.with(json!({ "fetchRewardCampaigns": true })))
            .await?;
        Ok(resp["data"]["currentUser"]["dropCampaigns"]
            .as_array()
            .map(|c| {
                c.iter()
                    .filter(|x| x["status"].as_str() == Some("ACTIVE"))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    pub async fn campaign_details(&self, ids: &[String]) -> Result<Vec<Value>> {
        let mut result = Vec::new();
        for chunk in ids.chunks(20) {
            let body: Vec<Value> = chunk
                .iter()
                .map(|id| {
                    DROP_CAMPAIGN_DETAILS.with(json!({
                        "dropID": id,
                        "channelLogin": self.creds.user_id,
                    }))
                })
                .collect();
            let resp = self.gql(&Value::Array(body)).await?;
            for item in resp.as_array().into_iter().flatten() {
                let campaign = &item["data"]["user"]["dropCampaign"];
                if !campaign.is_null() {
                    result.push(campaign.clone());
                }
            }
        }
        Ok(result)
    }

    pub async fn claim_drop(&self, drop_instance_id: &str) -> bool {
        let Ok(resp) = self
            .gql(&CLAIM_DROP_REWARDS.with(json!({ "input": { "dropInstanceID": drop_instance_id } })))
            .await
        else {
            return false;
        };
        matches!(
            resp["data"]["claimDropRewards"]["status"].as_str(),
            Some("ELIGIBLE_FOR_ALL" | "DROP_INSTANCE_ALREADY_CLAIMED")
        )
    }

    pub async fn followers(&self, order: FollowersOrder) -> Result<Vec<String>> {
        let order = match order {
            FollowersOrder::Asc => "ASC",
            FollowersOrder::Desc => "DESC",
        };
        let mut follows = Vec::new();
        let mut cursor = Value::String(String::new());
        loop {
            let resp = self
                .gql(&CHANNEL_FOLLOWS.with(json!({ "limit": 100, "order": order, "cursor": cursor })))
                .await?;
            let page = &resp["data"]["user"]["follows"];
            let edges = page["edges"]
                .as_array()
                .ok_or_else(|| anyhow!("unexpected follows response: {resp}"))?;
            for edge in edges {
                if let Some(login) = edge["node"]["login"].as_str() {
                    follows.push(login.to_lowercase());
                }
                cursor = edge["cursor"].clone();
            }
            if page["pageInfo"]["hasNextPage"].as_bool() != Some(true) || edges.is_empty() {
                return Ok(follows);
            }
        }
    }

    /// Simulates one minute of watching: fetch the HLS playlist, touch the newest
    /// segment of the lowest quality, then send the minute-watched analytics event.
    pub async fn watch(&self, login: &str, spade_url: &str, payload: &str) -> Result<bool> {
        let resp = self
            .gql(&PLAYBACK_ACCESS_TOKEN.with(json!({
                "login": login,
                "isLive": true,
                "isVod": false,
                "vodID": "",
                "playerType": "site",
                "platform": "web",
            })))
            .await?;
        let token = &resp["data"]["streamPlaybackAccessToken"];
        let (Some(signature), Some(value)) = (token["signature"].as_str(), token["value"].as_str()) else {
            bail!("missing playback access token in {resp}");
        };

        let timeout = Duration::from_secs(20);
        let master = self
            .http
            .get(format!("https://usher.ttvnw.net/api/channel/hls/{login}.m3u8"))
            .query(&[("sig", signature), ("token", value)])
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?;
        if !master.status().is_success() {
            bail!("usher playlist returned {}", master.status());
        }
        let master = master.text().await?;
        let variant_url = last_url_line(&master).context("no variant in master playlist")?;

        let variant = self
            .http
            .get(variant_url)
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?
            .text()
            .await?;
        let segment_url = last_url_line(&variant).context("no segment in media playlist")?;

        let head = self
            .http
            .head(segment_url)
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?;
        if !head.status().is_success() {
            bail!("segment HEAD returned {}", head.status());
        }

        let spade = self
            .http
            .post(spade_url)
            .form(&[("data", payload)])
            .header("User-Agent", CHROME_USER_AGENT)
            .timeout(timeout)
            .send()
            .await?;
        debug!("minute-watched for {login}: {}", spade.status());
        Ok(spade.status().as_u16() == 204)
    }
}

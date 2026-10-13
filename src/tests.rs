use serde_json::json;

use crate::{
    config::{Config, EXAMPLE_CONFIG},
    entities::streamer::{ChatPresence, Source},
    pubsub::PubSubMessage,
    store::Store,
};

#[test]
fn example_config_parses() {
    let config: Config = toml::from_str(EXAMPLE_CONFIG).unwrap();
    assert_eq!(config.web.port, 5000);
    assert!(config.streamers.is_empty());
}

#[test]
fn per_streamer_overrides_merge_with_defaults() {
    let text = EXAMPLE_CONFIG.replace(
        "streamers = []",
        r#"streamers = ["a", { username = "B", follow_raid = false, chat = "NEVER" }]"#,
    );
    let config: Config = toml::from_str(&text).unwrap();
    let defaults = &config.streamer_defaults;
    let b = &config.streamers[1];
    assert_eq!(b.username(), "b");
    let settings = b.settings(defaults).unwrap();
    assert!(!settings.follow_raid);
    assert!(settings.claim_drops);
    assert_eq!(settings.chat, ChatPresence::Never);
}

fn ids(store: &Store) -> Vec<String> {
    store.channels().unwrap().into_iter().map(|c| c.id).collect()
}

#[test]
fn store_adds_removes_and_blacklists() {
    let store = Store::open_in_memory().unwrap();
    let none = json!({});
    store.add_channel("1", "a", Source::Manual, false, &none).unwrap();
    store.add_channel("2", "b", Source::Followers, false, &none).unwrap();
    store.add_channel("3", "c", Source::Manual, true, &json!({ "chat": "NEVER" })).unwrap();
    assert_eq!(ids(&store), ["1", "2", "3"]);

    let c = &store.channels().unwrap()[2];
    assert!(c.prioritized);
    assert_eq!(c.settings, json!({ "chat": "NEVER" }));

    // Adding an existing channel keeps its place; adding it by hand makes it a manual channel.
    store.add_channel("2", "b_renamed", Source::Manual, false, &none).unwrap();
    store.add_channel("1", "a", Source::Followers, false, &none).unwrap();
    let rows = store.channels().unwrap();
    assert_eq!(ids(&store), ["1", "2", "3"]);
    assert_eq!(rows[0].source, Source::Manual);
    assert_eq!(rows[1].source, Source::Manual);
    assert_eq!(rows[1].login, "b_renamed");

    store.remove_channel("2").unwrap();
    store.add_to_blacklist("2", "b").unwrap();
    assert_eq!(ids(&store), ["1", "3"]);
    assert!(store.blacklist().unwrap().contains("2"));
    store.remove_from_blacklist("2").unwrap();
    assert!(store.blacklist().unwrap().is_empty());

    store.set_prioritized("1", true).unwrap();
    store.set_login("1", "new_name").unwrap();
    let a = &store.channels().unwrap()[0];
    assert!(a.prioritized);
    assert_eq!(a.login, "new_name");

    assert_eq!(store.meta("config_imported").unwrap(), None);
    store.set_meta("config_imported", "1").unwrap();
    assert_eq!(store.meta("config_imported").unwrap().as_deref(), Some("1"));
}

#[test]
fn store_reorders() {
    let store = Store::open_in_memory().unwrap();
    for id in ["1", "2", "3", "4"] {
        store.add_channel(id, id, Source::Manual, false, &json!({})).unwrap();
    }
    let order = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    store.reorder(&order(&["3", "1", "4", "2"])).unwrap();
    assert_eq!(ids(&store), ["3", "1", "4", "2"]);

    // Unknown ids are ignored, missing ones keep their relative order at the end.
    store.reorder(&order(&["2", "nope", "2"])).unwrap();
    assert_eq!(ids(&store), ["2", "3", "1", "4"]);

    // New channels go to the end.
    store.add_channel("5", "5", Source::Manual, false, &json!({})).unwrap();
    assert_eq!(ids(&store), ["2", "3", "1", "4", "5"]);
}

#[test]
fn parses_points_earned_message() {
    let inner = json!({
        "type": "points-earned",
        "data": {
            "timestamp": "2024-01-01T12:00:00.123Z",
            "channel_id": "123",
            "point_gain": { "total_points": 10, "reason_code": "WATCH" },
            "balance": { "balance": 510, "channel_id": "123" }
        }
    });
    let raw = json!({
        "type": "MESSAGE",
        "data": { "topic": "community-points-user-v1.999", "message": inner.to_string() }
    })
    .to_string();
    let msg = PubSubMessage::parse(&raw).unwrap();
    assert_eq!(msg.topic, "community-points-user-v1");
    assert_eq!(msg.kind, "points-earned");
    assert_eq!(msg.channel_id, "123");
    assert_eq!(msg.identifier(), "points-earned.community-points-user-v1.123");
}

#[test]
fn channel_topic_falls_back_to_topic_id() {
    let inner = json!({ "type": "stream-up", "server_time": 1700000000.5 });
    let raw = json!({ "type": "MESSAGE", "data": { "topic": "video-playback-by-id.42", "message": inner.to_string() } })
        .to_string();
    let msg = PubSubMessage::parse(&raw).unwrap();
    assert_eq!(msg.channel_id, "42");
    assert_eq!(msg.timestamp_raw, "1700000000.5");
}

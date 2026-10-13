use serde_json::json;

use crate::{
    config::{Config, ConfigStore, EXAMPLE_CONFIG},
    entities::streamer::ChatPresence,
    pubsub::PubSubMessage,
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

#[test]
fn config_store_adds_and_removes_preserving_comments() {
    let dir = std::env::temp_dir().join(format!("points-miner-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");
    std::fs::write(&path, EXAMPLE_CONFIG.replace("blacklist = []", "blacklist = [\"foo\"]")).unwrap();
    let store = ConfigStore::new(path.clone());

    store.add_streamer("foo").unwrap();
    store.add_streamer("bar").unwrap();
    store.add_streamer("bar").unwrap();
    let config: Config = toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let names: Vec<_> = config.streamers.iter().map(|s| s.username()).collect();
    assert_eq!(names, ["foo", "bar"]);
    assert!(config.blacklist.is_empty(), "adding a channel un-blacklists it");

    store.remove_streamer("foo").unwrap();
    store.add_to_blacklist("baz").unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    let config: Config = toml::from_str(&text).unwrap();
    let names: Vec<_> = config.streamers.iter().map(|s| s.username()).collect();
    assert_eq!(names, ["bar"]);
    assert_eq!(config.blacklist, ["baz"]);
    assert!(text.contains("# Twitch account to mine with"));
    std::fs::remove_dir_all(dir).ok();
}

#[test]
fn config_store_toggles_prioritize() {
    let dir = std::env::temp_dir().join(format!("points-miner-prio-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");
    std::fs::write(
        &path,
        EXAMPLE_CONFIG.replace("streamers = []", r#"streamers = ["a", { username = "b", chat = "NEVER" }]"#),
    )
    .unwrap();
    let store = ConfigStore::new(path.clone());
    let load = || -> Config { toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap() };

    store.set_prioritize("a", true).unwrap();
    store.set_prioritize("b", true).unwrap();
    store.set_prioritize("followed", true).unwrap();
    let config = load();
    let names: Vec<_> = config.streamers.iter().map(|s| s.username()).collect();
    assert_eq!(names, ["a", "b", "followed"]);
    for entry in &config.streamers {
        assert!(entry.settings(&config.streamer_defaults).unwrap().prioritize);
    }

    store.set_prioritize("a", false).unwrap();
    store.set_prioritize("b", false).unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains(r#"streamers = ["a", { username = "b", chat = "NEVER" }, { username = "followed", prioritize = true }]"#), "{text}");
    std::fs::remove_dir_all(dir).ok();
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

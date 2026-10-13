use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use toml_edit::{DocumentMut, Item, Value};

use crate::entities::streamer::StreamerSettings;

pub const EXAMPLE_CONFIG: &str = include_str!("../config.example.toml");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Priority {
    Order,
    Streak,
    Drops,
    Subscribed,
    PointsAscending,
    PointsDescending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FollowersOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".into(),
            port: 5000,
        }
    }
}

/// A streamer in the config: either a bare name or a table with per-streamer overrides.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StreamerEntry {
    Name(String),
    Full {
        username: String,
        #[serde(flatten)]
        overrides: toml::Table,
    },
}

impl StreamerEntry {
    pub fn username(&self) -> String {
        match self {
            Self::Name(name) => name,
            Self::Full { username, .. } => username,
        }
        .trim()
        .to_lowercase()
    }

    /// Applies this entry's overrides on top of the default streamer settings.
    pub fn settings(&self, defaults: &StreamerSettings) -> Result<StreamerSettings> {
        let Self::Full { overrides, .. } = self else {
            return Ok(defaults.clone());
        };
        let mut base = serde_json::to_value(defaults)?;
        merge(&mut base, serde_json::to_value(overrides)?);
        serde_json::from_value(base)
            .with_context(|| format!("invalid settings for streamer '{}'", self.username()))
    }
}

fn merge(base: &mut serde_json::Value, patch: serde_json::Value) {
    match (base, patch) {
        (serde_json::Value::Object(base), serde_json::Value::Object(patch)) => {
            for (key, value) in patch {
                merge(base.entry(key).or_insert(serde_json::Value::Null), value);
            }
        }
        (base, patch) => *base = patch,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub username: String,
    pub claim_drops_startup: bool,
    pub priority: Vec<Priority>,
    pub followers: bool,
    pub followers_order: FollowersOrder,
    pub blacklist: Vec<String>,
    pub web: WebConfig,
    pub streamer_defaults: StreamerSettings,
    pub streamers: Vec<StreamerEntry>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            username: String::new(),
            claim_drops_startup: false,
            priority: vec![Priority::Streak, Priority::Drops, Priority::Order],
            followers: false,
            followers_order: FollowersOrder::Asc,
            blacklist: Vec::new(),
            web: WebConfig::default(),
            streamer_defaults: StreamerSettings::default(),
            streamers: Vec::new(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut config: Config =
            toml::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))?;
        config.username = config.username.trim().to_lowercase();
        if config.username.is_empty() {
            bail!("'username' must be set in {}", path.display());
        }
        config.blacklist = config.blacklist.iter().map(|s| s.trim().to_lowercase()).collect();
        Ok(config)
    }
}

/// Edits the streamer list in the config file in place, keeping the user's comments and formatting.
pub struct ConfigStore {
    path: PathBuf,
    lock: Mutex<()>,
}

fn entry_username(value: &Value) -> Option<String> {
    let name = match value {
        Value::String(s) => s.value().as_str(),
        Value::InlineTable(t) => t.get("username")?.as_str()?,
        _ => return None,
    };
    Some(name.trim().to_lowercase())
}

impl ConfigStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            lock: Mutex::new(()),
        }
    }

    fn edit(&self, f: impl FnOnce(&mut DocumentMut) -> Result<()>) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let mut doc: DocumentMut = fs::read_to_string(&self.path)?.parse()?;
        f(&mut doc)?;
        fs::write(&self.path, doc.to_string())?;
        Ok(())
    }

    pub fn add_streamer(&self, username: &str) -> Result<()> {
        self.edit(|doc| {
            remove_from_string_array(doc, "blacklist", username);
            match doc.get_mut("streamers") {
                None => {
                    doc["streamers"] = toml_edit::value(toml_edit::Array::from_iter([username]));
                }
                Some(Item::Value(Value::Array(array))) => {
                    if !array.iter().any(|v| entry_username(v).as_deref() == Some(username)) {
                        array.push(username);
                    }
                }
                Some(Item::ArrayOfTables(tables)) => {
                    let exists = tables.iter().any(|t| {
                        t.get("username").and_then(|v| v.as_str()).map(|s| s.trim().to_lowercase())
                            == Some(username.to_string())
                    });
                    if !exists {
                        let mut table = toml_edit::Table::new();
                        table["username"] = toml_edit::value(username);
                        tables.push(table);
                    }
                }
                Some(_) => bail!("'streamers' in the config has an unexpected type"),
            }
            Ok(())
        })
    }

    pub fn remove_streamer(&self, username: &str) -> Result<()> {
        self.edit(|doc| {
            match doc.get_mut("streamers") {
                Some(Item::Value(Value::Array(array))) => {
                    array.retain(|v| entry_username(v).as_deref() != Some(username));
                }
                Some(Item::ArrayOfTables(tables)) => {
                    loop {
                        let index = tables.iter().position(|t| {
                            t.get("username").and_then(|v| v.as_str()).map(|s| s.trim().to_lowercase())
                                == Some(username.to_string())
                        });
                        match index {
                            Some(index) => tables.remove(index),
                            None => break,
                        }
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }

    /// Sets `prioritize` on a streamer's entry, adding the entry if needed (e.g. for a followed
    /// channel). A plain name becomes an inline table and turns back into a name once it has no overrides.
    pub fn set_prioritize(&self, username: &str, prioritize: bool) -> Result<()> {
        self.edit(|doc| {
            match doc.get_mut("streamers") {
                None => {
                    if prioritize {
                        let mut array = toml_edit::Array::new();
                        array.push(prioritized_entry(username));
                        doc["streamers"] = toml_edit::value(array);
                    }
                }
                Some(Item::Value(Value::Array(array))) => {
                    let index = array.iter().position(|v| entry_username(v).as_deref() == Some(username));
                    match index {
                        None if prioritize => array.push(prioritized_entry(username)),
                        None => {}
                        Some(index) => {
                            let decor = array.get(index).unwrap().decor().clone();
                            let mut entry = match array.get(index).unwrap() {
                                Value::InlineTable(t) => t.clone(),
                                _ => {
                                    let mut t = toml_edit::InlineTable::new();
                                    t.insert("username", username.into());
                                    t
                                }
                            };
                            if prioritize {
                                entry.insert("prioritize", true.into());
                            } else {
                                entry.remove("prioritize");
                            }
                            let mut value = if entry.len() == 1 && entry.contains_key("username") {
                                Value::from(username)
                            } else {
                                Value::InlineTable(entry)
                            };
                            *value.decor_mut() = decor;
                            array.replace(index, value);
                        }
                    }
                }
                Some(Item::ArrayOfTables(tables)) => {
                    let table = tables.iter_mut().find(|t| {
                        t.get("username").and_then(|v| v.as_str()).map(|s| s.trim().to_lowercase())
                            == Some(username.to_string())
                    });
                    match table {
                        Some(table) if prioritize => table["prioritize"] = toml_edit::value(true),
                        Some(table) => {
                            table.remove("prioritize");
                        }
                        None if prioritize => {
                            let mut table = toml_edit::Table::new();
                            table["username"] = toml_edit::value(username);
                            table["prioritize"] = toml_edit::value(true);
                            tables.push(table);
                        }
                        None => {}
                    }
                }
                Some(_) => bail!("'streamers' in the config has an unexpected type"),
            }
            Ok(())
        })
    }

    /// Used when a channel that came from the follow list is removed, so it stays gone after a restart.
    pub fn add_to_blacklist(&self, username: &str) -> Result<()> {
        self.edit(|doc| {
            match doc.get_mut("blacklist") {
                Some(Item::Value(Value::Array(array))) => {
                    if !array.iter().any(|v| entry_username(v).as_deref() == Some(username)) {
                        array.push(username);
                    }
                }
                _ => {
                    doc["blacklist"] = toml_edit::value(toml_edit::Array::from_iter([username]));
                }
            }
            Ok(())
        })
    }
}

fn prioritized_entry(username: &str) -> Value {
    let mut table = toml_edit::InlineTable::new();
    table.insert("username", username.into());
    table.insert("prioritize", true.into());
    Value::InlineTable(table)
}

fn remove_from_string_array(doc: &mut DocumentMut, key: &str, username: &str) {
    if let Some(Item::Value(Value::Array(array))) = doc.get_mut(key) {
        array.retain(|v| entry_username(v).as_deref() != Some(username));
    }
}

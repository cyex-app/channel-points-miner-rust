//! IRC chat presence: being in chat counts towards some channel point features.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use anyhow::{bail, Result};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    task::JoinHandle,
};
use tracing::{info, warn};

use crate::constants::{IRC_HOST, IRC_PORT};

pub struct ChatManager {
    login: String,
    token: String,
    sessions: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl ChatManager {
    pub fn new(login: String, token: String) -> Self {
        Self {
            login,
            token,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub fn join(&self, channel: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        if sessions.get(channel).is_some_and(|h| !h.is_finished()) {
            return;
        }
        let (login, token, channel_name) = (self.login.clone(), self.token.clone(), channel.to_string());
        let handle = tokio::spawn(async move {
            loop {
                match session(&login, &token, &channel_name).await {
                    Ok(()) => info!("IRC chat for {channel_name} closed, rejoining in 60s"),
                    Err(e) => warn!("IRC chat for {channel_name}: {e:#}, rejoining in 60s"),
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        sessions.insert(channel.to_string(), handle);
    }

    pub fn leave(&self, channel: &str) {
        if let Some(handle) = self.sessions.lock().unwrap().remove(channel) {
            handle.abort();
            info!("Leave IRC chat: {channel}");
        }
    }
}

async fn session(login: &str, token: &str, channel: &str) -> Result<()> {
    let stream = TcpStream::connect((IRC_HOST, IRC_PORT)).await?;
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(format!("PASS oauth:{token}\r\nNICK {login}\r\nJOIN #{channel}\r\n").as_bytes())
        .await?;
    info!("Join IRC chat: {channel}");

    let mut lines = BufReader::new(reader).lines();
    // Twitch pings roughly every 5 minutes; treat 10 minutes of silence as a dead connection.
    while let Some(line) = tokio::time::timeout(Duration::from_secs(600), lines.next_line()).await?? {
        if let Some(rest) = line.strip_prefix("PING") {
            writer.write_all(format!("PONG{rest}\r\n").as_bytes()).await?;
        } else if line.contains("Login authentication failed") {
            bail!("login authentication failed");
        }
    }
    Ok(())
}

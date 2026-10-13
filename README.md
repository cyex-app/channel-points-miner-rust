# points-miner

A Rust port of [Twitch-Channel-Points-Miner-v2](https://github.com/Tkd-Alex/Twitch-Channel-Points-Miner-v2)
(with the fixes from the maintained [rdavydov fork](https://github.com/rdavydov/Twitch-Channel-Points-Miner-v2)),
plus a small web UI for adding, removing and reordering channels while it runs.

## Run

```sh
cargo build --release
./target/release/points-miner            # first run writes config.toml and exits
# edit config.toml (username + streamers), then:
./target/release/points-miner [path/to/config.toml]
```

On the first login you get a code to enter at <https://www.twitch.tv/activate>. It shows up in the console
and on the web UI. The token is saved to `cookies/<username>.json` next to the config.

The UI is at <http://127.0.0.1:5000>. To reach it from other devices, set `[web] host = "0.0.0.0"`.

### Web UI login

The UI is protected by a login token. It's generated on the first start, printed in the console and saved to
`web_token` next to the config. Enter it on the login page (or open `http://<host>:5000/?token=<token>`) and the
browser stays logged in. If the token leaks, click **Regenerate login token** under *Web access* in the UI: you
get a new token, and every other browser is logged out. Deleting `web_token` and restarting also makes a new one.
Scripts can send the token as `Authorization: Bearer <token>`.

### Channels

Channels are stored in `miner.db` (SQLite) next to the config, keyed by their Twitch user id, so a channel
that changes its name keeps its place, star and settings: logins are refreshed from the ids on every start
(you'll see "x was renamed to y" in the log). On the first start, `streamers` and `blacklist` from
`config.toml` are imported into the database; after that those lists are ignored and channels are managed in
the web UI:

- **Add** a channel by name or URL. It goes to the end of the list.
- **Drag** the ⠿ handle to reorder (or focus it and use the arrow keys). Higher channels win when picking
  what to watch for the `ORDER` priority, and when more than 2 starred channels are live.
- **Star** a channel to always give it a watch slot while it's live.
- **Remove** a channel. It's also blacklisted so it doesn't come back from your follow list; adding it again
  un-blacklists it.

With `followers = true`, new follows are added to the end of the list on each start and unfollowed channels
are dropped. Per-channel overrides imported from the config (`follow_raid`, `chat`, ...) are kept in the
database's `settings` column.

Log verbosity: `RUST_LOG=points_miner=debug`.

## Docker

Everything the miner stores (`config.toml`, `miner.db`, `web_token`, `cookies/`) goes in `./data`, mounted at
`/data`.

```sh
mkdir data
cp config.example.toml data/config.toml    # set username + streamers
docker compose up -d --build
docker compose logs -f                     # Twitch activation code + web UI login token
```

The UI is then at <http://localhost:5000>. Inside the container the UI binds to `0.0.0.0` regardless of
`[web] host` (set by `POINTS_MINER_WEB_HOST`). If you change `[web] port`, change the port mapping in
`docker-compose.yml` to match. `docker compose stop` shuts the miner down cleanly and prints the session report
in the logs. You can also read the token with `cat data/web_token`.

Without Compose:

```sh
docker build -t points-miner-rust .
docker run -d --name points-miner-rust --restart unless-stopped -p 5000:5000 -v "$PWD/data:/data" points-miner-rust
```

## Features

- Watches up to 2 channels at a time (Twitch's limit), picked by `priority`: `STREAK`, `DROPS`, `ORDER`,
  `SUBSCRIBED`, `POINTS_ASCENDING`, `POINTS_DESCENDING`
- Prioritized channels (star in the UI, or `prioritize = true` per channel) always get a watch slot when
  live, ahead of the `priority` rules, e.g. to build up watch time for a badge
- Claims bonus chests and moments, joins raids, keeps watch streaks
- Drop campaigns: tracks progress and claims drops automatically (`claim_drops_startup` claims the inventory on start)
- IRC chat presence (`ALWAYS` / `NEVER` / `ONLINE` / `OFFLINE`)
- Can load the channels you follow
- Prints a session report on Ctrl+C

Not ported: predictions/betting, Telegram/Discord/webhook notifications, the analytics charts server, and community goals.

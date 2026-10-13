# points-miner

A Rust port of [Twitch-Channel-Points-Miner-v2](https://github.com/Tkd-Alex/Twitch-Channel-Points-Miner-v2)
(with the fixes from the maintained [rdavydov fork](https://github.com/rdavydov/Twitch-Channel-Points-Miner-v2)),
plus a small web UI for adding and removing channels while it runs.

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

Channels added or removed in the UI are written back to the
`streamers` list in `config.toml`, and your comments are kept. If you remove a channel that came from your
follow list (`followers = true`), it is added to `blacklist` so it doesn't come back.

Log verbosity: `RUST_LOG=points_miner=debug`.

## Docker

Everything the miner stores (`config.toml`, `web_token`, `cookies/`) goes in `./data`, mounted at `/data`.

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
docker build -t points-miner .
docker run -d --name points-miner --restart unless-stopped -p 5000:5000 -v "$PWD/data:/data" points-miner
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

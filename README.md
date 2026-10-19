# discord-search

MCP server that keeps a local, full-text-searchable copy of a Discord server for
agents. One Rust binary (~1.4 MB, ~3 MB idle RSS) with bundled SQLite FTS5; HTTPS
goes through the system `curl`.

- `discord_sync` fetches new messages from every channel and thread the bot can
  read. The first run backfills full history in budgeted passes; later runs only
  request channels whose latest message is newer than what is stored.
- `discord_search` runs FTS5 queries (stemmed; phrases, `OR`, `NOT`, `prefix*`,
  `NEAR`) with channel, author and date filters, optional surrounding context and
  jump links. Without a query it browses by filters.

Only new messages are captured; later edits and deletions are not reflected.

## Setup

1. In the [Discord Developer Portal](https://discord.com/developers/applications),
   create an application and bot, enable the **Message Content** intent, and copy
   the token.
2. Invite the bot with the `bot` scope and **View Channels** + **Read Message
   History**:
   `https://discord.com/oauth2/authorize?client_id=APP_ID&scope=bot&permissions=66560`
3. Save the token where the server reads it (or pass `DISCORD_TOKEN` instead):

   ```sh
   mkdir -p ~/.config/discord-search && (umask 077; read -rs 'T?Token: ' && printf '%s' "$T" > ~/.config/discord-search/token)
   ```

4. Install from the repo root (it carries the SQLite build flags):
   `cargo install --locked --path . --root ~/.local`
5. Register the stdio server with each MCP client:
   `{ "command": "/abs/home/.local/bin/discord-search" }`.
   From another machine, run it over SSH so there is one database:
   `{ "command": "ssh", "args": ["-o", "BatchMode=yes", "HOST", ".local/bin/discord-search"] }`.

6. Keep the archive fresh with a scheduled sync (`discord-search sync`, 15 min
   budget), so agents rarely call `discord_sync` themselves. With systemd
   (`loginctl enable-linger` keeps user timers running while logged out):

   ```sh
   mkdir -p ~/.config/systemd/user && cd ~/.config/systemd/user
   printf '[Service]\nType=oneshot\nExecStart=%%h/.local/bin/discord-search sync\n' > discord-search-sync.service
   printf '[Timer]\nOnBootSec=1min\nOnUnitActiveSec=5min\n\n[Install]\nWantedBy=timers.target\n' > discord-search-sync.timer
   systemctl --user daemon-reload && systemctl --user enable --now discord-search-sync.timer
   ```

The database lives at `~/.local/share/discord-search/discord.db`; override with
`DISCORD_SEARCH_DB`. Dates without an offset are read as UTC.

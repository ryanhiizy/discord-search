# discord-search

MCP server that keeps a local, full-text-searchable copy of a Discord server for
agents. Two tools, no dependencies, Node 22.13+ (built-in `node:sqlite` with FTS5).

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
3. Register the stdio server with your MCP client, passing `DISCORD_TOKEN` in its
   environment:

   ```json
   { "command": "node", "args": ["/abs/path/discord-search/src/server.mjs"], "env": { "DISCORD_TOKEN": "..." } }
   ```

The database lives at `~/.local/share/discord-search/discord.db`; override with
`DISCORD_SEARCH_DB`.

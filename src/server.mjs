#!/usr/bin/env node
// Minimal MCP stdio server: newline-delimited JSON-RPC, two tools.
import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { open, search } from './store.mjs';
import { client, sync } from './sync.mjs';

const DB_PATH = process.env.DISCORD_SEARCH_DB || join(homedir(), '.local/share/discord-search/discord.db');
const TOKEN_PATH = join(homedir(), '.config/discord-search/token');
let db;
const getDb = () => (db ??= open(DB_PATH));

const TOOLS = [
  {
    name: 'discord_search',
    description:
      'Search a local archive of Discord messages from every channel and thread the bot can read. ' +
      'Query uses SQLite FTS5: words are ANDed and stemmed; "exact phrase", OR, NOT, prefix*, NEAR(a b, 5). ' +
      'Omit query to browse by filters, e.g. a channel\'s latest messages, or sort:"oldest" with after to read forward. ' +
      'Each result has its channel, a jump link, and time (UTC) author: text; ↩ marks a reply. ' +
      'The header says when the archive was last synced; call discord_sync first if you need newer messages.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'FTS5 query. Omit to list by filters only.' },
        channel: { type: 'string', description: 'Channel name or id; includes its threads.' },
        author: { type: 'string', description: 'Display name, username (substring) or user id.' },
        after: { type: 'string', description: 'ISO date/time or message id (exclusive).' },
        before: { type: 'string', description: 'ISO date/time or message id (exclusive).' },
        sort: { type: 'string', enum: ['relevance', 'newest', 'oldest'], description: 'Default relevance with a query, else newest.' },
        context: { type: 'integer', minimum: 0, maximum: 20, description: 'Messages to show before and after each hit. Default 0.' },
        limit: { type: 'integer', minimum: 1, maximum: 200, description: 'Default 20.' },
      },
      additionalProperties: false,
    },
    annotations: { title: 'Search Discord', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'discord_sync',
    description:
      'Fetch new Discord messages into the local archive; incremental and safe to call any time. ' +
      'The first run backfills full history and may need several calls: repeat while it reports Partial. ' +
      'Captures new messages only; later edits and deletions are not reflected.',
    inputSchema: {
      type: 'object',
      properties: {
        budget_seconds: { type: 'integer', minimum: 5, maximum: 600, description: 'Stop after this long. Default 50.' },
      },
      additionalProperties: false,
    },
    annotations: { title: 'Sync Discord', readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: true },
  },
];

function readToken() {
  try {
    return readFileSync(TOKEN_PATH, 'utf8').trim();
  } catch {
    return '';
  }
}

async function callTool(name, args = {}) {
  if (name === 'discord_search') return search(getDb(), args);
  if (name === 'discord_sync') {
    const token = process.env.DISCORD_TOKEN || readToken();
    if (!token) throw new Error(`No bot token: set DISCORD_TOKEN or write it to ${TOKEN_PATH}.`);
    return sync(getDb(), client(token), { budgetMs: (args.budget_seconds ?? 50) * 1000 });
  }
  throw new Error(`Unknown tool: ${name}`);
}

async function handle(msg) {
  switch (msg.method) {
    case 'initialize':
      return {
        protocolVersion: msg.params?.protocolVersion ?? '2025-06-18',
        capabilities: { tools: {} },
        serverInfo: { name: 'discord-search', version: '0.1.0' },
      };
    case 'ping':
      return {};
    case 'tools/list':
      return { tools: TOOLS };
    case 'tools/call':
      try {
        const text = await callTool(msg.params.name, msg.params.arguments);
        return { content: [{ type: 'text', text }] };
      } catch (err) {
        return { content: [{ type: 'text', text: err.message }], isError: true };
      }
    default:
      throw Object.assign(new Error(`Method not found: ${msg.method}`), { code: -32601 });
  }
}

const send = (obj) => process.stdout.write(`${JSON.stringify(obj)}\n`);

createInterface({ input: process.stdin }).on('line', async (line) => {
  if (!line.trim()) return;
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    return send({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } });
  }
  if (msg.id === undefined) return; // notification
  try {
    send({ jsonrpc: '2.0', id: msg.id, result: await handle(msg) });
  } catch (err) {
    send({ jsonrpc: '2.0', id: msg.id, error: { code: err.code ?? -32603, message: err.message } });
  }
});

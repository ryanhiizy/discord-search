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

// Clients such as Executor show only the tool description, so it carries the argument semantics.
const TOOLS = [
  {
    name: 'discord_search',
    description: [
      'Search the team Discord chat history (local archive of every channel and thread the bot can read).',
      'query: SQLite FTS5, words ANDed and stemmed; "exact phrase", OR, NOT, prefix*, NEAR(a b, 5). Omit it to browse by filters.',
      'channel: name or id, includes its threads. author: display name or username substring, or user id.',
      'after/before: ISO date/time or message id, exclusive. sort: relevance (default with query) | newest (default without) | oldest.',
      'context: messages shown before/after each hit, default 0. limit: default 20, max 200.',
      'Output: header with result count and last sync time, then per hit the channel, jump link, and "time(UTC) author: text" lines; ↩ marks a reply.',
      'Call discord_sync first when you need messages newer than the last sync.',
    ].join('\n'),
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string' },
        channel: { type: 'string' },
        author: { type: 'string' },
        after: { type: 'string' },
        before: { type: 'string' },
        sort: { type: 'string', enum: ['relevance', 'newest', 'oldest'] },
        context: { type: 'integer', minimum: 0, maximum: 20 },
        limit: { type: 'integer', minimum: 1, maximum: 200 },
      },
      additionalProperties: false,
    },
    annotations: { title: 'Search Discord', readOnlyHint: true, openWorldHint: false },
  },
  {
    name: 'discord_sync',
    description: [
      'Fetch new team Discord messages into the local archive that discord_search reads. Incremental; seconds when little changed.',
      'Each call stops after about 50s; the first run backfills full history, so repeat while it reports Partial.',
      'Captures new messages only; later edits and deletions are not reflected.',
    ].join('\n'),
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
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
    return sync(getDb(), client(token));
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

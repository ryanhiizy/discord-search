import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

const EPOCH = 1420070400000n;
export const idToMs = (id) => Number((BigInt(id) >> 22n) + EPOCH);
export const msToId = (ms) => (BigInt(ms) - EPOCH) << 22n;

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 15000;
CREATE TABLE IF NOT EXISTS guilds (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS channels (
  id INTEGER PRIMARY KEY,
  guild_id INTEGER NOT NULL,
  parent_id INTEGER,
  name TEXT NOT NULL,
  type INTEGER NOT NULL,
  cursor INTEGER NOT NULL DEFAULT 0,  -- newest stored message id
  blocked TEXT,                        -- permission overwrites when access was denied
  blocked_at INTEGER,                  -- when access was denied (ms)
  threads_scanned TEXT                 -- ISO time of last archived-thread scan
);
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY,
  channel_id INTEGER NOT NULL,
  author_id INTEGER NOT NULL,
  author TEXT NOT NULL,
  username TEXT NOT NULL,
  reply_to INTEGER,
  text TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_channel ON messages (channel_id, id);
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5 (
  text, content = 'messages', content_rowid = 'id',
  tokenize = 'porter unicode61 remove_diacritics 2'
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
`;

export function open(path) {
  if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
  const db = new DatabaseSync(path, { readBigInts: true }); // snowflakes exceed 2^53
  db.exec(SCHEMA);
  return db;
}

export const getMeta = (db, key) => db.prepare('SELECT value FROM meta WHERE key = ?').get(key)?.value;
export const setMeta = (db, key, value) =>
  db.prepare('INSERT INTO meta VALUES (?, ?) ON CONFLICT DO UPDATE SET value = excluded.value').run(key, value);

export function tx(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = fn();
    db.exec('COMMIT');
    return result;
  } catch (err) {
    db.exec('ROLLBACK');
    throw err;
  }
}

// Inserts new messages; returns how many were new. Messages are immutable here.
export function insertMessages(db, rows) {
  const ins = db.prepare(
    'INSERT OR IGNORE INTO messages (id, channel_id, author_id, author, username, reply_to, text) VALUES (?, ?, ?, ?, ?, ?, ?)',
  );
  const fts = db.prepare('INSERT INTO messages_fts (rowid, text) VALUES (?, ?)');
  let added = 0;
  for (const r of rows) {
    if (ins.run(r.id, r.channel_id, r.author_id, r.author, r.username, r.reply_to, r.text).changes) {
      fts.run(r.id, r.text);
      added++;
    }
  }
  return added;
}

const fmtTime = (ms) => new Date(ms).toISOString().slice(0, 16).replace('T', ' ');

function ago(ms) {
  const m = Math.round((Date.now() - ms) / 60000);
  if (m < 60) return `${m}m ago`;
  if (m < 2880) return `${Math.round(m / 60)}h ago`;
  return `${Math.round(m / 1440)}d ago`;
}

// Accepts an ISO date/time or a message id.
function bound(value, name) {
  if (/^\d{15,20}$/.test(value)) return BigInt(value);
  const ms = Date.parse(value);
  if (Number.isNaN(ms)) throw new Error(`${name}: expected ISO date or message id, got "${value}"`);
  return msToId(ms);
}

// Free text that is not valid FTS5 becomes an AND of quoted terms.
const toPlainQuery = (q) =>
  q
    .split(/\s+/)
    .map((t) => t.replace(/"/g, ''))
    .filter(Boolean)
    .map((t) => `"${t}"`)
    .join(' ');

export function search(db, args) {
  const syncedAt = getMeta(db, 'synced_at');
  if (!syncedAt) return 'No data yet. Call discord_sync first.';

  const channels = new Map();
  for (const c of db.prepare('SELECT id, guild_id, parent_id, name, type FROM channels').all()) channels.set(String(c.id), c);
  const guilds = db.prepare('SELECT id, name FROM guilds').all();
  const guildName = new Map(guilds.map((g) => [String(g.id), g.name]));
  const label = (id) => {
    const c = channels.get(String(id));
    if (!c) return `#${id}`;
    const parent = c.parent_id != null && channels.get(String(c.parent_id));
    const name = parent && parent.type !== 4n ? `#${parent.name} › ${c.name}` : `#${c.name}`;
    return guilds.length > 1 ? `${guildName.get(String(c.guild_id))} ${name}` : name;
  };

  const where = [];
  const params = [];
  if (args.channel) {
    const key = String(args.channel).replace(/^#/, '').toLowerCase();
    const ids = db
      .prepare(
        `SELECT id FROM channels WHERE CAST(id AS TEXT) = ?1 OR lower(name) = ?1
         OR parent_id IN (SELECT id FROM channels WHERE CAST(id AS TEXT) = ?1 OR lower(name) = ?1)`,
      )
      .all(key)
      .map((r) => r.id);
    if (!ids.length) {
      const names = db
        .prepare('SELECT DISTINCT name FROM channels WHERE parent_id IS NULL OR type IN (0, 2, 5, 13, 15, 16) ORDER BY name')
        .all()
        .map((r) => r.name);
      return `No channel "${args.channel}". Channels: ${names.join(', ')}`;
    }
    where.push(`m.channel_id IN (${ids.map(() => '?').join(',')})`);
    params.push(...ids);
  }
  if (args.author) {
    const a = String(args.author).replace(/^@/, '').toLowerCase();
    where.push('(instr(lower(m.author), ?) OR instr(lower(m.username), ?) OR CAST(m.author_id AS TEXT) = ?)');
    params.push(a, a, a);
  }
  if (args.after) {
    where.push('m.id > ?');
    params.push(bound(args.after, 'after'));
  }
  if (args.before) {
    where.push('m.id < ?');
    params.push(bound(args.before, 'before'));
  }

  const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 200);
  const query = args.query?.trim();
  const sort = query ? (args.sort ?? 'relevance') : args.sort === 'oldest' ? 'oldest' : 'newest';
  const order = { relevance: 'rank', newest: 'm.id DESC', oldest: 'm.id ASC' }[sort] ?? 'rank';
  const cols = 'm.id, m.channel_id, m.author, m.reply_to, m.text';

  const run = (q) => {
    const conds = q ? ['messages_fts MATCH ?', ...where] : where;
    const sql = q
      ? `SELECT ${cols} FROM messages_fts JOIN messages m ON m.id = messages_fts.rowid`
      : `SELECT ${cols} FROM messages m`;
    return db
      .prepare(`${sql}${conds.length ? ` WHERE ${conds.join(' AND ')}` : ''} ORDER BY ${order} LIMIT ?`)
      .all(...(q ? [q] : []), ...params, limit);
  };

  let hits;
  let note = '';
  try {
    hits = run(query);
  } catch (err) {
    if (!query || !/fts5|syntax|no such column/i.test(err.message)) throw err;
    const plain = toPlainQuery(query);
    hits = run(plain);
    note = ` (invalid FTS5 syntax; searched ${plain})`;
  }

  const context = Math.min(Math.max(Number(args.context) || 0, 0), 20);
  const before = db.prepare(`SELECT ${cols} FROM messages m WHERE channel_id = ? AND id < ? ORDER BY id DESC LIMIT ?`);
  const after = db.prepare(`SELECT ${cols} FROM messages m WHERE channel_id = ? AND id > ? ORDER BY id ASC LIMIT ?`);
  const authorOf = db.prepare('SELECT author FROM messages WHERE id = ?');
  const line = (m, mark) => {
    const reply = m.reply_to != null ? authorOf.get(m.reply_to)?.author : undefined;
    return `${mark}${fmtTime(idToMs(m.id))} ${m.author}${reply ? ` ↩ ${reply}` : ''}: ${m.text}`;
  };

  const blocks = hits.map((m) => {
    const c = channels.get(String(m.channel_id));
    const head = `${label(m.channel_id)} · https://discord.com/channels/${c?.guild_id}/${m.channel_id}/${m.id}`;
    if (!context) return `${head}\n${line(m, '')}`;
    const rows = [
      ...before.all(m.channel_id, m.id, context).reverse().map((r) => line(r, '  ')),
      line(m, '> '),
      ...after.all(m.channel_id, m.id, context).map((r) => line(r, '  ')),
    ];
    return `${head}\n${rows.join('\n')}`;
  });

  const pending = Number(getMeta(db, 'pending'));
  const header =
    `${hits.length}${hits.length === limit ? '+' : ''} results, ${sort}${note} · synced ${ago(Date.parse(syncedAt))}` +
    (pending ? ` · ${pending} channels still backfilling (discord_sync)` : '');
  return [header, ...blocks].join('\n\n');
}

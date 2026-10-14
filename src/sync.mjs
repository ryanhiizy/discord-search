import { insertMessages, setMeta, tx } from './store.mjs';

const API = 'https://discord.com/api/v10';
const MESSAGE_TYPES = new Set([0, 2, 5, 10, 11, 12, 13]); // text, voice, news, threads, stage
const THREAD_PARENTS = new Set([0, 5, 15, 16]); // text, news, forum, media
const CONCURRENCY = 8;
const RETRY_DENIED_MS = 3600_000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function client(token, fetchImpl = fetch) {
  const auth = token.startsWith('Bot ') ? token : `Bot ${token}`;
  let globalUntil = 0;
  return async function get(path) {
    for (;;) {
      const wait = globalUntil - Date.now();
      if (wait > 0) await sleep(wait);
      const res = await fetchImpl(API + path, {
        headers: { Authorization: auth, 'User-Agent': 'DiscordBot (https://github.com/ryanhiizy/discord-search, 0.1)' },
      });
      if (res.status === 429) {
        const body = await res.json().catch(() => ({}));
        const ms = (body.retry_after || Number(res.headers.get('retry-after')) || 1) * 1000;
        if (body.global) globalUntil = Date.now() + ms;
        await sleep(ms);
        continue;
      }
      // Buckets are per channel and each worker owns one channel at a time, so waiting here is enough.
      if (res.headers.get('x-ratelimit-remaining') === '0') {
        await sleep(Number(res.headers.get('x-ratelimit-reset-after') ?? 1) * 1000);
      }
      if (!res.ok) {
        const err = new Error(`Discord ${res.status} on ${path}: ${(await res.text()).slice(0, 200)}`);
        err.status = res.status;
        throw err;
      }
      return res.json();
    }
  };
}

// Searchable text: content, forwarded content, attachment names and embed text.
function textOf(m) {
  const parts = [m.content];
  for (const s of m.message_snapshots ?? []) if (s.message?.content) parts.push(`[forwarded] ${s.message.content}`);
  for (const a of m.attachments ?? []) parts.push(`[file: ${a.filename}]`);
  for (const e of m.embeds ?? []) {
    const t = [e.title, e.description].filter(Boolean).join(' — ');
    if (t) parts.push(`[embed: ${t}]`);
  }
  if (m.poll?.question?.text) parts.push(`[poll: ${m.poll.question.text}]`);
  return parts.filter(Boolean).join('\n');
}

// Public archived threads, newest first, until we reach the previous scan.
async function archivedThreads(get, parentId, since) {
  const out = [];
  let before = '';
  for (;;) {
    let page;
    try {
      page = await get(`/channels/${parentId}/threads/archived/public?limit=100${before}`);
    } catch (err) {
      if (err.status === 403) return out;
      throw err;
    }
    out.push(...page.threads);
    const last = page.threads.at(-1)?.thread_metadata?.archive_timestamp;
    if (!page.has_more || !last || (since && last < since)) return out;
    before = `&before=${encodeURIComponent(last)}`;
  }
}

async function pool(items, fn) {
  let i = 0;
  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, items.length) }, async () => {
      while (i < items.length) await fn(items[i++]);
    }),
  );
}

export async function sync(db, get, { budgetMs = 50_000 } = {}) {
  const started = Date.now();
  const deadline = started + budgetMs;
  const stats = { added: 0, channels: 0, denied: [], failed: [], pending: 0, emptyContent: 0, fetched: 0 };

  const upsertGuild = db.prepare('INSERT INTO guilds VALUES (?, ?) ON CONFLICT DO UPDATE SET name = excluded.name');
  const upsertChannel = db.prepare(
    `INSERT INTO channels (id, guild_id, parent_id, name, type) VALUES (?, ?, ?, ?, ?)
     ON CONFLICT DO UPDATE SET parent_id = excluded.parent_id, name = excluded.name, type = excluded.type`,
  );
  const state = db.prepare('SELECT cursor, blocked, blocked_at, threads_scanned FROM channels WHERE id = ?');

  const work = [];
  for (const g of await get('/users/@me/guilds')) {
    upsertGuild.run(g.id, g.name);
    const [chans, active] = await Promise.all([get(`/guilds/${g.id}/channels`), get(`/guilds/${g.id}/threads/active`)]);
    const all = [...chans, ...active.threads];
    tx(db, () => all.forEach((c) => upsertChannel.run(c.id, g.id, c.parent_id ?? null, c.name, c.type)));

    const scanAt = new Date().toISOString();
    await pool(
      chans.filter((c) => THREAD_PARENTS.has(c.type)),
      async (parent) => {
        const threads = await archivedThreads(get, parent.id, state.get(parent.id).threads_scanned);
        tx(db, () => {
          threads.forEach((t) => upsertChannel.run(t.id, g.id, t.parent_id, t.name, t.type));
          db.prepare('UPDATE channels SET threads_scanned = ? WHERE id = ?').run(scanAt, parent.id);
        });
        all.push(...threads);
      },
    );

    // Only channels whose newest message is past our cursor cost a request. A denied channel is
    // retried when its (or its parent's) permission overwrites change, or hourly for role changes.
    const perms = new Map(chans.map((c) => [c.id, JSON.stringify(c.permission_overwrites ?? [])]));
    for (const c of all) {
      if (!MESSAGE_TYPES.has(c.type) || !c.last_message_id) continue;
      const s = state.get(c.id);
      const last = BigInt(c.last_message_id);
      const access = perms.get(c.id) ?? perms.get(c.parent_id) ?? '';
      const denied = s.blocked === access && Date.now() - Number(s.blocked_at) < RETRY_DENIED_MS;
      if (last > BigInt(s.cursor) && !denied) work.push({ id: c.id, cursor: BigInt(s.cursor), last, access });
    }
  }

  const setCursor = db.prepare('UPDATE channels SET cursor = ?, blocked = NULL, blocked_at = NULL WHERE id = ?');
  const setBlocked = db.prepare('UPDATE channels SET blocked = ?, blocked_at = ? WHERE id = ?');
  const names = db.prepare('SELECT name FROM channels WHERE id = ?');

  await pool(work, async (ch) => {
    let cursor = ch.cursor;
    let touched = false;
    let done = false;
    while (!done && Date.now() < deadline) {
      let page;
      try {
        page = await get(`/channels/${ch.id}/messages?limit=100&after=${cursor}`);
      } catch (err) {
        const name = `#${names.get(ch.id)?.name ?? ch.id}`;
        if (err.status === 403 || err.status === 404) {
          setBlocked.run(ch.access, Date.now(), ch.id);
          stats.denied.push(name);
        } else {
          stats.failed.push(`${name} (${err.message})`);
          stats.pending++;
        }
        return;
      }
      done = page.length < 100;
      if (!page.length) break;
      const rows = [];
      for (const m of page) {
        const id = BigInt(m.id);
        if (id > cursor) cursor = id;
        const text = textOf(m);
        if (!m.content && !m.author.bot && (m.type === 0 || m.type === 19)) stats.emptyContent++;
        if (!text) continue;
        rows.push({
          id,
          channel_id: ch.id,
          author_id: m.author.id,
          author: m.author.global_name ?? m.author.username,
          username: m.author.username,
          reply_to: m.message_reference?.message_id ?? null,
          text,
        });
      }
      stats.fetched += page.length;
      stats.added += tx(db, () => {
        const n = insertMessages(db, rows);
        setCursor.run(cursor, ch.id);
        return n;
      });
      touched = true;
    }
    if (touched) stats.channels++;
    // Reached the end: anything up to the listed last_message_id is fetched or was deleted.
    if (done && cursor < ch.last) setCursor.run(ch.last, ch.id);
    else if (!done) stats.pending++;
  });

  setMeta(db, 'synced_at', new Date(started).toISOString());
  setMeta(db, 'pending', String(stats.pending));
  return report(db, stats, Date.now() - started);
}

function report(db, s, ms) {
  const { n } = db.prepare('SELECT count(*) n FROM messages').get();
  const lines = [
    `${s.pending ? 'Partial' : 'Up to date'}: +${s.added} messages from ${s.channels} channels in ${(ms / 1000).toFixed(1)}s. Total ${n} messages.`,
  ];
  if (s.pending) lines.push(`${s.pending} channels still have older history to fetch. Call discord_sync again to continue.`);
  if (s.failed.length) lines.push(`Failed: ${s.failed.join('; ')}`);
  if (s.denied.length) lines.push(`No access (needs View Channel + Read Message History): ${s.denied.join(', ')}`);
  if (s.fetched >= 20 && s.emptyContent / s.fetched > 0.5) {
    lines.push('Most messages arrived without content: enable the Message Content intent for the bot in the Developer Portal.');
  }
  return lines.join('\n');
}

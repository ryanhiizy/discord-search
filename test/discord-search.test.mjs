import { test } from 'node:test';
import assert from 'node:assert/strict';
import { open, msToId, search } from '../src/store.mjs';
import { client, sync } from '../src/sync.mjs';

const T0 = Date.parse('2026-01-01T00:00:00Z');
const sid = (minutes) => String(msToId(T0 + minutes * 60000) + 1n);

// A fake guild: #general with 250 messages, #secret (403), forum #help with an archived thread.
function fakeDiscord() {
  const general = Array.from({ length: 250 }, (_, i) => ({
    id: sid(i),
    type: 0,
    content: i === 100 ? 'the deploy failed on staging' : i === 200 ? 'Deploying again' : `chatter ${i}`,
    author: { id: '9', username: 'alice', global_name: 'Alice' },
    ...(i === 101 && { message_reference: { message_id: sid(100) } }),
  }));
  const thread = [{ id: sid(300), type: 0, content: '', attachments: [{ filename: 'crash-log.txt' }], author: { id: '8', username: 'bob' } }];
  const channels = [
    { id: '10', type: 4, name: 'Text' },
    { id: '11', type: 0, name: 'general', parent_id: '10', last_message_id: general.at(-1).id },
    { id: '12', type: 0, name: 'secret', last_message_id: sid(5) },
    { id: '13', type: 15, name: 'help' },
  ];
  const archived = { threads: [{ id: '14', type: 11, name: 'Crash help', parent_id: '13', last_message_id: thread[0].id, thread_metadata: { archive_timestamp: '2026-01-02T00:00:00+00:00' } }], has_more: false };
  const calls = [];
  const json = (body, status = 200) => ({ ok: status < 300, status, headers: new Headers(), json: async () => body, text: async () => JSON.stringify(body) });
  const page = (list, after) => list.filter((m) => BigInt(m.id) > BigInt(after)).slice(0, 100).reverse();
  const fetchImpl = async (url) => {
    const path = url.replace('https://discord.com/api/v10', '');
    calls.push(path);
    const q = new URL(url).searchParams;
    if (path === '/users/@me/guilds') return json([{ id: '1', name: 'Team' }]);
    if (path === '/guilds/1/channels') return json(channels);
    if (path === '/guilds/1/threads/active') return json({ threads: [] });
    if (path.startsWith('/channels/13/threads/archived/public')) return json(archived);
    if (path.includes('/threads/archived/')) return json({ threads: [], has_more: false });
    if (path.startsWith('/channels/11/messages')) return json(page(general, q.get('after')));
    if (path.startsWith('/channels/14/messages')) return json(page(thread, q.get('after')));
    if (path.startsWith('/channels/12/messages')) return json({ message: 'Missing Access' }, 403);
    throw new Error(`unexpected ${path}`);
  };
  return { fetchImpl, calls, general, channels };
}

test('sync backfills, then only fetches what changed', async () => {
  const db = open(':memory:');
  const d = fakeDiscord();
  const get = client('x', d.fetchImpl);

  const first = await sync(db, get);
  assert.match(first, /Up to date: \+251 messages from 2 channels/);
  assert.match(first, /No access .*#secret/);
  assert.equal(d.calls.filter((c) => c.startsWith('/channels/11/messages')).length, 3);

  d.calls.length = 0;
  const second = await sync(db, get);
  assert.match(second, /\+0 messages from 0 channels/);
  assert.deepEqual(d.calls.filter((c) => c.includes('/messages')), [], 'no message fetches when nothing is new');
  assert.doesNotMatch(second, /secret/, 'denied channel is not retried until it changes');

  d.general.push({ id: sid(400), type: 0, content: 'new deploy note', author: { id: '9', username: 'alice' } });
  d.channels[1].last_message_id = d.general.at(-1).id;
  assert.match(await sync(db, get), /\+1 messages from 1 channels/);
});

test('sync stops at the budget and resumes', async () => {
  const db = open(':memory:');
  const get = client('x', fakeDiscord().fetchImpl);
  assert.match(await sync(db, get, { budgetMs: -1 }), /Partial: \+0 .*\n3 channels still have older history/);
  assert.match(search(db, { query: 'deploy' }), /3 channels still backfilling/);
  assert.match(await sync(db, get), /Up to date: \+251/);
});

test('search: ranking, stemming, filters, context, links', async () => {
  const db = open(':memory:');
  await sync(db, client('x', fakeDiscord().fetchImpl));

  const r = search(db, { query: 'deploy' });
  assert.match(r, /^2 results, relevance/);
  assert.match(r, /Deploying again/, 'porter stemming');
  assert.match(r, /#Text › general|#general/);
  assert.match(r, new RegExp(`https://discord.com/channels/1/11/${sid(100)}`), 'exact snowflake in link');

  const ctx = search(db, { query: '"deploy failed"', context: 1 });
  assert.match(ctx, /  2026-01-01 01:39 Alice: chatter 99\n> 2026-01-01 01:40 Alice: the deploy failed on staging\n  2026-01-01 01:41 Alice ↩ Alice: chatter 101/);

  assert.match(search(db, { query: 'crash' }), /#help › Crash help .*\n.*bob: \[file: crash-log.txt\]/);
  assert.match(search(db, { channel: 'help' }), /crash-log/, 'channel filter includes threads');
  assert.match(search(db, { channel: 'nope' }), /No channel "nope". Channels: .*general/);
  assert.match(search(db, { author: 'bob' }), /^1 results/);
  assert.match(search(db, { query: 'deploy', after: '2026-01-01T03:00Z' }), /^1 results/);
  assert.match(search(db, { channel: '#general', sort: 'oldest', limit: 2 }), /^2\+ results, oldest[\s\S]*chatter 0\n\n[\s\S]*chatter 1$/);
  assert.match(search(db, { query: "what's the deploy-status?" }), /invalid FTS5 syntax/);
});

test('search before any sync', () => {
  assert.equal(search(open(':memory:'), { query: 'x' }), 'No data yet. Call discord_sync first.');
});

use crate::Error;
use crate::store::{NewMessage, insert_messages, iso, now_ms, set_meta, tx};
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

const API: &str = "https://discord.com/api/v10";
const USER_AGENT: &str = "DiscordBot (https://github.com/ryanhiizy/discord-search, 0.1)";
const MESSAGE_TYPES: [i64; 7] = [0, 2, 5, 10, 11, 12, 13]; // text, voice, news, threads, stage
const THREAD_PARENTS: [i64; 4] = [0, 5, 15, 16]; // text, news, forum, media
const CONCURRENCY: usize = 8;
const RETRY_DENIED_MS: i64 = 3_600_000;

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

/// GET url with the given Authorization header value.
pub type Fetch = dyn Fn(&str, &str) -> Result<Response, String> + Sync;

/// HTTPS via the system curl; the token goes through stdin, never argv.
pub fn curl(url: &str, auth: &str) -> Result<Response, String> {
    let mut child = Command::new("curl")
        .args(["-sSi", "--compressed", "--max-time", "60", "--config", "-", url])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("curl: {e}"))?;
    let quote = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
    let config = format!("header = \"Authorization: {}\"\nheader = \"User-Agent: {USER_AGENT}\"\n", quote(auth));
    child.stdin.take().map(|mut stdin| stdin.write_all(config.as_bytes())).transpose().map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| format!("curl: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    parse_http(&out.stdout)
}

/// Parses `curl -i` output, skipping interim and proxy header blocks.
pub fn parse_http(mut raw: &[u8]) -> Result<Response, String> {
    loop {
        let end = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or("curl: malformed response")?;
        let (head, body) = (String::from_utf8_lossy(&raw[..end]), &raw[end + 4..]);
        if body.starts_with(b"HTTP/") {
            raw = body;
            continue;
        }
        let mut lines = head.split("\r\n");
        let status = lines.next().and_then(|l| l.split(' ').nth(1)?.parse().ok()).ok_or("curl: malformed status")?;
        let headers = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        return Ok(Response { status, headers, body: body.to_vec() });
    }
}

pub struct Fail {
    pub status: u16,
    pub message: String,
}

impl From<Fail> for Error {
    fn from(f: Fail) -> Self {
        Error(f.message)
    }
}

pub struct Client<'a> {
    auth: String,
    fetch: &'a Fetch,
    global_until: Mutex<i64>,
}

fn sleep_ms(ms: i64) {
    if ms > 0 {
        thread::sleep(Duration::from_millis(ms as u64));
    }
}

impl<'a> Client<'a> {
    pub fn new(token: &str, fetch: &'a Fetch) -> Self {
        let auth = if token.starts_with("Bot ") { token.to_string() } else { format!("Bot {token}") };
        Client { auth, fetch, global_until: Mutex::new(0) }
    }

    pub fn get(&self, path: &str) -> Result<Value, Fail> {
        let fail = |message| Fail { status: 0, message };
        loop {
            sleep_ms(*self.global_until.lock().unwrap() - now_ms());
            let res = (self.fetch)(&format!("{API}{path}"), &self.auth).map_err(fail)?;
            let seconds = |v: Option<&str>| v.and_then(|s| s.trim().parse::<f64>().ok());
            if res.status == 429 {
                let body: Value = serde_json::from_slice(&res.body).unwrap_or(Value::Null);
                let secs = [body["retry_after"].as_f64(), seconds(res.header("retry-after"))]
                    .into_iter()
                    .flatten()
                    .find(|s| *s != 0.0 && !s.is_nan())
                    .unwrap_or(1.0);
                let ms = (secs * 1000.0) as i64;
                if body["global"].as_bool() == Some(true) {
                    *self.global_until.lock().unwrap() = now_ms() + ms;
                }
                sleep_ms(ms);
                continue;
            }
            // Buckets are per channel and each worker owns one channel at a time, so waiting here is enough.
            if res.header("x-ratelimit-remaining") == Some("0") {
                let reset = res.header("x-ratelimit-reset-after");
                sleep_ms((reset.map_or(Some(1.0), |r| seconds(Some(r))).unwrap_or(0.0) * 1000.0) as i64);
            }
            if !(200..300).contains(&res.status) {
                let text: String = String::from_utf8_lossy(&res.body).chars().take(200).collect();
                return Err(Fail { status: res.status, message: format!("Discord {} on {path}: {text}", res.status) });
            }
            return serde_json::from_slice(&res.body).map_err(|e| fail(e.to_string()));
        }
    }
}

fn array(v: Value, what: &str) -> Result<Vec<Value>, Fail> {
    match v {
        Value::Array(list) => Ok(list),
        _ => Err(Fail { status: 0, message: format!("Discord returned malformed {what}") }),
    }
}

fn id(v: &Value) -> Option<i64> {
    v.as_str()?.parse().ok()
}

fn str_of(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

/// Searchable text: content, forwarded content, attachment names and embed text.
fn text_of(m: &Value) -> String {
    let list = |k: &str| m[k].as_array().map_or(&[][..], Vec::as_slice);
    let mut parts = vec![str_of(&m["content"]).to_string()];
    for s in list("message_snapshots") {
        if let Some(c) = s["message"]["content"].as_str().filter(|c| !c.is_empty()) {
            parts.push(format!("[forwarded] {c}"));
        }
    }
    for a in list("attachments") {
        parts.push(format!("[file: {}]", a["filename"].as_str().unwrap_or("undefined")));
    }
    for e in list("embeds") {
        let t = [&e["title"], &e["description"]].map(str_of).into_iter().filter(|s| !s.is_empty()).collect::<Vec<_>>();
        if !t.is_empty() {
            parts.push(format!("[embed: {}]", t.join(" — ")));
        }
    }
    if let Some(q) = m["poll"]["question"]["text"].as_str().filter(|q| !q.is_empty()) {
        parts.push(format!("[poll: {q}]"));
    }
    parts.retain(|p| !p.is_empty());
    parts.join("\n")
}

fn encode_component(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Public archived threads, newest first, until we reach the previous scan. None when denied.
fn archived_threads(get: &Client, parent_id: i64, since: Option<&str>) -> Result<Option<Vec<Value>>, Error> {
    let mut out = Vec::new();
    let mut before = String::new();
    loop {
        let mut page = match get.get(&format!("/channels/{parent_id}/threads/archived/public?limit=100{before}")) {
            Ok(page) => page,
            Err(f) if f.status == 403 => return Ok(None),
            Err(f) => return Err(f.into()),
        };
        let threads = array(page["threads"].take(), "threads")?;
        let last = threads.last().and_then(|t| t["thread_metadata"]["archive_timestamp"].as_str()).map(String::from);
        out.extend(threads);
        match last {
            Some(last)
                if page["has_more"].as_bool() == Some(true) && !since.is_some_and(|s| !s.is_empty() && *last < *s) =>
            {
                before = format!("&before={}", encode_component(&last));
            }
            _ => return Ok(Some(out)),
        }
    }
}

/// Runs f over items on up to CONCURRENCY threads; returns the first error.
fn pool<T: Sync>(items: &[T], f: impl Fn(&T) -> Result<(), Error> + Sync) -> Result<(), Error> {
    let next = AtomicUsize::new(0);
    thread::scope(|s| {
        let workers: Vec<_> = (0..CONCURRENCY.min(items.len()))
            .map(|_| {
                s.spawn(|| {
                    while let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) {
                        f(item)?;
                    }
                    Ok(())
                })
            })
            .collect();
        workers.into_iter().try_for_each(|w| w.join().unwrap())
    })
}

#[derive(Default)]
struct Stats {
    added: usize,
    channels: usize,
    denied: Vec<String>,
    failed: Vec<String>,
    pending: usize,
    empty_content: usize,
    fetched: usize,
}

struct State {
    cursor: i64,
    blocked: Option<String>,
    blocked_at: Option<f64>,
    threads_scanned: Option<String>,
}

fn state(db: &Connection, id: i64) -> Result<State, Error> {
    let s = db
        .query_row("SELECT cursor, blocked, blocked_at, threads_scanned FROM channels WHERE id = ?", [id], |r| {
            Ok(State { cursor: r.get(0)?, blocked: r.get(1)?, blocked_at: r.get(2)?, threads_scanned: r.get(3)? })
        })
        .optional()?;
    Ok(s.unwrap_or(State { cursor: 0, blocked: None, blocked_at: None, threads_scanned: None }))
}

const UPSERT_CHANNEL: &str = "INSERT INTO channels (id, guild_id, parent_id, name, type) VALUES (?, ?, ?, ?, ?)
     ON CONFLICT DO UPDATE SET parent_id = excluded.parent_id, name = excluded.name, type = excluded.type";

fn upsert_channels(db: &Connection, guild: i64, list: &[Value]) -> Result<(), Error> {
    let mut st = db.prepare(UPSERT_CHANNEL)?;
    for c in list {
        st.execute((id(&c["id"]), guild, id(&c["parent_id"]), str_of(&c["name"]), c["type"].as_i64()))?;
    }
    Ok(())
}

fn set_blocked(db: &Connection, access: &str, id: i64) -> Result<(), Error> {
    db.execute("UPDATE channels SET blocked = ?, blocked_at = ? WHERE id = ?", (access, now_ms(), id))?;
    Ok(())
}

struct Work {
    id: i64,
    cursor: i64,
    last: i64,
    access: String,
}

pub fn sync(db: &Mutex<Connection>, get: &Client, budget_ms: i64) -> Result<String, Error> {
    let started = now_ms();
    let deadline = started + budget_ms;
    let stats = Mutex::new(Stats::default());
    let lock = || db.lock().unwrap();

    let mut work = Vec::new();
    for g in array(get.get("/users/@me/guilds")?, "guilds")? {
        let gid = id(&g["id"]).ok_or_else(|| Error("Discord returned a guild without an id".into()))?;
        lock().execute(
            "INSERT INTO guilds VALUES (?, ?) ON CONFLICT DO UPDATE SET name = excluded.name",
            (gid, str_of(&g["name"])),
        )?;
        let (chans, active) = thread::scope(|s| {
            let active = s.spawn(|| get.get(&format!("/guilds/{gid}/threads/active")));
            (get.get(&format!("/guilds/{gid}/channels")), active.join().unwrap())
        });
        let chans = array(chans?, "channels")?;
        let mut all: Vec<Value> = chans.clone();
        all.extend(array(active?["threads"].take(), "threads")?);
        {
            let db = lock();
            tx(&db, || upsert_channels(&db, gid, &all))?;
        }

        // A denied channel is skipped until its (or its parent's) permission overwrites change,
        // or hourly to catch role changes.
        let perms: HashMap<&str, String> = chans
            .iter()
            .map(|c| {
                (
                    str_of(&c["id"]),
                    c["permission_overwrites"].as_array().map_or("[]".into(), |p| Value::from(p.clone()).to_string()),
                )
            })
            .collect();
        let access_of = |c: &Value| {
            perms.get(str_of(&c["id"])).or_else(|| perms.get(str_of(&c["parent_id"]))).cloned().unwrap_or_default()
        };
        let is_denied = |c: &Value, s: &State| {
            s.blocked.as_deref() == Some(&access_of(c))
                && ((now_ms() as f64) - s.blocked_at.unwrap_or(0.0)) < RETRY_DENIED_MS as f64
        };

        let scan_at = iso(now_ms());
        let mut parents = Vec::new();
        for c in &chans {
            let pid = id(&c["id"]).unwrap_or(0);
            let s = state(&lock(), pid)?;
            if c["type"].as_i64().is_some_and(|t| THREAD_PARENTS.contains(&t)) && !is_denied(c, &s) {
                parents.push((c, pid, s.threads_scanned));
            }
        }
        let found = Mutex::new(Vec::new());
        pool(&parents, |(parent, pid, since)| {
            let Some(threads) = archived_threads(get, *pid, since.as_deref())? else {
                set_blocked(&lock(), &access_of(parent), *pid)?;
                stats.lock().unwrap().denied.push(format!("#{}", str_of(&parent["name"])));
                return Ok(());
            };
            let db = lock();
            tx(&db, || {
                upsert_channels(&db, gid, &threads)?;
                db.execute("UPDATE channels SET threads_scanned = ? WHERE id = ?", (&scan_at, pid))?;
                Ok(())
            })?;
            found.lock().unwrap().extend(threads);
            Ok(())
        })?;
        all.extend(found.into_inner().unwrap());

        // Only channels whose newest message is past our cursor cost a request.
        for c in &all {
            let (Some(cid), Some(last)) = (id(&c["id"]), id(&c["last_message_id"])) else { continue };
            if !c["type"].as_i64().is_some_and(|t| MESSAGE_TYPES.contains(&t)) {
                continue;
            }
            let s = state(&lock(), cid)?;
            if last > s.cursor && !is_denied(c, &s) {
                work.push(Work { id: cid, cursor: s.cursor, last, access: access_of(c) });
            }
        }
    }

    const SET_CURSOR: &str = "UPDATE channels SET cursor = ?, blocked = NULL, blocked_at = NULL WHERE id = ?";
    pool(&work, |ch| {
        let mut cursor = ch.cursor;
        let mut touched = false;
        let mut done = false;
        while !done && now_ms() < deadline {
            // A malformed page fails the channel like a network error, so the cursor never skips history.
            let page = get
                .get(&format!("/channels/{}/messages?limit=100&after={cursor}", ch.id))
                .and_then(|page| array(page, "messages"))
                .and_then(|page| match page.iter().all(|m| id(&m["id"]).is_some()) {
                    true => Ok(page),
                    false => Err(Fail { status: 0, message: "Discord returned a message without an id".into() }),
                });
            let page = match page {
                Ok(page) => page,
                Err(f) => {
                    let name: Option<String> =
                        lock().query_row("SELECT name FROM channels WHERE id = ?", [ch.id], |r| r.get(0)).optional()?;
                    let name = format!("#{}", name.unwrap_or_else(|| ch.id.to_string()));
                    let mut stats = stats.lock().unwrap();
                    if f.status == 403 || f.status == 404 {
                        set_blocked(&lock(), &ch.access, ch.id)?;
                        stats.denied.push(name);
                    } else {
                        stats.failed.push(format!("{name} ({})", f.message));
                        stats.pending += 1;
                    }
                    return Ok(());
                }
            };
            done = page.len() < 100;
            if page.is_empty() {
                break;
            }
            let mut rows = Vec::new();
            let mut empty = 0;
            for m in &page {
                let mid = id(&m["id"]).unwrap_or(0);
                cursor = cursor.max(mid);
                let text = text_of(m);
                let author = &m["author"];
                if str_of(&m["content"]).is_empty()
                    && author["bot"].as_bool() != Some(true)
                    && matches!(m["type"].as_i64(), Some(0 | 19))
                {
                    empty += 1;
                }
                if text.is_empty() {
                    continue;
                }
                let username = str_of(&author["username"]).to_string();
                rows.push(NewMessage {
                    id: mid,
                    channel_id: ch.id,
                    author_id: id(&author["id"]).unwrap_or(0),
                    author: author["global_name"].as_str().map_or_else(|| username.clone(), String::from),
                    username,
                    reply_to: id(&m["message_reference"]["message_id"]),
                    text,
                });
            }
            let db = lock();
            let added = tx(&db, || {
                let n = insert_messages(&db, &rows)?;
                db.execute(SET_CURSOR, (cursor, ch.id))?;
                Ok(n)
            })?;
            drop(db);
            let mut stats = stats.lock().unwrap();
            stats.fetched += page.len();
            stats.empty_content += empty;
            stats.added += added;
            touched = true;
        }
        let mut stats = stats.lock().unwrap();
        if touched {
            stats.channels += 1;
        }
        // Reached the end: anything up to the listed last_message_id is fetched or was deleted.
        if done && cursor < ch.last {
            lock().execute(SET_CURSOR, (ch.last, ch.id))?;
        } else if !done {
            stats.pending += 1;
        }
        Ok(())
    })?;

    let stats = stats.into_inner().unwrap();
    let db = lock();
    set_meta(&db, "synced_at", &iso(started))?;
    set_meta(&db, "pending", &stats.pending.to_string())?;
    report(&db, &stats, now_ms() - started)
}

fn report(db: &Connection, s: &Stats, ms: i64) -> Result<String, Error> {
    let n: i64 = db.query_row("SELECT count(*) n FROM messages", [], |r| r.get(0))?;
    let mut lines = vec![format!(
        "{}: +{} messages from {} channels in {:.1}s. Total {n} messages.",
        if s.pending > 0 { "Partial" } else { "Up to date" },
        s.added,
        s.channels,
        ms as f64 / 1000.0
    )];
    if s.pending > 0 {
        lines.push(format!(
            "{} channels still have older history to fetch. Call discord_sync again to continue.",
            s.pending
        ));
    }
    if !s.failed.is_empty() {
        lines.push(format!("Failed: {}", s.failed.join("; ")));
    }
    if !s.denied.is_empty() {
        lines.push(format!("No access (needs View Channel + Read Message History): {}", s.denied.join(", ")));
    }
    if s.fetched >= 20 && s.empty_content as f64 / s.fetched as f64 > 0.5 {
        lines.push(
            "Most messages arrived without content: enable the Message Content intent for the bot in the Developer Portal."
                .into(),
        );
    }
    Ok(lines.join("\n"))
}

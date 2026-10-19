use crate::Error;
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension, Row, params_from_iter};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

const EPOCH: i64 = 1420070400000;
pub fn id_to_ms(id: i64) -> i64 {
    (id >> 22) + EPOCH
}
/// None when the time is too far out for a snowflake.
pub fn ms_to_id(ms: i64) -> Option<i64> {
    (ms - EPOCH).checked_mul(1 << 22)
}

const SCHEMA: &str = "
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
";

pub fn open(path: &str) -> Result<Connection, Error> {
    if path != ":memory:"
        && let Some(dir) = std::path::Path::new(path).parent()
    {
        std::fs::create_dir_all(dir).map_err(|e| Error(format!("{path}: {e}")))?;
    }
    let db = Connection::open(path)?;
    db.execute_batch(SCHEMA)?;
    Ok(db)
}

pub fn get_meta(db: &Connection, key: &str) -> Result<Option<String>, Error> {
    Ok(db.query_row("SELECT value FROM meta WHERE key = ?", [key], |r| r.get(0)).optional()?.flatten())
}
pub fn set_meta(db: &Connection, key: &str, value: &str) -> Result<(), Error> {
    db.execute("INSERT INTO meta VALUES (?, ?) ON CONFLICT DO UPDATE SET value = excluded.value", [key, value])?;
    Ok(())
}

pub fn tx<T>(db: &Connection, f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
    db.execute_batch("BEGIN IMMEDIATE")?;
    let result = f().and_then(|value| Ok(db.execute_batch("COMMIT").map(|()| value)?));
    if result.is_err() && !db.is_autocommit() {
        db.execute_batch("ROLLBACK")?;
    }
    result
}

pub struct NewMessage {
    pub id: i64,
    pub channel_id: i64,
    pub author_id: i64,
    pub author: String,
    pub username: String,
    pub reply_to: Option<i64>,
    pub text: String,
}

/// Inserts new messages; returns how many were new. Messages are immutable here.
pub fn insert_messages(db: &Connection, rows: &[NewMessage]) -> Result<usize, Error> {
    let mut ins = db.prepare(
        "INSERT OR IGNORE INTO messages (id, channel_id, author_id, author, username, reply_to, text) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )?;
    let mut fts = db.prepare("INSERT INTO messages_fts (rowid, text) VALUES (?, ?)")?;
    let mut added = 0;
    for r in rows {
        if ins.execute((r.id, r.channel_id, r.author_id, &r.author, &r.username, r.reply_to, &r.text))? > 0 {
            fts.execute((r.id, &r.text))?;
            added += 1;
        }
    }
    Ok(added)
}

/// Whitespace as JS `trim` and `\s` see it: Unicode White_Space plus the BOM.
pub fn js_trim(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || c == '\u{feff}')
}

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// Like JS `toISOString`: 2026-01-01T00:00:00.000Z.
pub fn iso(ms: i64) -> String {
    let (y, mo, d) = civil_from_days(ms.div_euclid(86_400_000));
    let t = ms.rem_euclid(86_400_000);
    let (h, mi, s, f) = (t / 3_600_000, t / 60_000 % 60, t / 1000 % 60, t % 1000);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{f:03}Z")
}

/// ISO date or date-time; without an offset it is UTC, as in the output.
pub fn parse_iso(s: &str) -> Option<i64> {
    fn num(s: &str, len: usize) -> Option<(i64, &str)> {
        let (head, rest) = s.split_at_checked(len)?;
        head.bytes().all(|b| b.is_ascii_digit()).then(|| (head.parse().ok().unwrap_or(0), rest))
    }
    fn sep(s: &str, c: char) -> Option<&str> {
        s.strip_prefix(c)
    }
    let (y, mut rest) = num(s.trim(), 4)?;
    let (mut mo, mut d, mut h, mut mi, mut sec, mut ms) = (1, 1, 0, 0, 0, 0);
    if let Some(r) = sep(rest, '-') {
        (mo, rest) = num(r, 2)?;
        if let Some(r) = sep(rest, '-') {
            (d, rest) = num(r, 2)?;
        }
    }
    let mut offset = 0;
    if let Some(r) = sep(rest, 'T').or_else(|| sep(rest, 't')).or_else(|| sep(rest, ' ')) {
        (h, rest) = num(r, 2)?;
        (mi, rest) = num(sep(rest, ':')?, 2)?;
        if let Some(r) = sep(rest, ':') {
            (sec, rest) = num(r, 2)?;
            if let Some(r) = sep(rest, '.') {
                let digits = r.bytes().take_while(u8::is_ascii_digit).count();
                if digits == 0 {
                    return None;
                }
                let frac = &r[..digits.min(3)];
                ms = frac.parse::<i64>().ok()? * 10_i64.pow(3 - frac.len() as u32);
                rest = &r[digits..];
            }
        }
        if let Some(r) = sep(rest, 'Z').or_else(|| sep(rest, 'z')) {
            rest = r;
        } else if let Some(sign) = rest.chars().next().filter(|c| *c == '+' || *c == '-') {
            let (oh, r) = num(&rest[1..], 2)?;
            let (om, r) = num(sep(r, ':').unwrap_or(r), 2)?;
            if oh > 23 || om > 59 {
                return None;
            }
            offset = (oh * 60 + om) * if sign == '-' { -60_000 } else { 60_000 };
            rest = r;
        }
    }
    let valid = rest.is_empty()
        && (1..=12).contains(&mo)
        && (1..=31).contains(&d)
        && (h < 24 || (h == 24 && mi == 0 && sec == 0 && ms == 0))
        && mi < 60
        && sec < 60;
    valid.then(|| days_from_civil(y, mo, d) * 86_400_000 + ((h * 60 + mi) * 60 + sec) * 1000 + ms - offset)
}

fn fmt_time(ms: i64) -> String {
    iso(ms)[..16].replace('T', " ")
}

fn round(x: f64) -> i64 {
    (x + 0.5).floor() as i64
}

fn ago(ms: i64) -> String {
    let m = round((now_ms() - ms) as f64 / 60000.0);
    if m < 60 {
        format!("{m}m ago")
    } else if m < 2880 {
        format!("{}h ago", round(m as f64 / 60.0))
    } else {
        format!("{}d ago", round(m as f64 / 1440.0))
    }
}

/// Accepts an ISO date/time or a message id.
fn bound(value: &str, name: &str) -> Result<i64, Error> {
    if (15..=20).contains(&value.len())
        && value.bytes().all(|b| b.is_ascii_digit())
        && let Ok(id) = value.parse()
    {
        return Ok(id);
    }
    parse_iso(value)
        .and_then(ms_to_id)
        .ok_or_else(|| Error(format!("{name}: expected ISO date or message id, got \"{value}\"")))
}

/// Free text that is not valid FTS5 becomes an AND of quoted terms.
fn to_plain_query(q: &str) -> String {
    q.split(|c: char| c.is_whitespace() || c == '\u{feff}')
        .map(|t| t.replace('"', ""))
        .filter(|t| !t.is_empty())
        .map(|t| format!("\"{t}\""))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A string argument as JS `String(x)` would see it; absent, null and "" are None.
fn arg(args: &Map<String, Value>, key: &str) -> Option<String> {
    match args.get(key)? {
        Value::Null => None,
        Value::String(s) if s.is_empty() => None,
        Value::String(s) => Some(s.clone()),
        v => Some(v.to_string()),
    }
}

/// JS `Number(x) || fallback`, clamped.
fn int_arg(args: &Map<String, Value>, key: &str, fallback: f64, lo: f64, hi: f64) -> i64 {
    let n = match args.get(key) {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => js_trim(s).parse().unwrap_or(if js_trim(s).is_empty() { 0.0 } else { f64::NAN }),
        Some(Value::Bool(b)) => f64::from(u8::from(*b)),
        _ => f64::NAN,
    };
    let n = if n.is_nan() || n == 0.0 { fallback } else { n };
    n.clamp(lo, hi) as i64
}

struct Chan {
    guild_id: i64,
    parent_id: Option<i64>,
    name: String,
    kind: i64,
}

struct Msg {
    id: i64,
    channel_id: i64,
    author: String,
    reply_to: Option<i64>,
    text: String,
}

const COLS: &str = "m.id, m.channel_id, m.author, m.reply_to, m.text";

fn msg(r: &Row) -> rusqlite::Result<Msg> {
    Ok(Msg { id: r.get(0)?, channel_id: r.get(1)?, author: r.get(2)?, reply_to: r.get(3)?, text: r.get(4)? })
}

fn all<T>(
    db: &Connection,
    sql: &str,
    params: impl IntoIterator<Item = Sql>,
    f: impl FnMut(&Row) -> rusqlite::Result<T>,
) -> rusqlite::Result<Vec<T>> {
    db.prepare(sql)?.query_map(params_from_iter(params), f)?.collect()
}

pub fn search(db: &Connection, args: &Map<String, Value>) -> Result<String, Error> {
    let Some(synced_at) = get_meta(db, "synced_at")? else {
        return Ok("No data yet. Call discord_sync first.".into());
    };

    let channels: HashMap<i64, Chan> = all(db, "SELECT id, guild_id, parent_id, name, type FROM channels", [], |r| {
        Ok((r.get(0)?, Chan { guild_id: r.get(1)?, parent_id: r.get(2)?, name: r.get(3)?, kind: r.get(4)? }))
    })?
    .into_iter()
    .collect();
    let guilds: HashMap<i64, String> =
        all(db, "SELECT id, name FROM guilds", [], |r| Ok((r.get(0)?, r.get(1)?)))?.into_iter().collect();
    let label = |id: i64| {
        let Some(c) = channels.get(&id) else { return format!("#{id}") };
        let name = match c.parent_id.and_then(|p| channels.get(&p)) {
            Some(parent) if parent.kind != 4 => format!("#{} › {}", parent.name, c.name),
            _ => format!("#{}", c.name),
        };
        if guilds.len() > 1 {
            format!("{} {name}", guilds.get(&c.guild_id).map_or("undefined", String::as_str))
        } else {
            name
        }
    };

    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Sql> = Vec::new();
    if let Some(channel) = arg(args, "channel") {
        let key = channel.strip_prefix('#').unwrap_or(&channel).to_lowercase();
        let ids = all(
            db,
            "SELECT id FROM channels WHERE CAST(id AS TEXT) = ?1 OR lower(name) = ?1
             OR parent_id IN (SELECT id FROM channels WHERE CAST(id AS TEXT) = ?1 OR lower(name) = ?1)",
            [Sql::Text(key)],
            |r| r.get::<_, i64>(0),
        )?;
        if ids.is_empty() {
            let names = all(
                db,
                "SELECT DISTINCT name FROM channels WHERE parent_id IS NULL OR type IN (0, 2, 5, 13, 15, 16) ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )?;
            return Ok(format!("No channel \"{channel}\". Channels: {}", names.join(", ")));
        }
        conds.push(format!("m.channel_id IN ({})", vec!["?"; ids.len()].join(",")));
        params.extend(ids.into_iter().map(Sql::Integer));
    }
    if let Some(author) = arg(args, "author") {
        let a = author.strip_prefix('@').unwrap_or(&author).to_lowercase();
        conds
            .push("(instr(lower(m.author), ?) OR instr(lower(m.username), ?) OR CAST(m.author_id AS TEXT) = ?)".into());
        params.extend([Sql::Text(a.clone()), Sql::Text(a.clone()), Sql::Text(a)]);
    }
    if let Some(after) = arg(args, "after") {
        conds.push("m.id > ?".into());
        params.push(Sql::Integer(bound(&after, "after")?));
    }
    if let Some(before) = arg(args, "before") {
        conds.push("m.id < ?".into());
        params.push(Sql::Integer(bound(&before, "before")?));
    }

    let limit = int_arg(args, "limit", 20.0, 1.0, 200.0);
    let query = arg(args, "query").map(|q| js_trim(&q).to_string()).filter(|q| !q.is_empty());
    let sort_arg = arg(args, "sort");
    let sort = match (&query, sort_arg.as_deref()) {
        (Some(_), s) => s.unwrap_or("relevance").to_string(),
        (None, Some("oldest")) => "oldest".into(),
        (None, _) => "newest".into(),
    };
    let order = match sort.as_str() {
        "newest" => "m.id DESC",
        "oldest" => "m.id ASC",
        _ => "rank",
    };

    let run = |q: Option<&str>| {
        let mut c: Vec<&str> = q.map(|_| "messages_fts MATCH ?").into_iter().collect();
        c.extend(conds.iter().map(String::as_str));
        let from = if q.is_some() {
            format!("SELECT {COLS} FROM messages_fts JOIN messages m ON m.id = messages_fts.rowid")
        } else {
            format!("SELECT {COLS} FROM messages m")
        };
        let filter = if c.is_empty() { String::new() } else { format!(" WHERE {}", c.join(" AND ")) };
        let p = q.map(|q| Sql::Text(q.into())).into_iter().chain(params.iter().cloned()).chain([Sql::Integer(limit)]);
        all(db, &format!("{from}{filter} ORDER BY {order} LIMIT ?"), p, msg)
    };

    let mut note = String::new();
    let hits = match run(query.as_deref()) {
        Ok(hits) => hits,
        Err(err) => {
            let text = err.to_string().to_lowercase();
            let fts_error = ["fts5", "syntax", "no such column"].iter().any(|s| text.contains(s));
            let Some(query) = query.as_deref().filter(|_| fts_error) else { return Err(err.into()) };
            let plain = to_plain_query(query);
            note = format!(" (invalid FTS5 syntax; searched {plain})");
            run(Some(&plain))?
        }
    };

    let context = int_arg(args, "context", 0.0, 0.0, 20.0);
    let around =
        |m: &Msg, sql: &str| all(db, sql, [Sql::Integer(m.channel_id), Sql::Integer(m.id), Sql::Integer(context)], msg);
    let line = |m: &Msg, mark: &str| -> Result<String, Error> {
        let reply = match m.reply_to {
            Some(id) => {
                db.query_row("SELECT author FROM messages WHERE id = ?", [id], |r| r.get::<_, String>(0)).optional()?
            }
            None => None,
        };
        let reply = reply.filter(|r| !r.is_empty()).map(|r| format!(" ↩ {r}")).unwrap_or_default();
        Ok(format!("{mark}{} {}{reply}: {}", fmt_time(id_to_ms(m.id)), m.author, m.text))
    };

    let mut out = Vec::with_capacity(hits.len() + 1);
    let pending = get_meta(db, "pending")?.and_then(|p| p.trim().parse::<f64>().ok()).filter(|p| *p != 0.0);
    out.push(format!(
        "{}{} results, {sort}{note} · synced {}{}",
        hits.len(),
        if hits.len() as i64 == limit { "+" } else { "" },
        parse_iso(&synced_at).map_or_else(|| "NaNd ago".into(), ago),
        pending.map_or_else(String::new, |p| format!(" · {p} channels still backfilling (discord_sync)")),
    ));
    for m in &hits {
        let guild = channels.get(&m.channel_id).map_or_else(|| "undefined".into(), |c| c.guild_id.to_string());
        let mut block =
            format!("{} · https://discord.com/channels/{guild}/{}/{}", label(m.channel_id), m.channel_id, m.id);
        let mut push = |text: String| {
            block.push('\n');
            block.push_str(&text);
        };
        if context == 0 {
            push(line(m, "")?);
        } else {
            let before = "SELECT m.id, m.channel_id, m.author, m.reply_to, m.text FROM messages m WHERE channel_id = ? AND id < ? ORDER BY id DESC LIMIT ?";
            let after = "SELECT m.id, m.channel_id, m.author, m.reply_to, m.text FROM messages m WHERE channel_id = ? AND id > ? ORDER BY id ASC LIMIT ?";
            for r in around(m, before)?.iter().rev() {
                push(line(r, "  ")?);
            }
            push(line(m, "> ")?);
            for r in &around(m, after)? {
                push(line(r, "  ")?);
            }
        }
        out.push(block);
    }
    Ok(out.join("\n\n"))
}

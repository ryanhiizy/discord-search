use crate::store::{iso, ms_to_id, open, parse_iso, search};
use crate::sync::{Client, Response, parse_http, sync};
use regex::Regex;
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};

fn t0() -> i64 {
    parse_iso("2026-01-01T00:00:00Z").unwrap()
}
fn sid(minutes: i64) -> String {
    (ms_to_id(t0() + minutes * 60000).unwrap() + 1).to_string()
}

#[track_caller]
fn matches(text: &str, pattern: &str) {
    assert!(Regex::new(pattern).unwrap().is_match(text), "/{pattern}/ did not match:\n{text}");
}

fn args(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

#[derive(Default)]
struct Fake {
    general: Vec<Value>,
    channels: Vec<Value>,
    calls: Vec<String>,
}

type Shared = Arc<Mutex<Fake>>;

// A fake guild: #general with 250 messages, #secret (403), forum #help with an archived thread.
fn fake_discord() -> (Shared, impl Fn(&str, &str) -> Result<Response, String> + Sync) {
    let general: Vec<Value> = (0..250)
        .map(|i| {
            let content = match i {
                100 => "the deploy failed on staging".into(),
                200 => "Deploying again".into(),
                _ => format!("chatter {i}"),
            };
            let mut m = json!({ "id": sid(i), "type": 0, "content": content, "author": { "id": "9", "username": "alice", "global_name": "Alice" } });
            if i == 101 {
                m["message_reference"] = json!({ "message_id": sid(100) });
            }
            m
        })
        .collect();
    let thread = vec![
        json!({ "id": sid(300), "type": 0, "content": "", "attachments": [{ "filename": "crash-log.txt" }], "author": { "id": "8", "username": "bob" } }),
    ];
    let channels = vec![
        json!({ "id": "10", "type": 4, "name": "Text" }),
        json!({ "id": "11", "type": 0, "name": "general", "parent_id": "10", "last_message_id": general[249]["id"] }),
        json!({ "id": "12", "type": 0, "name": "secret", "last_message_id": sid(5) }),
        json!({ "id": "13", "type": 15, "name": "help" }),
    ];
    let archived = json!({ "threads": [{ "id": "14", "type": 11, "name": "Crash help", "parent_id": "13", "last_message_id": thread[0]["id"], "thread_metadata": { "archive_timestamp": "2026-01-02T00:00:00+00:00" } }], "has_more": false });
    let state = Arc::new(Mutex::new(Fake { general, channels, calls: Vec::new() }));
    let shared = state.clone();
    let fetch = move |url: &str, auth: &str| {
        assert_eq!(auth, "Bot x");
        let path = url.strip_prefix("https://discord.com/api/v10").unwrap();
        let mut s = shared.lock().unwrap();
        s.calls.push(path.to_string());
        let json_res =
            |body: &Value, status| Ok(Response { status, headers: vec![], body: body.to_string().into_bytes() });
        let after: u64 = path.split("after=").nth(1).map_or(0, |a| a.parse().unwrap());
        let page = |list: &[Value]| {
            let mut p: Vec<Value> = list
                .iter()
                .filter(|m| m["id"].as_str().unwrap().parse::<u64>().unwrap() > after)
                .take(100)
                .cloned()
                .collect();
            p.reverse();
            Value::from(p)
        };
        match path {
            "/users/@me/guilds" => json_res(&json!([{ "id": "1", "name": "Team" }]), 200),
            "/guilds/1/channels" => json_res(&Value::from(s.channels.clone()), 200),
            "/guilds/1/threads/active" => json_res(&json!({ "threads": [] }), 200),
            p if p.starts_with("/channels/13/threads/archived/public") => json_res(&archived, 200),
            p if p.starts_with("/channels/12/") => json_res(&json!({ "message": "Missing Access" }), 403),
            p if p.contains("/threads/archived/") => json_res(&json!({ "threads": [], "has_more": false }), 200),
            p if p.starts_with("/channels/11/messages") => json_res(&page(&s.general), 200),
            p if p.starts_with("/channels/14/messages") => json_res(&page(&thread), 200),
            p => panic!("unexpected {p}"),
        }
    };
    (state, fetch)
}

fn message_calls(state: &Shared, prefix: &str) -> usize {
    state.lock().unwrap().calls.iter().filter(|c| c.contains(prefix)).count()
}

#[test]
fn sync_backfills_then_only_fetches_what_changed() {
    let db = Mutex::new(open(":memory:").unwrap());
    let (d, fetch) = fake_discord();
    let get = Client::new("x", &fetch);
    let run = || sync(&db, &get, 50_000).unwrap();

    let first = run();
    matches(&first, r"Up to date: \+251 messages from 2 channels");
    matches(&first, r"No access .*#secret");
    assert_eq!(message_calls(&d, "/channels/11/messages"), 3);

    d.lock().unwrap().calls.clear();
    let second = run();
    matches(&second, r"\+0 messages from 0 channels");
    assert_eq!(message_calls(&d, "/messages"), 0, "no message fetches when nothing is new");
    assert!(!second.contains("secret"), "denied channel is not retried while its permissions are unchanged");
    assert_eq!(message_calls(&d, "/channels/12/"), 0, "no requests at all to a denied channel");
    d.lock().unwrap().channels[2]["permission_overwrites"] =
        json!([{ "id": "bot", "type": 1, "allow": "66560", "deny": "0" }]);
    matches(&run(), r"No access .*#secret"); // retried after a permission change

    d.lock().unwrap().channels[1]["last_message_id"] = sid(350).into(); // newest message was deleted
    matches(&run(), r"^Up to date: \+0");
    d.lock().unwrap().calls.clear();
    matches(&run(), r"^Up to date: \+0");
    assert_eq!(message_calls(&d, "/messages"), 0, "deleted last message does not leave the channel pending");

    {
        let mut s = d.lock().unwrap();
        s.general.push(json!({ "id": sid(400), "type": 0, "content": "new deploy note", "author": { "id": "9", "username": "alice" } }));
        s.channels[1]["last_message_id"] = sid(400).into();
    }
    matches(&run(), r"\+1 messages from 1 channels");
}

#[test]
fn sync_stops_at_the_budget_and_resumes() {
    let db = Mutex::new(open(":memory:").unwrap());
    let (_, fetch) = fake_discord();
    let get = Client::new("x", &fetch);
    matches(&sync(&db, &get, -1).unwrap(), r"Partial: \+0 .*\n2 channels still have older history");
    matches(&search(&db.lock().unwrap(), &args(json!({ "query": "deploy" }))).unwrap(), "2 channels still backfilling");
    matches(&sync(&db, &get, 50_000).unwrap(), r"Up to date: \+251");
}

#[test]
fn search_ranking_stemming_filters_context_links() {
    let db = Mutex::new(open(":memory:").unwrap());
    let (_, fetch) = fake_discord();
    sync(&db, &Client::new("x", &fetch), 50_000).unwrap();
    let db = db.lock().unwrap();
    let s = |v: Value| search(&db, &args(v)).unwrap();

    let r = s(json!({ "query": "deploy" }));
    matches(&r, "^2 results, relevance");
    matches(&r, "Deploying again"); // porter stemming
    matches(&r, "#Text › general|#general");
    matches(&r, &format!("https://discord.com/channels/1/11/{}", sid(100))); // exact snowflake in link

    let ctx = s(json!({ "query": "\"deploy failed\"", "context": 1 }));
    matches(
        &ctx,
        "  2026-01-01 01:39 Alice: chatter 99\n> 2026-01-01 01:40 Alice: the deploy failed on staging\n  2026-01-01 01:41 Alice ↩ Alice: chatter 101",
    );

    matches(&s(json!({ "query": "crash" })), r"#help › Crash help .*\n.*bob: \[file: crash-log.txt\]");
    matches(&s(json!({ "channel": "help" })), "crash-log"); // channel filter includes threads
    matches(&s(json!({ "channel": "nope" })), r#"No channel "nope". Channels: .*general"#);
    matches(&s(json!({ "author": "bob" })), "^1 results");
    matches(&s(json!({ "query": "deploy", "after": "2026-01-01T03:00Z" })), "^1 results");
    matches(
        &s(json!({ "channel": "#general", "sort": "oldest", "limit": 2 })),
        r"^2\+ results, oldest[\s\S]*chatter 0\n\n[\s\S]*chatter 1$",
    );
    matches(&s(json!({ "query": "what's the deploy-status?" })), "invalid FTS5 syntax");
    for bad in ["soon", "2100-01-01"] {
        assert!(search(&db, &args(json!({ "after": bad }))).unwrap_err().0.contains("expected ISO date or message id"));
    }
}

#[test]
fn search_before_any_sync() {
    let db = open(":memory:").unwrap();
    assert_eq!(search(&db, &args(json!({ "query": "x" }))).unwrap(), "No data yet. Call discord_sync first.");
}

#[test]
fn rate_limits_are_retried() {
    let calls = Mutex::new(0);
    let fetch = move |_: &str, _: &str| {
        let mut n = calls.lock().unwrap();
        *n += 1;
        Ok(match *n {
            1 => Response { status: 429, headers: vec![], body: br#"{"retry_after":0.01,"global":true}"#.to_vec() },
            2 => Response {
                status: 200,
                headers: vec![
                    ("x-ratelimit-remaining".into(), "0".into()),
                    ("x-ratelimit-reset-after".into(), "0.01".into()),
                ],
                body: b"[1]".to_vec(),
            },
            _ => Response { status: 404, headers: vec![], body: b"{\"message\":\"Unknown\"}".to_vec() },
        })
    };
    let get = Client::new("Bot x", &fetch);
    assert_eq!(get.get("/a").ok().unwrap(), json!([1]));
    let err = get.get("/b").err().unwrap();
    assert_eq!((err.status, err.message.as_str()), (404, "Discord 404 on /b: {\"message\":\"Unknown\"}"));
}

#[test]
fn parses_curl_output() {
    let raw = b"HTTP/1.1 200 Connection established\r\n\r\nHTTP/2 429\r\nRetry-After: 2\r\nx-a: b:c\r\n\r\n{\"x\":1}";
    let r = parse_http(raw).ok().unwrap();
    assert_eq!((r.status, r.body.as_slice()), (429, &b"{\"x\":1}"[..]));
    assert_eq!(r.headers, vec![("retry-after".into(), "2".into()), ("x-a".into(), "b:c".into())]);
}

#[test]
fn iso_times() {
    assert_eq!(iso(parse_iso("2026-10-19T09:34:09.784Z").unwrap()), "2026-10-19T09:34:09.784Z");
    assert_eq!(parse_iso("2026-01-01"), parse_iso("2026-01-01T00:00Z"));
    assert_eq!(parse_iso("2026-01-02T00:00:00+00:00"), parse_iso("2026-01-02T10:00+10:00"));
    assert_eq!(parse_iso("1970-01-01T00:00:01.5Z"), Some(1500));
    assert_eq!(parse_iso("2026-13-01"), None);
    assert_eq!(parse_iso("2026-01-01T24:00Z"), parse_iso("2026-01-02"));
    for bad in ["2026-01-01T24:01Z", "2026-01-01T00:00+24:00", "2026-01-01T00:00+00:60"] {
        assert_eq!(parse_iso(bad), None, "{bad}");
    }
}

#[test]
fn malformed_message_page_keeps_the_cursor() {
    let db = Mutex::new(open(":memory:").unwrap());
    let (_, inner) = fake_discord();
    let fetch = move |url: &str, auth: &str| match url.contains("/channels/11/messages") {
        true => Ok(Response { status: 200, headers: vec![], body: b"{\"message\":\"odd\"}".to_vec() }),
        false => inner(url, auth),
    };
    let report = sync(&db, &Client::new("x", &fetch), 50_000).unwrap();
    matches(&report, r"^Partial: \+1 .*\n1 channels still have older history");
    matches(&report, r"Failed: #general \(Discord returned malformed messages\)");
    let cursor: i64 =
        db.lock().unwrap().query_row("SELECT cursor FROM channels WHERE id = 11", [], |r| r.get(0)).unwrap();
    assert_eq!(cursor, 0);
}

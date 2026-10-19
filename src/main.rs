//! Minimal MCP stdio server: newline-delimited JSON-RPC, two tools.
mod store;
mod sync;
#[cfg(test)]
mod tests;

use rusqlite::Connection;
use serde_json::{Map, Value, json};
use std::io::{BufRead, Write};
use std::sync::{Mutex, OnceLock};

#[derive(Debug)]
pub struct Error(pub String);

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error(e.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn home() -> String {
    std::env::var("HOME").unwrap_or_default()
}

fn token_path() -> String {
    format!("{}/.config/discord-search/token", home())
}

static DB: OnceLock<Mutex<Connection>> = OnceLock::new();

fn db() -> Result<&'static Mutex<Connection>, Error> {
    if let Some(db) = DB.get() {
        return Ok(db);
    }
    let path = std::env::var("DISCORD_SEARCH_DB")
        .ok()
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| format!("{}/.local/share/discord-search/discord.db", home()));
    let _ = DB.set(Mutex::new(store::open(&path)?));
    Ok(DB.get().unwrap())
}

fn sync_now(budget_ms: i64) -> Result<String, Error> {
    let token = std::env::var("DISCORD_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .or_else(|| std::fs::read_to_string(token_path()).ok().map(|t| store::js_trim(&t).to_string()))
        .filter(|t| !t.is_empty())
        .ok_or_else(|| Error(format!("No bot token: set DISCORD_TOKEN or write it to {}.", token_path())))?;
    sync::sync(db()?, &sync::Client::new(&token, &sync::curl), budget_ms)
}

// Clients such as Executor show only the tool description, so it carries the argument semantics.
fn tools() -> Value {
    json!([
        {
            "name": "discord_search",
            "description": ([
                "Search the team Discord chat history (local archive of every channel and thread the bot can read).",
                "query: SQLite FTS5, words ANDed and stemmed; \"exact phrase\", OR, NOT, prefix*, NEAR(a b, 5). Omit it to browse by filters.",
                "channel: name or id, includes its threads. author: display name or username substring, or user id.",
                "after/before: ISO date/time or message id, exclusive. sort: relevance (default with query) | newest (default without) | oldest.",
                "context: messages shown before/after each hit, default 0. limit: default 20, max 200.",
                "Times are local time; ↩ marks a reply.",
                "Synced every few minutes; the header shows when.",
            ].join("\n")),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "channel": { "type": "string" },
                    "author": { "type": "string" },
                    "after": { "type": "string" },
                    "before": { "type": "string" },
                    "sort": { "type": "string", "enum": ["relevance", "newest", "oldest"] },
                    "context": { "type": "integer", "minimum": 0, "maximum": 20 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200 },
                },
                "additionalProperties": false,
            },
            "annotations": { "title": "Search Discord", "readOnlyHint": true, "openWorldHint": false },
        },
        {
            "name": "discord_sync",
            "description": ([
                "Fetch new team Discord messages into the local archive that discord_search reads. Incremental; seconds when little changed.",
                "Each call stops after about 50s; the first run backfills full history, so repeat while it reports Partial.",
                "Captures new messages only; later edits and deletions are not reflected.",
            ].join("\n")),
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false },
            "annotations": { "title": "Sync Discord", "readOnlyHint": false, "destructiveHint": false, "idempotentHint": true, "openWorldHint": true },
        },
    ])
}

fn call_tool(params: &Value) -> Value {
    let result = match params["name"].as_str() {
        Some("discord_search") => db().and_then(|db| {
            let args = params["arguments"].as_object().cloned().unwrap_or_else(Map::new);
            store::search(&db.lock().unwrap(), &args)
        }),
        Some("discord_sync") => sync_now(50_000),
        name => Err(Error(format!("Unknown tool: {}", name.unwrap_or("undefined")))),
    };
    match result {
        Ok(text) => json!({ "content": [{ "type": "text", "text": text }] }),
        Err(e) => json!({ "content": [{ "type": "text", "text": e.0 }], "isError": true }),
    }
}

/// The JSON-RPC result, or (code, message) for an error response.
fn handle(msg: &Value) -> Result<Value, (i64, String)> {
    match msg["method"].as_str().unwrap_or("") {
        "initialize" => Ok(json!({
            "protocolVersion": msg["params"].get("protocolVersion").filter(|v| !v.is_null()).cloned().unwrap_or("2025-06-18".into()),
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "discord-search", "version": env!("CARGO_PKG_VERSION") },
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools() })),
        "tools/call" => Ok(call_tool(&msg["params"])),
        method => Err((-32601, format!("Method not found: {method}"))),
    }
}

fn reply(id: &Value, outcome: Result<Value, (i64, String)>) -> String {
    match outcome {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err((code, message)) => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }),
    }
    .to_string()
}

fn send(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}").and_then(|()| out.flush());
}

fn main() {
    // `discord-search sync` runs one long-budget sync for schedulers, then exits.
    if std::env::args().nth(1).as_deref() == Some("sync") {
        match sync_now(15 * 60_000) {
            Ok(text) => println!("{text}"),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
        return;
    }

    // A sync can take 50s, so it runs on its own thread and other requests keep flowing.
    let mut syncs = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if store::js_trim(&line).is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            send(&reply(&Value::Null, Err((-32700, "Parse error".into()))));
            continue;
        };
        let Some(id) = msg.get("id").cloned() else { continue }; // notification
        if msg["method"] == "tools/call" && msg["params"]["name"] == "discord_sync" {
            syncs.retain(|s: &std::thread::JoinHandle<()>| !s.is_finished()); // dropping detaches and frees them
            let job = (id.clone(), msg.clone());
            match std::thread::Builder::new().spawn(move || send(&reply(&job.0, handle(&job.1)))) {
                Ok(sync) => syncs.push(sync),
                Err(_) => send(&reply(&id, handle(&msg))),
            }
        } else {
            send(&reply(&id, handle(&msg)));
        }
    }
    syncs.into_iter().for_each(|s| drop(s.join()));
}

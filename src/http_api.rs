//! Small local control API.
//!
//! It exists for two reasons: MMS submission needs a way to hand over binary
//! attachments (which SIP MESSAGE is a poor fit for), and the simplified MMS
//! notifications sent over SIP need somewhere to point at for downloads.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::mms::SendRequest;
use crate::state::Shared;

pub async fn run(shared: Arc<Shared>) -> Result<()> {
    if !shared.cfg.http.enabled {
        info!("HTTP API disabled");
        return Ok(());
    }
    let addr: SocketAddr = shared
        .cfg
        .http
        .bind
        .parse()
        .with_context(|| format!("http.bind is not a socket address: {}", shared.cfg.http.bind))?;
    let listener = TcpListener::bind(addr).await.with_context(|| format!("binding {addr}"))?;
    info!(%addr, "HTTP API listening");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "HTTP accept failed");
                continue;
            }
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, shared).await {
                debug!(%peer, error = %e, "HTTP connection failed");
            }
        });
    }
}

struct HttpRequest {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn query_param(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            (k == name).then(|| percent_decode(v))
        })
    }
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s).decode_utf8_lossy().into_owned()
}

async fn handle(mut stream: TcpStream, shared: Arc<Shared>) -> Result<()> {
    let req = match read_request(&mut stream).await? {
        Some(r) => r,
        None => return Ok(()),
    };

    if let Some(token) = &shared.cfg.http.token {
        let ok = req
            .header("authorization")
            .map(|v| v.trim_start_matches("Bearer ").trim() == token)
            .unwrap_or(false);
        if !ok {
            return respond_json(&mut stream, 401, &json!({"error": "unauthorized"})).await;
        }
    }

    let segments: Vec<&str> = req.path.trim_matches('/').split('/').collect();
    match (req.method.as_str(), segments.as_slice()) {
        ("GET", [""]) | ("GET", ["health"]) => {
            let modem = shared.modem().await;
            let body = match &modem {
                Some(m) => json!({
                    "status": "up",
                    "modem": {
                        "path": m.info.path,
                        "manufacturer": m.info.manufacturer,
                        "model": m.info.model,
                        "imei": m.info.equipment_id,
                        "device": m.info.device,
                        "primary_port": m.info.primary_port,
                        "own_number": m.info.own_number,
                        "operator": m.info.operator,
                        "signal_quality": m.signal_quality().await,
                    },
                    "alsa": m.alsa.as_ref().map(|c| json!({
                        "card": c.index,
                        "id": c.id,
                        "name": c.name,
                        "device": c.device_string(true),
                    })),
                    "mms_enabled": shared.mms.enabled(),
                }),
                None => json!({"status": "down", "reason": "no modem"}),
            };
            respond_json(&mut stream, if modem.is_some() { 200 } else { 503 }, &body).await
        }

        ("GET", ["cards"]) => {
            let cards: Vec<_> = crate::audio::list_cards()
                .into_iter()
                .map(|c| {
                    json!({
                        "card": c.index,
                        "id": c.id,
                        "name": c.name,
                        "device_path": c.device_path.to_string_lossy(),
                        "playback": c.has_playback,
                        "capture": c.has_capture,
                    })
                })
                .collect();
            respond_json(&mut stream, 200, &json!({ "cards": cards })).await
        }

        ("GET", ["messages"]) => {
            let limit = req
                .query_param("limit")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(50)
                .clamp(1, 500);
            let before = req.query_param("before").and_then(|v| v.parse::<i64>().ok());
            let messages = shared.db.list_messages(limit, before).await?;
            respond_json(&mut stream, 200, &json!({ "messages": messages })).await
        }

        ("GET", ["messages", id]) => match id.parse::<i64>() {
            Ok(id) => match shared.db.get_message(id).await? {
                Some(m) => respond_json(&mut stream, 200, &json!(m)).await,
                None => respond_json(&mut stream, 404, &json!({"error": "not found"})).await,
            },
            Err(_) => respond_json(&mut stream, 400, &json!({"error": "bad id"})).await,
        },

        ("GET", ["messages", id, "attachments", index]) => {
            let (Ok(id), Ok(index)) = (id.parse::<i64>(), index.parse::<i64>()) else {
                return respond_json(&mut stream, 400, &json!({"error": "bad id"})).await;
            };
            match shared.db.attachment_path(id, index).await? {
                Some((path, content_type)) => match tokio::fs::read(&path).await {
                    Ok(data) => respond_bytes(&mut stream, 200, &content_type, data).await,
                    Err(e) => {
                        respond_json(&mut stream, 500, &json!({"error": e.to_string()})).await
                    }
                },
                None => respond_json(&mut stream, 404, &json!({"error": "not found"})).await,
            }
        }

        ("POST", ["sms"]) => {
            #[derive(serde::Deserialize)]
            struct SmsBody {
                to: String,
                text: String,
            }
            let body: SmsBody = match serde_json::from_slice(&req.body) {
                Ok(b) => b,
                Err(e) => {
                    return respond_json(&mut stream, 400, &json!({"error": e.to_string()})).await
                }
            };
            let Some(modem) = shared.modem().await else {
                return respond_json(&mut stream, 503, &json!({"error": "no modem"})).await;
            };
            let number = crate::gateway::sanitize_number(&body.to);
            match modem.send_sms(&number, &body.text, shared.cfg.sms.delivery_report).await {
                Ok(path) => {
                    let id = shared
                        .db
                        .insert_message(crate::db::NewMessage {
                            kind: "sms",
                            direction: crate::db::Direction::Outgoing,
                            peer: number.clone(),
                            own_number: modem.info.own_number.clone(),
                            subject: None,
                            text: Some(body.text.clone()),
                            timestamp: None,
                            status: "sent".into(),
                            external_id: Some(path.to_string()),
                            raw: None,
                        })
                        .await
                        .ok()
                        .flatten();
                    respond_json(&mut stream, 202, &json!({"status": "sent", "id": id})).await
                }
                Err(e) => respond_json(&mut stream, 500, &json!({"error": e.to_string()})).await,
            }
        }

        // Retry a notification that was never downloaded (MMS disabled at the
        // time, bearer down, MMSC error).
        ("POST", ["messages", id, "retrieve"]) => {
            let Ok(id) = id.parse::<i64>() else {
                return respond_json(&mut stream, 400, &json!({"error": "bad id"})).await;
            };
            match shared.mms.retrieve_stored(id).await {
                Ok(()) => {
                    let msg = shared.db.get_message(id).await?;
                    respond_json(&mut stream, 200, &json!({"status": "retrieved", "message": msg}))
                        .await
                }
                Err(e) => {
                    respond_json(&mut stream, 502, &json!({"error": format!("{e:#}")})).await
                }
            }
        }

        ("POST", ["mms"]) => {
            let body: SendRequest = match serde_json::from_slice(&req.body) {
                Ok(b) => b,
                Err(e) => {
                    return respond_json(&mut stream, 400, &json!({"error": e.to_string()})).await
                }
            };
            if !shared.is_ready() {
                return respond_json(&mut stream, 503, &json!({"error": "no modem"})).await;
            }
            match shared.mms.send(body).await {
                Ok(id) => respond_json(&mut stream, 202, &json!({"status": "sent", "id": id})).await,
                Err(e) => respond_json(&mut stream, 500, &json!({"error": e.to_string()})).await,
            }
        }

        _ => respond_json(&mut stream, 404, &json!({"error": "no such endpoint"})).await,
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<Option<HttpRequest>> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];

    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            anyhow::bail!("request headers too large");
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let start = lines.next().unwrap_or_default();
    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();

    let mut body = buf[header_end + 4..].to_vec();
    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > 32 * 1024 * 1024 {
        anyhow::bail!("request body too large");
    }
    while body.len() < content_length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);

    Ok(Some(HttpRequest { method, path, query, headers, body }))
}

async fn respond_json(
    stream: &mut TcpStream,
    status: u16,
    body: &serde_json::Value,
) -> Result<()> {
    let data = serde_json::to_vec_pretty(body)?;
    respond_bytes(stream, status, "application/json; charset=utf-8", data).await
}

async fn respond_bytes(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: Vec<u8>,
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

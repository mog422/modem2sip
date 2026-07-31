//! SQLite storage for SMS / MMS / call detail records.
//!
//! rusqlite is blocking, so every public method here is `async` and hops onto
//! the blocking pool.  A single connection behind a mutex is plenty for the
//! traffic a single modem produces.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
    attachments_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Incoming,
    Outgoing,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Incoming => "incoming",
            Direction::Outgoing => "outgoing",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NewMessage {
    pub kind: &'static str, // "sms" | "mms"
    pub direction: Direction,
    pub peer: String,
    pub own_number: Option<String>,
    pub subject: Option<String>,
    pub text: Option<String>,
    /// Network timestamp as reported by the modem / MMSC (ISO-8601).
    pub timestamp: Option<String>,
    pub status: String,
    /// ModemManager object path or MMS transaction id, used for dedup.
    pub external_id: Option<String>,
    pub raw: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Attachment {
    pub index: i64,
    pub content_type: String,
    pub name: Option<String>,
    pub content_id: Option<String>,
    pub size: i64,
    pub path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoredMessage {
    pub id: i64,
    pub kind: String,
    pub direction: String,
    pub peer: String,
    pub own_number: Option<String>,
    pub subject: Option<String>,
    pub text: Option<String>,
    pub timestamp: Option<String>,
    pub received_at: String,
    pub status: String,
    pub external_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
}

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS messages (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,
    direction    TEXT NOT NULL,
    peer         TEXT NOT NULL,
    own_number   TEXT,
    subject      TEXT,
    text         TEXT,
    timestamp    TEXT,
    received_at  TEXT NOT NULL,
    status       TEXT NOT NULL,
    external_id  TEXT,
    error        TEXT,
    raw          BLOB
);
CREATE INDEX IF NOT EXISTS idx_messages_peer ON messages(peer);
CREATE INDEX IF NOT EXISTS idx_messages_received ON messages(received_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_external
    ON messages(kind, direction, external_id) WHERE external_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS attachments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id   INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    idx          INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    name         TEXT,
    content_id   TEXT,
    size         INTEGER NOT NULL,
    path         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attachments_msg ON attachments(message_id);
-- Retrying a retrieval used to insert a second copy of every part, so a
-- database written by an older build can hold duplicates.  They have to go
-- before the index below can exist; the newest row of each pair wins.
DELETE FROM attachments WHERE id NOT IN (
    SELECT MAX(id) FROM attachments GROUP BY message_id, idx
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_attachments_msg_idx
    ON attachments(message_id, idx);

CREATE TABLE IF NOT EXISTS calls (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    direction   TEXT NOT NULL,
    peer        TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    answered_at TEXT,
    ended_at    TEXT,
    disposition TEXT
);
"#;

pub fn now_iso() -> String {
    chrono::Local::now().to_rfc3339()
}

impl Db {
    pub async fn open(db_path: &Path, attachments_dir: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
        }
        tokio::fs::create_dir_all(attachments_dir)
            .await
            .with_context(|| format!("creating {}", attachments_dir.display()))?;

        let path = db_path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            let conn = Connection::open(&path)
                .with_context(|| format!("opening sqlite db {}", path.display()))?;
            conn.execute_batch(SCHEMA).context("applying schema")?;
            Ok(conn)
        })
        .await??;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            attachments_dir: attachments_dir.to_path_buf(),
        })
    }

    async fn with_conn<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = conn.blocking_lock();
            f(&mut guard)
        })
        .await?
    }

    /// The row a previous copy of this message was stored as, with its status.
    ///
    /// Used to tell a re-announcement apart from a re-delivery that still
    /// needs work: an MMS notification the carrier repeats because the body
    /// was never fetched has the same identity as the one already stored.
    pub async fn find_by_external_id(
        &self,
        kind: &'static str,
        direction: Direction,
        external_id: &str,
    ) -> Result<Option<(i64, String)>> {
        let external_id = external_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT id, status FROM messages
                      WHERE kind = ?1 AND direction = ?2 AND external_id = ?3",
                    params![kind, direction.as_str(), external_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    /// Insert a message.  Returns `None` when `external_id` was already
    /// stored (the modem re-announcing a message after a restart).
    pub async fn insert_message(&self, msg: NewMessage) -> Result<Option<i64>> {
        self.with_conn(move |conn| {
            if let Some(ext) = &msg.external_id {
                let existing: Option<i64> = conn
                    .query_row(
                        "SELECT id FROM messages WHERE kind = ?1 AND direction = ?2 AND external_id = ?3",
                        params![msg.kind, msg.direction.as_str(), ext],
                        |r| r.get(0),
                    )
                    .optional()?;
                if existing.is_some() {
                    return Ok(None);
                }
            }
            conn.execute(
                "INSERT INTO messages
                   (kind, direction, peer, own_number, subject, text, timestamp,
                    received_at, status, external_id, raw)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    msg.kind,
                    msg.direction.as_str(),
                    msg.peer,
                    msg.own_number,
                    msg.subject,
                    msg.text,
                    msg.timestamp,
                    now_iso(),
                    msg.status,
                    msg.external_id,
                    msg.raw,
                ],
            )?;
            Ok(Some(conn.last_insert_rowid()))
        })
        .await
    }

    /// Persist one MMS part to disk and record it.
    pub async fn add_attachment(
        &self,
        message_id: i64,
        index: i64,
        content_type: &str,
        name: Option<&str>,
        content_id: Option<&str>,
        data: &[u8],
    ) -> Result<Attachment> {
        let dir = self.attachments_dir.join(message_id.to_string());
        tokio::fs::create_dir_all(&dir).await?;
        let safe = sanitize_name(name, index, content_type);
        let path = dir.join(&safe);
        tokio::fs::write(&path, data).await?;

        let att = Attachment {
            index,
            content_type: content_type.to_string(),
            name: name.map(|s| s.to_string()),
            content_id: content_id.map(|s| s.to_string()),
            size: data.len() as i64,
            path: path.to_string_lossy().into_owned(),
        };
        let a = att.clone();
        self.with_conn(move |conn| {
            // A retried retrieval re-stores the same parts, so an index that
            // is already there is replaced rather than duplicated.
            conn.execute(
                "INSERT INTO attachments (message_id, idx, content_type, name, content_id, size, path)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)
                 ON CONFLICT(message_id, idx) DO UPDATE SET
                     content_type = excluded.content_type,
                     name         = excluded.name,
                     content_id   = excluded.content_id,
                     size         = excluded.size,
                     path         = excluded.path",
                params![message_id, a.index, a.content_type, a.name, a.content_id, a.size, a.path],
            )?;
            Ok(())
        })
        .await?;
        Ok(att)
    }

    /// Drop attachments numbered above `keep`.
    ///
    /// A retried retrieval overwrites each part in place, so the only rows
    /// that need removing afterwards are the tail of a previous, longer run.
    /// Doing it this way round means a retry that is interrupted leaves the
    /// message with the parts it already had rather than with none.
    pub async fn prune_attachments(&self, message_id: i64, keep: i64) -> Result<()> {
        let stale: Vec<String> = self
            .with_conn(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT path FROM attachments WHERE message_id = ?1 AND idx > ?2",
                )?;
                let rows = stmt
                    .query_map(params![message_id, keep], |r| r.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                conn.execute(
                    "DELETE FROM attachments WHERE message_id = ?1 AND idx > ?2",
                    params![message_id, keep],
                )?;
                Ok(rows)
            })
            .await?;
        for path in stale {
            let _ = tokio::fs::remove_file(&path).await;
        }
        Ok(())
    }

    /// Fill in the fields that only become known once an MMS body has been
    /// downloaded from the MMSC.
    pub async fn update_received_mms(
        &self,
        id: i64,
        subject: Option<&str>,
        text: Option<&str>,
        timestamp: Option<String>,
    ) -> Result<()> {
        let subject = subject.map(|s| s.to_string());
        let text = text.map(|s| s.to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE messages
                    SET subject   = COALESCE(?1, subject),
                        text      = COALESCE(?2, text),
                        timestamp = COALESCE(?3, timestamp)
                  WHERE id = ?4",
                params![subject, text, timestamp, id],
            )?;
            Ok(())
        })
        .await
    }

    /// The raw PDU stored with a message (an MMS notification, for retries).
    pub async fn message_raw(&self, id: i64) -> Result<Option<Vec<u8>>> {
        self.with_conn(move |conn| {
            let raw = conn
                .query_row("SELECT raw FROM messages WHERE id = ?1", params![id], |r| {
                    r.get::<_, Option<Vec<u8>>>(0)
                })
                .optional()?
                .flatten();
            Ok(raw)
        })
        .await
    }

    pub async fn set_status(&self, id: i64, status: &str, error: Option<&str>) -> Result<()> {
        let status = status.to_string();
        let error = error.map(|s| s.to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE messages SET status = ?1, error = ?2 WHERE id = ?3",
                params![status, error, id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_message(&self, id: i64) -> Result<Option<StoredMessage>> {
        self.with_conn(move |conn| {
            let msg = conn
                .query_row(
                    "SELECT id, kind, direction, peer, own_number, subject, text, timestamp,
                            received_at, status, external_id
                     FROM messages WHERE id = ?1",
                    params![id],
                    row_to_message,
                )
                .optional()?;
            match msg {
                None => Ok(None),
                Some(mut m) => {
                    m.attachments = load_attachments(conn, m.id)?;
                    Ok(Some(m))
                }
            }
        })
        .await
    }

    pub async fn list_messages(&self, limit: i64, before_id: Option<i64>) -> Result<Vec<StoredMessage>> {
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, kind, direction, peer, own_number, subject, text, timestamp,
                        received_at, status, external_id
                 FROM messages
                 WHERE (?1 IS NULL OR id < ?1)
                 ORDER BY id DESC LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(params![before_id, limit], row_to_message)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(stmt);
            let mut out = Vec::with_capacity(rows.len());
            for mut m in rows {
                m.attachments = load_attachments(conn, m.id)?;
                out.push(m);
            }
            Ok(out)
        })
        .await
    }

    pub async fn attachment_path(&self, message_id: i64, index: i64) -> Result<Option<(String, String)>> {
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT path, content_type FROM attachments WHERE message_id = ?1 AND idx = ?2",
                    params![message_id, index],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn start_call(&self, direction: Direction, peer: &str) -> Result<i64> {
        let peer = peer.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO calls (direction, peer, started_at) VALUES (?1,?2,?3)",
                params![direction.as_str(), peer, now_iso()],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .await
    }

    pub async fn call_answered(&self, id: i64) -> Result<()> {
        self.with_conn(move |conn| {
            conn.execute("UPDATE calls SET answered_at = ?1 WHERE id = ?2", params![now_iso(), id])?;
            Ok(())
        })
        .await
    }

    pub async fn call_ended(&self, id: i64, disposition: &str) -> Result<()> {
        let disposition = disposition.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE calls SET ended_at = ?1, disposition = ?2 WHERE id = ?3",
                params![now_iso(), disposition, id],
            )?;
            Ok(())
        })
        .await
    }
}

fn row_to_message(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage> {
    Ok(StoredMessage {
        id: r.get(0)?,
        kind: r.get(1)?,
        direction: r.get(2)?,
        peer: r.get(3)?,
        own_number: r.get(4)?,
        subject: r.get(5)?,
        text: r.get(6)?,
        timestamp: r.get(7)?,
        received_at: r.get(8)?,
        status: r.get(9)?,
        external_id: r.get(10)?,
        attachments: Vec::new(),
    })
}

fn load_attachments(conn: &Connection, message_id: i64) -> Result<Vec<Attachment>> {
    let mut stmt = conn.prepare(
        "SELECT idx, content_type, name, content_id, size, path
         FROM attachments WHERE message_id = ?1 ORDER BY idx",
    )?;
    let rows = stmt
        .query_map(params![message_id], |r| {
            Ok(Attachment {
                index: r.get(0)?,
                content_type: r.get(1)?,
                name: r.get(2)?,
                content_id: r.get(3)?,
                size: r.get(4)?,
                path: r.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn sanitize_name(name: Option<&str>, index: i64, content_type: &str) -> String {
    let base = name
        .map(|n| {
            n.chars()
                .map(|c| if c.is_alphanumeric() || "._-".contains(c) { c } else { '_' })
                .collect::<String>()
        })
        .filter(|n| !n.is_empty() && n != "." && n != "..")
        .unwrap_or_else(|| format!("part{index}{}", ext_for(content_type)));
    format!("{index}_{base}")
}

fn ext_for(content_type: &str) -> &'static str {
    match content_type.split(';').next().unwrap_or("").trim() {
        "text/plain" => ".txt",
        "text/html" => ".html",
        "image/jpeg" => ".jpg",
        "image/png" => ".png",
        "image/gif" => ".gif",
        "audio/amr" => ".amr",
        "video/3gpp" => ".3gp",
        "application/smil" => ".smil",
        _ => ".bin",
    }
}

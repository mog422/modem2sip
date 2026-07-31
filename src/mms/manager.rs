//! MMS orchestration: notification -> retrieval -> SQLite, and submission.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db::{Db, Direction, NewMessage};
use crate::mm::SmsInfo;

use super::http::{self, HttpOptions};
use super::pdu::{self, MmsMessage, MmsPart, SendReq};
use super::{is_mms_push, parse_wap_push};

pub struct MmsManager {
    cfg: Arc<Config>,
    db: Db,
    /// Kept in step with [`crate::state::Shared`] so MMS traffic can be bound
    /// to whatever the modem's data bearer currently is.
    modem: tokio::sync::RwLock<Option<Arc<crate::mm::ModemHandle>>>,
}

/// What the HTTP API / SIP side hands over when submitting an MMS.
#[derive(Debug, Clone, Deserialize)]
pub struct SendRequest {
    pub to: Vec<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub attachments: Vec<SendAttachment>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SendAttachment {
    pub content_type: String,
    #[serde(default)]
    pub name: Option<String>,
    /// base64 payload, or use `path` to read from disk.
    #[serde(default)]
    pub data_base64: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
}

impl MmsManager {
    pub fn new(cfg: Arc<Config>, db: Db) -> Self {
        Self { cfg, db, modem: tokio::sync::RwLock::new(None) }
    }

    pub fn enabled(&self) -> bool {
        self.cfg.mms.enabled
    }

    pub async fn set_modem(&self, handle: Option<Arc<crate::mm::ModemHandle>>) {
        *self.modem.write().await = handle;
    }

    /// Transport options for one request.  When the configuration does not
    /// pin an interface or source address, they are taken from the modem's
    /// connected data bearer - which is where MMS has to go, and whose
    /// address changes on every attach.
    async fn http_options(&self) -> HttpOptions {
        let mut opts = HttpOptions {
            proxy: self.cfg.mms.proxy.clone(),
            interface: self.cfg.mms.interface.clone(),
            local_ip: self.cfg.mms.local_ip,
            timeout: Duration::from_secs(self.cfg.mms.timeout_secs),
            user_agent: self.cfg.mms.user_agent.clone(),
            ua_profile: self.cfg.mms.ua_profile.clone(),
            max_size: self.cfg.mms.max_size,
            dns_servers: self.cfg.mms.dns.clone(),
        };

        let needs_binding = opts.interface.is_none() && opts.local_ip.is_none();
        if needs_binding || opts.dns_servers.is_empty() {
            let modem = self.modem.read().await.clone();
            if let Some(modem) = modem {
                match modem.data_bearer().await {
                    Some(bearer) => {
                        debug!(
                            interface = %bearer.interface,
                            address = ?bearer.address,
                            dns = ?bearer.dns,
                            "binding MMS traffic to the modem's data bearer"
                        );
                        if needs_binding {
                            opts.interface = Some(bearer.interface);
                            opts.local_ip = bearer.address;
                        }
                        if opts.dns_servers.is_empty() {
                            opts.dns_servers = bearer.dns;
                        }
                    }
                    None => warn!(
                        "no connected data bearer; MMS will use the host's default route \
                         and resolver.  Connect one (see contrib/mms-bearer) or set \
                         mms.interface / mms.dns"
                    ),
                }
            }
        }
        opts
    }

    async fn run_setup_command(&self) -> Result<()> {
        let Some(cmd) = &self.cfg.mms.setup_command else { return Ok(()) };
        debug!(%cmd, "running mms.setup_command");
        let status = tokio::process::Command::new("sh").arg("-c").arg(cmd).status().await?;
        if !status.success() {
            bail!("mms.setup_command exited with {status}");
        }
        Ok(())
    }

    /// Handle a binary SMS that looks like a WAP push.  Returns the database
    /// id of the stored message when the push really was an MMS.
    pub async fn handle_push(&self, sms: &SmsInfo) -> Result<Option<i64>> {
        let push = parse_wap_push(&sms.data).context("decoding the WAP push")?;
        if !is_mms_push(&push) {
            debug!(content_type = %push.content_type, "ignoring non-MMS WAP push");
            return Ok(None);
        }
        let notification = pdu::decode(&push.body).context("decoding the MMS PDU")?;
        if notification.message_type != pdu::msg_type::NOTIFICATION_IND {
            debug!(
                kind = pdu::msg_type::name(notification.message_type),
                "MMS PDU is not a notification; storing as-is"
            );
        }

        let peer = notification
            .sender()
            .or_else(|| Some(sms.number.clone()))
            .unwrap_or_default();
        let tid = notification.transaction_id.clone();

        let id = self
            .db
            .insert_message(NewMessage {
                kind: "mms",
                direction: Direction::Incoming,
                peer: peer.clone(),
                own_number: None,
                subject: notification.subject.clone(),
                text: None,
                timestamp: notification.date.map(unix_to_iso),
                status: "notified".into(),
                external_id: tid.clone().or_else(|| notification.message_id.clone()),
                raw: Some(push.body.clone()),
            })
            .await?;

        let Some(id) = id else {
            debug!("duplicate MMS notification ignored");
            return Ok(None);
        };

        info!(
            id,
            from = %peer,
            size = notification.message_size.unwrap_or(0),
            location = notification.content_location.as_deref().unwrap_or("-"),
            "MMS notification received"
        );

        if !self.cfg.mms.enabled {
            self.db
                .set_status(id, "notified", Some("mms disabled in the configuration"))
                .await?;
            return Ok(Some(id));
        }
        if !self.cfg.mms.auto_retrieve {
            // Deferred: the body stays on the MMSC until it is fetched
            // through the HTTP API.
            self.notify_resp(&notification, 0x83).await;
            return Ok(Some(id));
        }

        if let Err(e) = self.retrieve(&notification, id).await {
            // Deliberately unanswered: "deferred" would tell the MMSC we
            // intend to fetch it ourselves and stop it re-notifying, and
            // nothing here retries automatically.  Staying quiet keeps the
            // carrier's own retry going.
            warn!(id, error = %format!("{e:#}"), "MMS retrieval failed; leaving it to the carrier to re-notify");
            self.db.set_status(id, "retrieve_failed", Some(&e.to_string())).await?;
        }
        Ok(Some(id))
    }

    /// Retry a stored notification: used for messages that arrived while MMS
    /// was disabled or whose download failed (bearer down, MMSC hiccup).
    pub async fn retrieve_stored(&self, id: i64) -> Result<()> {
        if !self.cfg.mms.enabled {
            bail!("MMS is disabled (set mms.enabled = true and mms.mmsc)");
        }
        let raw = self
            .db
            .message_raw(id)
            .await?
            .ok_or_else(|| anyhow!("message {id} has no stored notification"))?;
        let notification = pdu::decode(&raw).context("decoding the stored notification")?;
        match self.retrieve(&notification, id).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.db.set_status(id, "retrieve_failed", Some(&e.to_string())).await?;
                Err(e)
            }
        }
    }

    /// Download the message body from the MMSC and store it.
    async fn retrieve(&self, notification: &MmsMessage, id: i64) -> Result<()> {
        let url = notification
            .content_location
            .clone()
            .ok_or_else(|| anyhow!("notification has no X-Mms-Content-Location"))?;
        if let Some(size) = notification.message_size {
            if size as usize > self.cfg.mms.max_size {
                bail!("message of {size} bytes exceeds mms.max_size");
            }
        }

        self.run_setup_command().await?;
        let opts = self.http_options().await;
        let resp = http::get(&url, &opts).await.context("fetching the MMS body")?;
        if resp.status != 200 {
            bail!("MMSC returned HTTP {}", resp.status);
        }

        let message = pdu::decode(&resp.body).context("decoding M-Retrieve.conf")?;

        // A 200 with an error page or an unsupported-content status decodes
        // into an empty message.  Storing that as "received" and then
        // acknowledging it makes the MMSC drop the real message for good, so
        // both are checked before anything is written.
        if message.message_type != pdu::msg_type::RETRIEVE_CONF {
            // Not every MMSC labels its reply, so the absence of the header
            // alone is not fatal - but a captive portal or error page decodes
            // to no parts at all, and that is what has to be caught.
            if message.parts.is_empty() {
                bail!(
                    "the MMSC returned a {} with no content ({}, {} bytes) instead of an \
                     M-Retrieve.conf",
                    pdu::msg_type::name(message.message_type),
                    resp.content_type().unwrap_or("no content type"),
                    resp.body.len()
                );
            }
            warn!(
                kind = pdu::msg_type::name(message.message_type),
                parts = message.parts.len(),
                "the MMSC did not label its reply as an M-Retrieve.conf; storing it anyway"
            );
        }
        if let Some(status) = message.retrieve_status.filter(|s| *s != 0x80) {
            let text = message.response_text.clone().unwrap_or_default();
            bail!("the MMSC refused the retrieval: status 0x{status:02x} {text}");
        }

        self.store_retrieved(id, &message).await?;

        // Tell the MMSC we got it.  Failure here is not fatal.
        if let Some(tid) = message.transaction_id.clone().or_else(|| notification.transaction_id.clone()) {
            if let Some(mmsc) = &self.cfg.mms.mmsc {
                let ack = pdu::encode_acknowledge_ind(&tid, false);
                match http::post(mmsc, "application/vnd.wap.mms-message", ack, &opts).await {
                    Ok(r) => debug!(status = r.status, "M-Acknowledge.ind sent"),
                    Err(e) => debug!(error = %format!("{e:#}"), "M-Acknowledge.ind failed"),
                }
            }
        }
        Ok(())
    }

    /// Answer a notification we are not going to retrieve right now.
    ///
    /// Silence makes the carrier re-notify on a timer and then expire the
    /// message; telling it what happened stops both.
    ///
    /// status: 0x80 expired, 0x81 retrieved, 0x82 rejected, 0x83 deferred,
    /// 0x84 unrecognised.
    async fn notify_resp(&self, notification: &MmsMessage, status: u8) {
        let Some(tid) = notification.transaction_id.as_deref() else { return };
        let Some(mmsc) = self.cfg.mms.mmsc.as_deref() else { return };
        let pdu = pdu::encode_notify_resp_ind(tid, status, false);
        let opts = self.http_options().await;
        match http::post(mmsc, "application/vnd.wap.mms-message", pdu, &opts).await {
            Ok(r) => debug!(status = r.status, notify_status = status, "M-NotifyResp.ind sent"),
            Err(e) => debug!(error = %format!("{e:#}"), "M-NotifyResp.ind failed"),
        }
    }

    async fn store_retrieved(&self, id: i64, message: &MmsMessage) -> Result<()> {
        let text = message.body_text();
        let subject = message.subject.clone();
        self.db
            .update_received_mms(id, subject.as_deref(), text.as_deref(), message.date.map(unix_to_iso))
            .await?;

        // Text parts are stored twice on purpose: inline as the message text
        // (that is what SIP peers see) and as a part, so nothing is lost.
        // Each part overwrites the one with the same index from any earlier
        // attempt; the tail of a longer previous run is pruned at the end.
        let mut index = 0i64;
        for part in &message.parts {
            if part.content_type.contains("smil") {
                continue;
            }
            index += 1;
            self.db
                .add_attachment(
                    id,
                    index,
                    &part.content_type,
                    part.name().as_deref(),
                    part.content_id.as_deref(),
                    &part.data,
                )
                .await?;
        }
        self.db.prune_attachments(id, index).await?;
        self.db.set_status(id, "received", None).await?;
        info!(id, parts = index, "MMS retrieved and stored");
        Ok(())
    }

    /// Submit an MMS through the MMSC.  Returns the database id.
    pub async fn send(&self, req: SendRequest) -> Result<i64> {
        if !self.cfg.mms.enabled {
            bail!("MMS is disabled (set mms.enabled = true and mms.mmsc)");
        }
        let mmsc = self
            .cfg
            .mms
            .mmsc
            .clone()
            .ok_or_else(|| anyhow!("mms.mmsc is not configured"))?;
        if req.to.is_empty() {
            bail!("at least one recipient is required");
        }

        // Build the parts: SMIL first (referenced as the presentation), then
        // the text, then the attachments.
        let mut parts: Vec<MmsPart> = Vec::new();
        let mut content: Vec<MmsPart> = Vec::new();

        if let Some(text) = req.text.as_ref().filter(|t| !t.is_empty()) {
            let mut p = MmsPart::new("text/plain", text.as_bytes().to_vec());
            p.content_id = Some("text0".into());
            p.params.push(("name".into(), "text0.txt".into()));
            content.push(p);
        }
        for (i, att) in req.attachments.iter().enumerate() {
            let data = load_attachment(att).await?;
            let mut p = MmsPart::new(&att.content_type, data);
            p.content_id = Some(format!("part{i}"));
            if let Some(name) = &att.name {
                p.params.push(("name".into(), name.clone()));
            }
            content.push(p);
        }
        if content.is_empty() {
            bail!("an MMS needs at least a text or one attachment");
        }

        if content.len() > 1 {
            let smil = pdu::build_smil(&content);
            let mut p = MmsPart::new("application/smil", smil.into_bytes());
            p.content_id = Some("smil".into());
            p.params.push(("name".into(), "presentation.smil".into()));
            parts.push(p);
        }
        parts.extend(content);

        let total: usize = parts.iter().map(|p| p.data.len()).sum();
        if total > self.cfg.mms.max_size {
            bail!("message of {total} bytes exceeds mms.max_size");
        }

        let tid = format!("T{}", crate::sip::auth::random_hex(6));
        let payload = pdu::encode_send_req(&SendReq {
            transaction_id: &tid,
            from: None, // insert-address-token: the MMSC fills in our number
            to: &req.to,
            subject: req.subject.as_deref(),
            parts: &parts,
            delivery_report: false,
            read_report: false,
        });

        let id = self
            .db
            .insert_message(NewMessage {
                kind: "mms",
                direction: Direction::Outgoing,
                peer: req.to.join(","),
                own_number: None,
                subject: req.subject.clone(),
                text: req.text.clone(),
                timestamp: None,
                status: "sending".into(),
                external_id: Some(tid.clone()),
                raw: None,
            })
            .await?
            .ok_or_else(|| anyhow!("duplicate transaction id"))?;

        for (i, part) in parts.iter().enumerate() {
            if part.content_type.contains("smil") {
                continue;
            }
            self.db
                .add_attachment(
                    id,
                    i as i64,
                    &part.content_type,
                    part.name().as_deref(),
                    part.content_id.as_deref(),
                    &part.data,
                )
                .await?;
        }

        self.run_setup_command().await?;
        let opts = self.http_options().await;
        let result = http::post(&mmsc, "application/vnd.wap.mms-message", payload, &opts).await;

        match result {
            Ok(resp) if resp.status == 200 => {
                // An M-Send.conf that will not decode, or that omits the
                // status, is not a confirmation - treating it as one records
                // a message the carrier may never have accepted.
                // No status at all means the MMSC told us nothing beyond the
                // 200, so the submission is taken at face value.  An explicit
                // non-Ok status is a refusal and must not be recorded as
                // sent.
                let conf = pdu::decode(&resp.body).ok();
                let status = conf.as_ref().and_then(|c| c.response_status).unwrap_or(0x80);
                if conf.is_none() {
                    warn!(id, "the MMSC replied 200 with a body we cannot decode; assuming sent");
                }
                if status == 0x80 {
                    self.db.set_status(id, "sent", None).await?;
                    info!(id, to = %req.to.join(","), "MMS sent");
                    Ok(id)
                } else {
                    let text = conf
                        .as_ref()
                        .and_then(|c| c.response_text.clone())
                        .unwrap_or_else(|| format!("response status 0x{status:02x}"));
                    self.db.set_status(id, "failed", Some(&text)).await?;
                    Err(anyhow!("MMSC rejected the message: {text}"))
                }
            }
            Ok(resp) => {
                let msg = format!("MMSC returned HTTP {}", resp.status);
                self.db.set_status(id, "failed", Some(&msg)).await?;
                Err(anyhow!(msg))
            }
            Err(e) => {
                self.db.set_status(id, "failed", Some(&e.to_string())).await?;
                Err(e)
            }
        }
    }
}

async fn load_attachment(att: &SendAttachment) -> Result<Vec<u8>> {
    use base64::Engine as _;
    if let Some(b64) = &att.data_base64 {
        return base64::engine::general_purpose::STANDARD
            .decode(b64.trim())
            .context("decoding data_base64");
    }
    if let Some(path) = &att.path {
        return tokio::fs::read(path)
            .await
            .with_context(|| format!("reading attachment {path}"));
    }
    bail!("attachment needs either data_base64 or path")
}

fn unix_to_iso(secs: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0)
        .map(|dt| dt.with_timezone(&chrono::Local).to_rfc3339())
        .unwrap_or_default()
}

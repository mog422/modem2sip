//! Store-and-forward for what the mobile side sends to SIP.
//!
//! An SMS used to be announced to SIP exactly once, the moment it arrived.
//! If nobody was registered just then - a phone asleep, a PBX restarting -
//! the attempt failed and that was the end of it: the message sat in the
//! database, and nothing on the SIP side ever heard of it.
//!
//! Now a message is marked as owed when it is stored, and this task hands
//! over whatever is owed: at once if it can, otherwise again whenever a
//! phone registers, and on a timer for a configured target that was down.
//! Messages go out one at a time and oldest first, so a backlog arrives in
//! the order it was received.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::db::StoredMessage;
use crate::gateway::{format_mms_summary, send_sip_message, Delivery};
use crate::sip::SipCore;
use crate::state::Shared;

/// How often the head of the queue is tried again while it cannot go out.
const RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// How many times the peer may turn one message down before it is skipped.
/// A message the peer will never take must not hold up the ones behind it.
const MAX_REFUSALS: u32 = 3;

/// Rows fetched per query; the sweep keeps going until the queue is empty.
const BATCH: i64 = 16;

pub async fn run(shared: Arc<Shared>, core: Arc<SipCore>) {
    let mut worker = Worker { shared, core, refused: None, announced: None };
    loop {
        let stalled = worker.sweep().await;
        tokio::select! {
            _ = worker.shared.outbox.notified() => {}
            _ = worker.core.registrar.changed.notified() => {}
            _ = tokio::time::sleep(RETRY_INTERVAL), if stalled => {}
        }
    }
}

struct Worker {
    shared: Arc<Shared>,
    core: Arc<SipCore>,
    /// The message the peer last turned down, and how often in a row.
    refused: Option<(i64, u32)>,
    /// The message a hold-up has already been logged for, so a peer that
    /// stays away for a day is one line in the log and not fourteen hundred.
    announced: Option<i64>,
}

impl Worker {
    /// Hand over everything that is owed.  Returns true when something is
    /// still waiting and worth another try later.
    async fn sweep(&mut self) -> bool {
        loop {
            let batch = match self.shared.db.sip_pending(BATCH).await {
                Ok(batch) => batch,
                Err(e) => {
                    warn!(error = %format!("{e:#}"), "cannot read the messages owed to SIP");
                    return true;
                }
            };
            if batch.is_empty() {
                return false;
            }
            for msg in batch {
                if !self.hand_over(&msg).await {
                    return true;
                }
            }
        }
    }

    /// Returns false when the queue has to wait: this message could not go
    /// out and is being kept for another try.
    async fn hand_over(&mut self, msg: &StoredMessage) -> bool {
        let body = if msg.kind == "mms" {
            format_mms_summary(&self.shared, msg)
        } else {
            msg.text.clone().unwrap_or_default()
        };
        let outcome =
            send_sip_message(&self.shared, &self.core, &msg.peer, "text/plain", body).await;

        let why = match outcome {
            Delivery::Delivered => {
                if self.announced.take() == Some(msg.id) {
                    info!(id = msg.id, "the SIP side is back; delivering what was held for it");
                }
                self.settle(msg.id).await;
                return true;
            }
            Delivery::Refused(code) => {
                let times = match self.refused {
                    Some((id, n)) if id == msg.id => n + 1,
                    _ => 1,
                };
                self.refused = Some((msg.id, times));
                if times >= MAX_REFUSALS {
                    warn!(id = msg.id, code, "the SIP peer keeps refusing this message; skipping it");
                    self.settle(msg.id).await;
                    return true;
                }
                format!("the SIP peer answered {code}")
            }
            Delivery::Unreachable(why) => why,
        };

        if past_retry_window(msg, self.shared.cfg.sms.notify_retry_hours) {
            warn!(
                id = msg.id,
                reason = %why,
                "could not hand the message to SIP; it stays in the database only"
            );
            self.settle(msg.id).await;
            return true;
        }
        if self.announced != Some(msg.id) {
            self.announced = Some(msg.id);
            info!(
                id = msg.id,
                reason = %why,
                "holding the message for SIP until it can be delivered"
            );
        } else {
            debug!(id = msg.id, reason = %why, "still cannot deliver to SIP");
        }
        false
    }

    async fn settle(&mut self, id: i64) {
        if self.refused.map(|(refused, _)| refused) == Some(id) {
            self.refused = None;
        }
        if let Err(e) = self.shared.db.set_sip_pending(id, false).await {
            // Left pending it would be delivered again on the next sweep,
            // and again on every one after that.
            warn!(id, error = %format!("{e:#}"), "cannot record that the message was handled");
        }
    }
}

/// Has this message been waiting longer than it is worth retrying?
///
/// `hours == 0` means "one attempt, as it always was".  A timestamp that
/// will not parse counts as too old: retrying something forever because its
/// age is unknown is the worse mistake.
fn past_retry_window(msg: &StoredMessage, hours: u64) -> bool {
    let Ok(received) = chrono::DateTime::parse_from_rfc3339(&msg.received_at) else {
        return true;
    };
    let age = chrono::Local::now().signed_duration_since(received);
    age.num_seconds() >= (hours as i64).saturating_mul(3600)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(received_at: String) -> StoredMessage {
        StoredMessage {
            id: 1,
            kind: "sms".into(),
            direction: "incoming".into(),
            peer: "01012345678".into(),
            own_number: None,
            subject: None,
            text: Some("hi".into()),
            timestamp: None,
            received_at,
            status: "received".into(),
            external_id: None,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn a_message_is_retried_only_inside_its_window() {
        let ago = |hours: i64| {
            stored((chrono::Local::now() - chrono::Duration::hours(hours)).to_rfc3339())
        };
        assert!(!past_retry_window(&ago(1), 72));
        assert!(!past_retry_window(&ago(71), 72));
        assert!(past_retry_window(&ago(73), 72));
        // A window of zero is the old behaviour: one attempt and no more.
        assert!(past_retry_window(&ago(0), 0));
        // Unknown age is not a licence to retry forever.
        assert!(past_retry_window(&stored("yesterday-ish".into()), 72));
    }
}

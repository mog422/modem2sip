//! MMS support: WAP-push decoding, MMSC transfer, storage.
//!
//! ModemManager has no MMS API - MMS rides on top of a binary SMS
//! (a WAP push carrying an M-Notification.ind) plus HTTP to the carrier's
//! MMSC over the data bearer.  Both halves live here.

pub mod dns;
pub mod http;
pub mod manager;
pub mod pdu;
pub mod wsp;

pub use manager::{MmsManager, SendRequest};

use anyhow::{anyhow, bail, Result};

/// WSP Push (0x06) / ConfirmedPush (0x07) as delivered inside a binary SMS.
pub fn is_wap_push(data: &[u8]) -> bool {
    data.len() > 3 && matches!(data[1], 0x06 | 0x07)
}

pub struct WapPush {
    pub transaction_id: u8,
    pub content_type: String,
    pub body: Vec<u8>,
}

pub fn parse_wap_push(data: &[u8]) -> Result<WapPush> {
    if !is_wap_push(data) {
        bail!("not a WSP push PDU");
    }
    let mut r = wsp::Reader::new(data);
    let transaction_id = r.u8().ok_or_else(|| anyhow!("truncated push PDU"))?;
    let _pdu_type = r.u8();
    let headers_len = r.uintvar().ok_or_else(|| anyhow!("bad push headers length"))? as usize;
    let headers_start = r.pos;
    let headers_end = headers_start
        .checked_add(headers_len)
        .filter(|e| *e <= data.len())
        .ok_or_else(|| anyhow!("push headers run past the end of the PDU"))?;

    let mut hr = wsp::Reader::new(&data[headers_start..headers_end]);
    let (content_type, _params) = hr
        .content_type()
        .ok_or_else(|| anyhow!("push PDU without a content type"))?;

    Ok(WapPush { transaction_id, content_type, body: data[headers_end..].to_vec() })
}

/// True when the push carries an MMS PDU.
pub fn is_mms_push(push: &WapPush) -> bool {
    push.content_type.eq_ignore_ascii_case("application/vnd.wap.mms-message")
        || push.content_type.contains("mms-message")
}

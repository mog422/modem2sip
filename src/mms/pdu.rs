//! MMS encapsulation (OMA MMS-ENC): decode M-Notification.ind and
//! M-Retrieve.conf, encode M-Send.req, M-NotifyResp.ind, M-Acknowledge.ind.

use anyhow::{anyhow, Result};

use super::wsp::{self, Reader};

/// Header field codes, already carrying the 0x80 "well-known" bit.
pub mod field {
    pub const BCC: u8 = 0x81;
    pub const CC: u8 = 0x82;
    pub const CONTENT_LOCATION: u8 = 0x83;
    pub const CONTENT_TYPE: u8 = 0x84;
    pub const DATE: u8 = 0x85;
    pub const DELIVERY_REPORT: u8 = 0x86;
    pub const DELIVERY_TIME: u8 = 0x87;
    pub const EXPIRY: u8 = 0x88;
    pub const FROM: u8 = 0x89;
    pub const MESSAGE_CLASS: u8 = 0x8A;
    pub const MESSAGE_ID: u8 = 0x8B;
    pub const MESSAGE_TYPE: u8 = 0x8C;
    pub const MMS_VERSION: u8 = 0x8D;
    pub const MESSAGE_SIZE: u8 = 0x8E;
    pub const PRIORITY: u8 = 0x8F;
    pub const READ_REPORT: u8 = 0x90;
    pub const REPORT_ALLOWED: u8 = 0x91;
    pub const RESPONSE_STATUS: u8 = 0x92;
    pub const RESPONSE_TEXT: u8 = 0x93;
    pub const SENDER_VISIBILITY: u8 = 0x94;
    pub const STATUS: u8 = 0x95;
    pub const SUBJECT: u8 = 0x96;
    pub const TO: u8 = 0x97;
    pub const TRANSACTION_ID: u8 = 0x98;
    pub const RETRIEVE_STATUS: u8 = 0x99;
    pub const RETRIEVE_TEXT: u8 = 0x9A;
}

pub mod msg_type {
    pub const SEND_REQ: u8 = 0x80;
    pub const SEND_CONF: u8 = 0x81;
    pub const NOTIFICATION_IND: u8 = 0x82;
    pub const NOTIFYRESP_IND: u8 = 0x83;
    pub const RETRIEVE_CONF: u8 = 0x84;
    pub const ACKNOWLEDGE_IND: u8 = 0x85;
    pub const DELIVERY_IND: u8 = 0x86;
    pub const READ_REC_IND: u8 = 0x87;
    pub const READ_ORIG_IND: u8 = 0x88;

    pub fn name(t: u8) -> &'static str {
        match t {
            0x80 => "m-send-req",
            0x81 => "m-send-conf",
            0x82 => "m-notification-ind",
            0x83 => "m-notifyresp-ind",
            0x84 => "m-retrieve-conf",
            0x85 => "m-acknowledge-ind",
            0x86 => "m-delivery-ind",
            0x87 => "m-read-rec-ind",
            0x88 => "m-read-orig-ind",
            _ => "unknown",
        }
    }
}

/// MMS version 1.2 (0x92 = 0x80 | 0x12).
pub const VERSION_1_2: u8 = 0x92;

#[derive(Debug, Clone, Default)]
pub struct MmsPart {
    pub content_type: String,
    pub params: Vec<(String, String)>,
    pub content_id: Option<String>,
    pub content_location: Option<String>,
    pub data: Vec<u8>,
}

impl MmsPart {
    pub fn new(content_type: &str, data: Vec<u8>) -> Self {
        Self { content_type: content_type.to_string(), data, ..Default::default() }
    }

    /// Best-effort file name from the content-type parameters.
    pub fn name(&self) -> Option<String> {
        self.params
            .iter()
            .find(|(k, _)| k == "name" || k == "filename")
            .map(|(_, v)| v.clone())
            .or_else(|| self.content_location.clone())
    }

    pub fn is_text(&self) -> bool {
        self.content_type.starts_with("text/")
    }

    pub fn text(&self) -> Option<String> {
        if !self.is_text() {
            return None;
        }
        let charset = self
            .params
            .iter()
            .find(|(k, _)| k == "charset")
            .and_then(|(_, v)| encoding_rs::Encoding::for_label(v.as_bytes()))
            .unwrap_or(encoding_rs::UTF_8);
        Some(charset.decode(&self.data).0.into_owned())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MmsMessage {
    pub message_type: u8,
    pub transaction_id: Option<String>,
    pub version: Option<u8>,
    pub from: Option<String>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub subject: Option<String>,
    pub date: Option<u64>,
    pub message_id: Option<String>,
    pub message_class: Option<String>,
    /// M-Notification.ind: URL the body must be fetched from.
    pub content_location: Option<String>,
    pub message_size: Option<u64>,
    pub expiry: Option<u64>,
    pub response_status: Option<u8>,
    pub retrieve_status: Option<u8>,
    pub response_text: Option<String>,
    pub content_type: Option<String>,
    pub content_type_params: Vec<(String, String)>,
    pub parts: Vec<MmsPart>,
}

impl MmsMessage {
    /// The parts that carry actual content (SMIL layout excluded).
    pub fn body_text(&self) -> Option<String> {
        let texts: Vec<String> = self
            .parts
            .iter()
            .filter(|p| p.is_text() && !p.content_type.contains("smil"))
            .filter_map(|p| p.text())
            .collect();
        if texts.is_empty() {
            None
        } else {
            Some(texts.join("\n"))
        }
    }

    pub fn attachments(&self) -> Vec<&MmsPart> {
        self.parts
            .iter()
            .filter(|p| !(p.is_text() && !p.content_type.contains("smil")))
            .filter(|p| !p.content_type.contains("smil"))
            .collect()
    }

    /// Sender without the `/TYPE=PLMN` suffix MMS addresses carry.
    pub fn sender(&self) -> Option<String> {
        self.from.as_deref().map(strip_address_type)
    }
}

pub fn strip_address_type(addr: &str) -> String {
    addr.split('/').next().unwrap_or(addr).trim().to_string()
}

// ------------------------------------------------------------------ decode

pub fn decode(data: &[u8]) -> Result<MmsMessage> {
    let mut r = Reader::new(data);
    let mut msg = MmsMessage::default();
    let mut body_start: Option<usize> = None;

    while !r.eof() {
        let Some(b) = r.peek() else { break };
        if b < 0x80 {
            // Application specific textual header: "name" NUL value.
            let _name = r.text_string();
            if r.skip_value().is_none() {
                break;
            }
            continue;
        }
        r.pos += 1;
        match b {
            field::MESSAGE_TYPE => msg.message_type = r.u8().unwrap_or(0),
            field::TRANSACTION_ID => msg.transaction_id = r.text_string(),
            field::MMS_VERSION => msg.version = r.u8(),
            field::FROM => {
                let len = r.bounded_value_length().unwrap_or(0);
                let end = r.pos + len;
                let mut inner = Reader::new(&r.data[r.pos..end]);
                r.pos = end;
                match inner.u8() {
                    Some(0x80) => msg.from = inner.encoded_string(),
                    // 0x81 = insert-address-token (the MMSC fills it in)
                    _ => msg.from = None,
                }
            }
            field::TO => {
                if let Some(v) = r.encoded_string() {
                    msg.to.push(v);
                }
            }
            field::CC => {
                if let Some(v) = r.encoded_string() {
                    msg.cc.push(v);
                }
            }
            field::SUBJECT => msg.subject = r.encoded_string(),
            field::MESSAGE_ID => msg.message_id = r.text_string(),
            field::CONTENT_LOCATION => msg.content_location = r.text_string(),
            field::DATE => msg.date = r.long_int(),
            field::MESSAGE_SIZE => msg.message_size = r.integer_value(),
            field::MESSAGE_CLASS => {
                msg.message_class = match r.peek() {
                    Some(v) if v & 0x80 != 0 => {
                        r.pos += 1;
                        Some(
                            match v & 0x7F {
                                0x00 => "personal",
                                0x01 => "advertisement",
                                0x02 => "informational",
                                0x03 => "auto",
                                _ => "unknown",
                            }
                            .to_string(),
                        )
                    }
                    _ => r.text_string(),
                }
            }
            field::EXPIRY => {
                let len = r.bounded_value_length().unwrap_or(0);
                let end = r.pos + len;
                let mut inner = Reader::new(&r.data[r.pos..end]);
                r.pos = end;
                let _token = inner.u8();
                // Delta-seconds is an Integer-value, so anything up to 127
                // arrives as a short integer rather than a long one.
                msg.expiry = inner.integer_value();
            }
            // Kept as the raw well-known value: 0x80 is "Ok" for both.
            field::RESPONSE_STATUS => msg.response_status = r.u8(),
            field::RETRIEVE_STATUS => msg.retrieve_status = r.u8(),
            field::RESPONSE_TEXT | field::RETRIEVE_TEXT => msg.response_text = r.encoded_string(),
            field::CONTENT_TYPE => {
                let (ct, params) = r
                    .content_type()
                    .ok_or_else(|| anyhow!("malformed MMS Content-Type"))?;
                msg.content_type = Some(ct);
                msg.content_type_params = params;
                body_start = Some(r.pos);
                break; // Content-Type is always the last header.
            }
            _ => {
                if r.skip_value().is_none() {
                    break;
                }
            }
        }
    }

    if let Some(start) = body_start {
        let body = &data[start..];
        let ct = msg.content_type.clone().unwrap_or_default();
        msg.parts = if is_multipart(&ct) {
            decode_multipart(body)
        } else if !body.is_empty() {
            vec![MmsPart {
                content_type: ct,
                params: msg.content_type_params.clone(),
                data: body.to_vec(),
                ..Default::default()
            }]
        } else {
            Vec::new()
        };
    }

    Ok(msg)
}

fn is_multipart(content_type: &str) -> bool {
    content_type.starts_with("multipart/")
        || content_type.starts_with("application/vnd.wap.multipart")
}

fn decode_multipart(body: &[u8]) -> Vec<MmsPart> {
    let mut r = Reader::new(body);
    let Some(count) = r.uintvar() else { return Vec::new() };
    let mut parts = Vec::new();
    for _ in 0..count.min(256) {
        let Some(headers_len) = r.uintvar().and_then(|v| usize::try_from(v).ok()) else { break };
        let Some(data_len) = r.uintvar().and_then(|v| usize::try_from(v).ok()) else { break };
        let Some(header_bytes) = r.take(headers_len) else { break };
        let Some(data) = r.take(data_len) else { break };

        let mut hr = Reader::new(header_bytes);
        let (content_type, params) =
            hr.content_type().unwrap_or_else(|| ("application/octet-stream".into(), Vec::new()));

        let mut part = MmsPart {
            content_type,
            params,
            data: data.to_vec(),
            ..Default::default()
        };
        // Remaining part headers: Content-ID (0xC0), Content-Location (0x8E).
        while !hr.eof() {
            let Some(h) = hr.u8() else { break };
            match h {
                0xC0 => part.content_id = hr.text_string().map(|s| s.trim_matches(['<', '>']).to_string()),
                0x8E => part.content_location = hr.text_string(),
                _ => {
                    if h < 0x80 {
                        hr.pos -= 1;
                        let _ = hr.text_string();
                    }
                    if hr.skip_value().is_none() {
                        break;
                    }
                }
            }
        }
        parts.push(part);
    }
    parts
}

// ------------------------------------------------------------------ encode

fn push_field(out: &mut Vec<u8>, field: u8) {
    out.push(field);
}

/// Format a recipient the way MMSCs expect.
pub fn format_address(addr: &str) -> String {
    let addr = addr.trim();
    if addr.contains('@') || addr.contains("/TYPE=") {
        addr.to_string()
    } else {
        format!("{addr}/TYPE=PLMN")
    }
}

pub struct SendReq<'a> {
    pub transaction_id: &'a str,
    pub from: Option<&'a str>,
    pub to: &'a [String],
    pub subject: Option<&'a str>,
    pub parts: &'a [MmsPart],
    pub delivery_report: bool,
    pub read_report: bool,
}

/// Build an M-Send.req PDU ready to be POSTed to the MMSC.
pub fn encode_send_req(req: &SendReq<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024);

    push_field(&mut out, field::MESSAGE_TYPE);
    out.push(msg_type::SEND_REQ);

    push_field(&mut out, field::TRANSACTION_ID);
    wsp::write_text_string(&mut out, req.transaction_id);

    push_field(&mut out, field::MMS_VERSION);
    out.push(VERSION_1_2);

    push_field(&mut out, field::FROM);
    match req.from {
        Some(addr) => {
            let mut inner = vec![0x80]; // address-present-token
            wsp::write_encoded_string(&mut inner, &format_address(addr));
            wsp::write_value_length(&mut out, inner.len());
            out.extend_from_slice(&inner);
        }
        None => {
            // insert-address-token: the MMSC fills in our own number.
            wsp::write_value_length(&mut out, 1);
            out.push(0x81);
        }
    }

    for to in req.to {
        push_field(&mut out, field::TO);
        wsp::write_encoded_string(&mut out, &format_address(to));
    }

    if let Some(subject) = req.subject.filter(|s| !s.is_empty()) {
        push_field(&mut out, field::SUBJECT);
        wsp::write_encoded_string(&mut out, subject);
    }

    push_field(&mut out, field::MESSAGE_CLASS);
    out.push(0x80); // personal

    push_field(&mut out, field::DELIVERY_REPORT);
    out.push(if req.delivery_report { 0x80 } else { 0x81 });
    push_field(&mut out, field::READ_REPORT);
    out.push(if req.read_report { 0x80 } else { 0x81 });

    // Content-Type must be the last header.
    push_field(&mut out, field::CONTENT_TYPE);
    let mut ct = Vec::new();
    if req.parts.len() > 1 {
        // application/vnd.wap.multipart.related with a SMIL presentation part
        // is what every handset expects for a picture + text message.
        let start_cid = req.parts.first().and_then(|p| p.content_id.clone());
        ct.push(0x80 | 0x33); // application/vnd.wap.multipart.related
        ct.push(0x80 | 0x09); // parameter: type
        wsp::write_text_string(&mut ct, "application/smil");
        if let Some(cid) = start_cid {
            ct.push(0x80 | 0x19); // parameter: start
            wsp::write_text_string(&mut ct, &format!("<{cid}>"));
        }
        wsp::write_value_length(&mut out, ct.len());
        out.extend_from_slice(&ct);
    } else {
        ct.push(0x80 | 0x23); // application/vnd.wap.multipart.mixed
        wsp::write_value_length(&mut out, ct.len());
        out.extend_from_slice(&ct);
    }

    encode_multipart(&mut out, req.parts);
    out
}

fn encode_multipart(out: &mut Vec<u8>, parts: &[MmsPart]) {
    wsp::write_uintvar(out, parts.len() as u64);
    for part in parts {
        let mut headers = Vec::new();
        // Content-Type (well-known when possible, otherwise textual) with a
        // charset for text parts and a name for everything else.
        let mut ct = Vec::new();
        match wsp::content_type_code(&part.content_type) {
            Some(code) => ct.push(0x80 | code),
            None => wsp::write_text_string(&mut ct, &part.content_type),
        }
        if part.is_text() {
            ct.push(0x80 | 0x01); // charset
            wsp::write_integer_value(&mut ct, wsp::CHARSET_UTF8);
        }
        if let Some(name) = part.name() {
            ct.push(0x80 | 0x17); // name
            wsp::write_text_string(&mut ct, &name);
        }
        wsp::write_value_length(&mut headers, ct.len());
        headers.extend_from_slice(&ct);

        if let Some(cid) = &part.content_id {
            headers.push(0xC0); // Content-ID
            wsp::write_text_string(&mut headers, &format!("<{cid}>"));
        }
        if let Some(loc) = &part.content_location {
            headers.push(0x8E); // Content-Location
            wsp::write_text_string(&mut headers, loc);
        }

        wsp::write_uintvar(out, headers.len() as u64);
        wsp::write_uintvar(out, part.data.len() as u64);
        out.extend_from_slice(&headers);
        out.extend_from_slice(&part.data);
    }
}

/// M-NotifyResp.ind: tell the MMSC what happened to a notification.
/// status: 0x80 expired, 0x81 retrieved, 0x82 rejected, 0x83 deferred,
/// 0x84 unrecognised.
pub fn encode_notify_resp_ind(transaction_id: &str, status: u8, report_allowed: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    push_field(&mut out, field::MESSAGE_TYPE);
    out.push(msg_type::NOTIFYRESP_IND);
    push_field(&mut out, field::TRANSACTION_ID);
    wsp::write_text_string(&mut out, transaction_id);
    push_field(&mut out, field::MMS_VERSION);
    out.push(VERSION_1_2);
    push_field(&mut out, field::STATUS);
    out.push(status);
    push_field(&mut out, field::REPORT_ALLOWED);
    out.push(if report_allowed { 0x80 } else { 0x81 });
    out
}

/// M-Acknowledge.ind, sent after a successful retrieval.
pub fn encode_acknowledge_ind(transaction_id: &str, report_allowed: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    push_field(&mut out, field::MESSAGE_TYPE);
    out.push(msg_type::ACKNOWLEDGE_IND);
    push_field(&mut out, field::TRANSACTION_ID);
    wsp::write_text_string(&mut out, transaction_id);
    push_field(&mut out, field::MMS_VERSION);
    out.push(VERSION_1_2);
    push_field(&mut out, field::REPORT_ALLOWED);
    out.push(if report_allowed { 0x80 } else { 0x81 });
    out
}

/// Minimal SMIL presentation so handsets render text + image sensibly.
pub fn build_smil(parts: &[MmsPart]) -> String {
    let mut regions = String::new();
    let mut par = String::new();
    for part in parts {
        let Some(cid) = &part.content_id else { continue };
        if part.content_type.starts_with("image/") {
            par.push_str(&format!("      <img src=\"cid:{cid}\" region=\"Image\"/>\n"));
        } else if part.content_type.starts_with("text/") {
            par.push_str(&format!("      <text src=\"cid:{cid}\" region=\"Text\"/>\n"));
        } else if part.content_type.starts_with("audio/") || part.content_type.starts_with("video/")
        {
            par.push_str(&format!("      <video src=\"cid:{cid}\" region=\"Image\"/>\n"));
        }
    }
    regions.push_str("      <region id=\"Image\" top=\"0%\" left=\"0%\" height=\"80%\" width=\"100%\" fit=\"meet\"/>\n");
    regions.push_str("      <region id=\"Text\" top=\"80%\" left=\"0%\" height=\"20%\" width=\"100%\" fit=\"scroll\"/>\n");
    format!(
        "<smil>\n  <head>\n    <layout>\n      <root-layout width=\"320px\" height=\"480px\"/>\n{regions}    </layout>\n  </head>\n  <body>\n    <par dur=\"8000ms\">\n{par}    </par>\n  </body>\n</smil>\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_round_trip() {
        // Hand-built M-Notification.ind.
        let mut pdu = Vec::new();
        pdu.push(field::MESSAGE_TYPE);
        pdu.push(msg_type::NOTIFICATION_IND);
        pdu.push(field::TRANSACTION_ID);
        wsp::write_text_string(&mut pdu, "TID123");
        pdu.push(field::MMS_VERSION);
        pdu.push(VERSION_1_2);
        pdu.push(field::FROM);
        let mut inner = vec![0x80];
        wsp::write_text_string(&mut inner, "+821012345678/TYPE=PLMN");
        wsp::write_value_length(&mut pdu, inner.len());
        pdu.extend_from_slice(&inner);
        pdu.push(field::SUBJECT);
        wsp::write_encoded_string(&mut pdu, "사진");
        pdu.push(field::MESSAGE_SIZE);
        wsp::write_long_int(&mut pdu, 45678);
        pdu.push(field::CONTENT_LOCATION);
        wsp::write_text_string(&mut pdu, "http://mmsc.example/msg?id=1");

        let msg = decode(&pdu).unwrap();
        assert_eq!(msg.message_type, msg_type::NOTIFICATION_IND);
        assert_eq!(msg.transaction_id.as_deref(), Some("TID123"));
        assert_eq!(msg.sender().as_deref(), Some("+821012345678"));
        assert_eq!(msg.subject.as_deref(), Some("사진"));
        assert_eq!(msg.message_size, Some(45678));
        assert_eq!(msg.content_location.as_deref(), Some("http://mmsc.example/msg?id=1"));
    }

    #[test]
    fn send_conf_status_is_not_masked() {
        // A carrier "Ok" is 0x80; masking the well-known bit off turned every
        // success into a failure.
        let mut pdu = vec![field::MESSAGE_TYPE, msg_type::SEND_CONF, field::TRANSACTION_ID];
        wsp::write_text_string(&mut pdu, "T1");
        pdu.extend_from_slice(&[field::MMS_VERSION, VERSION_1_2, field::RESPONSE_STATUS, 0x80]);
        pdu.push(field::MESSAGE_ID);
        wsp::write_text_string(&mut pdu, "msg-42");

        let msg = decode(&pdu).unwrap();
        assert_eq!(msg.message_type, msg_type::SEND_CONF);
        assert_eq!(msg.response_status, Some(0x80));
        assert_eq!(msg.message_id.as_deref(), Some("msg-42"));
    }

    #[test]
    fn send_req_is_decodable() {
        let mut text = MmsPart::new("text/plain", "hello".as_bytes().to_vec());
        text.content_id = Some("text0".into());
        let mut image = MmsPart::new("image/jpeg", vec![0xFF, 0xD8, 0xFF]);
        image.content_id = Some("img0".into());
        image.params.push(("name".into(), "p.jpg".into()));
        let parts = vec![text, image];

        let to = vec!["+821012345678".to_string()];
        let pdu = encode_send_req(&SendReq {
            transaction_id: "T1",
            from: None,
            to: &to,
            subject: Some("제목"),
            parts: &parts,
            delivery_report: false,
            read_report: false,
        });

        let decoded = decode(&pdu).unwrap();
        assert_eq!(decoded.message_type, msg_type::SEND_REQ);
        assert_eq!(decoded.subject.as_deref(), Some("제목"));
        assert_eq!(decoded.to, vec!["+821012345678/TYPE=PLMN".to_string()]);
        assert_eq!(decoded.parts.len(), 2);
        assert_eq!(decoded.parts[0].content_type, "text/plain");
        assert_eq!(decoded.parts[0].text().as_deref(), Some("hello"));
        assert_eq!(decoded.parts[1].content_type, "image/jpeg");
        assert_eq!(decoded.parts[1].data, vec![0xFF, 0xD8, 0xFF]);
    }
}

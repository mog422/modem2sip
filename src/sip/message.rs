//! A small, dependency-free SIP message parser/serialiser.
//!
//! Only the subset needed by the gateway is modelled: REGISTER, INVITE, ACK,
//! CANCEL, BYE, OPTIONS, INFO and MESSAGE over UDP.

use std::fmt::Write as _;

use super::uri::{NameAddr, Uri};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Invite,
    Ack,
    Bye,
    Cancel,
    Register,
    Options,
    Info,
    Message,
    Notify,
    Subscribe,
    Update,
    Prack,
    Refer,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Invite => "INVITE",
            Method::Ack => "ACK",
            Method::Bye => "BYE",
            Method::Cancel => "CANCEL",
            Method::Register => "REGISTER",
            Method::Options => "OPTIONS",
            Method::Info => "INFO",
            Method::Message => "MESSAGE",
            Method::Notify => "NOTIFY",
            Method::Subscribe => "SUBSCRIBE",
            Method::Update => "UPDATE",
            Method::Prack => "PRACK",
            Method::Refer => "REFER",
        }
    }

    pub fn parse(s: &str) -> Option<Method> {
        Some(match s.to_ascii_uppercase().as_str() {
            "INVITE" => Method::Invite,
            "ACK" => Method::Ack,
            "BYE" => Method::Bye,
            "CANCEL" => Method::Cancel,
            "REGISTER" => Method::Register,
            "OPTIONS" => Method::Options,
            "INFO" => Method::Info,
            "MESSAGE" => Method::Message,
            "NOTIFY" => Method::Notify,
            "SUBSCRIBE" => Method::Subscribe,
            "UPDATE" => Method::Update,
            "PRACK" => Method::Prack,
            "REFER" => Method::Refer,
            _ => return None,
        })
    }
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Headers(pub Vec<(String, String)>);

/// Compact header forms (RFC 3261 §7.3.3 + RFC 3428).
fn canonical(name: &str) -> String {
    let lower = name.trim().to_ascii_lowercase();
    let full = match lower.as_str() {
        "i" => "call-id",
        "m" => "contact",
        "f" => "from",
        "t" => "to",
        "v" => "via",
        "c" => "content-type",
        "l" => "content-length",
        "s" => "subject",
        "k" => "supported",
        "e" => "content-encoding",
        "o" => "event",
        "r" => "refer-to",
        "x" => "session-expires",
        other => other,
    };
    full.to_string()
}

/// Pretty (canonical) casing for wire output.
fn pretty(name: &str) -> String {
    let canon = canonical(name);
    match canon.as_str() {
        "call-id" => return "Call-ID".into(),
        "cseq" => return "CSeq".into(),
        "www-authenticate" => return "WWW-Authenticate".into(),
        "mime-version" => return "MIME-Version".into(),
        _ => {}
    }
    let mut out = String::with_capacity(canon.len());
    let mut upper = true;
    for ch in canon.chars() {
        if upper {
            out.extend(ch.to_uppercase());
            upper = false;
        } else {
            out.push(ch);
        }
        if ch == '-' {
            upper = true;
        }
    }
    out
}

impl Headers {
    pub fn get(&self, name: &str) -> Option<&str> {
        let n = canonical(name);
        self.0.iter().find(|(k, _)| *k == n).map(|(_, v)| v.as_str())
    }

    pub fn get_all(&self, name: &str) -> Vec<&str> {
        let n = canonical(name);
        self.0.iter().filter(|(k, _)| *k == n).map(|(_, v)| v.as_str()).collect()
    }

    pub fn set(&mut self, name: &str, value: impl Into<String>) {
        let n = canonical(name);
        self.0.retain(|(k, _)| *k != n);
        self.0.push((n, value.into()));
    }

    pub fn push(&mut self, name: &str, value: impl Into<String>) {
        self.0.push((canonical(name), value.into()));
    }

    /// Replace the first occurrence in place, keeping position and any further
    /// occurrences.  Required for Via: a response must carry the request's
    /// Via list unchanged apart from the topmost entry.
    pub fn replace_first(&mut self, name: &str, value: impl Into<String>) {
        let n = canonical(name);
        match self.0.iter_mut().find(|(k, _)| *k == n) {
            Some(slot) => slot.1 = value.into(),
            None => self.0.push((n, value.into())),
        }
    }

    pub fn remove(&mut self, name: &str) {
        let n = canonical(name);
        self.0.retain(|(k, _)| *k != n);
    }

    pub fn from(&self) -> Option<NameAddr> {
        self.get("from").and_then(NameAddr::parse)
    }
    pub fn to(&self) -> Option<NameAddr> {
        self.get("to").and_then(NameAddr::parse)
    }
    pub fn contact(&self) -> Option<NameAddr> {
        self.get("contact").and_then(NameAddr::parse)
    }
    pub fn call_id(&self) -> Option<&str> {
        self.get("call-id")
    }
    pub fn cseq(&self) -> Option<(u32, Method)> {
        let raw = self.get("cseq")?;
        let mut it = raw.split_whitespace();
        let num = it.next()?.parse().ok()?;
        let m = Method::parse(it.next()?)?;
        Some((num, m))
    }
    pub fn top_via(&self) -> Option<Via> {
        self.get("via").and_then(Via::parse)
    }
    pub fn content_type(&self) -> Option<&str> {
        self.get("content-type")
    }
}

#[derive(Debug, Clone)]
pub struct Via {
    pub protocol: String, // "SIP/2.0/UDP"
    pub host: String,
    pub port: Option<u16>,
    pub params: Vec<(String, Option<String>)>,
}

impl Via {
    pub fn parse(s: &str) -> Option<Via> {
        // Only the first value of a comma separated list matters for us.
        let s = s.split(',').next()?.trim();
        let (proto, rest) = s.split_once(char::is_whitespace)?;
        let rest = rest.trim();
        let (hostport, params_str) = match rest.split_once(';') {
            Some((h, p)) => (h.trim(), Some(p)),
            None => (rest, None),
        };
        let (host, port) = super::uri::split_host_port(hostport);
        let mut params = Vec::new();
        if let Some(ps) = params_str {
            for p in ps.split(';') {
                let p = p.trim();
                if p.is_empty() {
                    continue;
                }
                match p.split_once('=') {
                    Some((k, v)) => {
                        params.push((k.to_ascii_lowercase(), Some(v.to_string())))
                    }
                    None => params.push((p.to_ascii_lowercase(), None)),
                }
            }
        }
        Some(Via { protocol: proto.to_string(), host, port, params })
    }

    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| v.as_deref())
    }

    pub fn branch(&self) -> Option<&str> {
        self.param("branch")
    }

    pub fn has_param(&self, name: &str) -> bool {
        self.params.iter().any(|(k, _)| k.eq_ignore_ascii_case(name))
    }

    pub fn set_param(&mut self, name: &str, value: Option<&str>) {
        self.params.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.params.push((name.to_string(), value.map(|v| v.to_string())));
    }
}

impl std::fmt::Display for Via {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.protocol, self.host)?;
        if let Some(p) = self.port {
            write!(f, ":{p}")?;
        }
        for (k, v) in &self.params {
            match v {
                Some(v) => write!(f, ";{k}={v}")?,
                None => write!(f, ";{k}")?,
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub method: Method,
    pub uri: Uri,
    pub headers: Headers,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub code: u16,
    pub reason: String,
    pub headers: Headers,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum Message {
    Request(Request),
    Response(Response),
}

impl Message {
    pub fn parse(buf: &[u8]) -> Option<Message> {
        let split = find_body_start(buf)?;
        let head = std::str::from_utf8(&buf[..split.0]).ok()?;
        let body = buf[split.1..].to_vec();

        let head = unfold(head);
        let mut lines = head.split("\r\n").filter(|l| !l.is_empty());
        let start = lines.next()?;

        let mut headers = Headers::default();
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                headers.0.push((canonical(name), value.trim().to_string()));
            }
        }

        // Trim the body to Content-Length when present; UDP datagrams may
        // carry padding, and some UAs under-report.
        let body = match headers.get("content-length").and_then(|v| v.trim().parse::<usize>().ok()) {
            Some(len) if len <= body.len() => body[..len].to_vec(),
            _ => body,
        };

        if start.starts_with("SIP/") {
            let mut parts = start.splitn(3, ' ');
            let _ver = parts.next()?;
            let code: u16 = parts.next()?.parse().ok()?;
            let reason = parts.next().unwrap_or("").to_string();
            Some(Message::Response(Response { code, reason, headers, body }))
        } else {
            let mut parts = start.split_whitespace();
            let method = Method::parse(parts.next()?)?;
            let uri = Uri::parse(parts.next()?)?;
            Some(Message::Request(Request { method, uri, headers, body }))
        }
    }
}

/// Returns (end-of-headers, start-of-body) handling both CRLF and LF.
fn find_body_start(buf: &[u8]) -> Option<(usize, usize)> {
    if let Some(pos) = find_sub(buf, b"\r\n\r\n") {
        return Some((pos, pos + 4));
    }
    if let Some(pos) = find_sub(buf, b"\n\n") {
        return Some((pos, pos + 2));
    }
    // Headers only, no trailing blank line.
    Some((buf.len(), buf.len()))
}

fn find_sub(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Normalise line endings and unfold continuation lines (leading WSP).
fn unfold(head: &str) -> String {
    let normalised = head.replace("\r\n", "\n");
    let mut out = String::with_capacity(normalised.len());
    for (i, line) in normalised.split('\n').enumerate() {
        if i > 0 {
            if line.starts_with(' ') || line.starts_with('\t') {
                out.push(' ');
                out.push_str(line.trim_start());
                continue;
            }
            out.push_str("\r\n");
        }
        out.push_str(line);
    }
    out
}

/// Strip anything that would end a header line early.
///
/// Header values are assembled from numbers, subjects and addresses the
/// network supplied; a CR or LF in one of them would close the header and let
/// whoever sent it write the rest of the message.  Nothing legitimate needs a
/// control character here, so they are dropped rather than escaped.
fn header_safe(value: &str) -> std::borrow::Cow<'_, str> {
    if value.bytes().any(|b| b < 0x20 || b == 0x7F) {
        std::borrow::Cow::Owned(value.chars().filter(|c| !c.is_control()).collect())
    } else {
        std::borrow::Cow::Borrowed(value)
    }
}

fn serialize(start_line: &str, headers: &Headers, body: &[u8]) -> Vec<u8> {
    let mut head = String::with_capacity(512);
    let _ = writeln!(head, "{}\r", header_safe(start_line));
    let mut wrote_len = false;
    for (k, v) in &headers.0 {
        if k == "content-length" {
            wrote_len = true;
            let _ = writeln!(head, "Content-Length: {}\r", body.len());
        } else {
            let _ = writeln!(head, "{}: {}\r", pretty(k), header_safe(v));
        }
    }
    if !wrote_len {
        let _ = writeln!(head, "Content-Length: {}\r", body.len());
    }
    head.push_str("\r\n");
    let mut out = head.into_bytes();
    out.extend_from_slice(body);
    out
}

impl Request {
    pub fn new(method: Method, uri: Uri) -> Self {
        Self { method, uri, headers: Headers::default(), body: Vec::new() }
    }

    pub fn encode(&self) -> Vec<u8> {
        serialize(&format!("{} {} SIP/2.0", self.method, self.uri), &self.headers, &self.body)
    }

    pub fn body_str(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// Build a response that mirrors the mandatory headers of this request.
    pub fn reply(&self, code: u16, reason: &str) -> Response {
        let mut headers = Headers::default();
        for via in self.headers.get_all("via") {
            headers.push("via", via.to_string());
        }
        for rr in self.headers.get_all("record-route") {
            headers.push("record-route", rr.to_string());
        }
        if let Some(f) = self.headers.get("from") {
            headers.set("from", f.to_string());
        }
        if let Some(t) = self.headers.get("to") {
            headers.set("to", t.to_string());
        }
        if let Some(c) = self.headers.get("call-id") {
            headers.set("call-id", c.to_string());
        }
        if let Some(c) = self.headers.get("cseq") {
            headers.set("cseq", c.to_string());
        }
        Response { code, reason: reason.to_string(), headers, body: Vec::new() }
    }
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        serialize(&format!("SIP/2.0 {} {}", self.code, self.reason), &self.headers, &self.body)
    }

    pub fn is_provisional(&self) -> bool {
        (100..200).contains(&self.code)
    }
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.code)
    }
    pub fn body_str(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

pub fn reason_phrase(code: u16) -> &'static str {
    match code {
        100 => "Trying",
        180 => "Ringing",
        183 => "Session Progress",
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        415 => "Unsupported Media Type",
        420 => "Bad Extension",
        423 => "Interval Too Brief",
        480 => "Temporarily Unavailable",
        481 => "Call/Transaction Does Not Exist",
        486 => "Busy Here",
        487 => "Request Terminated",
        488 => "Not Acceptable Here",
        491 => "Request Pending",
        500 => "Server Internal Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Server Time-out",
        600 => "Busy Everywhere",
        603 => "Decline",
        604 => "Does Not Exist Anywhere",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_request() {
        let raw = b"INVITE sip:+8210@gw SIP/2.0\r\n\
                    Via: SIP/2.0/UDP 10.0.0.1:5060;branch=z9hG4bK1\r\n\
                    From: <sip:alice@gw>;tag=a\r\n\
                    To: <sip:+8210@gw>\r\n\
                    Call-ID: abc\r\n\
                    CSeq: 1 INVITE\r\n\
                    Content-Type: application/sdp\r\n\
                    Content-Length: 3\r\n\r\nv=0";
        let msg = Message::parse(raw).unwrap();
        let Message::Request(req) = msg else { panic!("expected request") };
        assert_eq!(req.method, Method::Invite);
        assert_eq!(req.headers.call_id(), Some("abc"));
        assert_eq!(req.body, b"v=0");
        assert_eq!(req.headers.top_via().unwrap().branch(), Some("z9hG4bK1"));
        let encoded = req.encode();
        assert!(String::from_utf8_lossy(&encoded).contains("Call-ID: abc"));
    }

    #[test]
    fn compact_forms_and_folding() {
        let raw = b"MESSAGE sip:gw SIP/2.0\r\ni: xyz\r\nf: <sip:a@b>;tag=1\r\n\
                    Subject: hello\r\n world\r\nl: 2\r\n\r\nhi";
        let Message::Request(req) = Message::parse(raw).unwrap() else { panic!() };
        assert_eq!(req.headers.call_id(), Some("xyz"));
        assert_eq!(req.headers.get("subject"), Some("hello world"));
        assert_eq!(req.body, b"hi");
    }

    /// Header values are built from numbers and subjects the mobile network
    /// supplied; a CRLF in one of them must not be able to close the header
    /// and write the rest of the message.
    #[test]
    fn a_header_value_cannot_end_its_own_line() {
        let mut req = Request::new(Method::Message, Uri::parse("sip:gw").unwrap());
        req.headers.set("from", "<sip:a@h>\r\nContact: <sip:evil@h>");
        req.headers.set("subject", "hi\nthere");
        let text = String::from_utf8(req.encode()).unwrap();
        // The injected text survives as part of the value, which is harmless;
        // what must not survive is it starting a line of its own.
        assert!(
            !text.lines().any(|l| l.starts_with("Contact:")),
            "injected header survived: {text}"
        );
        assert!(text.contains("From: <sip:a@h>Contact: <sip:evil@h>\r\n"));
        assert!(text.contains("Subject: hithere\r\n"));
    }
}

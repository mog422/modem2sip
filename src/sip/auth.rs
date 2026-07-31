//! HTTP digest authentication (RFC 2617, MD5 / MD5-sess, qop=auth) for SIP.

use std::collections::HashMap;

use md5::{Digest, Md5};
use rand::Rng;

pub fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    let out = h.finalize();
    out.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn random_hex(bytes: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..bytes).map(|_| format!("{:02x}", rng.gen::<u8>())).collect()
}

#[derive(Debug, Clone, Default)]
pub struct Challenge {
    pub realm: String,
    pub nonce: String,
    pub opaque: Option<String>,
    pub qop: Option<String>,
    pub algorithm: Option<String>,
}

impl Challenge {
    /// Parse a WWW-Authenticate / Proxy-Authenticate header value.
    pub fn parse(value: &str) -> Option<Challenge> {
        let value = value.trim();
        let (_scheme, rest) = value.split_once(char::is_whitespace)?;
        let params = parse_params(rest);
        Some(Challenge {
            realm: params.get("realm").cloned().unwrap_or_default(),
            nonce: params.get("nonce").cloned().unwrap_or_default(),
            opaque: params.get("opaque").cloned(),
            qop: params.get("qop").cloned(),
            algorithm: params.get("algorithm").cloned(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub username: String,
    pub realm: String,
    pub nonce: String,
    pub uri: String,
    pub response: String,
    pub algorithm: Option<String>,
    pub cnonce: Option<String>,
    pub nc: Option<String>,
    pub qop: Option<String>,
    pub opaque: Option<String>,
}

impl Credentials {
    /// Parse an Authorization / Proxy-Authorization header value.
    pub fn parse(value: &str) -> Option<Credentials> {
        let value = value.trim();
        let (scheme, rest) = value.split_once(char::is_whitespace)?;
        if !scheme.eq_ignore_ascii_case("Digest") {
            return None;
        }
        let p = parse_params(rest);
        Some(Credentials {
            username: p.get("username").cloned().unwrap_or_default(),
            realm: p.get("realm").cloned().unwrap_or_default(),
            nonce: p.get("nonce").cloned().unwrap_or_default(),
            uri: p.get("uri").cloned().unwrap_or_default(),
            response: p.get("response").cloned().unwrap_or_default(),
            algorithm: p.get("algorithm").cloned(),
            cnonce: p.get("cnonce").cloned(),
            nc: p.get("nc").cloned(),
            qop: p.get("qop").cloned(),
            opaque: p.get("opaque").cloned(),
        })
    }

    pub fn to_header(&self) -> String {
        let mut s = format!(
            "Digest username=\"{}\", realm=\"{}\", nonce=\"{}\", uri=\"{}\", response=\"{}\"",
            self.username, self.realm, self.nonce, self.uri, self.response
        );
        if let Some(a) = &self.algorithm {
            s.push_str(&format!(", algorithm={a}"));
        }
        if let Some(q) = &self.qop {
            s.push_str(&format!(", qop={q}"));
            if let Some(nc) = &self.nc {
                s.push_str(&format!(", nc={nc}"));
            }
            if let Some(cn) = &self.cnonce {
                s.push_str(&format!(", cnonce=\"{cn}\""));
            }
        }
        if let Some(o) = &self.opaque {
            s.push_str(&format!(", opaque=\"{o}\""));
        }
        s
    }
}

fn parse_params(s: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i] == b',' || bytes[i].is_ascii_whitespace()) {
            i += 1;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && bytes[i] != b',' {
            i += 1;
        }
        let key = s[key_start..i].trim().to_ascii_lowercase();
        if key.is_empty() {
            break;
        }
        if i >= bytes.len() || bytes[i] == b',' {
            out.insert(key, String::new());
            continue;
        }
        i += 1; // '='
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let value = if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            let v = s[start..i].to_string();
            if i < bytes.len() {
                i += 1;
            }
            v
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b',' {
                i += 1;
            }
            s[start..i].trim().to_string()
        };
        out.insert(key, value);
    }
    out
}

/// Compute the digest response value.
#[allow(clippy::too_many_arguments)]
pub fn digest_response(
    username: &str,
    password: &str,
    realm: &str,
    nonce: &str,
    method: &str,
    uri: &str,
    qop: Option<&str>,
    cnonce: Option<&str>,
    nc: Option<&str>,
    algorithm: Option<&str>,
    body: &[u8],
) -> String {
    let mut ha1 = md5_hex(format!("{username}:{realm}:{password}").as_bytes());
    if algorithm.map(|a| a.eq_ignore_ascii_case("MD5-sess")).unwrap_or(false) {
        ha1 = md5_hex(
            format!("{ha1}:{}:{}", nonce, cnonce.unwrap_or_default()).as_bytes(),
        );
    }
    let ha2 = match qop {
        Some(q) if q.eq_ignore_ascii_case("auth-int") => {
            let hbody = md5_hex(body);
            md5_hex(format!("{method}:{uri}:{hbody}").as_bytes())
        }
        _ => md5_hex(format!("{method}:{uri}").as_bytes()),
    };
    match qop {
        Some(q) if !q.is_empty() => md5_hex(
            format!(
                "{ha1}:{nonce}:{}:{}:{}:{ha2}",
                nc.unwrap_or("00000001"),
                cnonce.unwrap_or_default(),
                q
            )
            .as_bytes(),
        ),
        _ => md5_hex(format!("{ha1}:{nonce}:{ha2}").as_bytes()),
    }
}

/// Client side: answer a challenge.
pub fn answer(
    challenge: &Challenge,
    username: &str,
    password: &str,
    method: &str,
    uri: &str,
    body: &[u8],
    nc_value: u32,
) -> Credentials {
    // Pick the first supported qop offered.
    let qop = challenge.qop.as_deref().and_then(|q| {
        q.split(',')
            .map(|s| s.trim())
            .find(|s| s.eq_ignore_ascii_case("auth"))
            .map(|s| s.to_string())
    });
    let cnonce = qop.as_ref().map(|_| random_hex(8));
    let nc = qop.as_ref().map(|_| format!("{nc_value:08x}"));
    let response = digest_response(
        username,
        password,
        &challenge.realm,
        &challenge.nonce,
        method,
        uri,
        qop.as_deref(),
        cnonce.as_deref(),
        nc.as_deref(),
        challenge.algorithm.as_deref(),
        body,
    );
    Credentials {
        username: username.to_string(),
        realm: challenge.realm.clone(),
        nonce: challenge.nonce.clone(),
        uri: uri.to_string(),
        response,
        algorithm: challenge.algorithm.clone(),
        cnonce,
        nc,
        qop,
        opaque: challenge.opaque.clone(),
    }
}

/// Server side: a nonce issuer with a simple validity window.
pub struct NonceFactory {
    secret: String,
}

impl NonceFactory {
    pub fn new() -> Self {
        Self { secret: random_hex(16) }
    }

    pub fn issue(&self) -> String {
        let ts = chrono::Local::now().timestamp();
        let digest = md5_hex(format!("{ts}:{}", self.secret).as_bytes());
        format!("{ts}:{digest}")
    }

    pub fn is_valid(&self, nonce: &str, max_age_secs: i64) -> bool {
        let Some((ts, digest)) = nonce.split_once(':') else { return false };
        let Ok(ts_num) = ts.parse::<i64>() else { return false };
        if md5_hex(format!("{ts_num}:{}", self.secret).as_bytes()) != digest {
            return false;
        }
        let age = chrono::Local::now().timestamp() - ts_num;
        (0..=max_age_secs).contains(&age)
    }
}

impl Default for NonceFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// Server side: verify credentials sent by a UA.
pub fn verify(creds: &Credentials, password: &str, method: &str, body: &[u8]) -> bool {
    let expected = digest_response(
        &creds.username,
        password,
        &creds.realm,
        &creds.nonce,
        method,
        &creds.uri,
        creds.qop.as_deref(),
        creds.cnonce.as_deref(),
        creds.nc.as_deref(),
        creds.algorithm.as_deref(),
        body,
    );
    // Constant-time-ish comparison; these are hex strings of equal length.
    expected.len() == creds.response.len()
        && expected
            .bytes()
            .zip(creds.response.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc2617_example() {
        // Classic RFC 2617 §3.5 vector (HTTP, but the algorithm is identical).
        let r = digest_response(
            "Mufasa",
            "Circle Of Life",
            "testrealm@host.com",
            "dcd98b7102dd2f0e8b11d0f600bfb0c093",
            "GET",
            "/dir/index.html",
            Some("auth"),
            Some("0a4f113b"),
            Some("00000001"),
            None,
            b"",
        );
        assert_eq!(r, "6629fae49393a05397450978507c4ef1");
    }

    #[test]
    fn parse_challenge() {
        let c = Challenge::parse(
            "Digest realm=\"asterisk\", nonce=\"1234\", qop=\"auth\", algorithm=MD5",
        )
        .unwrap();
        assert_eq!(c.realm, "asterisk");
        assert_eq!(c.qop.as_deref(), Some("auth"));
        assert_eq!(c.algorithm.as_deref(), Some("MD5"));
    }

    /// Answering our own challenge has to verify, or nothing can authenticate
    /// at all - and the response must be tied to the method and the URI, so
    /// that one captured header does not authorise a different request.
    #[test]
    fn our_own_challenge_round_trips_and_is_bound_to_the_request() {
        let factory = NonceFactory::new();
        let challenge = Challenge::parse(&format!(
            "Digest realm=\"gw.local\", nonce=\"{}\", algorithm=MD5, qop=\"auth\"",
            factory.issue()
        ))
        .unwrap();
        let creds =
            answer(&challenge, "gateway", "s3cret", "MESSAGE", "sip:+8210@gw.local", b"hi", 1);

        assert!(factory.is_valid(&creds.nonce, 300));
        assert_eq!(creds.nc.as_deref(), Some("00000001"));
        assert!(verify(&creds, "s3cret", "MESSAGE", b"hi"));
        // Same credentials, different method or password: no.
        assert!(!verify(&creds, "s3cret", "INVITE", b"hi"));
        assert!(!verify(&creds, "wrong", "MESSAGE", b"hi"));

        // And the header survives a round trip through the wire format.
        let reparsed = Credentials::parse(&creds.to_header()).unwrap();
        assert_eq!(reparsed.uri, "sip:+8210@gw.local");
        assert_eq!(reparsed.nc.as_deref(), Some("00000001"));
        assert!(verify(&reparsed, "s3cret", "MESSAGE", b"hi"));
    }

    #[test]
    fn a_nonce_we_did_not_issue_is_rejected() {
        let factory = NonceFactory::new();
        assert!(!factory.is_valid("1700000000:deadbeef", 300));
        assert!(!factory.is_valid("not-a-nonce", 300));
        assert!(!factory.is_valid("", 300));
        // Another instance has its own secret.
        assert!(!NonceFactory::new().is_valid(&factory.issue(), 300));
    }
}

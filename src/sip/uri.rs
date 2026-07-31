//! Minimal SIP URI and name-addr handling.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Uri {
    pub scheme: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub host: String,
    pub port: Option<u16>,
    pub params: Vec<(String, Option<String>)>,
    pub headers: Vec<(String, String)>,
}

impl Uri {
    pub fn new(user: Option<&str>, host: &str, port: Option<u16>) -> Self {
        Self {
            scheme: "sip".into(),
            user: user.map(|u| u.to_string()),
            password: None,
            host: host.to_string(),
            port,
            params: Vec::new(),
            headers: Vec::new(),
        }
    }

    pub fn parse(s: &str) -> Option<Uri> {
        let s = s.trim();
        let (scheme, rest) = s.split_once(':')?;
        let scheme = scheme.trim().to_ascii_lowercase();
        if scheme != "sip" && scheme != "sips" && scheme != "tel" {
            return None;
        }

        // headers first (after '?')
        let (rest, headers_str) = match rest.split_once('?') {
            Some((a, b)) => (a, Some(b)),
            None => (rest, None),
        };
        // params (after ';')
        let (userinfo_host, params_str) = match rest.split_once(';') {
            Some((a, b)) => (a, Some(b)),
            None => (rest, None),
        };

        let (userinfo, hostport) = match userinfo_host.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, userinfo_host),
        };

        let (user, password) = match userinfo {
            Some(ui) => match ui.split_once(':') {
                Some((u, p)) => (Some(u.to_string()), Some(p.to_string())),
                None => (Some(ui.to_string()), None),
            },
            None => (None, None),
        };

        // "tel:+821012345678" has no host part at all - the whole thing is
        // the number, which is what every caller of `user` is after.
        let (user, host, port) = if scheme == "tel" {
            (Some(hostport.trim().to_string()), String::new(), None)
        } else {
            let (host, port) = split_host_port(hostport);
            (user, host, port)
        };

        let mut params = Vec::new();
        if let Some(ps) = params_str {
            for p in ps.split(';').filter(|p| !p.is_empty()) {
                match p.split_once('=') {
                    Some((k, v)) => params.push((k.to_ascii_lowercase(), Some(v.to_string()))),
                    None => params.push((p.to_ascii_lowercase(), None)),
                }
            }
        }
        let mut hdrs = Vec::new();
        if let Some(hs) = headers_str {
            for h in hs.split('&').filter(|h| !h.is_empty()) {
                if let Some((k, v)) = h.split_once('=') {
                    hdrs.push((k.to_string(), v.to_string()));
                }
            }
        }

        Some(Uri { scheme, user, password, host, port, params, headers: hdrs })
    }

    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| v.as_deref())
    }

    pub fn has_param(&self, name: &str) -> bool {
        self.params.iter().any(|(k, _)| k.eq_ignore_ascii_case(name))
    }

    pub fn set_param(&mut self, name: &str, value: Option<&str>) {
        self.params.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.params.push((name.to_string(), value.map(|v| v.to_string())));
    }

    pub fn host_port(&self) -> String {
        match self.port {
            Some(p) => format!("{}:{}", self.host, p),
            None => self.host.clone(),
        }
    }

    /// URI without parameters/headers - used for dialog target comparison.
    pub fn bare(&self) -> Uri {
        Uri {
            scheme: self.scheme.clone(),
            user: self.user.clone(),
            password: None,
            host: self.host.clone(),
            port: self.port,
            params: Vec::new(),
            headers: Vec::new(),
        }
    }

    /// Transport parameter, defaulting to UDP.
    pub fn transport(&self) -> String {
        self.param("transport").unwrap_or("udp").to_ascii_lowercase()
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:", self.scheme)?;
        if let Some(u) = &self.user {
            write!(f, "{u}")?;
            if let Some(p) = &self.password {
                write!(f, ":{p}")?;
            }
            // A tel: URI is all user and no host, so there is nothing for the
            // separator to separate.
            if !self.host.is_empty() {
                write!(f, "@")?;
            }
        }
        write!(f, "{}", self.host)?;
        if let Some(p) = self.port {
            write!(f, ":{p}")?;
        }
        for (k, v) in &self.params {
            match v {
                Some(v) => write!(f, ";{k}={v}")?,
                None => write!(f, ";{k}")?,
            }
        }
        if !self.headers.is_empty() {
            let joined: Vec<String> =
                self.headers.iter().map(|(k, v)| format!("{k}={v}")).collect();
            write!(f, "?{}", joined.join("&"))?;
        }
        Ok(())
    }
}

/// `"Display" <sip:user@host>;tag=xyz`
#[derive(Debug, Clone, Default)]
pub struct NameAddr {
    pub display: Option<String>,
    pub uri: Uri,
    pub params: Vec<(String, String)>,
}

impl NameAddr {
    pub fn new(uri: Uri) -> Self {
        Self { display: None, uri, params: Vec::new() }
    }

    pub fn parse(s: &str) -> Option<NameAddr> {
        let s = s.trim();
        let (display, rest) = if let Some(stripped) = s.strip_prefix('"') {
            let end = stripped.find('"')?;
            (Some(stripped[..end].to_string()), stripped[end + 1..].trim_start())
        } else if s.starts_with('<') {
            (None, s)
        } else if let Some(idx) = s.find('<') {
            let d = s[..idx].trim();
            let d = if d.is_empty() { None } else { Some(d.to_string()) };
            (d, &s[idx..])
        } else {
            (None, s)
        };

        let (uri_str, params_str) = if let Some(rest) = rest.strip_prefix('<') {
            let end = rest.find('>')?;
            (&rest[..end], rest[end + 1..].trim_start())
        } else {
            // bare URI: parameters after the first ';' belong to the header
            match rest.split_once(';') {
                Some((u, p)) => (u, p),
                None => (rest, ""),
            }
        };

        let uri = Uri::parse(uri_str)?;
        let mut params = Vec::new();
        for p in params_str.trim_start_matches(';').split(';') {
            let p = p.trim();
            if p.is_empty() {
                continue;
            }
            match p.split_once('=') {
                Some((k, v)) => params.push((k.trim().to_ascii_lowercase(), v.trim().to_string())),
                None => params.push((p.to_ascii_lowercase(), String::new())),
            }
        }
        Some(NameAddr { display, uri, params })
    }

    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn tag(&self) -> Option<&str> {
        self.param("tag")
    }

    pub fn with_tag(mut self, tag: &str) -> Self {
        self.set_param("tag", Some(tag));
        self
    }

    pub fn set_param(&mut self, name: &str, value: Option<&str>) {
        self.params.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
        self.params.push((name.to_ascii_lowercase(), value.unwrap_or_default().to_string()));
    }
}

impl fmt::Display for NameAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(d) = &self.display {
            write!(f, "\"{d}\" ")?;
        }
        write!(f, "<{}>", self.uri)?;
        for (k, v) in &self.params {
            if v.is_empty() {
                write!(f, ";{k}")?;
            } else {
                write!(f, ";{k}={v}")?;
            }
        }
        Ok(())
    }
}

pub fn split_host_port(s: &str) -> (String, Option<u16>) {
    let s = s.trim();
    if let Some(rest) = s.strip_prefix('[') {
        // IPv6 reference
        if let Some(end) = rest.find(']') {
            let host = rest[..end].to_string();
            let port = rest[end + 1..].strip_prefix(':').and_then(|p| p.parse().ok());
            return (host, port);
        }
    }
    match s.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => match p.parse() {
            Ok(port) => (h.to_string(), Some(port)),
            Err(_) => (s.to_string(), None),
        },
        _ => (s.to_string(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_uri() {
        let u = Uri::parse("sip:+821012345678@example.com:5062;transport=udp;user=phone").unwrap();
        assert_eq!(u.user.as_deref(), Some("+821012345678"));
        assert_eq!(u.host, "example.com");
        assert_eq!(u.port, Some(5062));
        assert_eq!(u.param("transport"), Some("udp"));
    }

    #[test]
    fn parse_name_addr() {
        let n = NameAddr::parse("\"Alice\" <sip:alice@atlanta.com>;tag=1928301774").unwrap();
        assert_eq!(n.display.as_deref(), Some("Alice"));
        assert_eq!(n.tag(), Some("1928301774"));
        let n = NameAddr::parse("sip:bob@biloxi.com;tag=xyz").unwrap();
        assert_eq!(n.uri.user.as_deref(), Some("bob"));
        assert_eq!(n.tag(), Some("xyz"));
    }

    /// A tel: URI is all number and no host; leaving it in `host` made every
    /// call and SMS from a PBX that uses them fail with 484.
    #[test]
    fn tel_uris_carry_the_number_in_the_user_part() {
        let u = Uri::parse("tel:+821012345678").unwrap();
        assert_eq!(u.user.as_deref(), Some("+821012345678"));
        assert_eq!(u.scheme, "tel");
        let u = Uri::parse("tel:0212345678;phone-context=+82").unwrap();
        assert_eq!(u.user.as_deref(), Some("0212345678"));
        assert_eq!(u.param("phone-context"), Some("+82"));

        // It has to survive being written back out: the digest `uri` check
        // and the dialog headers both compare re-serialised URIs.
        let u = Uri::parse("tel:+821012345678").unwrap();
        assert_eq!(u.to_string(), "tel:+821012345678");
        assert_eq!(Uri::parse(&u.to_string()), Some(u));
    }

    /// The sip: path must be untouched by the tel: handling above.
    #[test]
    fn sip_uris_still_round_trip() {
        for text in [
            "sip:alice@atlanta.com",
            "sip:alice@atlanta.com:5062",
            "sip:alice@atlanta.com;transport=udp",
            "sips:bob@biloxi.com:5061",
            "sip:atlanta.com",
        ] {
            let u = Uri::parse(text).expect(text);
            assert_eq!(u.to_string(), text);
            assert_eq!(Uri::parse(&u.to_string()).as_ref(), Some(&u));
        }
    }

    /// Replacing a parameter must not leave the old copy behind.
    #[test]
    fn setting_a_parameter_replaces_it() {
        let mut n = NameAddr::parse("<sip:a@h>;expires=120").unwrap();
        n.set_param("expires", Some("118"));
        assert_eq!(n.to_string(), "<sip:a@h>;expires=118");
        assert_eq!(n.params.len(), 1);
    }
}

//! A tiny DNS client that queries the carrier's resolvers over the modem.
//!
//! MMSC host names commonly do not exist in public DNS at all, and when they
//! do they can resolve to addresses that are only routable inside the
//! operator's network (KT publishes an IPv6 ULA plus private IPv4 addresses
//! for `mmsc.ktfwing.com`).  Handing the name to the system resolver
//! therefore gives the wrong answer - or none - so MMS resolves its own names
//! through the bearer, using the DNS servers ModemManager reports for it.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use rand::Rng;
use tokio::net::UdpSocket;
use tracing::debug;

const TYPE_A: u16 = 1;
const TYPE_CNAME: u16 = 5;
const TYPE_AAAA: u16 = 28;

pub struct Resolver {
    /// Carrier resolvers, tried in order.
    pub servers: Vec<IpAddr>,
    /// Interface to bind the query socket to (SO_BINDTODEVICE).
    pub interface: Option<String>,
    /// Source address of the query socket - the bearer's address, which is
    /// what the policy routing rules match on.
    pub local_ip: Option<IpAddr>,
    pub timeout: Duration,
}

impl Resolver {
    pub fn is_usable(&self) -> bool {
        !self.servers.is_empty()
    }

    /// Resolve `host`, IPv4 answers first (the bearer is IPv4 in practice).
    pub async fn lookup(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        if !self.is_usable() {
            bail!("no carrier DNS server known");
        }

        let mut addrs = Vec::new();
        for (qtype, label) in [(TYPE_A, "A"), (TYPE_AAAA, "AAAA")] {
            match self.query_all_servers(host, qtype).await {
                Ok(found) => {
                    debug!(host, family = label, count = found.len(), "carrier DNS answer");
                    addrs.extend(found);
                }
                Err(e) => debug!(host, family = label, error = %e, "carrier DNS query failed"),
            }
            // An IPv4 answer is enough; do not pay for a second round trip.
            if !addrs.is_empty() && qtype == TYPE_A {
                break;
            }
        }

        if addrs.is_empty() {
            bail!("carrier DNS has no address for {host}");
        }
        Ok(addrs.into_iter().map(|ip| SocketAddr::new(ip, port)).collect())
    }

    async fn query_all_servers(&self, host: &str, qtype: u16) -> Result<Vec<IpAddr>> {
        let mut last = None;
        for server in &self.servers {
            match self.query(*server, host, qtype).await {
                Ok(Answer { addrs, cname }) => {
                    if !addrs.is_empty() {
                        return Ok(addrs);
                    }
                    // Answer carried only a CNAME: chase it once.
                    if let Some(target) = cname {
                        if let Ok(a) = self.query(*server, &target, qtype).await {
                            if !a.addrs.is_empty() {
                                return Ok(a.addrs);
                            }
                        }
                    }
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow!("no answer")))
    }

    async fn query(&self, server: IpAddr, host: &str, qtype: u16) -> Result<Answer> {
        let bind: SocketAddr = match (self.local_ip, server) {
            (Some(ip @ IpAddr::V4(_)), IpAddr::V4(_)) => SocketAddr::new(ip, 0),
            (_, IpAddr::V4(_)) => "0.0.0.0:0".parse().unwrap(),
            (Some(ip @ IpAddr::V6(_)), IpAddr::V6(_)) => SocketAddr::new(ip, 0),
            (_, IpAddr::V6(_)) => "[::]:0".parse().unwrap(),
        };
        let sock = UdpSocket::bind(bind).await?;

        #[cfg(target_os = "linux")]
        {
            if let Some(iface) = &self.interface {
                super::http::bind_to_device_fd(&sock, iface)?;
            }
        }

        // Connect the socket so the kernel drops datagrams from anyone but
        // the resolver we asked.  Without it any host that can reach the
        // bearer address could race in a forged A record and point MMS
        // retrieval - and outgoing message bodies - at itself.
        sock.connect(SocketAddr::new(server, 53)).await?;

        let id: u16 = rand::thread_rng().gen();
        let query = build_query(id, host, qtype)?;
        sock.send(&query).await?;

        let deadline = tokio::time::Instant::now() + self.timeout;
        let mut buf = vec![0u8; 4096];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                bail!("{server} did not answer in {:?}", self.timeout);
            }
            let len = match tokio::time::timeout(remaining, sock.recv(&mut buf)).await {
                Ok(r) => r?,
                Err(_) => bail!("{server} did not answer in {:?}", self.timeout),
            };
            // Source filtering still leaves an on-path attacker, and a late
            // reply to an earlier query looks just like a fresh one, so the
            // transaction id and the echoed question both have to match.
            match classify(&buf[..len], id, host, qtype) {
                Reply::Ours => return parse_answer(&buf[..len], id, qtype),
                // A resolver that refuses or cannot parse the query answers
                // without echoing the question.  Waiting the rest of the
                // timeout out would only delay the fallback.
                Reply::Rejected(rcode) => bail!("{server} answered rcode {rcode}"),
                Reply::NotOurs => {
                    debug!(%server, host, "ignoring a DNS datagram that does not answer our question");
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Reply {
    /// Answers the exact question we asked.
    Ours,
    /// Carries our transaction id but no echoed question - what a resolver
    /// sends when it refuses or cannot parse the query.
    Rejected(u16),
    /// For somebody else, or forged.
    NotOurs,
}

/// Does this datagram answer the exact question we asked?
fn classify(msg: &[u8], want_id: u16, host: &str, qtype: u16) -> Reply {
    if msg.len() < 12 || u16::from_be_bytes([msg[0], msg[1]]) != want_id {
        return Reply::NotOurs;
    }
    let flags = u16::from_be_bytes([msg[2], msg[3]]);
    if flags & 0x8000 == 0 {
        return Reply::NotOurs;
    }
    let echoed = u16::from_be_bytes([msg[4], msg[5]]) == 1
        && matches!((read_name(msg, 12), skip_name(msg, 12)), (Ok(name), Ok(after))
            if msg.get(after..after + 4).is_some_and(|q| {
                u16::from_be_bytes([q[0], q[1]]) == qtype
                    && u16::from_be_bytes([q[2], q[3]]) == 1
                    && name.eq_ignore_ascii_case(host.trim_end_matches('.'))
            }));
    match (echoed, flags & 0x000F) {
        (true, _) => Reply::Ours,
        (false, 0) => Reply::NotOurs,
        (false, rcode) => Reply::Rejected(rcode),
    }
}

#[derive(Debug, Default)]
struct Answer {
    addrs: Vec<IpAddr>,
    cname: Option<String>,
}

fn build_query(id: u16, host: &str, qtype: u16) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(host.len() + 24);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&0x0100u16.to_be_bytes()); // recursion desired
    out.extend_from_slice(&1u16.to_be_bytes()); // one question
    out.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // no answer/authority/extra

    for label in host.trim_end_matches('.').split('.') {
        if label.is_empty() || label.len() > 63 {
            bail!("invalid host name: {host}");
        }
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out.extend_from_slice(&qtype.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes()); // IN
    Ok(out)
}

/// Skip a (possibly compressed) name, returning the offset just past it.
fn skip_name(msg: &[u8], mut pos: usize) -> Result<usize> {
    for _ in 0..128 {
        let len = *msg.get(pos).ok_or_else(|| anyhow!("truncated name"))?;
        if len == 0 {
            return Ok(pos + 1);
        }
        if len & 0xC0 == 0xC0 {
            return Ok(pos + 2); // pointer: the name ends here
        }
        pos += 1 + len as usize;
    }
    bail!("name is too long")
}

/// Read a (possibly compressed) name as text.
fn read_name(msg: &[u8], mut pos: usize) -> Result<String> {
    let mut out = String::new();
    let mut hops = 0;
    loop {
        let len = *msg.get(pos).ok_or_else(|| anyhow!("truncated name"))?;
        if len == 0 {
            return Ok(out);
        }
        if len & 0xC0 == 0xC0 {
            let hi = (len & 0x3F) as usize;
            let lo = *msg.get(pos + 1).ok_or_else(|| anyhow!("truncated pointer"))? as usize;
            pos = (hi << 8) | lo;
            hops += 1;
            if hops > 16 {
                bail!("compression loop");
            }
            continue;
        }
        let start = pos + 1;
        let end = start + len as usize;
        let label = msg.get(start..end).ok_or_else(|| anyhow!("truncated label"))?;
        if !out.is_empty() {
            out.push('.');
        }
        out.push_str(&String::from_utf8_lossy(label));
        pos = end;
    }
}

fn parse_answer(msg: &[u8], want_id: u16, qtype: u16) -> Result<Answer> {
    if msg.len() < 12 {
        bail!("short DNS response");
    }
    let id = u16::from_be_bytes([msg[0], msg[1]]);
    if id != want_id {
        bail!("DNS response id mismatch");
    }
    let flags = u16::from_be_bytes([msg[2], msg[3]]);
    if flags & 0x8000 == 0 {
        bail!("not a DNS response");
    }
    match flags & 0x000F {
        0 => {}
        3 => bail!("NXDOMAIN"),
        rcode => bail!("DNS rcode {rcode}"),
    }
    if flags & 0x0200 != 0 {
        // No TCP fallback here; whatever records did fit are still usable and
        // an MMSC name has never needed more than one datagram in practice.
        debug!("DNS answer is truncated; using the records that arrived");
    }

    let qdcount = u16::from_be_bytes([msg[4], msg[5]]);
    let ancount = u16::from_be_bytes([msg[6], msg[7]]);
    let mut pos = 12;
    for _ in 0..qdcount {
        pos = skip_name(msg, pos)? + 4;
    }

    let mut answer = Answer::default();
    for _ in 0..ancount {
        pos = skip_name(msg, pos)?;
        let header = msg.get(pos..pos + 10).ok_or_else(|| anyhow!("truncated record"))?;
        let rtype = u16::from_be_bytes([header[0], header[1]]);
        let rdlen = u16::from_be_bytes([header[8], header[9]]) as usize;
        pos += 10;
        let rdata = msg.get(pos..pos + rdlen).ok_or_else(|| anyhow!("truncated rdata"))?;
        match rtype {
            TYPE_A if rdlen == 4 && qtype == TYPE_A => {
                answer.addrs.push(IpAddr::V4(Ipv4Addr::new(rdata[0], rdata[1], rdata[2], rdata[3])));
            }
            TYPE_AAAA if rdlen == 16 && qtype == TYPE_AAAA => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(rdata);
                answer.addrs.push(IpAddr::V6(Ipv6Addr::from(octets)));
            }
            TYPE_CNAME => {
                if answer.cname.is_none() {
                    answer.cname = read_name(msg, pos).ok();
                }
            }
            _ => {}
        }
        pos += rdlen;
    }
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(labels: &[&str]) -> Vec<u8> {
        let mut v = Vec::new();
        for l in labels {
            v.push(l.len() as u8);
            v.extend_from_slice(l.as_bytes());
        }
        v.push(0);
        v
    }

    #[test]
    fn query_round_trip() {
        let q = build_query(0x1234, "mmsc.ktfwing.com", TYPE_A).unwrap();
        assert_eq!(&q[0..2], &[0x12, 0x34]);
        assert_eq!(&q[4..6], &[0, 1]);
        assert!(q.windows(5).any(|w| w == b"\x04mmsc"));
        assert_eq!(&q[q.len() - 4..], &[0, 1, 0, 1]);
        assert!(build_query(1, "", TYPE_A).is_err());
    }

    /// A response shaped like the one KT actually returns: CNAME then A.
    #[test]
    fn parses_cname_then_a() {
        let mut msg = Vec::new();
        msg.extend_from_slice(&0xBEEFu16.to_be_bytes());
        msg.extend_from_slice(&0x8180u16.to_be_bytes());
        msg.extend_from_slice(&1u16.to_be_bytes()); // qdcount
        msg.extend_from_slice(&2u16.to_be_bytes()); // ancount
        msg.extend_from_slice(&[0, 0, 0, 0]);
        msg.extend_from_slice(&name(&["mmsc", "ktfwing", "com"]));
        msg.extend_from_slice(&[0, 1, 0, 1]);

        // CNAME record
        msg.extend_from_slice(&[0xC0, 0x0C]);
        msg.extend_from_slice(&TYPE_CNAME.to_be_bytes());
        msg.extend_from_slice(&[0, 1, 0, 0, 0, 60]);
        let target = name(&["mms", "g", "mmsc", "ktfwing", "com"]);
        msg.extend_from_slice(&(target.len() as u16).to_be_bytes());
        msg.extend_from_slice(&target);

        // A record
        msg.extend_from_slice(&[0xC0, 0x0C]);
        msg.extend_from_slice(&TYPE_A.to_be_bytes());
        msg.extend_from_slice(&[0, 1, 0, 0, 0, 60, 0, 4]);
        msg.extend_from_slice(&[172, 31, 36, 98]);

        let a = parse_answer(&msg, 0xBEEF, TYPE_A).unwrap();
        assert_eq!(a.addrs, vec!["172.31.36.98".parse::<IpAddr>().unwrap()]);
        assert_eq!(a.cname.as_deref(), Some("mms.g.mmsc.ktfwing.com"));
    }

    #[test]
    fn rejects_wrong_id_and_nxdomain() {
        let mut msg = vec![0x00, 0x01, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0];
        msg.extend_from_slice(&name(&["a", "b"]));
        msg.extend_from_slice(&[0, 1, 0, 1]);
        assert!(parse_answer(&msg, 0x9999, TYPE_A).is_err());
        assert!(parse_answer(&msg, 0x0001, TYPE_A).unwrap().addrs.is_empty());

        msg[3] = 0x83; // NXDOMAIN
        assert!(parse_answer(&msg, 0x0001, TYPE_A).is_err());
    }

    /// Only a datagram that echoes our own question counts as the answer.
    #[test]
    fn only_our_question_is_accepted() {
        let mut msg = vec![0x12, 0x34, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0];
        msg.extend_from_slice(&name(&["mmsc", "example"]));
        msg.extend_from_slice(&[0, 1, 0, 1]); // A IN

        assert_eq!(classify(&msg, 0x1234, "mmsc.example", TYPE_A), Reply::Ours);
        assert_eq!(classify(&msg, 0x1234, "MMSC.Example.", TYPE_A), Reply::Ours);
        // Right id, wrong name: a forged answer for something else.
        assert_eq!(classify(&msg, 0x1234, "other.example", TYPE_A), Reply::NotOurs);
        // Right name, wrong id or query type.
        assert_eq!(classify(&msg, 0x9999, "mmsc.example", TYPE_A), Reply::NotOurs);
        assert_eq!(classify(&msg, 0x1234, "mmsc.example", TYPE_AAAA), Reply::NotOurs);
        // A query echoed back at us rather than a response.
        let mut query = msg.clone();
        query[2] = 0x01;
        assert_eq!(classify(&query, 0x1234, "mmsc.example", TYPE_A), Reply::NotOurs);
        assert_eq!(classify(b"short", 0x1234, "mmsc.example", TYPE_A), Reply::NotOurs);
    }

    /// A resolver that refuses the query answers without echoing it; waiting
    /// out the timeout for that would only delay the fallback.
    #[test]
    fn a_refusal_is_terminal_rather_than_ignored() {
        // REFUSED, no question section.
        let refused = vec![0x12, 0x34, 0x81, 0x85, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(classify(&refused, 0x1234, "mmsc.example", TYPE_A), Reply::Rejected(5));
        // Same shape but for a transaction that is not ours: still ignored.
        assert_eq!(classify(&refused, 0x9999, "mmsc.example", TYPE_A), Reply::NotOurs);
    }
}

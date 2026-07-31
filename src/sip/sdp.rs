//! Just enough SDP for a narrow-band G.711 audio gateway.

use std::net::{IpAddr, Ipv4Addr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Pcmu,
    Pcma,
}

impl Codec {
    pub fn payload_type(self) -> u8 {
        match self {
            Codec::Pcmu => 0,
            Codec::Pcma => 8,
        }
    }
    pub fn rtpmap(self) -> &'static str {
        match self {
            Codec::Pcmu => "PCMU/8000",
            Codec::Pcma => "PCMA/8000",
        }
    }
    pub fn from_payload_type(pt: u8) -> Option<Codec> {
        match pt {
            0 => Some(Codec::Pcmu),
            8 => Some(Codec::Pcma),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Sdp {
    pub origin_user: String,
    pub session_id: u64,
    pub session_version: u64,
    pub address: IpAddr,
    pub port: u16,
    /// Payload types offered, in preference order.
    pub payload_types: Vec<u8>,
    /// Dynamic payload type for RFC2833 telephone-event, if offered.
    pub telephone_event: Option<u8>,
    pub ptime: Option<u32>,
    pub sendrecv: Direction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    SendRecv,
    SendOnly,
    RecvOnly,
    Inactive,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Direction::SendRecv => "sendrecv",
            Direction::SendOnly => "sendonly",
            Direction::RecvOnly => "recvonly",
            Direction::Inactive => "inactive",
        }
    }

    /// The direction an answer takes to this offer (RFC 3264 §6.1).
    ///
    /// The two ends describe the same stream from opposite sides, so a peer
    /// that says it will only send is told we will only receive.
    pub fn reversed(self) -> Direction {
        match self {
            Direction::SendOnly => Direction::RecvOnly,
            Direction::RecvOnly => Direction::SendOnly,
            same => same,
        }
    }

    /// Is the peer that offered this going to send us anything?
    pub fn peer_sends(self) -> bool {
        matches!(self, Direction::SendRecv | Direction::SendOnly)
    }
}

impl Sdp {
    pub fn offer(address: IpAddr, port: u16, telephone_event: Option<u8>, ptime: u32) -> Sdp {
        let now = chrono::Local::now().timestamp() as u64;
        Sdp {
            origin_user: "modem2sip".into(),
            session_id: now,
            session_version: now,
            address,
            port,
            payload_types: vec![Codec::Pcmu.payload_type(), Codec::Pcma.payload_type()],
            telephone_event,
            ptime: Some(ptime),
            sendrecv: Direction::SendRecv,
        }
    }

    pub fn parse(text: &str) -> Option<Sdp> {
        let mut address: Option<IpAddr> = None;
        let mut media_address: Option<IpAddr> = None;
        let mut port = 0u16;
        let mut payload_types: Vec<u8> = Vec::new();
        let mut telephone_event = None;
        let mut ptime = None;
        let mut direction = Direction::SendRecv;
        let mut in_audio = false;
        let mut seen_media = false;

        for line in text.lines() {
            let line = line.trim_end_matches('\r');
            let Some((key, value)) = line.split_once('=') else { continue };
            match key {
                "c" => {
                    // c=IN IP4 1.2.3.4
                    let ip = value.split_whitespace().nth(2).and_then(parse_ip);
                    if seen_media {
                        if in_audio {
                            media_address = ip;
                        }
                    } else {
                        address = ip;
                    }
                }
                "m" => {
                    seen_media = true;
                    let mut it = value.split_whitespace();
                    let kind = it.next().unwrap_or("");
                    in_audio = kind == "audio" && payload_types.is_empty();
                    if in_audio {
                        port = it.next().and_then(|p| p.parse().ok()).unwrap_or(0);
                        let _proto = it.next();
                        for pt in it {
                            if let Ok(v) = pt.parse::<u8>() {
                                payload_types.push(v);
                            }
                        }
                    }
                }
                "a" if in_audio || !seen_media => {
                    let attr = value.trim();
                    if let Some(rest) = attr.strip_prefix("rtpmap:") {
                        let mut it = rest.split_whitespace();
                        if let (Some(pt), Some(enc)) = (it.next(), it.next()) {
                            if enc.to_ascii_lowercase().starts_with("telephone-event") {
                                telephone_event = pt.parse::<u8>().ok();
                            }
                        }
                    } else if let Some(rest) = attr.strip_prefix("ptime:") {
                        ptime = rest.trim().parse::<u32>().ok();
                    } else if attr.eq_ignore_ascii_case("sendonly") {
                        direction = Direction::SendOnly;
                    } else if attr.eq_ignore_ascii_case("recvonly") {
                        direction = Direction::RecvOnly;
                    } else if attr.eq_ignore_ascii_case("inactive") {
                        direction = Direction::Inactive;
                    } else if attr.eq_ignore_ascii_case("sendrecv") {
                        direction = Direction::SendRecv;
                    }
                }
                _ => {}
            }
        }

        let address = media_address
            .or(address)
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        if payload_types.is_empty() {
            return None;
        }
        Some(Sdp {
            origin_user: "-".into(),
            session_id: 0,
            session_version: 0,
            address,
            port,
            payload_types,
            telephone_event,
            ptime,
            sendrecv: direction,
        })
    }

    /// Pick the first codec we support from a remote offer/answer.
    pub fn negotiate(&self) -> Option<Codec> {
        self.payload_types.iter().find_map(|pt| Codec::from_payload_type(*pt))
    }

    /// Is there somewhere to actually send RTP?
    ///
    /// A rejected or held stream is signalled with port 0 or a null
    /// connection address; accepting one leaves the sender blasting packets
    /// at 0.0.0.0:0 (which Linux turns into loopback) for the whole call.
    pub fn has_media(&self) -> bool {
        self.port != 0 && !self.address.is_unspecified()
    }

    /// Build an answer for `self` (a received offer) using our own transport.
    pub fn answer(&self, address: IpAddr, port: u16, codec: Codec, dtmf_pt: Option<u8>, ptime: u32) -> Sdp {
        let now = chrono::Local::now().timestamp() as u64;
        Sdp {
            origin_user: "modem2sip".into(),
            session_id: now,
            session_version: now,
            address,
            port,
            payload_types: vec![codec.payload_type()],
            // Echo the payload type the peer offered, not our own: an answer
            // may only name payload types that were in the offer, and the
            // receive path is keyed on the offered number.
            telephone_event: dtmf_pt.and(self.telephone_event),
            ptime: Some(ptime),
            // RFC 3264 §6.1: the answer mirrors the offer.  Answering a hold
            // (a=sendonly) with sendrecv told the peer we were still ready to
            // be heard, which is both a protocol error and a lie.
            sendrecv: self.sendrecv.reversed(),
        }
    }

    fn render(&self) -> String {
        let ipver = if self.address.is_ipv6() { "IP6" } else { "IP4" };
        let mut s = String::new();
        s.push_str("v=0\r\n");
        s.push_str(&format!(
            "o={} {} {} IN {} {}\r\n",
            self.origin_user, self.session_id, self.session_version, ipver, self.address
        ));
        s.push_str("s=modem2sip\r\n");
        s.push_str(&format!("c=IN {} {}\r\n", ipver, self.address));
        s.push_str("t=0 0\r\n");
        let pts: Vec<String> = self
            .payload_types
            .iter()
            .map(|p| p.to_string())
            .chain(self.telephone_event.map(|p| p.to_string()))
            .collect();
        s.push_str(&format!("m=audio {} RTP/AVP {}\r\n", self.port, pts.join(" ")));
        for pt in &self.payload_types {
            if let Some(c) = Codec::from_payload_type(*pt) {
                s.push_str(&format!("a=rtpmap:{} {}\r\n", pt, c.rtpmap()));
            }
        }
        if let Some(te) = self.telephone_event {
            s.push_str(&format!("a=rtpmap:{te} telephone-event/8000\r\n"));
            s.push_str(&format!("a=fmtp:{te} 0-16\r\n"));
        }
        if let Some(pt) = self.ptime {
            s.push_str(&format!("a=ptime:{pt}\r\n"));
        }
        s.push_str(&format!("a={}\r\n", self.sendrecv.as_str()));
        s
    }
}

impl std::fmt::Display for Sdp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.render())
    }
}

fn parse_ip(s: &str) -> Option<IpAddr> {
    // Strip any TTL/multicast suffix ("224.0.0.1/127").
    s.split('/').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_typical_offer() {
        let text = "v=0\r\no=- 1 1 IN IP4 10.0.0.5\r\ns=-\r\nc=IN IP4 10.0.0.5\r\nt=0 0\r\n\
                    m=audio 40000 RTP/AVP 8 0 101\r\na=rtpmap:101 telephone-event/8000\r\n\
                    a=ptime:20\r\na=sendrecv\r\n";
        let sdp = Sdp::parse(text).unwrap();
        assert_eq!(sdp.port, 40000);
        assert_eq!(sdp.negotiate(), Some(Codec::Pcma));
        assert_eq!(sdp.telephone_event, Some(101));
        assert_eq!(sdp.address.to_string(), "10.0.0.5");
        assert!(sdp.has_media());
    }

    /// An answer may only name payload types the offer listed.  Advertising
    /// our own configured number instead left the peer sending digits as one
    /// type while the receiver was decoding another.
    #[test]
    fn the_answer_echoes_the_offered_telephone_event() {
        let offer = Sdp::parse(
            "v=0\r\nc=IN IP4 10.0.0.5\r\nm=audio 40000 RTP/AVP 0 96\r\n\
             a=rtpmap:96 telephone-event/8000\r\n",
        )
        .unwrap();
        let answer = offer.answer("10.0.0.1".parse().unwrap(), 16000, Codec::Pcmu, Some(101), 20);
        assert_eq!(answer.telephone_event, Some(96));
        assert!(answer.to_string().contains("a=rtpmap:96 telephone-event/8000"));
        assert!(answer.to_string().contains("m=audio 16000 RTP/AVP 0 96"));

        // Nothing offered, nothing answered - either way round.
        let plain = Sdp::parse("v=0\r\nc=IN IP4 10.0.0.5\r\nm=audio 40000 RTP/AVP 0\r\n").unwrap();
        assert_eq!(
            plain.answer("10.0.0.1".parse().unwrap(), 16000, Codec::Pcmu, Some(101), 20).telephone_event,
            None
        );
        assert_eq!(
            offer.answer("10.0.0.1".parse().unwrap(), 16000, Codec::Pcmu, None, 20).telephone_event,
            None
        );
    }

    /// An answer mirrors the offer's direction, and a peer on hold has to be
    /// told we stopped too - otherwise it is being promised audio it asked
    /// not to receive.
    #[test]
    fn the_answer_mirrors_the_direction_of_the_offer() {
        let offer = |attr: &str| {
            Sdp::parse(&format!(
                "v=0\r\nc=IN IP4 10.0.0.5\r\nm=audio 40000 RTP/AVP 0\r\na={attr}\r\n"
            ))
            .unwrap()
        };
        let answer = |attr: &str| {
            offer(attr)
                .answer("10.0.0.1".parse().unwrap(), 16000, Codec::Pcmu, None, 20)
                .sendrecv
        };
        assert_eq!(answer("sendonly"), Direction::RecvOnly, "hold must be answered recvonly");
        assert_eq!(answer("recvonly"), Direction::SendOnly);
        assert_eq!(answer("inactive"), Direction::Inactive);
        assert_eq!(answer("sendrecv"), Direction::SendRecv);
        // No attribute at all means sendrecv, and so does the answer.
        assert_eq!(
            Sdp::parse("v=0\r\nc=IN IP4 10.0.0.5\r\nm=audio 40000 RTP/AVP 0\r\n")
                .unwrap()
                .answer("10.0.0.1".parse().unwrap(), 16000, Codec::Pcmu, None, 20)
                .sendrecv,
            Direction::SendRecv
        );
        // Which offers mean "expect no RTP from me", which the inactivity
        // watchdog has to know before it reads silence as a dead peer.
        // `sendonly` is not one of them: a peer holding us with music still
        // sends.  `recvonly` and `inactive` are.
        assert!(offer("sendonly").sendrecv.peer_sends());
        assert!(offer("sendrecv").sendrecv.peer_sends());
        assert!(!offer("recvonly").sendrecv.peer_sends());
        assert!(!offer("inactive").sendrecv.peer_sends());
    }

    /// Port 0 or a null address means "no media here"; taking it at face
    /// value leaves the sender blasting packets at loopback.
    #[test]
    fn a_held_or_rejected_stream_has_no_media() {
        let held = Sdp::parse("v=0\r\nc=IN IP4 10.0.0.5\r\nm=audio 0 RTP/AVP 0\r\n").unwrap();
        assert!(!held.has_media());
        let null = Sdp::parse("v=0\r\nc=IN IP4 0.0.0.0\r\nm=audio 40000 RTP/AVP 0\r\n").unwrap();
        assert!(!null.has_media());
    }
}

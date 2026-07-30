//! UDP transport plus the address bookkeeping a SIP element needs.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::net::UdpSocket;

use super::uri::Uri;

pub const MAX_DATAGRAM: usize = 65535;

pub struct Transport {
    sock: Arc<UdpSocket>,
    bound: SocketAddr,
    public_ip: Option<IpAddr>,
}

impl Transport {
    pub async fn bind(addr: SocketAddr, public_ip: Option<IpAddr>) -> Result<Self> {
        let sock = UdpSocket::bind(addr)
            .await
            .with_context(|| format!("binding SIP socket on {addr}"))?;
        let bound = sock.local_addr()?;
        Ok(Self { sock: Arc::new(sock), bound, public_ip })
    }

    pub fn socket(&self) -> Arc<UdpSocket> {
        self.sock.clone()
    }

    pub fn bound(&self) -> SocketAddr {
        self.bound
    }

    pub fn port(&self) -> u16 {
        self.bound.port()
    }

    pub async fn send(&self, data: &[u8], dest: SocketAddr) -> Result<()> {
        self.sock.send_to(data, dest).await?;
        Ok(())
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let r = self.sock.recv_from(buf).await?;
        Ok(r)
    }

    /// The address we should put in Via/Contact/SDP when talking to `dest`.
    pub fn advertised_ip(&self, dest: SocketAddr) -> IpAddr {
        if let Some(ip) = self.public_ip {
            return ip;
        }
        if !self.bound.ip().is_unspecified() {
            return self.bound.ip();
        }
        local_ip_for(dest).unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
    }
}

/// Ask the routing table which source address would be used for `dest`.
/// Connecting a UDP socket sends nothing on the wire.
pub fn local_ip_for(dest: SocketAddr) -> Option<IpAddr> {
    let bind: SocketAddr = if dest.is_ipv6() {
        "[::]:0".parse().ok()?
    } else {
        "0.0.0.0:0".parse().ok()?
    };
    let sock = std::net::UdpSocket::bind(bind).ok()?;
    sock.connect(dest).ok()?;
    sock.local_addr().ok().map(|a| a.ip())
}

/// Resolve a SIP URI to a socket address.  No NAPTR/SRV: A/AAAA plus the
/// explicit port, defaulting to 5060.  That covers the deployments this
/// gateway is meant for (a PBX or softphone on the same LAN).
pub async fn resolve_uri(uri: &Uri) -> Result<SocketAddr> {
    let port = uri.port.unwrap_or(5060);
    let host = uri.host.trim_matches(|c| c == '[' || c == ']');
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let mut addrs = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolving {host}"))?;
    addrs.next().ok_or_else(|| anyhow!("no address for {host}"))
}

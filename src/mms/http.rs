//! A very small HTTP/1.1 client.
//!
//! MMS traffic must leave through the modem's data interface (and often via
//! the carrier's WAP proxy), which needs source-address / SO_BINDTODEVICE
//! control that general purpose clients do not expose conveniently.  The
//! protocol surface needed here is tiny, so it is implemented directly.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpSocket;
use tracing::debug;

#[derive(Debug, Clone, Default)]
pub struct HttpOptions {
    /// "host:port" of a WAP/HTTP proxy.
    pub proxy: Option<String>,
    /// Network interface to bind to (SO_BINDTODEVICE, needs CAP_NET_RAW).
    pub interface: Option<String>,
    /// Source address to bind to.
    pub local_ip: Option<IpAddr>,
    pub timeout: Duration,
    pub user_agent: String,
    pub ua_profile: Option<String>,
    /// Refuse bodies larger than this.
    pub max_size: usize,
    /// Carrier DNS servers.  When set, names are resolved through the modem
    /// instead of the system resolver - MMSC names are frequently absent
    /// from public DNS or resolve to addresses only the operator can route.
    pub dns_servers: Vec<IpAddr>,
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    pub fn content_type(&self) -> Option<&str> {
        self.header("content-type")
    }
}

struct Url {
    host: String,
    port: u16,
    path: String,
}

fn parse_url(url: &str) -> Result<Url> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| {
            if url.starts_with("https://") {
                anyhow!("https MMSC URLs are not supported: {url}")
            } else {
                anyhow!("unsupported MMS URL: {url}")
            }
        })?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => (h.to_string(), p.parse().unwrap_or(80)),
        _ => (authority.to_string(), 80),
    };
    Ok(Url { host, port, path: path.to_string() })
}

pub async fn get(url: &str, opts: &HttpOptions) -> Result<HttpResponse> {
    request("GET", url, None, None, opts).await
}

pub async fn post(
    url: &str,
    content_type: &str,
    body: Vec<u8>,
    opts: &HttpOptions,
) -> Result<HttpResponse> {
    request("POST", url, Some(content_type), Some(body), opts).await
}

async fn request(
    method: &str,
    url: &str,
    content_type: Option<&str>,
    body: Option<Vec<u8>>,
    opts: &HttpOptions,
) -> Result<HttpResponse> {
    let target = parse_url(url)?;
    let (connect_host, connect_port, request_target) = match &opts.proxy {
        Some(proxy) => {
            let (h, p) = proxy
                .rsplit_once(':')
                .ok_or_else(|| anyhow!("mms.proxy must be host:port"))?;
            (h.to_string(), p.parse::<u16>().context("mms.proxy port")?, url.to_string())
        }
        None => (target.host.clone(), target.port, target.path.clone()),
    };

    let timeout = if opts.timeout.is_zero() { Duration::from_secs(60) } else { opts.timeout };
    let addrs = resolve(&connect_host, connect_port, opts).await?;
    debug!(?addrs, method, url, proxy = ?opts.proxy, "MMS HTTP request");

    let fut = async {
        let mut stream = connect_any(&addrs, opts).await?;

        let mut head = String::new();
        head.push_str(&format!("{method} {request_target} HTTP/1.1\r\n"));
        head.push_str(&format!("Host: {}\r\n", host_header(&target)));
        head.push_str(&format!("User-Agent: {}\r\n", opts.user_agent));
        if let Some(profile) = &opts.ua_profile {
            head.push_str(&format!("x-wap-profile: {profile}\r\n"));
        }
        head.push_str("Accept: application/vnd.wap.mms-message, */*\r\n");
        head.push_str("Accept-Charset: utf-8\r\n");
        head.push_str("Connection: close\r\n");
        if let Some(ct) = content_type {
            head.push_str(&format!("Content-Type: {ct}\r\n"));
        }
        head.push_str(&format!("Content-Length: {}\r\n", body.as_ref().map(|b| b.len()).unwrap_or(0)));
        head.push_str("\r\n");

        stream.write_all(head.as_bytes()).await?;
        if let Some(b) = &body {
            stream.write_all(b).await?;
        }
        stream.flush().await?;

        read_response(&mut stream, opts.max_size).await
    };

    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| anyhow!("MMS HTTP request timed out after {timeout:?}"))?
}

fn host_header(url: &Url) -> String {
    if url.port == 80 {
        url.host.clone()
    } else {
        format!("{}:{}", url.host, url.port)
    }
}

/// All addresses for the host, IPv4 first.
///
/// The carrier's own resolvers are asked first, over the modem: MMSC names
/// are often missing from public DNS, and when they are present they can
/// point at addresses only the operator can route (KT publishes an IPv6 ULA
/// for `mmsc.ktfwing.com`).  The system resolver is the fallback, and every
/// candidate address is tried in turn rather than just the first one.
async fn resolve(host: &str, port: u16, opts: &HttpOptions) -> Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let resolver = super::dns::Resolver {
        servers: opts.dns_servers.clone(),
        interface: opts.interface.clone(),
        local_ip: opts.local_ip,
        timeout: Duration::from_secs(5),
    };
    if resolver.is_usable() {
        match resolver.lookup(host, port).await {
            Ok(addrs) => {
                debug!(host, ?addrs, "resolved through the modem's DNS");
                return Ok(addrs);
            }
            Err(e) => debug!(
                host,
                error = %e,
                "carrier DNS did not resolve the name, falling back to the system resolver"
            ),
        }
    }

    let mut addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolving {host}"))?
        .collect();
    addrs.sort_by_key(|a| a.is_ipv6());
    if addrs.is_empty() {
        return Err(anyhow!("no address for {host}"));
    }
    Ok(addrs)
}

async fn connect_any(addrs: &[SocketAddr], opts: &HttpOptions) -> Result<tokio::net::TcpStream> {
    let mut last = None;
    for addr in addrs {
        match connect(*addr, opts).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                debug!(%addr, error = %e, "MMSC address not usable, trying the next one");
                last = Some(e);
            }
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("no address to connect to")))
}

async fn connect(addr: SocketAddr, opts: &HttpOptions) -> Result<tokio::net::TcpStream> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };

    #[cfg(target_os = "linux")]
    {
        if let Some(iface) = &opts.interface {
            bind_to_device(&socket, iface)
                .with_context(|| format!("binding MMS traffic to interface {iface}"))?;
        }
    }

    if let Some(ip) = opts.local_ip {
        socket
            .bind(SocketAddr::new(ip, 0))
            .with_context(|| format!("binding MMS traffic to source {ip}"))?;
    }

    Ok(socket.connect(addr).await?)
}

#[cfg(target_os = "linux")]
fn bind_to_device(socket: &TcpSocket, iface: &str) -> Result<()> {
    bind_to_device_fd(socket, iface)
}

/// SO_BINDTODEVICE on anything that owns a file descriptor (TCP or UDP).
#[cfg(target_os = "linux")]
pub fn bind_to_device_fd<T: std::os::fd::AsRawFd>(socket: &T, iface: &str) -> Result<()> {
    let cstr = std::ffi::CString::new(iface)?;
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            cstr.as_ptr() as *const libc::c_void,
            (cstr.as_bytes().len() + 1) as libc::socklen_t,
        )
    };
    if rc != 0 {
        bail!("SO_BINDTODEVICE failed: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

async fn read_response(
    stream: &mut tokio::net::TcpStream,
    max_size: usize,
) -> Result<HttpResponse> {
    let max_size = if max_size == 0 { 8 * 1024 * 1024 } else { max_size };
    let mut buf = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];

    // Headers.
    let header_end = loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            bail!("connection closed before the HTTP headers were complete");
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 64 * 1024 {
            bail!("HTTP headers too large");
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow!("malformed HTTP status line: {status_line}"))?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();

    let mut body = buf[header_end + 4..].to_vec();
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };

    if get("transfer-encoding").map(|v| v.to_ascii_lowercase().contains("chunked")).unwrap_or(false)
    {
        // Read everything, then de-chunk.
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
            if body.len() > max_size {
                bail!("HTTP body exceeds the configured limit ({max_size} bytes)");
            }
        }
        body = dechunk(&body)?;
    } else if let Some(len) = get("content-length").and_then(|v| v.trim().parse::<usize>().ok()) {
        if len > max_size {
            bail!("HTTP body of {len} bytes exceeds the configured limit ({max_size})");
        }
        while body.len() < len {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(len);
    } else {
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
            if body.len() > max_size {
                bail!("HTTP body exceeds the configured limit ({max_size} bytes)");
            }
        }
    }

    Ok(HttpResponse { status, headers, body })
}

fn dechunk(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len());
    let mut pos = 0;
    loop {
        let Some(eol) = find(&data[pos..], b"\r\n") else { break };
        let size_str = String::from_utf8_lossy(&data[pos..pos + eol]);
        let size_str = size_str.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_str, 16).unwrap_or(0);
        pos += eol + 2;
        if size == 0 {
            break;
        }
        if pos + size > data.len() {
            out.extend_from_slice(&data[pos..]);
            break;
        }
        out.extend_from_slice(&data[pos..pos + size]);
        pos += size + 2;
    }
    Ok(out)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

//! WSP (WAP-230) primitive encoding used by the MMS encapsulation format.
//!
//! Only the pieces MMS actually needs: uintvar, value-length, text strings,
//! encoded strings with a charset, integers, and content types with
//! parameters.  Unknown constructs can always be *skipped* safely, which is
//! what keeps the decoder from falling over on carrier-specific extensions.

/// Well-known content types (WSP assigned numbers).  Anything not listed is
/// reported as application/octet-stream, which is still storable.
pub const CONTENT_TYPES: &[(u8, &str)] = &[
    (0x00, "*/*"),
    (0x01, "text/*"),
    (0x02, "text/html"),
    (0x03, "text/plain"),
    (0x04, "text/x-hdml"),
    (0x05, "text/x-ttml"),
    (0x06, "text/x-vCalendar"),
    (0x07, "text/x-vCard"),
    (0x08, "text/vnd.wap.wml"),
    (0x09, "text/vnd.wap.wmlscript"),
    (0x0A, "text/vnd.wap.wta-event"),
    (0x0B, "multipart/*"),
    (0x0C, "multipart/mixed"),
    (0x0D, "multipart/form-data"),
    (0x0E, "multipart/byteranges"),
    (0x0F, "multipart/alternative"),
    (0x10, "application/*"),
    (0x11, "application/java-vm"),
    (0x12, "application/x-www-form-urlencoded"),
    (0x13, "application/x-hdmlc"),
    (0x14, "application/vnd.wap.wmlc"),
    (0x15, "application/vnd.wap.wmlscriptc"),
    (0x16, "application/vnd.wap.wta-eventc"),
    (0x17, "application/vnd.wap.uaprof"),
    (0x18, "application/vnd.wap.wtls-ca-certificate"),
    (0x19, "application/vnd.wap.wtls-user-certificate"),
    (0x1A, "application/x-x509-ca-cert"),
    (0x1B, "application/x-x509-user-cert"),
    (0x1C, "image/*"),
    (0x1D, "image/gif"),
    (0x1E, "image/jpeg"),
    (0x1F, "image/tiff"),
    (0x20, "image/png"),
    (0x21, "image/vnd.wap.wbmp"),
    (0x22, "application/vnd.wap.multipart.*"),
    (0x23, "application/vnd.wap.multipart.mixed"),
    (0x24, "application/vnd.wap.multipart.form-data"),
    (0x25, "application/vnd.wap.multipart.byteranges"),
    (0x26, "application/vnd.wap.multipart.alternative"),
    (0x27, "application/xml"),
    (0x28, "text/xml"),
    (0x29, "application/vnd.wap.wbxml"),
    (0x2A, "application/x-x968-cross-cert"),
    (0x2B, "application/x-x968-ca-cert"),
    (0x2C, "application/x-x968-user-cert"),
    (0x2D, "text/vnd.wap.si"),
    (0x2E, "application/vnd.wap.sic"),
    (0x2F, "text/vnd.wap.sl"),
    (0x30, "application/vnd.wap.slc"),
    (0x31, "text/vnd.wap.co"),
    (0x32, "application/vnd.wap.coc"),
    (0x33, "application/vnd.wap.multipart.related"),
    (0x34, "application/vnd.wap.sia"),
    (0x35, "text/vnd.wap.connectivity-xml"),
    (0x36, "application/vnd.wap.connectivity-wbxml"),
    (0x37, "application/pkcs7-mime"),
    (0x3E, "application/vnd.wap.mms-message"),
];

pub fn content_type_name(code: u8) -> String {
    CONTENT_TYPES
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, n)| (*n).to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

pub fn content_type_code(name: &str) -> Option<u8> {
    CONTENT_TYPES
        .iter()
        .find(|(_, n)| n.eq_ignore_ascii_case(name))
        .map(|(c, _)| *c)
}

/// WSP well-known parameter tokens we care about.
const PARAM_NAMES: &[(u8, &str)] = &[
    (0x00, "q"),
    (0x01, "charset"),
    (0x02, "level"),
    (0x03, "type"),
    (0x05, "name"),
    (0x06, "filename"),
    (0x07, "differences"),
    (0x08, "padding"),
    (0x09, "type"),
    (0x0A, "start"),
    (0x0B, "start-info"),
    (0x0C, "comment"),
    (0x0D, "domain"),
    (0x0E, "max-age"),
    (0x0F, "path"),
    (0x10, "secure"),
    (0x11, "sec"),
    (0x12, "mac"),
    (0x13, "creation-date"),
    (0x14, "modification-date"),
    (0x15, "read-date"),
    (0x16, "size"),
    (0x17, "name"),
    (0x18, "filename"),
    (0x19, "start"),
    (0x1A, "start-info"),
    (0x1B, "comment"),
    (0x1C, "domain"),
    (0x1D, "path"),
];

fn param_name(code: u8) -> Option<&'static str> {
    PARAM_NAMES.iter().find(|(c, _)| *c == code).map(|(_, n)| *n)
}

/// IANA MIBenum -> encoding_rs encoding.
pub fn charset_for(mib: u32) -> &'static encoding_rs::Encoding {
    match mib {
        3 => encoding_rs::WINDOWS_1252, // us-ascii, superset is fine
        4 => encoding_rs::WINDOWS_1252, // iso-8859-1
        106 => encoding_rs::UTF_8,
        1000 | 1015 => encoding_rs::UTF_16BE,
        1013 => encoding_rs::UTF_16BE,
        1014 => encoding_rs::UTF_16LE,
        36 | 38 => encoding_rs::EUC_KR, // ks_c_5601-1987 / euc-kr
        17 | 2024 => encoding_rs::SHIFT_JIS,
        18 | 2025 => encoding_rs::EUC_JP,
        2026 => encoding_rs::BIG5,
        113 | 2085 => encoding_rs::GBK,
        _ => encoding_rs::UTF_8,
    }
}

pub struct Reader<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn eof(&self) -> bool {
        self.pos >= self.data.len()
    }

    pub fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    pub fn u8(&mut self) -> Option<u8> {
        let b = self.data.get(self.pos).copied()?;
        self.pos += 1;
        Some(b)
    }

    pub fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.remaining() < n {
            return None;
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Some(out)
    }

    /// Variable length unsigned integer, 7 bits per byte.
    pub fn uintvar(&mut self) -> Option<u64> {
        let mut value: u64 = 0;
        for _ in 0..8 {
            let b = self.u8()?;
            value = (value << 7) | (b & 0x7F) as u64;
            if b & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }

    /// NUL-terminated text, with the optional 0x7F quote stripped.
    pub fn text_string(&mut self) -> Option<String> {
        if self.peek() == Some(0x7F) {
            self.pos += 1;
        }
        let start = self.pos;
        while let Some(b) = self.data.get(self.pos) {
            if *b == 0 {
                let text = String::from_utf8_lossy(&self.data[start..self.pos]).into_owned();
                self.pos += 1;
                return Some(text);
            }
            self.pos += 1;
        }
        // Unterminated: take the rest, it is still useful.
        if start < self.data.len() {
            let text = String::from_utf8_lossy(&self.data[start..]).into_owned();
            self.pos = self.data.len();
            return Some(text);
        }
        None
    }

    /// Short-length (0..30) or 0x1F + uintvar.
    pub fn value_length(&mut self) -> Option<usize> {
        let b = self.peek()?;
        if b <= 30 {
            self.pos += 1;
            Some(b as usize)
        } else if b == 0x1F {
            self.pos += 1;
            // A uintvar carries up to 56 bits; saturate rather than truncate
            // so a 32-bit build cannot wrap the length into something small.
            self.uintvar().map(|v| usize::try_from(v).unwrap_or(usize::MAX))
        } else {
            None
        }
    }

    /// Value length that is guaranteed to fit in what is left of the buffer.
    ///
    /// The length is a network-supplied field, so `pos + len` must never be
    /// computed from it directly: on a 32-bit target (the OpenWrt builds) a
    /// declared length near `usize::MAX` wraps and produces a backwards slice
    /// range, which panics.
    pub fn bounded_value_length(&mut self) -> Option<usize> {
        let len = self.value_length()?;
        Some(len.min(self.remaining()))
    }

    /// Long-integer: length byte followed by that many big-endian bytes.
    pub fn long_int(&mut self) -> Option<u64> {
        let len = self.u8()? as usize;
        if len == 0 || len > 8 {
            return None;
        }
        let bytes = self.take(len)?;
        Some(bytes.iter().fold(0u64, |acc, b| (acc << 8) | *b as u64))
    }

    /// Short-integer (high bit set) or long-integer.
    pub fn integer_value(&mut self) -> Option<u64> {
        let b = self.peek()?;
        if b & 0x80 != 0 {
            self.pos += 1;
            Some((b & 0x7F) as u64)
        } else {
            self.long_int()
        }
    }

    /// Encoded-string-value: plain text, or length + charset + text.
    pub fn encoded_string(&mut self) -> Option<String> {
        let b = self.peek()?;
        if b >= 0x20 && b != 0x7F {
            return self.text_string();
        }
        if b == 0x7F {
            return self.text_string();
        }
        let len = self.bounded_value_length()?;
        let end = self.pos + len;
        let mut inner = Reader::new(&self.data[self.pos..end]);
        self.pos = end;
        let mib = inner.integer_value().unwrap_or(106) as u32;
        let encoding = charset_for(mib);
        let raw = &inner.data[inner.pos..];
        // UTF-16 terminates with two NUL bytes, everything else with one.
        let terminator = if encoding.name().starts_with("UTF-16") { 2 } else { 1 };
        let raw = match raw.len().checked_sub(terminator) {
            Some(cut) if raw[cut..].iter().all(|b| *b == 0) => &raw[..cut],
            _ => raw,
        };
        let (text, _, _) = encoding.decode(raw);
        Some(text.into_owned())
    }

    /// Content-type value: either a bare well-known type/text or a
    /// length-prefixed type with parameters.
    pub fn content_type(&mut self) -> Option<(String, Vec<(String, String)>)> {
        let b = self.peek()?;
        if b & 0x80 != 0 {
            self.pos += 1;
            return Some((content_type_name(b & 0x7F), Vec::new()));
        }
        if b >= 0x20 {
            return self.text_string().map(|t| (t, Vec::new()));
        }
        let len = self.bounded_value_length()?;
        let end = self.pos + len;
        let mut inner = Reader::new(&self.data[self.pos..end]);
        self.pos = end;

        let name = match inner.peek()? {
            b if b & 0x80 != 0 => {
                inner.pos += 1;
                content_type_name(b & 0x7F)
            }
            _ => inner.text_string()?,
        };
        let mut params = Vec::new();
        while !inner.eof() {
            let Some((k, v)) = inner.parameter() else { break };
            params.push((k, v));
        }
        Some((name, params))
    }

    /// One typed parameter of a content type.
    pub fn parameter(&mut self) -> Option<(String, String)> {
        let b = self.u8()?;
        if b & 0x80 != 0 {
            let code = b & 0x7F;
            let name = param_name(code).unwrap_or("x-unknown").to_string();
            let value = match code {
                // No-value parameter: reading it as an integer would eat the
                // next parameter's first byte and desynchronise the list.
                0x10 => String::new(),
                // Q-value: a uintvar, not an integer-value.
                0x00 => self.uintvar().map(|v| v.to_string()).unwrap_or_default(),
                // Integer-valued parameters.
                0x02 | 0x0E | 0x16 => {
                    self.integer_value().map(|v| v.to_string()).unwrap_or_default()
                }
                0x01 => {
                    // charset: well-known integer or text
                    match self.peek() {
                        Some(p) if p & 0x80 != 0 || p < 0x20 => self
                            .integer_value()
                            .map(|v| charset_for(v as u32).name().to_string())
                            .unwrap_or_default(),
                        _ => self.text_string().unwrap_or_default(),
                    }
                }
                _ => self.encoded_string().unwrap_or_default(),
            };
            Some((name, value))
        } else {
            // Untyped parameter: token-text = value.
            self.pos -= 1;
            let name = self.text_string()?;
            let value = self.encoded_string().unwrap_or_default();
            Some((name, value))
        }
    }

    /// Skip a header value of unknown type (WSP §8.4.1.2 rules).
    pub fn skip_value(&mut self) -> Option<()> {
        let b = self.peek()?;
        if b <= 30 || b == 0x1F {
            // A length that overruns the buffer means the PDU is malformed;
            // consuming the remainder is the only sane recovery.
            let len = self.bounded_value_length()?;
            self.take(len)?;
        } else if b & 0x80 != 0 {
            self.pos += 1;
        } else {
            self.text_string()?;
        }
        Some(())
    }
}

// -------------------------------------------------------------- writing

pub fn write_uintvar(out: &mut Vec<u8>, mut value: u64) {
    let mut bytes = [0u8; 10];
    let mut i = bytes.len();
    loop {
        i -= 1;
        bytes[i] = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            break;
        }
    }
    for (n, b) in bytes[i..].iter().enumerate() {
        let last = n == bytes.len() - i - 1;
        out.push(if last { *b } else { *b | 0x80 });
    }
}

pub fn write_text_string(out: &mut Vec<u8>, text: &str) {
    // Quote text that would otherwise be mistaken for a binary value.
    if text.as_bytes().first().map(|b| *b >= 0x80).unwrap_or(false) {
        out.push(0x7F);
    }
    out.extend_from_slice(text.as_bytes());
    out.push(0);
}

pub fn write_value_length(out: &mut Vec<u8>, len: usize) {
    if len <= 30 {
        out.push(len as u8);
    } else {
        out.push(0x1F);
        write_uintvar(out, len as u64);
    }
}

/// Encoded-string-value with an explicit UTF-8 charset, which every MMSC
/// understands and keeps non-ASCII subjects intact.
pub fn write_encoded_string(out: &mut Vec<u8>, text: &str) {
    if text.is_ascii() {
        write_text_string(out, text);
        return;
    }
    let mut inner = Vec::with_capacity(text.len() + 4);
    write_integer_value(&mut inner, CHARSET_UTF8);
    inner.extend_from_slice(text.as_bytes());
    inner.push(0);
    write_value_length(out, inner.len());
    out.extend_from_slice(&inner);
}

/// IANA MIBenum for UTF-8.
pub const CHARSET_UTF8: u64 = 106;

pub fn write_short_int(out: &mut Vec<u8>, value: u8) {
    out.push(0x80 | (value & 0x7F));
}

/// Integer-value: the canonical form is a short integer whenever the value
/// fits in seven bits.  Carrier MMSCs do reject the long form for values
/// like the UTF-8 charset (106), which every real PDU writes as 0xEA.
pub fn write_integer_value(out: &mut Vec<u8>, value: u64) {
    if value <= 127 {
        out.push(0x80 | value as u8);
    } else {
        write_long_int(out, value);
    }
}

pub fn write_long_int(out: &mut Vec<u8>, value: u64) {
    let mut bytes = Vec::new();
    let mut v = value;
    while v > 0 {
        bytes.insert(0, (v & 0xFF) as u8);
        v >>= 8;
    }
    if bytes.is_empty() {
        bytes.push(0);
    }
    out.push(bytes.len() as u8);
    out.extend_from_slice(&bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uintvar_round_trip() {
        for v in [0u64, 1, 127, 128, 300, 16384, 1_000_000] {
            let mut buf = Vec::new();
            write_uintvar(&mut buf, v);
            let mut r = Reader::new(&buf);
            assert_eq!(r.uintvar(), Some(v), "value {v}");
            assert!(r.eof());
        }
    }

    #[test]
    fn text_and_encoded_strings() {
        let mut buf = Vec::new();
        write_text_string(&mut buf, "hello");
        let mut r = Reader::new(&buf);
        assert_eq!(r.text_string().as_deref(), Some("hello"));

        let mut buf = Vec::new();
        write_encoded_string(&mut buf, "안녕하세요");
        let mut r = Reader::new(&buf);
        assert_eq!(r.encoded_string().as_deref(), Some("안녕하세요"));
    }

    #[test]
    fn charset_uses_the_canonical_short_form() {
        // Real carrier PDUs write UTF-8 as 0xEA; the long form (0x01 0x6A) is
        // legal but gets rejected in practice.
        let mut buf = Vec::new();
        write_encoded_string(&mut buf, "한글");
        assert_eq!(buf[1], 0xEA, "charset must be a short integer: {buf:02x?}");

        let mut buf = Vec::new();
        write_integer_value(&mut buf, 106);
        assert_eq!(buf, vec![0xEA]);
        let mut buf = Vec::new();
        write_integer_value(&mut buf, 1000);
        assert_eq!(buf, vec![0x02, 0x03, 0xE8]);
    }

    #[test]
    fn well_known_content_type() {
        let buf = [0x80 | 0x1E];
        let mut r = Reader::new(&buf);
        let (name, params) = r.content_type().unwrap();
        assert_eq!(name, "image/jpeg");
        assert!(params.is_empty());
    }

    /// A length field that overruns the buffer used to produce a backwards
    /// slice range on 32-bit targets.  It must clamp, never panic.
    #[test]
    fn declared_lengths_cannot_run_past_the_buffer() {
        // 0x1F + uintvar(0xFFFFFFFF), i.e. "this value is 4 GiB long".
        const HUGE_LEN: [u8; 6] = [0x1F, 0x8F, 0xFF, 0xFF, 0xFF, 0x7F];
        let with = |tail: &[u8]| [&HUGE_LEN[..], tail].concat();

        assert_eq!(Reader::new(&HUGE_LEN).bounded_value_length(), Some(0));
        // charset UTF-8 (0xEA) then the text: only one byte of it arrived.
        assert_eq!(Reader::new(&with(&[0xEA, b'x'])).encoded_string().as_deref(), Some("x"));
        assert_eq!(Reader::new(&with(b"text/plain\0")).content_type().unwrap().0, "text/plain");
        assert!(Reader::new(&with(b"junk")).skip_value().is_some());
        assert!(Reader::new(&HUGE_LEN).content_type().is_none());

        // Plain short length that is simply longer than what is left.
        let over = [0x1Eu8, 0xEA, b'a'];
        assert_eq!(Reader::new(&over).encoded_string().as_deref(), Some("a"));
        assert_eq!(Reader::new(&over).bounded_value_length(), Some(2));
    }

    /// 0x10 ("secure") is a No-value parameter; consuming a byte for it used
    /// to desynchronise everything after it in the list.
    #[test]
    fn no_value_parameter_does_not_eat_the_next_one() {
        let mut buf = vec![0x80 | 0x10]; // secure, no value
        buf.push(0x80 | 0x05); // name
        write_text_string(&mut buf, "p.jpg");
        let mut r = Reader::new(&buf);
        assert_eq!(r.parameter(), Some(("secure".into(), String::new())));
        assert_eq!(r.parameter(), Some(("name".into(), "p.jpg".into())));
        assert!(r.eof());
    }
}

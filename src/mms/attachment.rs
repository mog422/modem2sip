//! Attachments handed over with an MMS submission.
//!
//! A submission names each attachment either inline (`data_base64`) or as a
//! file on this host (`path`).  The second form reads a file with the
//! gateway's privileges and sends it off the box to a number the submitter
//! chose, so it is only honoured for files inside the directory the operator
//! set aside for that (`mms.attachment_dir`) - and never for a submission
//! that arrived over SIP, where the sender is whoever can reach a UDP port.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SendAttachment {
    pub content_type: String,
    #[serde(default)]
    pub name: Option<String>,
    /// base64 payload, or use `path` to read from disk.
    #[serde(default)]
    pub data_base64: Option<String>,
    /// A file inside `mms.attachment_dir`, by name or by absolute path.
    #[serde(default)]
    pub path: Option<String>,
}

/// Whether a submission may attach files that live on this host.
#[derive(Debug, Clone, Copy)]
pub enum LocalFiles<'a> {
    /// No; the reason is what the submitter is told.
    Refused(&'static str),
    /// Yes, as long as they are inside this directory.
    Under(&'a Path),
}

pub async fn load(att: &SendAttachment, files: LocalFiles<'_>) -> Result<Vec<u8>> {
    use base64::Engine as _;
    if let Some(b64) = &att.data_base64 {
        return base64::engine::general_purpose::STANDARD
            .decode(b64.trim())
            .context("decoding data_base64");
    }
    let Some(requested) = &att.path else {
        bail!("attachment needs either data_base64 or path");
    };
    let dir = match files {
        LocalFiles::Under(dir) => dir,
        LocalFiles::Refused(why) => bail!("attachment {requested} refused: {why}"),
    };
    let file = resolve_inside(dir, requested).await?;
    tokio::fs::read(&file)
        .await
        .with_context(|| format!("reading attachment {}", file.display()))
}

/// The real location of `requested`, provided it is inside `dir`.
///
/// Both sides are canonicalised first, so neither `..` nor a symbolic link
/// planted in the directory can lead out of it.  A relative name is looked up
/// inside the directory; an absolute one has to point into it already.
async fn resolve_inside(dir: &Path, requested: &str) -> Result<PathBuf> {
    let root = tokio::fs::canonicalize(dir)
        .await
        .with_context(|| format!("mms.attachment_dir {} is not usable", dir.display()))?;
    let real = tokio::fs::canonicalize(root.join(requested))
        .await
        .with_context(|| format!("attachment {requested} not found"))?;
    if !real.starts_with(&root) {
        return Err(anyhow!(
            "attachment {requested} is outside mms.attachment_dir ({})",
            root.display()
        ));
    }
    Ok(real)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn att(path: Option<&str>, data: Option<&str>) -> SendAttachment {
        SendAttachment {
            content_type: "text/plain".into(),
            name: None,
            data_base64: data.map(str::to_string),
            path: path.map(str::to_string),
        }
    }

    /// A scratch tree: `<base>/outbox/photo.jpg` is allowed, `<base>/secret`
    /// sits right next to the directory and must stay out of reach.
    fn tree(name: &str) -> (PathBuf, PathBuf) {
        let base = std::env::temp_dir()
            .join(format!("modem2sip-attach-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let outbox = base.join("outbox");
        std::fs::create_dir_all(&outbox).unwrap();
        std::fs::write(outbox.join("photo.jpg"), b"jpeg").unwrap();
        std::fs::write(base.join("secret"), b"password").unwrap();
        (base, outbox)
    }

    /// A SIP MESSAGE used to be able to name any file the process could read
    /// and have it sent to a number of the sender's choosing.
    #[tokio::test]
    async fn a_path_is_never_read_when_local_files_are_refused() {
        let (base, _) = tree("refused");
        let secret = base.join("secret");
        let err = load(&att(secret.to_str(), None), LocalFiles::Refused("not over SIP"))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("not over SIP"), "{err:#}");
        // Inline data needs no file access and is unaffected.
        let inline = load(&att(None, Some("aGk=")), LocalFiles::Refused("not over SIP")).await;
        assert_eq!(inline.unwrap(), b"hi");
    }

    #[tokio::test]
    async fn only_files_inside_the_directory_are_read() {
        let (base, outbox) = tree("inside");
        let dir = LocalFiles::Under(&outbox);

        // By name, and by an absolute path that points into the directory.
        assert_eq!(load(&att(Some("photo.jpg"), None), dir).await.unwrap(), b"jpeg");
        let absolute = outbox.join("photo.jpg");
        assert_eq!(load(&att(absolute.to_str(), None), dir).await.unwrap(), b"jpeg");

        // Everything else: an absolute path elsewhere, a climb out with `..`,
        // and a symbolic link inside the directory that points out of it.
        let secret = base.join("secret");
        std::os::unix::fs::symlink(&secret, outbox.join("link")).unwrap();
        for outside in [secret.to_str().unwrap(), "../secret", "link", "/etc/passwd"] {
            let err = load(&att(Some(outside), None), dir).await.unwrap_err();
            assert!(
                format!("{err:#}").contains("outside mms.attachment_dir"),
                "{outside} was not refused as outside: {err:#}"
            );
        }
    }
}

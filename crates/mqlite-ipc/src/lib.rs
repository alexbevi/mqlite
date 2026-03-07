use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use blake3::Hash;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

pub trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub type BoxedStream = Box<dyn AsyncStream>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerManifest {
    pub pid: u32,
    pub version: String,
    pub database_path: PathBuf,
    pub endpoint: String,
    pub fingerprint: String,
    pub idle_shutdown_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerPaths {
    pub database_path: PathBuf,
    pub manifest_path: PathBuf,
    pub endpoint: String,
    pub fingerprint: String,
}

pub fn broker_paths(database_path: impl AsRef<Path>) -> Result<BrokerPaths> {
    let database_path = normalize_path(database_path.as_ref())?;
    let fingerprint = fingerprint_for_path(&database_path);
    let file_name = database_path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .unwrap_or("database.mongodb");
    let manifest_path = database_path.with_file_name(format!("{file_name}.mqlite.json"));

    Ok(BrokerPaths {
        database_path,
        manifest_path,
        endpoint: endpoint_for_fingerprint(&fingerprint),
        fingerprint,
    })
}

pub fn write_manifest(manifest: &BrokerManifest, manifest_path: impl AsRef<Path>) -> Result<()> {
    let manifest_path = manifest_path.as_ref();
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(manifest_path, serde_json::to_vec_pretty(manifest)?)?;
    Ok(())
}

pub fn read_manifest(manifest_path: impl AsRef<Path>) -> Result<BrokerManifest> {
    let bytes = fs::read(manifest_path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn remove_manifest(manifest_path: impl AsRef<Path>) -> Result<()> {
    let manifest_path = manifest_path.as_ref();
    if manifest_path.exists() {
        fs::remove_file(manifest_path)?;
    }
    Ok(())
}

pub fn cleanup_endpoint(endpoint: &str) -> Result<()> {
    #[cfg(unix)]
    {
        let path = Path::new(endpoint);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    #[cfg(windows)]
    {
        let _ = endpoint;
    }
    Ok(())
}

pub struct IpcListener {
    #[cfg(unix)]
    inner: tokio::net::UnixListener,
    #[cfg(windows)]
    endpoint: String,
}

impl IpcListener {
    pub async fn bind(endpoint: &str) -> Result<Self> {
        cleanup_endpoint(endpoint)?;

        #[cfg(unix)]
        {
            Ok(Self {
                inner: tokio::net::UnixListener::bind(endpoint)?,
            })
        }

        #[cfg(windows)]
        {
            Ok(Self {
                endpoint: endpoint.to_string(),
            })
        }
    }

    pub async fn accept(&self) -> Result<BoxedStream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.inner.accept().await?;
            Ok(Box::new(stream))
        }

        #[cfg(windows)]
        {
            use tokio::net::windows::named_pipe::ServerOptions;

            let server = ServerOptions::new().create(&self.endpoint)?;
            server.connect().await?;
            Ok(Box::new(server))
        }
    }
}

pub async fn connect(endpoint: &str) -> Result<BoxedStream> {
    #[cfg(unix)]
    {
        let stream = tokio::net::UnixStream::connect(endpoint).await?;
        Ok(Box::new(stream))
    }

    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;

        let client = ClientOptions::new().open(endpoint)?;
        Ok(Box::new(client))
    }
}

fn normalize_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Ok(path.canonicalize()?);
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    Ok(absolute)
}

fn fingerprint_for_path(path: &Path) -> String {
    let hash: Hash = blake3::hash(path.to_string_lossy().as_bytes());
    hash.to_hex().to_string()
}

fn endpoint_for_fingerprint(fingerprint: &str) -> String {
    #[cfg(unix)]
    {
        let short_fingerprint = &fingerprint[..fingerprint.len().min(24)];
        Path::new("/tmp")
            .join(format!("mqlite-{short_fingerprint}.sock"))
            .to_string_lossy()
            .to_string()
    }

    #[cfg(windows)]
    {
        let short_fingerprint = &fingerprint[..fingerprint.len().min(24)];
        format!(r"\\.\pipe\mqlite-{short_fingerprint}")
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{
        BrokerManifest, IpcListener, broker_paths, connect, read_manifest, write_manifest,
    };

    #[test]
    fn derives_manifest_and_endpoint_paths() {
        let paths = broker_paths("/tmp/example.mongodb").expect("broker paths");
        assert!(paths.manifest_path.ends_with("example.mongodb.mqlite.json"));
        assert!(paths.endpoint.contains("mqlite-"));
        #[cfg(unix)]
        assert!(paths.endpoint.len() < 104);
    }

    #[test]
    fn round_trips_manifest() {
        let temp_dir = tempdir().expect("tempdir");
        let manifest_path = temp_dir.path().join("db.mongodb.mqlite.json");
        let manifest = BrokerManifest {
            pid: 42,
            version: "0.1.0".to_string(),
            database_path: PathBuf::from("/tmp/db.mongodb"),
            endpoint: "/tmp/mqlite.sock".to_string(),
            fingerprint: "abc".to_string(),
            idle_shutdown_secs: 30,
        };

        write_manifest(&manifest, &manifest_path).expect("write manifest");
        let loaded = read_manifest(&manifest_path).expect("read manifest");
        assert_eq!(loaded, manifest);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn accepts_and_connects_over_unix_socket() {
        let temp_dir = tempdir().expect("tempdir");
        let endpoint = temp_dir.path().join("broker.sock");
        let listener = IpcListener::bind(endpoint.to_string_lossy().as_ref())
            .await
            .expect("bind listener");

        let accept_task = tokio::spawn(async move {
            let mut server = listener.accept().await.expect("accept");
            let mut buffer = [0_u8; 4];
            server.read_exact(&mut buffer).await.expect("read");
            assert_eq!(&buffer, b"ping");
            server.write_all(b"pong").await.expect("write");
        });

        let mut client = connect(endpoint.to_string_lossy().as_ref())
            .await
            .expect("connect");
        client.write_all(b"ping").await.expect("write");
        let mut buffer = [0_u8; 4];
        client.read_exact(&mut buffer).await.expect("read");
        assert_eq!(&buffer, b"pong");

        accept_task.await.expect("join");
    }
}

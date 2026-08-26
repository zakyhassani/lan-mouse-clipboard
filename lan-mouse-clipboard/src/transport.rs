//! TLS transport for the clipboard channel.
//!
//! A dedicated TCP/TLS channel carries clipboard data between peers, on
//! the same port as the UDP input path (TCP and UDP sockets can share a
//! port number). Security mirrors the existing UDP model: the *listening*
//! side requires a client certificate and verifies its SHA-256 fingerprint
//! against the `authorized_keys` allowlist; the *connecting* side presents
//! its certificate and does not verify the server's self-signed cert (the
//! allowlist is enforced where the connection is accepted).

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    DigitallySignedStruct, DistinguishedName, Error as RlsError, ServerConfig, SignatureScheme,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub use rustls::ClientConfig;

pub type ServerStream = tokio_rustls::server::TlsStream<TcpStream>;
pub type ClientStream = tokio_rustls::client::TlsStream<TcpStream>;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("failed to load identity: {0}")]
    Identity(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] RlsError),
    #[error("peer certificate missing or unauthorized")]
    Unauthorized,
    #[error("tls handshake error: {0}")]
    Handshake(String),
}

/// A parsed TLS identity (certificate chain + private key).
pub struct Identity {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

/// Load a PEM certificate + private key bundle (e.g. the lan-mouse cert
/// file written by webrtc-dtls) into a rustls identity.
pub fn load_identity(pem: &str) -> Result<Identity, TransportError> {
    // lan-mouse's cert writer uses the non-standard `PRIVATE_KEY` label for a
    // PKCS8 EC key; rustls-pemfile only recognizes `PRIVATE KEY` (PKCS8),
    // `RSA PRIVATE KEY` (PKCS1) and `EC PRIVATE KEY` (SEC1). Normalizing the
    // underscore form keeps all three recognized.
    let pem = pem.replace("PRIVATE_KEY", "PRIVATE KEY");
    let mut certs = Vec::new();
    let mut key = None;
    for item in rustls_pemfile::read_all(&mut pem.as_bytes()) {
        let item = item.map_err(TransportError::Io)?;
        match item {
            rustls_pemfile::Item::X509Certificate(c) => certs.push(c),
            rustls_pemfile::Item::Pkcs8Key(k) => key = Some(PrivateKeyDer::Pkcs8(k)),
            rustls_pemfile::Item::Pkcs1Key(k) => key = Some(PrivateKeyDer::Pkcs1(k)),
            rustls_pemfile::Item::Sec1Key(k) => key = Some(PrivateKeyDer::Sec1(k)),
            _ => {}
        }
    }
    let key = key.ok_or_else(|| TransportError::Identity("no private key in PEM".into()))?;
    if certs.is_empty() {
        return Err(TransportError::Identity("no certificate in PEM".into()));
    }
    Ok(Identity { certs, key })
}

impl Identity {
    /// The SHA-256 fingerprint (colon-hex) of this identity's certificate,
    /// as used for the `authorized_keys` allowlist.
    pub fn fingerprint(&self) -> String {
        fingerprint_hex(&self.certs[0])
    }
}

/// SHA-256 fingerprint in the same colon-separated hex format lan-mouse uses.
pub fn fingerprint_hex(cert: &CertificateDer) -> String {
    let mut hasher = Sha256::new();
    hasher.update(cert.as_ref());
    let bytes = hasher.finalize();
    bytes
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join(":")
        .to_lowercase()
}

/// Build the *server* config: requires a client certificate, accepting any
/// at the TLS layer; authorization is enforced after the handshake by
/// checking the peer fingerprint against the allowlist (held by the listener).
pub fn server_config(identity: &Identity) -> Result<ServerConfig, TransportError> {
    let provider = default_provider()?;
    let client_verifier = AcceptAnyClientCertVerifier { provider };
    ServerConfig::builder()
        .with_client_cert_verifier(Arc::new(client_verifier))
        .with_single_cert(identity.certs.clone(), identity.key.clone_key())
        .map_err(|e| TransportError::Identity(e.to_string()))
}

/// Build the *client* config: presents our certificate and skips server
/// certificate verification (self-signed peer; authorization enforced by the
/// peer's listener).
pub fn client_config(identity: &Identity) -> Result<ClientConfig, TransportError> {
    let provider = default_provider()?;
    let server_verifier = AcceptAnyServerCertVerifier { provider };
    ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(server_verifier))
        .with_client_auth_cert(identity.certs.clone(), identity.key.clone_key())
        .map_err(|e| TransportError::Identity(e.to_string()))
}

fn default_provider() -> Result<Arc<CryptoProvider>, TransportError> {
    if let Some(p) = CryptoProvider::get_default() {
        return Ok(p.clone());
    }
    // Install the ring provider if the host crate has not already set one.
    let provider = rustls::crypto::ring::default_provider();
    let _ = provider.install_default();
    CryptoProvider::get_default()
        .cloned()
        .ok_or_else(|| TransportError::Identity("no default CryptoProvider".into()))
}

/// A TLS server accepting clipboard connections.
pub struct TlsListener {
    listener: TokioTcpListener,
    acceptor: TlsAcceptor,
    authorized: HashMap<String, String>,
}

impl TlsListener {
    pub async fn bind(
        addr: SocketAddr,
        identity: &Identity,
        authorized: HashMap<String, String>,
    ) -> Result<Self, TransportError> {
        let listener = TokioTcpListener::bind(addr).await?;
        let config = server_config(identity)?;
        Ok(Self {
            listener,
            acceptor: TlsAcceptor::from(Arc::new(config)),
            authorized,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept the next connection, verifying the peer fingerprint against
    /// the allowlist. Returns the stream, the peer address, and the verified
    /// fingerprint.
    pub async fn accept(&self) -> Result<(ServerStream, SocketAddr, String), TransportError> {
        let (tcp, peer_addr) = self.listener.accept().await?;
        let stream = self.acceptor.accept(tcp).await?;
        let peer_cert = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|c| c.first().cloned())
            .ok_or(TransportError::Unauthorized)?;
        let fp = fingerprint_hex(&peer_cert);
        if !self.authorized.contains_key(&fp) {
            return Err(TransportError::Unauthorized);
        }
        Ok((stream, peer_addr, fp))
    }
}

/// Connect to a peer's clipboard TLS listener.
pub async fn connect(
    addr: SocketAddr,
    client_config: ClientConfig,
) -> Result<ClientStream, TransportError> {
    let tcp = TcpStream::connect(addr).await?;
    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from("lan-mouse")
        .map_err(|e| TransportError::Handshake(format!("invalid server name: {e}")))?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| TransportError::Handshake(e.to_string()))
}

/// Verifier that accepts any client certificate at the TLS layer; the
/// listening side checks the fingerprint after the handshake.
#[derive(Debug)]
struct AcceptAnyClientCertVerifier {
    provider: Arc<CryptoProvider>,
}

impl ClientCertVerifier for AcceptAnyClientCertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RlsError> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Verifier that accepts any server certificate (self-signed peers).
#[derive(Debug)]
struct AcceptAnyServerCertVerifier {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AcceptAnyServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity(cn: &str) -> Identity {
        let key = rcgen::KeyPair::generate().expect("generate key");
        let params = rcgen::CertificateParams::new(vec![cn.to_string()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed cert");
        let pem = format!("{}{}", cert.pem(), key.serialize_pem());
        load_identity(&pem).expect("load identity")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tls_loopback_handshakes_and_transfers() {
        let server_id = test_identity("server");
        let client_id = test_identity("client");

        let client_cert = &client_id.certs[0];
        let client_fp = fingerprint_hex(client_cert);
        let authorized: HashMap<String, String> =
            HashMap::from([(client_fp.clone(), "client".into())]);

        let listener = TlsListener::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            &server_id,
            authorized,
        )
        .await
        .expect("bind");

        let addr = listener.local_addr().expect("local addr");
        let client_cfg = client_config(&client_id).expect("client config");

        let accept_task = tokio::spawn(async move { listener.accept().await });
        let client_stream = connect(addr, client_cfg).await.expect("connect");

        let (server_stream, _peer_addr, fp) =
            accept_task.await.expect("accept task").expect("accept");
        assert_eq!(fp, client_fp);

        let (mut s_r, mut s_w) = tokio::io::split(server_stream);
        let (mut c_r, mut c_w) = tokio::io::split(client_stream);
        tokio::io::AsyncWriteExt::write_all(&mut c_w, b"ping")
            .await
            .unwrap();
        let mut buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut s_r, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf, b"ping");

        tokio::io::AsyncWriteExt::write_all(&mut s_w, b"pong")
            .await
            .unwrap();
        let mut buf = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut c_r, &mut buf)
            .await
            .unwrap();
        assert_eq!(&buf, b"pong");
    }

    #[test]
    fn identity_requires_key_and_cert() {
        assert!(load_identity("not a pem").is_err());
    }

    #[test]
    fn loads_nonstandard_private_key_label() {
        // lan-mouse writes EC keys with the underscore form; load_identity
        // must normalize it to the standard PKCS8 label.
        let key = rcgen::KeyPair::generate().expect("generate key");
        let params = rcgen::CertificateParams::new(vec!["cn".into()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed");
        let mut pem = format!("{}{}", cert.pem(), key.serialize_pem());
        pem = pem.replace("PRIVATE KEY", "PRIVATE_KEY");
        let identity = load_identity(&pem).expect("load identity with PRIVATE_KEY");
        assert!(!identity.certs.is_empty());
    }

    #[test]
    fn fingerprint_hex_is_colon_separated_lowercase_sha256() {
        let key = rcgen::KeyPair::generate().expect("generate key");
        let params = rcgen::CertificateParams::new(vec!["cn".into()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed");
        let fp = fingerprint_hex(&cert.der().to_owned());
        let parts: Vec<&str> = fp.split(':').collect();
        assert_eq!(parts.len(), 32);
        assert!(
            parts
                .iter()
                .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_hexdigit()))
        );
        assert_eq!(fp, fp.to_lowercase());
    }

    #[test]
    fn fingerprint_is_stable_for_same_cert() {
        let key = rcgen::KeyPair::generate().expect("generate key");
        let params = rcgen::CertificateParams::new(vec!["cn".into()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed");
        let der = cert.der().to_owned();
        assert_eq!(fingerprint_hex(&der), fingerprint_hex(&der));
    }

    #[test]
    fn load_identity_rejects_pem_missing_key() {
        // A cert-only PEM has no private key.
        let key = rcgen::KeyPair::generate().expect("generate key");
        let params = rcgen::CertificateParams::new(vec!["cn".into()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed");
        let err = load_identity(&cert.pem()).err().expect("must error");
        assert!(err.to_string().contains("private key"), "unexpected: {err}");
    }

    #[test]
    fn load_identity_rejects_pem_missing_cert() {
        let key = rcgen::KeyPair::generate().expect("generate key");
        let err = load_identity(&key.serialize_pem())
            .err()
            .expect("must error");
        assert!(err.to_string().contains("certificate"), "unexpected: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn accept_rejects_unauthorized_peer() {
        let server_id = test_identity("server");
        let authorized = HashMap::new(); // no one is allowed
        let listener = TlsListener::bind(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            &server_id,
            authorized,
        )
        .await
        .expect("bind");

        let addr = listener.local_addr().expect("local addr");
        let client_id = test_identity("intruder");
        let client_cfg = client_config(&client_id).expect("client config");

        let accept_task = tokio::spawn(async move { listener.accept().await });
        // Handshake succeeds (AcceptAnyClientCert), fingerprint check must fail.
        assert!(matches!(
            connect(addr, client_cfg).await,
            Ok(_) | Err(TransportError::Tls(_))
        ));
        // accept() surfaces the authorization failure.
        assert!(matches!(
            accept_task.await.expect("task").err(),
            Some(TransportError::Unauthorized)
        ));
    }
}

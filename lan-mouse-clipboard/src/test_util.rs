//! Helpers shared by the crate's unit tests.

/// A tiny deterministic PRNG (xorshift64) so randomized tests are repeatable
/// without pulling in a RNG dependency.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    pub fn range(&mut self, hi: usize) -> usize {
        (self.next() % hi.max(1) as u64) as usize
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

/// A freshly generated self-signed certificate: (cert+key PEM, cert PEM,
/// key PEM, cert DER).
pub fn test_cert(
    cn: &str,
) -> (
    String,
    String,
    String,
    rustls::pki_types::CertificateDer<'static>,
) {
    let key = rcgen::KeyPair::generate().expect("key");
    let params = rcgen::CertificateParams::new(vec![cn.to_string()]).expect("params");
    let cert = params.self_signed(&key).expect("cert");
    let cert_pem = cert.pem();
    let key_pem = key.serialize_pem();
    (
        format!("{cert_pem}{key_pem}"),
        cert_pem,
        key_pem,
        cert.der().clone(),
    )
}

/// Identity PEM bundle (cert + key) for TLS tests.
pub fn identity_pem(cn: &str) -> String {
    test_cert(cn).0
}

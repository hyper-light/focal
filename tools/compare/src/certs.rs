//! The bench's own certificates for NATS over TLS (`focal-compare certs`): a CA and a server certificate
//! for the `nats-tls` node, written as PEM (RFC 7468) for nats-server, which reads PEM alone. The
//! clients trust only this CA (`--tls-ca`), so the row measures NATS as a production deployment runs
//! it, its connections under TLS 1.3, beside the bus, which always encrypts.

use std::path::Path;

use crate::CompareError;

/// The names the server certificate is for: the compose service a client dials.
const SERVER_NAMES: [&str; 2] = ["nats-tls", "nats"];

/// Writes `ca.pem`, `server.pem` and `server.key` (PKCS #8) into `dir`.
pub fn write(dir: &Path) -> Result<(), CompareError> {
    let failed = |error: rcgen::Error| CompareError::Target(format!("a certificate: {error}"));
    std::fs::create_dir_all(dir).map_err(|error| CompareError::Report(error.to_string()))?;
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).map_err(failed)?;
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let key = rcgen::KeyPair::generate().map_err(failed)?;
    let root = params.self_signed(&key).map_err(failed)?;
    let issuer = rcgen::Issuer::new(params, key);
    let names: Vec<String> = SERVER_NAMES.iter().map(|name| (*name).to_owned()).collect();
    let server_params = rcgen::CertificateParams::new(names).map_err(failed)?;
    let server_key = rcgen::KeyPair::generate().map_err(failed)?;
    let server = server_params
        .signed_by(&server_key, &issuer)
        .map_err(failed)?;
    let write = |name: &str, label: &str, der: &[u8]| {
        std::fs::write(dir.join(name), pem(label, der))
            .map_err(|error| CompareError::Report(error.to_string()))
    };
    write("server.pem", "CERTIFICATE", server.der())?;
    write("server.key", "PRIVATE KEY", &server_key.serialize_der())?;
    // The authority last: its presence says the directory is whole.
    write("ca.pem", "CERTIFICATE", root.der())
}

/// `der` as PEM: its base64 (RFC 4648) in lines of 64 between RFC 7468's boundaries.
fn pem(label: &str, der: &[u8]) -> String {
    /// Format: RFC 7468 §2's line length.
    const LINE: usize = 64;
    let encoded = base64(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in encoded.as_bytes().chunks(LINE) {
        out.push_str(&String::from_utf8_lossy(line));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// RFC 4648 §4's base64, padded.
fn base64(bytes: &[u8]) -> String {
    /// Format: RFC 4648 §4's alphabet.
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let symbol = |index: u32| {
        ALPHABET
            .get(usize::try_from(index & 0x3f).unwrap_or(0))
            .copied()
            .map_or('=', char::from)
    };
    let mut out = String::with_capacity(bytes.len().div_ceil(3).saturating_mul(4));
    for group in bytes.chunks(3) {
        let [a, b, c] = [0, 1, 2].map(|at| u32::from(group.get(at).copied().unwrap_or(0)));
        let word = (a << 16) | (b << 8) | c;
        out.push(symbol(word >> 18));
        out.push(symbol(word >> 12));
        out.push(if group.len() > 1 {
            symbol(word >> 6)
        } else {
            '='
        });
        out.push(if group.len() > 2 { symbol(word) } else { '=' });
    }
    out
}

#[cfg(test)]
#[allow(clippy::disallowed_macros)]
mod tests {
    use super::*;

    /// RFC 4648 §10's test vectors.
    #[test]
    fn base64_is_rfc_4648_s() {
        let vectors = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];
        for (plain, encoded) in vectors {
            assert_eq!(base64(plain.as_bytes()), encoded);
        }
    }
}

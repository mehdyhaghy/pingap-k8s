//! Listener certificate Secrets into the PEM text of Pingap certificates.
//!
//! The PEM is checked here, because Pingap rejects the whole config reload when one
//! certificate does not parse.

use k8s_openapi::api::core::v1::Secret;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

pub const TLS_CERT: &str = "tls.crt";
pub const TLS_KEY: &str = "tls.key";

/// PEM labels of the private keys Pingap loads (PKCS#8, PKCS#1, SEC1).
const KEY_LABELS: [&str; 3] = ["PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"];

/// `(tls.crt, tls.key)` of a Secret, as the PEM text to store: the `CERTIFICATE` blocks of
/// `tls.crt` (at least one) and the first private key block of `tls.key` (PKCS#1, PKCS#8 or
/// SEC1, not encrypted), re-encoded. Pingap decodes every PEM block strictly and takes the
/// first block of `tls_key` as the key, rejecting the whole config reload otherwise; so
/// anything else in the files (an `EC PARAMETERS` block before the key, as `openssl
/// ecparam -genkey` writes it, text around the blocks) is dropped here, and a block that
/// does not decode makes the Secret invalid. The error is the reason, for a condition
/// message.
pub fn certificate_from_secret(secret: &Secret) -> Result<(String, String), String> {
    let field = |name: &str| -> Result<String, String> {
        let bytes = secret
            .data
            .as_ref()
            .and_then(|d| d.get(name))
            .map(|b| b.0.clone())
            .or_else(|| {
                secret
                    .string_data
                    .as_ref()
                    .and_then(|d| d.get(name))
                    .map(|s| s.clone().into_bytes())
            })
            .ok_or_else(|| format!("missing {name}"))?;
        String::from_utf8(bytes).map_err(|_| format!("{name} is not UTF-8 PEM"))
    };
    let blocks = |name: &str, text: &str| pem::parse_many(text).map_err(|e| format!("{name}: {e}"));
    let lf = pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF);

    let certs: String = blocks(TLS_CERT, &field(TLS_CERT)?)?
        .iter()
        .filter(|b| b.tag() == "CERTIFICATE")
        .map(|b| pem::encode_config(b, lf))
        .collect();
    if certs.is_empty() {
        return Err(format!("{TLS_CERT} holds no PEM certificate"));
    }
    let key_blocks = blocks(TLS_KEY, &field(TLS_KEY)?)?;
    let private = key_blocks
        .iter()
        .find(|b| KEY_LABELS.contains(&b.tag()))
        .ok_or_else(|| format!("{TLS_KEY} holds no PEM private key"))?;
    if private.headers().iter().next().is_some() {
        return Err(format!(
            "{TLS_KEY}: encrypted private keys are not supported"
        ));
    }
    let tls_key = pem::encode_config(private, lf);

    // What Pingap checks next, on the text it gets.
    for der in CertificateDer::pem_slice_iter(certs.as_bytes()) {
        der.map_err(|e| format!("{TLS_CERT}: {e}"))?;
    }
    PrivateKeyDer::from_pem_slice(tls_key.as_bytes()).map_err(|e| format!("{TLS_KEY}: {e}"))?;
    Ok((certs, tls_key))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::OnceLock;

    use k8s_openapi::ByteString;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use super::*;

    /// PEM text of `label` around `body`. Key labels are only put together at run time, so no
    /// key-looking text is stored in the repository.
    fn pem_text(label: &str, body: &str) -> String {
        format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
    }

    /// A self-signed P-256 certificate for `example.com` and its key, generated per test run.
    struct Fixture {
        cert: String,
        key: String,
        sec1: Vec<u8>,
    }

    fn fixture() -> &'static Fixture {
        static FIXTURE: OnceLock<Fixture> = OnceLock::new();
        FIXTURE.get_or_init(|| {
            let generated = rcgen::generate_simple_self_signed(vec!["example.com".into()]).unwrap();
            let lf = pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF);
            let reencode = |text: &str| pem::encode_config(&pem::parse(text).unwrap(), lf);
            let pkcs8 = generated.signing_key.serialized_der();
            // PrivateKeyInfo: SEQUENCE { version, algorithm, OCTET STRING { ECPrivateKey } }.
            let (_, info, _) = der_tlv(pkcs8);
            let (_, _, rest) = der_tlv(info);
            let (_, _, rest) = der_tlv(rest);
            let (tag, sec1, _) = der_tlv(rest);
            assert_eq!(tag, 0x04, "PKCS#8 key without an OCTET STRING");
            Fixture {
                cert: reencode(&generated.cert.pem()),
                key: reencode(&generated.signing_key.serialize_pem()),
                sec1: sec1.to_vec(),
            }
        })
    }

    /// `(tag, content, rest)` of the first DER element of `der`.
    fn der_tlv(der: &[u8]) -> (u8, &[u8], &[u8]) {
        let (len, start) = match der[1] {
            n if n < 0x80 => (n as usize, 2),
            n => {
                let width = (n & 0x7f) as usize;
                let len = der[2..2 + width]
                    .iter()
                    .fold(0, |len, b| len << 8 | *b as usize);
                (len, 2 + width)
            }
        };
        (der[0], &der[start..start + len], &der[start + len..])
    }

    /// The generated certificate.
    pub(crate) fn cert_pem() -> &'static str {
        &fixture().cert
    }

    /// The PKCS#8 key of [`cert_pem`].
    pub(crate) fn key_pem() -> &'static str {
        &fixture().key
    }

    /// The key of [`cert_pem`] as `openssl ecparam -genkey` writes it: an EC PARAMETERS block
    /// before the SEC1 key.
    fn ec_params_key_pem() -> String {
        let sec1 = pem::Pem::new("EC PRIVATE KEY", fixture().sec1.clone());
        let lf = pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF);
        format!(
            "{}{}",
            pem_text("EC PARAMETERS", "BggqhkjOPQMBBw=="),
            pem::encode_config(&sec1, lf)
        )
    }

    pub(crate) fn tls_secret(
        ns: &str,
        name: &str,
        cert: Option<&str>,
        key: Option<&str>,
    ) -> Secret {
        let mut data = BTreeMap::new();
        if let Some(cert) = cert {
            data.insert(TLS_CERT.to_string(), ByteString(cert.as_bytes().to_vec()));
        }
        if let Some(key) = key {
            data.insert(TLS_KEY.to_string(), ByteString(key.as_bytes().to_vec()));
        }
        Secret {
            metadata: ObjectMeta {
                namespace: Some(ns.into()),
                name: Some(name.into()),
                ..Default::default()
            },
            type_: Some("kubernetes.io/tls".into()),
            data: Some(data),
            ..Default::default()
        }
    }

    #[test]
    fn valid_secret() {
        let secret = tls_secret("ns", "s", Some(cert_pem()), Some(key_pem()));
        assert_eq!(
            certificate_from_secret(&secret),
            Ok((cert_pem().to_string(), key_pem().to_string()))
        );
        // A chain (two certificates) is fine too.
        let chain = format!("{0}{0}", cert_pem());
        assert!(
            certificate_from_secret(&tls_secret("ns", "s", Some(&chain), Some(key_pem()))).is_ok()
        );
    }

    #[test]
    fn only_the_blocks_pingap_reads_are_kept() {
        // Pingap takes the first block of tls_key as the key: the EC PARAMETERS block of
        // `openssl ecparam -genkey` goes, the SEC1 key stays.
        let secret = tls_secret("ns", "s", Some(cert_pem()), Some(&ec_params_key_pem()));
        let (_, key) = certificate_from_secret(&secret).unwrap();
        assert_eq!(pem::parse(&key).unwrap().tag(), "EC PRIVATE KEY", "{key}");
        assert_eq!(pem::parse_many(&key).unwrap().len(), 1);
        // Text around the blocks (openssl's "Bag Attributes", CRLF line ends) and a key
        // in tls.crt are dropped.
        let messy = format!(
            "Bag Attributes\r\n    friendlyName: web\r\n{}{}",
            cert_pem().replace('\n', "\r\n"),
            key_pem()
        );
        let secret = tls_secret("ns", "s", Some(&messy), Some(key_pem()));
        assert_eq!(
            certificate_from_secret(&secret),
            Ok((cert_pem().to_string(), key_pem().to_string()))
        );
    }

    #[test]
    fn string_data_is_read() {
        let mut secret = tls_secret("ns", "s", None, None);
        secret.string_data = Some(BTreeMap::from([
            (TLS_CERT.to_string(), cert_pem().to_string()),
            (TLS_KEY.to_string(), key_pem().to_string()),
        ]));
        assert!(certificate_from_secret(&secret).is_ok());
    }

    #[test]
    fn invalid_secrets() {
        let body: Vec<&str> = key_pem()
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        let encrypted_key = pem_text(
            "RSA PRIVATE KEY",
            &format!(
                "Proc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,00\n\n{}",
                body.join("\n")
            ),
        );
        let unpadded_key = pem_text("PRIVATE KEY", "not a key");
        let cases = [
            ("missing cert", None, Some(key_pem()), "missing tls.crt"),
            ("missing key", Some(cert_pem()), None, "missing tls.key"),
            (
                "no cert block",
                Some(key_pem()),
                Some(key_pem()),
                "no PEM certificate",
            ),
            (
                "garbage cert",
                Some("hello"),
                Some(key_pem()),
                "no PEM certificate",
            ),
            (
                "key is a cert",
                Some(cert_pem()),
                Some(cert_pem()),
                "tls.key",
            ),
            ("garbage key", Some(cert_pem()), Some("hello"), "tls.key"),
            (
                "bad base64",
                Some("-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n"),
                Some(key_pem()),
                "tls.crt",
            ),
            // Lenient decoders read this; Pingap's does not ("Invalid padding").
            (
                "unpadded base64",
                Some("-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n"),
                Some(key_pem()),
                "tls.crt",
            ),
            (
                "unpadded key",
                Some(cert_pem()),
                Some(&unpadded_key),
                "tls.key",
            ),
            (
                "only parameters",
                Some(cert_pem()),
                Some(
                    "-----BEGIN EC PARAMETERS-----\nBggqhkjOPQMBBw==\n-----END EC PARAMETERS-----\n",
                ),
                "no PEM private key",
            ),
            (
                "encrypted key",
                Some(cert_pem()),
                Some(&encrypted_key),
                "encrypted",
            ),
        ];
        for (what, cert, key, reason_part) in cases {
            let got = certificate_from_secret(&tls_secret("ns", "s", cert, key));
            match got {
                Err(reason) => {
                    assert!(reason.contains(reason_part), "{what}: {reason}");
                }
                other => panic!("{what}: {other:?}"),
            }
        }
        let mut no_data = tls_secret("ns", "s", None, None);
        no_data.data = None;
        assert!(certificate_from_secret(&no_data).is_err());
        let mut not_utf8 = tls_secret("ns", "s", None, Some(key_pem()));
        not_utf8
            .data
            .as_mut()
            .unwrap()
            .insert(TLS_CERT.into(), ByteString(vec![0xff, 0xfe]));
        assert!(certificate_from_secret(&not_utf8).is_err());
    }
}

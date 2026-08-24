//! Fail-fast TLS material loading for HTTP and peer trust boundaries.

use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    server::WebPkiClientVerifier,
};

use crate::config::TlsFileConfig;

#[derive(Clone, Debug)]
pub struct TlsMaterial {
    pub certificate: Vec<u8>,
    pub private_key: Vec<u8>,
    pub client_ca: Option<Vec<u8>>,
}

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn load_material(
    files: &TlsFileConfig,
    require_client_ca: bool,
) -> Result<Option<TlsMaterial>> {
    files.validate(require_client_ca)?;
    if !files.enabled() {
        return Ok(None);
    }
    let certificate = std::fs::read(
        files
            .certificate_file
            .as_ref()
            .context("TLS certificate path is missing")?,
    )
    .context("failed to read TLS certificate")?;
    let private_key = std::fs::read(
        files
            .private_key_file
            .as_ref()
            .context("TLS private-key path is missing")?,
    )
    .context("failed to read TLS private key")?;
    let client_ca = files
        .client_ca_file
        .as_ref()
        .map(std::fs::read)
        .transpose()
        .context("failed to read TLS client CA")?;
    Ok(Some(TlsMaterial {
        certificate,
        private_key,
        client_ca,
    }))
}

pub fn load_rustls_server(
    files: &TlsFileConfig,
    require_client_ca: bool,
) -> Result<Option<Arc<ServerConfig>>> {
    files.validate(require_client_ca)?;
    if !files.enabled() {
        return Ok(None);
    }
    let certificate_path = files
        .certificate_file
        .as_ref()
        .context("TLS certificate path is missing")?;
    let key_path = files
        .private_key_file
        .as_ref()
        .context("TLS private-key path is missing")?;
    let certificates = CertificateDer::pem_file_iter(certificate_path)
        .context("failed to open TLS certificate")?
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse TLS certificate")?;
    let private_key =
        PrivateKeyDer::from_pem_file(key_path).context("failed to parse TLS private key")?;

    let builder = ServerConfig::builder();
    let config = if require_client_ca {
        let ca_path = files
            .client_ca_file
            .as_ref()
            .context("TLS client-CA path is missing")?;
        let authorities = CertificateDer::pem_file_iter(ca_path)
            .context("failed to open TLS client CA")?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to parse TLS client CA")?;
        let mut roots = RootCertStore::empty();
        let (accepted, rejected) = roots.add_parsable_certificates(authorities);
        if accepted == 0 || rejected != 0 {
            anyhow::bail!("TLS client CA contains invalid or no certificates");
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("failed to build TLS client verifier")?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
    }
    .context("TLS certificate and private key do not match")?;
    Ok(Some(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use rcgen::{CertifiedKey, generate_simple_self_signed};

    use super::*;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn valid_material_builds_mutually_authenticated_server_config() {
        install_crypto_provider();
        let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory =
            std::env::temp_dir().join(format!("raft-kv-tls-test-{}-{suffix}", std::process::id()));
        std::fs::create_dir(&directory).expect("temporary TLS directory");
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(["localhost".to_owned()]).expect("test certificate");
        let certificate_file = directory.join("certificate.pem");
        let private_key_file = directory.join("private-key.pem");
        let client_ca_file = directory.join("client-ca.pem");
        std::fs::write(&certificate_file, cert.pem()).expect("certificate file");
        std::fs::write(&private_key_file, signing_key.serialize_pem()).expect("private key file");
        std::fs::write(&client_ca_file, cert.pem()).expect("client CA file");
        let files = TlsFileConfig {
            certificate_file: Some(certificate_file),
            private_key_file: Some(private_key_file),
            client_ca_file: Some(client_ca_file),
        };

        assert!(load_material(&files, true).expect("material").is_some());
        assert!(
            load_rustls_server(&files, true)
                .expect("mTLS server configuration")
                .is_some()
        );

        std::fs::remove_dir_all(directory).expect("remove TLS directory");
    }

    #[test]
    fn partial_tls_configuration_fails_closed() {
        let files = TlsFileConfig {
            certificate_file: Some("certificate.pem".into()),
            private_key_file: None,
            client_ca_file: None,
        };
        assert!(load_material(&files, false).is_err());
        assert!(load_rustls_server(&files, true).is_err());
    }
}

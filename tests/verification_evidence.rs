use std::{error::Error, fs, path::Path};

use config::{Config, File};
use serde::Deserialize;

#[derive(Deserialize)]
struct Requirements {
    specification: String,
    requirement: Vec<Requirement>,
}

#[derive(Deserialize)]
struct Requirement {
    id: String,
    status: String,
    evidence: Vec<String>,
}

#[derive(Deserialize)]
struct Manifest {
    property: Vec<Property>,
}

#[derive(Deserialize)]
struct Property {
    id: String,
    status: String,
    evidence: Vec<String>,
}

fn load<T>(root: &Path, relative: &str) -> Result<T, Box<dyn Error>>
where
    T: serde::de::DeserializeOwned,
{
    Ok(Config::builder()
        .add_source(File::from(root.join(relative)))
        .build()?
        .try_deserialize()?)
}

fn verify_evidence(root: &Path, owner: &str, evidence: &[String]) -> Result<(), Box<dyn Error>> {
    assert!(!evidence.is_empty(), "{owner} has no evidence");
    for relative in evidence {
        let path = root.join(relative);
        assert!(path.exists(), "{owner} references missing {relative}");
        if relative.starts_with("verification/evidence/") {
            let record = Config::builder().add_source(File::from(path)).build()?;
            let passed = record
                .get_string("result")
                .is_ok_and(|value| value == "pass")
                || [
                    "rust_result",
                    "clippy_result",
                    "formal_result",
                    "supply_chain_result",
                    "container_result",
                ]
                .into_iter()
                .all(|key| record.get_string(key).is_ok_and(|value| value == "pass"));
            assert!(passed, "{owner} references non-passing evidence {relative}");
        }
    }
    Ok(())
}

#[test]
fn release_manifests_are_closed_and_reference_passing_evidence() -> Result<(), Box<dyn Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let requirements: Requirements = load(root, "verification/requirements.toml")?;
    let specification = fs::read_to_string(root.join(&requirements.specification))?;
    assert!(
        specification.contains("- Status: Accepted 1.0"),
        "the release specification is not accepted"
    );
    for requirement in requirements.requirement {
        assert_eq!(
            requirement.status, "verified",
            "{} is not verified",
            requirement.id
        );
        verify_evidence(root, &requirement.id, &requirement.evidence)?;
    }

    let manifest: Manifest = load(root, "verification/manifest.toml")?;
    for property in manifest.property {
        assert_eq!(
            property.status, "verified",
            "{} is not verified",
            property.id
        );
        verify_evidence(root, &property.id, &property.evidence)?;
    }
    Ok(())
}

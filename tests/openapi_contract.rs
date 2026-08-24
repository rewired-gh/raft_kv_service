use std::{fs, path::PathBuf};

use serde_json::Value;

fn document() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs/openapi.yaml");
    let source = fs::read_to_string(path).expect("read OpenAPI document");
    serde_yaml_ng::from_str(&source).expect("parse OpenAPI document")
}

fn resolve_pointer<'a>(document: &'a Value, reference: &str) -> Option<&'a Value> {
    reference
        .strip_prefix('#')
        .and_then(|pointer| document.pointer(pointer))
}

fn assert_references_resolve(document: &Value, value: &Value) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get("$ref") {
                assert!(
                    resolve_pointer(document, reference).is_some(),
                    "unresolved OpenAPI reference: {reference}"
                );
            }
            for child in map.values() {
                assert_references_resolve(document, child);
            }
        }
        Value::Array(values) => {
            for child in values {
                assert_references_resolve(document, child);
            }
        }
        _ => {}
    }
}

#[test]
fn openapi_is_machine_readable_and_all_references_resolve() {
    let document = document();
    assert_eq!(
        document.pointer("/openapi").and_then(Value::as_str),
        Some("3.1.0")
    );
    assert_references_resolve(&document, &document);
}

#[test]
fn generated_operation_matrix_matches_the_v1_http_contract() {
    let document = document();
    let path = document
        .pointer("/paths/~1v1~1keys~1{key}")
        .and_then(Value::as_object)
        .expect("versioned key path");
    let expected = [
        ("get", ["200", "404", "503"].as_slice()),
        (
            "put",
            [
                "201", "204", "400", "409", "412", "413", "415", "428", "503",
            ]
            .as_slice(),
        ),
        (
            "delete",
            ["204", "400", "409", "412", "428", "503"].as_slice(),
        ),
    ];
    for (method, statuses) in expected {
        let operation = path
            .get(method)
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("missing {method} operation"));
        let responses = operation
            .get("responses")
            .and_then(Value::as_object)
            .expect("responses");
        assert_eq!(
            responses.keys().map(String::as_str).collect::<Vec<_>>(),
            statuses,
            "response contract drift for {method}"
        );
    }

    assert_eq!(
        document
            .pointer("/components/schemas/Problem/required")
            .and_then(Value::as_array)
            .expect("problem required fields")
            .len(),
        5
    );
    assert_eq!(
        document
            .pointer("/components/headers/CacheControl/schema/const")
            .and_then(Value::as_str),
        Some("no-store")
    );
    assert_eq!(
        document
            .pointer("/components/responses/PreconditionFailed/headers/Raft-KV-Revision/$ref")
            .and_then(Value::as_str),
        Some("#/components/headers/Revision")
    );
}

#[test]
fn operational_membership_contract_is_versioned_and_authenticated() {
    let document = document();
    let path = document
        .pointer("/paths/~1operational~1v1~1membership")
        .and_then(Value::as_object)
        .expect("operational membership path");
    for method in ["get", "put"] {
        let operation = path[method].as_object().expect("operation");
        assert!(
            operation.get("security").is_some(),
            "{method} requires auth"
        );
    }
    assert_eq!(
        document
            .pointer("/components/securitySchemes/OperationalBearer/scheme")
            .and_then(Value::as_str),
        Some("bearer")
    );
}

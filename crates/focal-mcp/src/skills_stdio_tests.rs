//! Skills over MCP through the real stdio server (SEP-2640): the extension
//! declared on the modern profile, listing, lookup, reading with the digests
//! the manifest names, refusals, the legacy profile's resources and code
//! mode's search.
use super::*;

fn sha256(text: &str) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, text.as_bytes());
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    format!("sha256:{hex}")
}

#[test]
fn the_modern_profile_serves_skills_with_manifests_that_match_the_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    runner.rpc(1, "server/discover", json!({}));
    let discovered = runner.response(1);
    let capabilities = &discovered["result"]["capabilities"];
    assert!(capabilities["resources"].is_object(), "{discovered}");
    assert!(
        capabilities["extensions"]["io.modelcontextprotocol/skills"].is_object(),
        "{discovered}"
    );
    assert!(
        capabilities["extensions"]["io.modelcontextprotocol/skills"]
            .get("directoryRead")
            .is_none(),
        "no directory reads are offered"
    );
    runner.rpc(2, "skills/list", json!({}));
    let listed = runner.response(2)["result"].clone();
    assert_eq!(listed["resultType"], "complete");
    assert_eq!(listed["cacheScope"], "public");
    assert!(listed["ttlMs"].is_u64());
    let skills = listed["skills"].as_array().unwrap();
    assert_eq!(skills.len(), 5);
    let mut id = 10;
    for skill in skills {
        let uri = skill["uri"].as_str().unwrap();
        let name = skill["frontmatter"]["name"].as_str().unwrap();
        assert_eq!(uri, format!("skill://{name}/SKILL.md"));
        // Every file the manifest names reads back as exactly those bytes.
        for resource in skill["resources"].as_array().unwrap() {
            id += 1;
            runner.rpc(id, "resources/read", json!({"uri": resource["uri"]}));
            let read = runner.response(id)["result"].clone();
            assert_eq!(read["resultType"], "complete");
            let content = &read["contents"][0];
            assert_eq!(content["uri"], resource["uri"]);
            assert_eq!(content["mimeType"], "text/markdown");
            let text = content["text"].as_str().unwrap();
            assert_eq!(resource["size"], text.len());
            assert_eq!(resource["digest"], sha256(text));
        }
        // The frontmatter is the file's, field for field.
        id += 1;
        runner.rpc(id, "skills/get", json!({"uri": uri}));
        let got = runner.response(id)["result"].clone();
        assert_eq!(got["skill"], *skill);
    }
    // Refusals: an unknown skill, a file outside every skill, a cursor this
    // server never issued, and a directory read it never offered.
    for (method, params, code) in [
        (
            "skills/get",
            json!({"uri": "skill://nothing/SKILL.md"}),
            -32602,
        ),
        (
            "resources/read",
            json!({"uri": "skill://focal-claims/../manifest.json"}),
            -32602,
        ),
        (
            "resources/read",
            json!({"uri": "file:///etc/passwd"}),
            -32602,
        ),
        ("skills/list", json!({"cursor": "forged"}), -32602),
        (
            "resources/directory/read",
            json!({"uri": "skill://focal-claims"}),
            -32601,
        ),
    ] {
        id += 1;
        runner.rpc(id, method, params.clone());
        let refused = runner.response(id);
        assert_eq!(
            refused["error"]["code"], code,
            "{method} {params}: {refused}"
        );
    }
    // Code mode's search reads the same skills.
    id += 1;
    let found = runner.tool_response(
        id,
        "code.search",
        json!({"program": "return skills.map(s => [s.name, s.files.length]).sort();"}),
    );
    let found = application(&found);
    let OperationOutput::Code { outcome, .. } = &found.result else {
        panic!("{found:?}")
    };
    let focal_client::operations::CodeOutcome::Returned { value } = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(
        value,
        &json!([
            ["focal-claims", 2],
            ["focal-cluster", 2],
            ["focal-evidence", 2],
            ["focal-peers", 3],
            ["focal-validation", 2]
        ])
    );
    runner.stop();
}

#[test]
fn a_legacy_client_reads_the_skill_files_as_resources() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    runner.send(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"legacy","version":"1"}}}));
    let initialized = runner.response(1);
    assert!(initialized["result"]["capabilities"]["resources"].is_object());
    assert!(
        initialized["result"]["capabilities"]
            .get("extensions")
            .is_none()
    );
    runner.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    runner.send(json!({"jsonrpc":"2.0","id":2,"method":"resources/list","params":{}}));
    let listed = runner.response(2)["result"].clone();
    let uris: Vec<&str> = listed["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|resource| resource["uri"].as_str().unwrap())
        .collect();
    assert_eq!(uris.len(), 11);
    assert!(uris.contains(&"skill://focal-peers/references/peer-workflows.md"));
    assert!(
        listed.get("resultType").is_none(),
        "legacy results carry no resultType"
    );
    runner.send(json!({"jsonrpc":"2.0","id":3,"method":"resources/read","params":{"uri":"skill://focal-claims/SKILL.md"}}));
    let read = runner.response(3)["result"].clone();
    assert!(
        read["contents"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("---\nname: focal-claims\n")
    );
    // The extension's own methods belong to the modern profile.
    runner.send(json!({"jsonrpc":"2.0","id":4,"method":"skills/list","params":{}}));
    assert_eq!(runner.response(4)["error"]["code"], -32601);
    runner.stop();
}

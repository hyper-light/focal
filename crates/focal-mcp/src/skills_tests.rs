use super::*;
use std::collections::BTreeSet;
use std::path::Path;

fn root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills")
}
fn walk(directory: &Path, found: &mut BTreeSet<String>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, found);
        } else {
            let relative = path.strip_prefix(root()).unwrap();
            found.insert(relative.to_str().unwrap().replace('\\', "/"));
        }
    }
}

#[test]
fn the_binary_serves_exactly_the_skill_files_in_the_tree() {
    let mut found = BTreeSet::new();
    walk(&root(), &mut found);
    assert!(
        found.remove("manifest.json"),
        "the release manifest is not a skill file"
    );
    let embedded: BTreeSet<String> = FILES.iter().map(|file| file.path.to_owned()).collect();
    assert_eq!(
        embedded, found,
        "add or remove the file in skills.rs's FILES"
    );
    for file in FILES {
        assert_eq!(file.bytes, std::fs::read(root().join(file.path)).unwrap());
    }
}

#[test]
fn each_skill_lists_its_frontmatter_and_a_complete_manifest_of_its_own_files() {
    let skills = Skills::embedded().unwrap();
    let names: BTreeSet<&str> = skills
        .list()
        .iter()
        .map(|skill| skill.frontmatter["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "focal-claims",
            "focal-cluster",
            "focal-evidence",
            "focal-peers",
            "focal-validation"
        ])
    );
    for skill in skills.list() {
        let name = skill.frontmatter["name"].as_str().unwrap();
        assert_eq!(skill.uri, format!("skill://{name}/SKILL.md"));
        assert!(
            !skill.frontmatter["description"]
                .as_str()
                .unwrap()
                .is_empty()
        );
        assert!(
            skill
                .resources
                .iter()
                .any(|resource| resource.uri == skill.uri)
        );
        let root = format!("skill://{name}/");
        for resource in &skill.resources {
            // Every file lies within the skill, and its digest and size are
            // those of the bytes served.
            assert!(resource.uri.starts_with(&root), "{}", resource.uri);
            let text = skills.read(&resource.uri).unwrap();
            assert_eq!(resource.size, text.len());
            let expected = format!("sha256:{}", hex(sha256(text.as_bytes()).as_ref()));
            assert_eq!(resource.digest, expected);
        }
        // Every relative link the skill makes resolves within its manifest.
        let text = skills.read(&skill.uri).unwrap();
        for target in text
            .split("](")
            .skip(1)
            .filter_map(|rest| rest.split(')').next())
        {
            let path = target.split('#').next().unwrap();
            if path.is_empty() || path.contains("://") {
                continue;
            }
            let uri = format!("{root}{path}");
            assert!(
                skill.resources.iter().any(|resource| resource.uri == uri),
                "{name} links {target}, outside its manifest"
            );
        }
        assert_eq!(skills.get(&skill.uri).unwrap().uri, skill.uri);
    }
    assert!(
        skills
            .get("skill://focal-claims/references/workflow-contract.md")
            .is_none()
    );
    assert!(
        skills
            .read("skill://focal-claims/../manifest.json")
            .is_none()
    );
    assert!(skills.read("file:///etc/passwd").is_none());
    assert!(skills.bytes() > 0);
}

/// A skill's frontmatter is parsed under a budget with no aliases or
/// anchors: a header that names nodes by alias is refused, not expanded.
#[test]
fn frontmatter_that_expands_by_aliases_is_refused() {
    let mut header = String::from("---\nname: bomb\ndescription: d\na: &a [1,1,1,1,1,1,1,1,1]\n");
    for (level, previous) in ["b", "c", "d", "e"].iter().zip(["a", "b", "c", "d"]) {
        let refs = vec![format!("*{previous}"); 9].join(",");
        header.push_str(&format!("{level}: &{level} [{refs}]\n"));
    }
    header.push_str("---\nbody\n");
    assert!(super::parse_frontmatter("bomb", header.as_bytes()).is_err());
    assert!(
        super::parse_frontmatter("plain", b"---\nname: plain\ndescription: d\n---\nbody\n").is_ok()
    );
}

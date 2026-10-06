//! Skills over MCP (the `io.modelcontextprotocol/skills` extension, SEP-2640,
//! on revision 2026-07-28): the skills this binary was built with, served as
//! resources under `skill://`, each listed with its frontmatter and a
//! manifest of every file's SHA-256 digest and size computed from the bytes
//! served. Each skill directory is self-contained (Agent Skills): every file
//! a skill links lies within it, so a host that reads only within a skill's
//! manifest finds everything the skill names.
use crate::ProtocolError;
use serde::Serialize;
use serde_json::{Map, Value};

/// One file of a packaged skill, its path relative to `skills/`.
struct File {
    path: &'static str,
    bytes: &'static [u8],
}
macro_rules! file {
    ($path:literal) => {
        File {
            path: $path,
            bytes: include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../skills/", $path)),
        }
    };
}
/// Every file under `skills/` but the release manifest (a test holds this
/// list to the directory).
const FILES: &[File] = &[
    file!("focal-claims/SKILL.md"),
    file!("focal-claims/references/workflow-contract.md"),
    file!("focal-cluster/SKILL.md"),
    file!("focal-cluster/references/admin-workflow.md"),
    file!("focal-evidence/SKILL.md"),
    file!("focal-evidence/references/workflow-contract.md"),
    file!("focal-peers/SKILL.md"),
    file!("focal-peers/references/peer-workflows.md"),
    file!("focal-peers/references/workflow-contract.md"),
    file!("focal-validation/SKILL.md"),
    file!("focal-validation/references/workflow-contract.md"),
];
const SCHEME: &str = "skill://";
const SKILL_FILE: &str = "SKILL.md";

/// One file's entry in a skill's manifest.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Resource {
    pub uri: String,
    pub digest: String,
    pub size: usize,
}
/// One skill as `skills/list` and `skills/get` describe it.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Skill {
    pub uri: String,
    pub frontmatter: Map<String, Value>,
    pub resources: Vec<Resource>,
}

/// The served skills, built once from the embedded files.
pub(crate) struct Skills {
    skills: Vec<Skill>,
}
impl Skills {
    pub(crate) fn embedded() -> Result<Self, ProtocolError> {
        let mut skills = Vec::new();
        for skill in FILES
            .iter()
            .filter_map(|file| file.path.strip_suffix("/SKILL.md"))
        {
            let root = format!("{skill}/");
            let mut resources = Vec::new();
            let mut frontmatter = None;
            for file in FILES.iter().filter(|file| file.path.starts_with(&root)) {
                if file.path.get(root.len()..) == Some(SKILL_FILE) {
                    frontmatter = Some(parse_frontmatter(skill, file.bytes)?);
                }
                resources.push(Resource {
                    uri: format!("{SCHEME}{}", file.path),
                    digest: format!("sha256:{}", hex(sha256(file.bytes).as_ref())),
                    size: file.bytes.len(),
                });
            }
            skills.push(Skill {
                uri: format!("{SCHEME}{root}{SKILL_FILE}"),
                frontmatter: frontmatter.ok_or(ProtocolError::Limits)?,
                resources,
            });
        }
        Ok(Self { skills })
    }
    pub(crate) fn list(&self) -> &[Skill] {
        &self.skills
    }
    pub(crate) fn get(&self, uri: &str) -> Option<&Skill> {
        self.skills.iter().find(|skill| skill.uri == uri)
    }
    /// A served file's text, by its URI.
    pub(crate) fn read(&self, uri: &str) -> Option<&'static str> {
        let path = uri.strip_prefix(SCHEME)?;
        FILES
            .iter()
            .find(|file| file.path == path)
            .and_then(|file| std::str::from_utf8(file.bytes).ok())
    }
    /// Every served file's URI and name, for `resources/list`.
    pub(crate) fn files(&self) -> impl Iterator<Item = (String, &'static str)> + '_ {
        FILES.iter().map(|file| {
            let name = file.path.rsplit('/').next().unwrap_or(file.path);
            (format!("{SCHEME}{}", file.path), name)
        })
    }
    /// The bytes the index holds beyond the embedded files.
    pub(crate) fn bytes(&self) -> usize {
        self.skills
            .iter()
            .map(|skill| {
                skill
                    .resources
                    .iter()
                    .map(|resource| resource.uri.len().saturating_add(resource.digest.len()))
                    .fold(skill.uri.len(), usize::saturating_add)
                    .saturating_add(
                        serde_json::to_string(&skill.frontmatter).map_or(0, |text| text.len()),
                    )
            })
            .fold(0, usize::saturating_add)
    }
    /// The skills as code mode's search sees them: each skill's frontmatter
    /// and every file's text, as JSON.
    pub(crate) fn search_listing(&self) -> Result<String, ProtocolError> {
        let entries: Vec<Value> = self
            .skills
            .iter()
            .map(|skill| {
                serde_json::json!({
                    "uri": skill.uri,
                    "name": skill.frontmatter.get("name"),
                    "description": skill.frontmatter.get("description"),
                    "files": skill
                        .resources
                        .iter()
                        .map(|resource| serde_json::json!({
                            "uri": resource.uri,
                            "text": self.read(&resource.uri),
                        }))
                        .collect::<Vec<_>>(),
                })
            })
            .collect();
        serde_json::to_string(&entries).map_err(|_| ProtocolError::Encode)
    }
}

/// The YAML frontmatter of a `SKILL.md`: every field, unchanged, whose
/// `name` is the skill's directory (Agent Skills; SEP-2640).
fn parse_frontmatter(skill: &str, bytes: &[u8]) -> Result<Map<String, Value>, ProtocolError> {
    let text = std::str::from_utf8(bytes).map_err(|_| ProtocolError::Limits)?;
    let header = text
        .strip_prefix("---\n")
        .and_then(|rest| rest.split_once("\n---\n"))
        .map(|(header, _)| header)
        .ok_or(ProtocolError::Limits)?;
    // The skills are compiled in, but their frontmatter is parsed as any
    // YAML focal reads: no aliases or anchors, within the header's bytes.
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {max_depth:8,max_events:1024,max_nodes:512,max_total_scalar_bytes:header.len(),max_aliases:0,max_anchors:0,max_documents:1},
    };
    let value: Value =
        serde_saphyr::from_str_with_options(header, options).map_err(|_| ProtocolError::Limits)?;
    let Value::Object(frontmatter) = value else {
        return Err(ProtocolError::Limits);
    };
    let described = frontmatter
        .get("description")
        .and_then(Value::as_str)
        .is_some_and(|description| !description.is_empty());
    if frontmatter.get("name").and_then(Value::as_str) != Some(skill) || !described {
        return Err(ProtocolError::Limits);
    }
    Ok(frontmatter)
}

fn sha256(bytes: &[u8]) -> aws_lc_rs::digest::Digest {
    aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
#[path = "skills_tests.rs"]
mod tests;

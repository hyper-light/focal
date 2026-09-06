use crate::input::*;
use focal_model::*;
use focal_wire::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraversalDocument {
    pub roots: Vec<String>,
    #[serde(default = "forward")]
    pub direction: String,
    #[serde(default)]
    pub edges: Vec<String>,
    #[serde(default = "depth")]
    pub depth: u16,
    #[serde(default = "nodes")]
    pub max_nodes: u32,
    #[serde(default = "edges")]
    pub max_edges: u32,
    #[serde(default = "super::documents::page_items")]
    pub limit: u32,
    #[serde(default = "super::documents::page_visits")]
    pub max_visits: u32,
    #[serde(default = "bytes")]
    pub max_bytes: u32,
    #[serde(default)]
    pub cursor: Option<String>,
}
fn forward() -> String {
    "forward".into()
}
fn depth() -> u16 {
    8
}
fn nodes() -> u32 {
    4096
}
fn edges() -> u32 {
    16_384
}
fn bytes() -> u32 {
    1024 * 1024
}
impl TraversalDocument {
    pub fn build(self, context: &BuildContext) -> Result<TraversalRequest, InputError> {
        if self.roots.is_empty() || self.roots.len() > MAX_TRAVERSAL_ROOTS || self.edges.len() > 32
        {
            return Err(InputError::Capacity);
        }
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(self.roots.len())
            .map_err(|_| InputError::Capacity)?;
        for root in &self.roots {
            let (kind, id) = root
                .split_once(':')
                .ok_or(InputError::Invalid("root must be kind:id"))?;
            let kind = match kind {
                "claim" => ObjectKind::Claim,
                "testament" => ObjectKind::Testament,
                "artifact" => ObjectKind::Artifact,
                "validation" => ObjectKind::Validation,
                _ => return Err(InputError::Invalid("unknown traversal root kind")),
            };
            roots.push(ObjectRef {
                ledger: context.ledger,
                kind,
                id: ObjectId(parse_id(id)?),
            });
        }
        roots.sort();
        roots.dedup();
        let mut edges = Vec::new();
        edges
            .try_reserve_exact(self.edges.len())
            .map_err(|_| InputError::Capacity)?;
        for edge in &self.edges {
            edges.push(match edge.as_str() {
                "requirement" => TraversalEdge::Requirement,
                "testament_of" => TraversalEdge::TestamentOf,
                "evidence" => TraversalEdge::Evidence,
                "artifact_input" => TraversalEdge::ArtifactInput,
                "validation_of" => TraversalEdge::ValidationOf,
                kind => TraversalEdge::Authored(parse_relation(kind)?),
            });
        }
        edges.sort();
        edges.dedup();
        let cursor = self
            .cursor
            .map(|text| {
                if text.is_empty()
                    || text.len() > MAX_TRAVERSAL_CURSOR_BYTES.saturating_mul(2)
                    || !text.len().is_multiple_of(2)
                {
                    return Err(InputError::Invalid("traversal cursor"));
                }
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(text.len() / 2)
                    .map_err(|_| InputError::Capacity)?;
                for pair in text.as_bytes().chunks_exact(2) {
                    let pair = std::str::from_utf8(pair)
                        .map_err(|_| InputError::Invalid("traversal cursor"))?;
                    bytes.push(
                        u8::from_str_radix(pair, 16)
                            .map_err(|_| InputError::Invalid("traversal cursor"))?,
                    );
                }
                Ok(TraversalCursor { bytes })
            })
            .transpose()?;
        let query = TraversalRequest {
            roots,
            direction: match self.direction.as_str() {
                "forward" => TraversalDirection::Forward,
                "reverse" => TraversalDirection::Reverse,
                _ => return Err(InputError::Invalid("traversal direction")),
            },
            edges,
            max_depth: self.depth,
            max_nodes: self.max_nodes,
            max_edges: self.max_edges,
            max_items: self.limit,
            max_visits: self.max_visits,
            max_bytes: self.max_bytes,
            cursor,
        };
        query
            .validate(context.ledger, &WireLimits::default())
            .map_err(|_| InputError::Invalid("traversal bounds"))?;
        Ok(query)
    }
}

use crate::{
    EncodedFrame, LEGACY_VERSION, Limits, MODERN_VERSION, ProtocolError,
    codec::{BoundedWriter, encode},
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::{Map, Value};

#[derive(Debug, Serialize)]
pub struct ServerInfo {
    pub name: String,
    pub version: String,
}
#[derive(Debug)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub read_only: bool,
    pub destructive: bool,
    pub idempotent: bool,
}
impl Serialize for Tool {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(5))?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("description", &self.description)?;
        map.serialize_entry("inputSchema", &self.input_schema)?;
        map.serialize_entry("outputSchema", &self.output_schema)?;
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Annotations {
            read_only_hint: bool,
            destructive_hint: bool,
            idempotent_hint: bool,
            open_world_hint: bool,
        }
        map.serialize_entry(
            "annotations",
            &Annotations {
                read_only_hint: self.read_only,
                destructive_hint: self.destructive,
                idempotent_hint: self.idempotent,
                open_world_hint: false,
            },
        )?;
        map.end()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallToken(u64);
pub struct ToolCall {
    pub token: CallToken,
    pub tool: String,
    pub arguments: Map<String, Value>,
    _allocation: Allocation,
}
impl std::fmt::Debug for ToolCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolCall")
            .field("token", &self.token)
            .field("tool", &self.tool)
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub enum Action {
    Reply(EncodedFrame),
    Call(ToolCall),
    Cancel(CallToken),
    NoReply,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Profile {
    Modern,
    Legacy,
}
impl Profile {
    fn result_type(self) -> Option<&'static str> {
        if self == Self::Modern {
            Some("complete")
        } else {
            None
        }
    }
}
#[derive(Debug)]
struct Active {
    token: CallToken,
    id: Value,
    profile: Profile,
    cancelled: bool,
}

pub struct Protocol {
    limits: Limits,
    budget: MemoryBudget,
    info: ServerInfo,
    tools: Vec<Tool>,
    catalog_hash: blake3::Hash,
    active: Vec<Active>,
    next: u64,
    legacy_initialized: bool,
    legacy_ready: bool,
    cursor_key: [u8; 32],
    clock_origin: std::time::Instant,
    clock_ms: u64,
    _workspace: Allocation,
    _catalog: Allocation,
    _active: Allocation,
}
impl Protocol {
    pub fn new(
        limits: Limits,
        budget: MemoryBudget,
        info: ServerInfo,
        mut tools: Vec<Tool>,
    ) -> Result<Self, ProtocolError> {
        let limits = limits.validate()?;
        if tools.len() > limits.max_tools
            || info.name.is_empty()
            || info.name.len() > 128
            || info.version.len() > 128
        {
            return Err(ProtocolError::Limits);
        }
        let workspace = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                limits.workspace()?,
            )?
            .commit();
        tools.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let mut size = std::mem::size_of::<Tool>()
            .checked_mul(tools.capacity())
            .and_then(|n| n.checked_add(info.name.capacity()))
            .and_then(|n| n.checked_add(info.version.capacity()))
            .ok_or(ProtocolError::Capacity)?;
        let mut previous = None;
        let mut hasher = blake3::Hasher::new();
        for tool in &tools {
            if tool.name.is_empty()
                || tool.name.len() > 128
                || !tool
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
                || tool.description.len() > limits.max_frame_bytes
                || previous == Some(tool.name.as_str())
                || tool.input_schema.get("type").and_then(Value::as_str) != Some("object")
                || !tool.output_schema.is_object()
            {
                return Err(ProtocolError::Limits);
            }
            previous = Some(&tool.name);
            let mut nodes = limits.max_nodes;
            size = size
                .checked_add(value_cost(
                    &tool.input_schema,
                    0,
                    limits.max_depth,
                    &mut nodes,
                )?)
                .and_then(|n| n.checked_add(tool.name.capacity()))
                .and_then(|n| n.checked_add(tool.description.capacity()))
                .ok_or(ProtocolError::Capacity)?;
            size = size
                .checked_add(value_cost(
                    &tool.output_schema,
                    0,
                    limits.max_depth,
                    &mut nodes,
                )?)
                .ok_or(ProtocolError::Capacity)?;
            struct HashWriter<'a>(&'a mut blake3::Hasher);
            impl std::io::Write for HashWriter<'_> {
                fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                    self.0.update(b);
                    Ok(b.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            serde_json::to_writer(HashWriter(&mut hasher), tool)
                .map_err(|_| ProtocolError::Encode)?;
        }
        let catalog = budget
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, size)?
            .commit();
        let active_bytes = limits
            .max_active_calls
            .checked_mul(
                limits
                    .max_id_bytes
                    .checked_add(256)
                    .ok_or(ProtocolError::Capacity)?,
            )
            .ok_or(ProtocolError::Capacity)?;
        let active_charge = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, active_bytes)?
            .commit();
        let mut active = Vec::new();
        active
            .try_reserve_exact(limits.max_active_calls)
            .map_err(|_| ProtocolError::Capacity)?;
        let mut cursor_key = [0; 32];
        getrandom::fill(&mut cursor_key).map_err(|_| ProtocolError::Dependency)?;
        Ok(Self {
            limits,
            budget,
            info,
            tools,
            catalog_hash: hasher.finalize(),
            active,
            next: 0,
            legacy_initialized: false,
            legacy_ready: false,
            cursor_key,
            clock_origin: std::time::Instant::now(),
            clock_ms: 0,
            _workspace: workspace,
            _catalog: catalog,
            _active: active_charge,
        })
    }
    pub fn active_calls(&self) -> usize {
        self.active.len()
    }
    pub fn receive(&mut self, bytes: &[u8]) -> Result<Action, ProtocolError> {
        let now = u64::try_from(self.clock_origin.elapsed().as_millis())
            .map_err(|_| ProtocolError::Capacity)?;
        self.receive_at(bytes, now)
    }
    /// Monotone process-local milliseconds; useful for deterministic hosts and tests.
    /// Mixing clock origins never extends an already-issued cursor's lifetime.
    pub fn receive_at(&mut self, bytes: &[u8], now_ms: u64) -> Result<Action, ProtocolError> {
        self.clock_ms = self.clock_ms.max(now_ms);
        if bytes.len() > self.limits.max_frame_bytes {
            return Err(ProtocolError::Frame);
        }
        let value = match crate::json::parse(bytes, self.limits) {
            Ok(v) => v,
            Err(()) => return self.error(Value::Null, -32700, "Parse error", None),
        };
        let Value::Object(mut object) = value else {
            return self.error(Value::Null, -32600, "Invalid request", None);
        };
        let id = object.remove("id");
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || object.contains_key("result")
            || object.contains_key("error")
        {
            return self.error(Value::Null, -32600, "Invalid request", None);
        }
        let method = match object.remove("method") {
            Some(Value::String(m)) => m,
            _ => return self.error(Value::Null, -32600, "Invalid request", None),
        };
        let params = object.remove("params").unwrap_or(Value::Object(Map::new()));
        if id.is_none() {
            return Ok(self.notification(&method, &params));
        }
        let id = id.ok_or(ProtocolError::Dependency)?;
        if !valid_id(&id, self.limits.max_id_bytes) {
            return self.error(Value::Null, -32600, "Invalid request ID", None);
        }
        if self.active.iter().any(|v| v.id == id) {
            return Err(ProtocolError::DuplicateId);
        }
        let Value::Object(mut params) = params else {
            return self.error(id, -32602, "Invalid parameters", None);
        };
        if method == "initialize" {
            return self.initialize(id, &params);
        }
        if params.get("_meta").is_some_and(|m| !m.is_object()) {
            return self.error(id, -32602, "Invalid protocol metadata", None);
        }
        let profile = if let Some(meta) = params
            .get("_meta")
            .and_then(Value::as_object)
            .filter(|m| m.keys().any(|k| k.starts_with("io.modelcontextprotocol/")))
        {
            let Some(Value::String(version)) = meta.get("io.modelcontextprotocol/protocolVersion")
            else {
                return self.error(id, -32602, "Invalid protocol metadata", None);
            };
            if version != MODERN_VERSION {
                #[derive(Serialize)]
                struct Versions<'a> {
                    supported: [&'static str; 2],
                    requested: &'a str,
                }
                let data = serde_json::to_value(Versions {
                    supported: [MODERN_VERSION, LEGACY_VERSION],
                    requested: version,
                })
                .map_err(|_| ProtocolError::Encode)?;
                return self.error(id, -32022, "Unsupported protocol version", Some(data));
            }
            if !meta.keys().all(|k| valid_meta_key(k))
                || !meta
                    .get("io.modelcontextprotocol/clientCapabilities")
                    .is_some_and(valid_capabilities)
                || !meta
                    .get("io.modelcontextprotocol/clientInfo")
                    .is_none_or(valid_info)
                || !meta
                    .get("progressToken")
                    .is_none_or(|v| v.is_string() || v.is_number())
            {
                return self.error(id, -32602, "Invalid protocol metadata", None);
            }
            Profile::Modern
        } else if self.legacy_ready || (method == "ping" && self.legacy_initialized) {
            Profile::Legacy
        } else {
            return self.error(
                id,
                -32602,
                "Protocol metadata or initialization required",
                None,
            );
        };
        params.remove("_meta");
        match method.as_str() {
            "server/discover" if profile == Profile::Modern => {
                if !params.is_empty() {
                    return self.error(id, -32602, "Invalid parameters", None);
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct Discovery<'a> {
                    result_type: &'static str,
                    supported_versions: [&'static str; 2],
                    capabilities: Capabilities,
                    _meta: ServerMeta<'a>,
                    ttl_ms: u64,
                    cache_scope: &'static str,
                }
                self.reply(
                    id,
                    &Discovery {
                        result_type: "complete",
                        supported_versions: [MODERN_VERSION, LEGACY_VERSION],
                        capabilities: Capabilities::default(),
                        _meta: ServerMeta { info: &self.info },
                        ttl_ms: 0,
                        cache_scope: "private",
                    },
                )
            }
            "ping" if profile == Profile::Legacy => {
                if !params.is_empty() {
                    return self.error(id, -32602, "Invalid parameters", None);
                }
                self.reply(id, &Map::<String, Value>::new())
            }
            "tools/list" => self.list(id, profile, params),
            "tools/call" => self.call(id, profile, params),
            _ => self.error(id, -32601, "Method not found", None),
        }
    }
    fn initialize(
        &mut self,
        id: Value,
        params: &Map<String, Value>,
    ) -> Result<Action, ProtocolError> {
        if self.legacy_initialized
            || !params.get("protocolVersion").is_some_and(Value::is_string)
            || !params.get("capabilities").is_some_and(valid_capabilities)
            || !params.get("clientInfo").is_some_and(valid_info)
        {
            return self.error(id, -32602, "Invalid initialization", None);
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Init<'a> {
            protocol_version: &'static str,
            capabilities: Capabilities,
            server_info: &'a ServerInfo,
        }
        let action = self.reply(
            id,
            &Init {
                protocol_version: LEGACY_VERSION,
                capabilities: Capabilities::default(),
                server_info: &self.info,
            },
        )?;
        self.legacy_initialized = true;
        Ok(action)
    }
    fn notification(&mut self, method: &str, params: &Value) -> Action {
        match method {
            "notifications/initialized"
                if self.legacy_initialized
                    && params
                        .as_object()
                        .is_some_and(|p| p.keys().all(|k| k == "_meta")) =>
            {
                self.legacy_ready = true;
                Action::NoReply
            }
            "notifications/cancelled" => {
                if params.get("reason").is_some_and(|v| !v.is_string()) {
                    return Action::NoReply;
                }
                let Some(id) = params
                    .get("requestId")
                    .filter(|v| valid_id(v, self.limits.max_id_bytes))
                else {
                    return Action::NoReply;
                };
                if let Some(active) = self.active.iter_mut().find(|v| v.id == *id && !v.cancelled) {
                    active.cancelled = true;
                    Action::Cancel(active.token)
                } else {
                    Action::NoReply
                }
            }
            _ => Action::NoReply,
        }
    }
    fn call(
        &mut self,
        id: Value,
        profile: Profile,
        mut params: Map<String, Value>,
    ) -> Result<Action, ProtocolError> {
        let Some(Value::String(tool)) = params.remove("name") else {
            return self.error(id, -32602, "Invalid tool name", None);
        };
        let arguments = match params.remove("arguments") {
            Some(Value::Object(a)) => a,
            None => Map::new(),
            _ => return self.error(id, -32602, "Invalid tool arguments", None),
        };
        if !params.is_empty() {
            return self.error(id, -32602, "Unsupported tool parameters", None);
        }
        if !self.tools.iter().any(|t| t.name == tool) {
            return self.error(id, -32602, "Unknown tool", None);
        }
        if self.active.len() >= self.limits.max_active_calls {
            return self.tool_failure(id, profile, "Tool capacity exhausted");
        }
        let allocation = match self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            self.limits.workspace()?,
        ) {
            Ok(v) => v.commit(),
            Err(_) => return self.tool_failure(id, profile, "Tool capacity exhausted"),
        };
        let next = self.next.checked_add(1).ok_or(ProtocolError::Capacity)?;
        let token = CallToken(next);
        self.next = next;
        self.active.push(Active {
            token,
            id,
            profile,
            cancelled: false,
        });
        Ok(Action::Call(ToolCall {
            token,
            tool,
            arguments,
            _allocation: allocation,
        }))
    }
    fn list(
        &self,
        id: Value,
        profile: Profile,
        mut params: Map<String, Value>,
    ) -> Result<Action, ProtocolError> {
        let cursor = match params.remove("cursor") {
            None | Some(Value::Null) => None,
            Some(Value::String(c)) => Some(c),
            _ => return self.error(id, -32602, "Invalid cursor", None),
        };
        if !params.is_empty() {
            return self.error(id, -32602, "Invalid parameters", None);
        }
        let start = match cursor {
            None => 0,
            Some(c) => match self.parse_cursor(&c) {
                Some(v) => v,
                None => return self.error(id, -32602, "Invalid cursor", None),
            },
        };
        let end = start
            .checked_add(self.limits.tools_per_page)
            .ok_or(ProtocolError::Capacity)?
            .min(self.tools.len());
        let Some(tools) = self.tools.get(start..end) else {
            return self.error(id, -32602, "Invalid cursor", None);
        };
        let next = if end < self.tools.len() {
            Some(self.cursor(end)?)
        } else {
            None
        };
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Page<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            result_type: Option<&'static str>,
            tools: &'a [Tool],
            #[serde(skip_serializing_if = "Option::is_none")]
            next_cursor: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            ttl_ms: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            cache_scope: Option<&'static str>,
        }
        self.reply(
            id,
            &Page {
                result_type: profile.result_type(),
                tools,
                next_cursor: next,
                ttl_ms: (profile == Profile::Modern).then_some(0),
                cache_scope: (profile == Profile::Modern).then_some("private"),
            },
        )
    }
    fn cursor(&self, index: usize) -> Result<String, ProtocolError> {
        use std::fmt::Write;
        let mut cursor = String::new();
        cursor
            .try_reserve_exact(128)
            .map_err(|_| ProtocolError::Capacity)?;
        let expires = self
            .clock_ms
            .checked_add(self.limits.cursor_ttl_ms)
            .ok_or(ProtocolError::Capacity)?;
        let tag = self.cursor_tag(index, expires)?;
        write!(&mut cursor, "{index}:{expires}:{}", tag.to_hex())
            .map_err(|_| ProtocolError::Encode)?;
        Ok(cursor)
    }
    fn parse_cursor(&self, cursor: &str) -> Option<usize> {
        let mut parts = cursor.split(':');
        let index = parts.next()?.parse::<usize>().ok()?;
        let expires = parts.next()?.parse::<u64>().ok()?;
        let tag = blake3::Hash::from_hex(parts.next()?).ok()?;
        if parts.next().is_some()
            || expires <= self.clock_ms
            || tag != self.cursor_tag(index, expires).ok()?
            || index >= self.tools.len()
            || index == 0
            || index.checked_rem(self.limits.tools_per_page) != Some(0)
        {
            return None;
        }
        Some(index)
    }
    fn cursor_tag(&self, index: usize, expires: u64) -> Result<blake3::Hash, ProtocolError> {
        let mut h = blake3::Hasher::new_keyed(&self.cursor_key);
        h.update(b"focal.mcp.catalog.v1");
        h.update(self.catalog_hash.as_bytes());
        h.update(
            &u64::try_from(index)
                .map_err(|_| ProtocolError::Capacity)?
                .to_le_bytes(),
        );
        h.update(&expires.to_le_bytes());
        Ok(h.finalize())
    }
    /// The caller keeps the returned frame alive until its complete write or discard.
    pub fn complete<T: Serialize>(
        &mut self,
        token: CallToken,
        result: &T,
        is_error: bool,
    ) -> Result<Option<EncodedFrame>, ProtocolError> {
        let Some(index) = self.active.iter().position(|v| v.token == token) else {
            return Ok(None);
        };
        let active = self.active.get(index).ok_or(ProtocolError::Dependency)?;
        if active.cancelled {
            self.active.remove(index);
            return Ok(None);
        }
        let _scratch = self.budget.reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            self.limits.max_response_bytes,
        )?;
        let mut writer = BoundedWriter {
            bytes: Vec::new(),
            limit: self.limits.max_response_bytes,
        };
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            serde_json::to_writer(&mut writer, result)
        }))
        .map_err(|_| ProtocolError::Dependency)?
        .map_err(|_| ProtocolError::Encode)?;
        let text = std::str::from_utf8(&writer.bytes).map_err(|_| ProtocolError::Encode)?;
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Completed<'a, T> {
            #[serde(skip_serializing_if = "Option::is_none")]
            result_type: Option<&'static str>,
            content: [Text<'a>; 1],
            structured_content: &'a T,
            is_error: bool,
        }
        let frame = encode(
            &Response {
                jsonrpc: "2.0",
                id: &active.id,
                result: Completed {
                    result_type: active.profile.result_type(),
                    content: [Text { kind: "text", text }],
                    structured_content: result,
                    is_error,
                },
            },
            self.limits.max_response_bytes,
            &self.budget,
        )?;
        self.active.remove(index);
        Ok(Some(frame))
    }
    /// Retire a host-failed call without manufacturing a business outcome.
    pub fn fail(
        &mut self,
        token: CallToken,
        message: &str,
    ) -> Result<Option<EncodedFrame>, ProtocolError> {
        let Some(index) = self.active.iter().position(|v| v.token == token) else {
            return Ok(None);
        };
        let active = self.active.get(index).ok_or(ProtocolError::Dependency)?;
        if active.cancelled {
            self.active.remove(index);
            return Ok(None);
        }
        let action = self.tool_failure(active.id.clone(), active.profile, message)?;
        self.active.remove(index);
        if let Action::Reply(frame) = action {
            Ok(Some(frame))
        } else {
            Err(ProtocolError::Dependency)
        }
    }
    fn tool_failure(
        &self,
        id: Value,
        profile: Profile,
        message: &str,
    ) -> Result<Action, ProtocolError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Failure<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            result_type: Option<&'static str>,
            content: [Text<'a>; 1],
            is_error: bool,
        }
        self.reply(
            id,
            &Failure {
                result_type: profile.result_type(),
                content: [Text {
                    kind: "text",
                    text: message,
                }],
                is_error: true,
            },
        )
    }
    fn reply<T: Serialize>(&self, id: Value, result: &T) -> Result<Action, ProtocolError> {
        Ok(Action::Reply(encode(
            &Response {
                jsonrpc: "2.0",
                id: &id,
                result,
            },
            self.limits.max_response_bytes,
            &self.budget,
        )?))
    }
    fn error(
        &self,
        id: Value,
        code: i32,
        message: &'static str,
        data: Option<Value>,
    ) -> Result<Action, ProtocolError> {
        #[derive(Serialize)]
        struct ErrorBody {
            code: i32,
            message: &'static str,
            #[serde(skip_serializing_if = "Option::is_none")]
            data: Option<Value>,
        }
        #[derive(Serialize)]
        struct ErrorResponse {
            jsonrpc: &'static str,
            id: Value,
            error: ErrorBody,
        }
        Ok(Action::Reply(encode(
            &ErrorResponse {
                jsonrpc: "2.0",
                id,
                error: ErrorBody {
                    code,
                    message,
                    data,
                },
            },
            self.limits.max_response_bytes,
            &self.budget,
        )?))
    }
}
#[derive(Serialize)]
struct Response<'a, T> {
    jsonrpc: &'static str,
    id: &'a Value,
    result: T,
}
#[derive(Serialize)]
struct Text<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
}
#[derive(Default, Serialize)]
struct Capabilities {
    tools: Map<String, Value>,
}
#[derive(Serialize)]
struct ServerMeta<'a> {
    #[serde(rename = "io.modelcontextprotocol/serverInfo")]
    info: &'a ServerInfo,
}
fn valid_info(v: &Value) -> bool {
    v.is_object()
        && v.get("name").is_some_and(Value::is_string)
        && v.get("version").is_some_and(Value::is_string)
}
fn valid_capabilities(v: &Value) -> bool {
    v.as_object().is_some_and(|m| {
        m.iter().all(|(key, v)| match key.as_str() {
            "extensions" => v.as_object().is_some_and(|m| {
                m.iter()
                    .all(|(k, v)| k.contains('/') && valid_meta_key(k) && v.is_object())
            }),
            "experimental" => v
                .as_object()
                .is_some_and(|m| m.values().all(Value::is_object)),
            "sampling" => v.as_object().is_some_and(|m| {
                ["context", "tools"]
                    .iter()
                    .all(|k| m.get(*k).is_none_or(Value::is_object))
            }),
            "elicitation" => v.as_object().is_some_and(|m| {
                ["form", "url"]
                    .iter()
                    .all(|k| m.get(*k).is_none_or(Value::is_object))
            }),
            "roots" | "tasks" => v.is_object(),
            _ => true,
        })
    })
}
fn valid_meta_key(key: &str) -> bool {
    let name = if let Some((prefix, name)) = key.split_once('/') {
        if !prefix.split('.').all(|label| {
            label
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        }) {
            return false;
        }
        name
    } else {
        key
    };
    name.is_empty()
        || (name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
            && name
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')))
}
fn valid_id(v: &Value, max: usize) -> bool {
    match v {
        Value::String(s) => s.len() <= max,
        Value::Number(n) => n.is_i64() || n.is_u64(),
        _ => false,
    }
}
fn value_cost(
    v: &Value,
    depth: usize,
    max_depth: usize,
    left: &mut usize,
) -> Result<usize, ProtocolError> {
    *left = left.checked_sub(1).ok_or(ProtocolError::Capacity)?;
    if depth > max_depth {
        return Err(ProtocolError::Limits);
    }
    let next = depth.checked_add(1).ok_or(ProtocolError::Capacity)?;
    let mut cost = 128usize;
    match v {
        Value::String(s) => {
            cost = cost
                .checked_add(s.capacity())
                .ok_or(ProtocolError::Capacity)?
        }
        Value::Array(a) => {
            cost = cost
                .checked_add(
                    a.capacity()
                        .checked_mul(std::mem::size_of::<Value>())
                        .ok_or(ProtocolError::Capacity)?,
                )
                .ok_or(ProtocolError::Capacity)?;
            for item in a {
                cost = cost
                    .checked_add(value_cost(item, next, max_depth, left)?)
                    .ok_or(ProtocolError::Capacity)?;
            }
        }
        Value::Object(o) => {
            for (key, item) in o {
                cost = cost
                    .checked_add(key.capacity())
                    .and_then(|n| n.checked_add(128))
                    .ok_or(ProtocolError::Capacity)?;
                cost = cost
                    .checked_add(value_cost(item, next, max_depth, left)?)
                    .ok_or(ProtocolError::Capacity)?;
            }
        }
        _ => {}
    }
    Ok(cost)
}

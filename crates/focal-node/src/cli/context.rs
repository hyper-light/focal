//! Named client connections own separate durable request stores. Selecting a
//! connection never changes the server's authenticated principal or grants.
use super::*;
use clap::Subcommand;
use focal_client::{ClientTransport, QuicTransport, TransportFuture};
use focal_enrollment::PrivateJournal;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::Path,
    sync::{Mutex, OnceLock},
};

const CATALOG: &str = "CLIENT.contexts";
const MARKER: &str = "CLIENT.contexts.initialized";
const LIMIT: usize = 60 * 1024;

#[derive(Subcommand)]
pub(crate) enum ContextCommand {
    /// Save a connection. Existing names cannot silently change identity.
    Add {
        name: String,
        #[arg(
            long,
            conflicts_with_all = ["file", "enrolled_as"],
            required_unless_present_any = ["file", "enrolled_as"]
        )]
        node_data_dir: Option<PathBuf>,
        /// With `--node-data-dir` and `--session`: the tenant of the session
        /// the node serves through its local socket.
        #[arg(long, requires = "node_data_dir", requires = "session")]
        tenant: Option<String>,
        /// Strict connection document; contains private credential paths, never key bytes.
        #[arg(
            long,
            conflicts_with_all = ["node_data_dir", "enrolled_as"],
            required_unless_present_any = ["node_data_dir", "enrolled_as"]
        )]
        file: Option<PathBuf>,
        /// The enrolled context whose identity this one reuses, addressing
        /// another session of the same tenant (`--session`).
        #[arg(long, requires = "session")]
        enrolled_as: Option<String>,
        /// The session the new context addresses, as a 32-hex-digit id.
        #[arg(long)]
        session: Option<String>,
    },
    /// Redeem a private client invitation; retry with the same name after interruption.
    Enroll {
        name: String,
        #[arg(long)]
        invite_file: PathBuf,
    },
    /// List saved connections and the selected default; credentials are redacted.
    List,
    /// Show redacted connection metadata.
    Show { name: Option<String> },
    /// Select a default; `local` restores the ordinary --data-dir connection.
    Use { name: String },
    /// Remove a connection from discovery. Retain its exact request recovery state.
    Remove { name: String },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Profile {
    Unix {
        node_data_dir: PathBuf,
        /// Another ledger the node serves through its local socket: a session
        /// of a served tenant that an operator created or restored (26 §6).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<String>,
    },
    Enrolled {
        enrollment: PathBuf,
        /// A session of the enrolled tenant other than the founder's own:
        /// one an operator created or restored (26 §6).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<String>,
    },
    Quic(Box<QuicProfile>),
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QuicProfile {
    endpoint: String,
    server_name: String,
    cluster: String,
    tenant: String,
    ledger: String,
    principal: String,
    root: String,
    ca: Vec<PathBuf>,
    certificates: Vec<PathBuf>,
    private_key: PathBuf,
}
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    schema: u16,
    selected: Option<String>,
    profiles: BTreeMap<String, Profile>,
    // Tombstones prevent a removed name from selecting a different identity
    // beside retained request/upload history. Removing is not history erasure.
    removed: BTreeMap<String, Profile>,
}
struct Store {
    journal: PrivateJournal,
    catalog: Catalog,
}
fn other(error: impl std::error::Error + Send + Sync + 'static) -> CliError {
    CliError::Other(Box::new(error))
}
fn exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
fn name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || value == "local"
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(InputError::Invalid("context name must contain 1..64 letters, digits, hyphens or underscores; local is reserved").into());
    }
    Ok(())
}
impl Store {
    fn open(root: &Path, create: bool) -> Result<Option<Self>> {
        Self::open_with(root, create, false)
    }
    /// Open the catalogue for reading beside other processes of this data
    /// directory: ordinary commands select their context without excluding
    /// each other, while `context` commands still own the catalogue exclusively.
    fn open_read(root: &Path) -> Result<Option<Self>> {
        Self::open_with(root, false, true)
    }
    fn open_with(root: &Path, create: bool, shared: bool) -> Result<Option<Self>> {
        let path = root.join(CATALOG);
        let marker = root.join(MARKER);
        let initialized = exists(&marker)?;
        let present = exists(&path)?;
        if !initialized && !present && !create {
            return Ok(None);
        }
        if initialized != present {
            return Err(InputError::Invalid(
                "client context catalogue is incomplete; restore its saved state",
            )
            .into());
        }
        if !initialized {
            super::private_parent(root)?;
            let mut file = focal_platform::fs::open_private(&marker, false, true, true)?;
            file.write_all(b"FCLCTX01")?;
            file.sync_all()?;
            // Unix fsyncs the directory so the new entry is durable; Windows
            // uses write-through semantics and does not flush a directory.
            #[cfg(unix)]
            {
                File::open(root)?.sync_all()?;
            }
        }
        let mut journal = if shared {
            PrivateJournal::open_shared(&path).map_err(other)?
        } else {
            PrivateJournal::open(&path).map_err(other)?
        };
        let catalog = match journal.read().map_err(other)? {
            Some(bytes) => parse_document::<Catalog>(&bytes, InputFormat::Json)?,
            None if !initialized && !shared => {
                let catalog = Catalog {
                    schema: 1,
                    ..Catalog::default()
                };
                journal
                    .replace(&serde_json::to_vec(&catalog).map_err(other)?)
                    .map_err(other)?;
                catalog
            }
            None => return Err(InputError::Invalid("client context catalogue is missing").into()),
        };
        if catalog.schema != 1
            || catalog.profiles.len().saturating_add(catalog.removed.len()) > 64
            || catalog
                .selected
                .as_ref()
                .is_some_and(|n| !catalog.profiles.contains_key(n))
        {
            return Err(InputError::Invalid("invalid client context catalogue").into());
        }
        for (n, profile) in catalog.profiles.iter().chain(&catalog.removed) {
            name(n)?;
            profile.validate()?;
        }
        Ok(Some(Self { journal, catalog }))
    }
    fn save(&mut self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.catalog).map_err(other)?;
        if bytes.len() > LIMIT {
            return Err(InputError::Capacity.into());
        }
        self.journal.replace(&bytes).map_err(other)
    }
}
impl Profile {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Unix {
                node_data_dir,
                tenant,
                session,
            } => {
                if let Some(tenant) = tenant {
                    parse_id(tenant)?;
                }
                if let Some(session) = session {
                    parse_id(session)?;
                }
                if tenant.is_some() != session.is_some() {
                    return Err(InputError::Invalid("tenant and session go together").into());
                }
                absolute(node_data_dir)
            }
            Self::Enrolled {
                enrollment,
                session,
            } => {
                if let Some(session) = session {
                    parse_id(session)?;
                }
                absolute(enrollment)
            }
            Self::Quic(profile) => {
                let QuicProfile {
                    endpoint,
                    server_name,
                    cluster,
                    tenant,
                    ledger,
                    principal,
                    root,
                    ca,
                    certificates,
                    private_key,
                } = profile.as_ref();
                if endpoint.len() > 512
                    || endpoint.parse::<std::net::SocketAddr>().is_err()
                    || server_name.is_empty()
                    || server_name.len() > 253
                    || ca.is_empty()
                    || ca.len() > 8
                    || certificates.is_empty()
                    || certificates.len() > 8
                {
                    return Err(InputError::Invalid(
                        "invalid remote endpoint, server name or certificate chain",
                    )
                    .into());
                }
                for id in [cluster, tenant, ledger, principal, root] {
                    parse_id(id)?;
                }
                for path in ca
                    .iter()
                    .chain(certificates)
                    .chain(std::iter::once(private_key))
                {
                    absolute(path)?;
                }
                Ok(())
            }
        }
    }
    fn redacted(&self) -> serde_json::Value {
        match self {
            Self::Unix {
                node_data_dir,
                tenant,
                session,
            } => match (tenant, session) {
                (Some(tenant), Some(session)) => {
                    serde_json::json!({"transport":"unix","node_data_dir":node_data_dir,"tenant":tenant,"session":session})
                }
                _ => serde_json::json!({"transport":"unix","node_data_dir":node_data_dir}),
            },
            Self::Enrolled { session, .. } => match session {
                Some(session) => {
                    serde_json::json!({"transport":"quic","credentials":"enrolled private identity","session":session})
                }
                None => {
                    serde_json::json!({"transport":"quic","credentials":"enrolled private identity"})
                }
            },
            Self::Quic(profile) => {
                let QuicProfile {
                    endpoint,
                    server_name,
                    cluster,
                    tenant,
                    ledger,
                    principal,
                    root,
                    ..
                } = profile.as_ref();
                serde_json::json!({"transport":"quic","endpoint":endpoint,"server_name":server_name,"cluster":cluster,"tenant":tenant,"ledger":ledger,"principal":principal,"root":root,"credentials":"private files"})
            }
        }
    }
}
fn absolute(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path.as_os_str().len() > 4096
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(InputError::Invalid(
            "context paths must be absolute and cannot contain parent traversal",
        )
        .into());
    }
    Ok(())
}
pub(crate) fn run(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    command: ContextCommand,
) -> Result<()> {
    if let ContextCommand::Enroll { name, invite_file } = command {
        return enroll(runtime, settings, &name, &invite_file);
    }
    let root = settings.data_dir().map_err(other)?;
    let create = matches!(command, ContextCommand::Add { .. });
    let mut store = Store::open(&root, create)?;
    let result = match command {
        ContextCommand::Enroll { .. } => {
            return Err(InputError::Invalid("invalid enrollment dispatch").into());
        }
        ContextCommand::List => match &store {
            Some(store) => {
                let profiles: Vec<_> = store.catalog.profiles.iter().map(|(name, p)| serde_json::json!({"name":name,"selected":store.catalog.selected.as_ref()==Some(name),"connection":p.redacted()})).collect();
                serde_json::json!({"selected":store.catalog.selected.as_deref().unwrap_or("local"),"contexts":profiles})
            }
            None => serde_json::json!({"selected":"local","contexts":[]}),
        },
        ContextCommand::Show { name: requested } => {
            let selected = requested
                .as_deref()
                .or_else(|| store.as_ref().and_then(|s| s.catalog.selected.as_deref()))
                .unwrap_or("local");
            if selected == "local" {
                serde_json::json!({"name":"local","connection":{"transport":"unix","node_data_dir":root}})
            } else {
                let profile = store
                    .as_ref()
                    .and_then(|s| s.catalog.profiles.get(selected))
                    .ok_or(CliError::NotFound)?;
                serde_json::json!({"name":selected,"connection":profile.redacted()})
            }
        }
        ContextCommand::Add {
            name: selected,
            node_data_dir,
            tenant,
            file,
            enrolled_as,
            session,
        } => {
            name(&selected)?;
            let profile = match (node_data_dir, file, enrolled_as) {
                (None, None, Some(source)) => {
                    let session = session.ok_or(InputError::Invalid("session"))?;
                    parse_id(&session)?;
                    match store
                        .as_ref()
                        .and_then(|store| store.catalog.profiles.get(&source))
                    {
                        Some(Profile::Enrolled { enrollment, .. }) => Profile::Enrolled {
                            enrollment: enrollment.clone(),
                            session: Some(session),
                        },
                        Some(_) => {
                            return Err(InputError::Invalid(
                                "only an enrolled context can address another session",
                            )
                            .into());
                        }
                        None => return Err(CliError::NotFound),
                    }
                }
                (Some(path), None, None) => Profile::Unix {
                    node_data_dir: fs::canonicalize(path)?,
                    tenant,
                    session,
                },
                (None, Some(path), None) => parse_document(
                    &super::documents::read_bytes(&path, LIMIT)?,
                    if path
                        .extension()
                        .is_some_and(|ext| ext == "yaml" || ext == "yml")
                    {
                        InputFormat::Yaml
                    } else {
                        InputFormat::Json
                    },
                )?,
                _ => {
                    return Err(InputError::Invalid(
                        "choose node-data-dir, file, or enrolled-as with session",
                    )
                    .into());
                }
            };
            profile.validate()?;
            let store = store.as_mut().ok_or(CliError::InvalidResponse)?;
            if let Some(old) = store
                .catalog
                .profiles
                .get(&selected)
                .or_else(|| store.catalog.removed.get(&selected))
            {
                if old != &profile {
                    return Err(InputError::Invalid(
                        "context name is bound to a different connection; choose a new name",
                    )
                    .into());
                }
            } else if store
                .catalog
                .profiles
                .len()
                .saturating_add(store.catalog.removed.len())
                >= 64
            {
                return Err(InputError::Capacity.into());
            }
            let history = root.join(CATALOG).join(format!("history-{selected}"));
            if (store.catalog.profiles.contains_key(&selected)
                || store.catalog.removed.contains_key(&selected))
                && !exists(&history)?
            {
                return Err(
                    InputError::Invalid("client context request history is missing").into(),
                );
            }
            super::private_parent(&history)?;
            store.catalog.removed.remove(&selected);
            let connection = profile.redacted();
            store.catalog.profiles.insert(selected.clone(), profile);
            store.save()?;
            serde_json::json!({"condition":"Saved","name":selected,"connection":connection})
        }
        ContextCommand::Use { name: selected } => {
            if selected != "local" {
                name(&selected)?;
            }
            if let Some(store) = &mut store {
                if selected != "local" && !store.catalog.profiles.contains_key(&selected) {
                    return Err(CliError::NotFound);
                }
                store.catalog.selected = if selected == "local" {
                    None
                } else {
                    Some(selected.clone())
                };
                store.save()?;
            } else if selected != "local" {
                return Err(CliError::NotFound);
            }
            serde_json::json!({"condition":"Selected","name":selected})
        }
        ContextCommand::Remove { name: selected } => {
            name(&selected)?;
            let store = store.as_mut().ok_or(CliError::NotFound)?;
            let profile = store
                .catalog
                .profiles
                .remove(&selected)
                .ok_or(CliError::NotFound)?;
            store.catalog.removed.insert(selected.clone(), profile);
            if store.catalog.selected.as_ref() == Some(&selected) {
                store.catalog.selected = None;
            }
            store.save()?;
            serde_json::json!({"condition":"Removed","name":selected,"request_history_retained":true})
        }
    };
    super::super::print_json(&result).map_err(CliError::Other)
}

pub(super) fn selected(
    settings: &Settings,
    requested: Option<&str>,
) -> Result<Option<(Profile, PathBuf, String)>> {
    if requested == Some("local") {
        return Ok(None);
    }
    let root = settings.data_dir().map_err(other)?;
    let store = Store::open_read(&root)?;
    let selection =
        requested.or_else(|| store.as_ref().and_then(|s| s.catalog.selected.as_deref()));
    let Some(selection) = selection else {
        return Ok(None);
    };
    name(selection)?;
    let profile = store
        .as_ref()
        .and_then(|s| s.catalog.profiles.get(selection))
        .ok_or(CliError::NotFound)?
        .clone();
    let history = root.join(CATALOG).join(format!("history-{selection}"));
    if !exists(&history)? {
        return Err(InputError::Invalid(
            "client context request history is missing; restore the saved directory",
        )
        .into());
    }
    Ok(Some((profile, history, selection.to_owned())))
}

pub(super) enum Transport {
    Unix(UnixTransport),
    Quic(Box<Remote>),
}
pub(super) struct Remote {
    tls: quinn::ClientConfig,
    initial: RouteHint,
    limits: WireLimits,
    initialized: Mutex<()>,
    transport: OnceLock<QuicTransport>,
}
impl ClientTransport for Transport {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        match self {
            Self::Unix(local) => local.request(route, request),
            Self::Quic(remote) => Box::pin(async move {
                if remote.transport.get().is_none() {
                    let _guard = remote
                        .initialized
                        .lock()
                        .map_err(|_| WireError::Connection)?;
                    if remote.transport.get().is_none() {
                        let address = if remote.initial.endpoint.starts_with('[') {
                            "[::]:0"
                        } else {
                            "0.0.0.0:0"
                        }
                        .parse()
                        .map_err(|_| WireError::Connection)?;
                        let connector = QuicConnector::bind(
                            address,
                            remote.tls.clone(),
                            remote.limits.clone(),
                        )?;
                        let transport = QuicTransport::new(connector, remote.initial.clone(), 16)?;
                        remote
                            .transport
                            .set(transport)
                            .map_err(|_| WireError::Connection)?;
                    }
                }
                remote
                    .transport
                    .get()
                    .ok_or(WireError::Connection)?
                    .request(route, request)
                    .await
            }),
        }
    }
}
pub(super) fn connect(profile: Profile, history: PathBuf) -> Result<Context> {
    profile.validate()?;
    match profile {
        Profile::Unix {
            node_data_dir,
            tenant,
            session,
        } => {
            let mut settings = Settings::default();
            settings.node.data_dir = Some(node_data_dir);
            let mut context = Context::open_local(&settings)?;
            context.root = history;
            // The node serves the sessions of every tenant it admits through
            // its local socket; a saved connection may address one of them.
            if let (Some(tenant), Some(session)) = (tenant, session) {
                let ledger = LedgerId {
                    tenant: TenantId(parse_id(&tenant)?),
                    session: SessionId(parse_id(&session)?),
                };
                context.build.ledger = ledger;
                context.operation.ledger = ledger;
            }
            Ok(context)
        }
        Profile::Enrolled {
            enrollment,
            session,
        } => {
            let pending = focal_node::network_join::PendingClientJoin::resume_shared(enrollment)
                .map_err(other)?;
            let receipt = pending.enrollment().map_err(other)?.ok_or(InputError::Invalid("client enrollment is pending; retry context enroll with its original invitation"))?;
            let credentials = pending.credentials(now()?).map_err(other)?;
            let founder = &pending.invitation().genesis().founder;
            let trust = pending.invitation().invitation().trust();
            let limits = WireLimits::default();
            let tls = client_tls(
                TlsIdentity::from_pkcs8(
                    credentials.certificate_chain().to_vec(),
                    credentials.private_key_der().to_vec(),
                ),
                vec![trust.ca_certificate.clone()],
                &limits,
            )
            .map_err(other)?;
            let actor = ParticipantId(receipt.identity.principal);
            // The enrolled identity may address another session of its
            // tenant: one the operator created or restored.
            let ledger = match session {
                Some(session) => LedgerId {
                    tenant: founder.ledger.tenant,
                    session: SessionId(parse_id(&session)?),
                },
                None => founder.ledger,
            };
            let build = BuildContext {
                ledger,
                actor,
                root: founder.root,
                policy_revision: 1,
            };
            let operation = OperationContext {
                cluster: founder.cluster,
                ledger,
                principal: actor,
            };
            let transport = Transport::Quic(Box::new(Remote {
                tls,
                initial: RouteHint {
                    epoch: RouteEpoch(1),
                    endpoint: trust.endpoint.clone(),
                    server_name: pending.invitation().data_server_name().map_err(other)?,
                },
                limits: limits.clone(),
                initialized: Mutex::new(()),
                transport: OnceLock::new(),
            }));
            Ok(Context {
                client: Client::new(transport, RetryPolicy::default(), limits, 1)?,
                build,
                operation,
                root: history,
                admin_root: None,
                invocation: None,
            })
        }
        Profile::Quic(profile) => {
            let QuicProfile {
                endpoint,
                server_name,
                cluster,
                tenant,
                ledger,
                principal,
                root,
                ca,
                certificates,
                private_key,
            } = *profile;
            let limits = WireLimits::default();
            let ledger = LedgerId {
                tenant: TenantId(parse_id(&tenant)?),
                session: SessionId(parse_id(&ledger)?),
            };
            let actor = ParticipantId(parse_id(&principal)?);
            let build = BuildContext {
                ledger,
                actor,
                root: RootCommandId(parse_id(&root)?),
                policy_revision: 1,
            };
            let operation = OperationContext {
                cluster: parse_id(&cluster)?,
                ledger,
                principal: actor,
            };
            let roots = ca
                .iter()
                .map(|p| read_credential(p, false))
                .collect::<Result<Vec<_>>>()?;
            let chain = certificates
                .iter()
                .map(|p| read_credential(p, false))
                .collect::<Result<Vec<_>>>()?;
            let key = read_credential(&private_key, true)?;
            let tls =
                client_tls(TlsIdentity::from_pkcs8(chain, key), roots, &limits).map_err(other)?;
            let transport = Transport::Quic(Box::new(Remote {
                tls,
                initial: RouteHint {
                    epoch: RouteEpoch(1),
                    endpoint,
                    server_name,
                },
                limits: limits.clone(),
                initialized: Mutex::new(()),
                transport: OnceLock::new(),
            }));
            Ok(Context {
                client: Client::new(transport, RetryPolicy::default(), limits, 1)?,
                build,
                operation,
                root: history,
                admin_root: None,
                invocation: None,
            })
        }
    }
}

/// Administrative authority is the selected physical node's local Unix socket.
/// Never substitute an unrelated data directory for an enrolled remote context.
pub(crate) fn admin_settings(settings: &Settings, selection: Option<&str>) -> Result<Settings> {
    match selected(settings, selection)? {
        None => Ok(settings.clone()),
        Some((Profile::Unix { node_data_dir, .. }, _, _)) => {
            let mut resolved=Settings::default();
            resolved.node.data_dir=Some(node_data_dir);
            Ok(resolved)
        }
        Some(_) => Err(InputError::Invalid("cluster administration requires a local Unix context; select local or a saved Unix connection").into()),
    }
}
pub(super) fn now() -> Result<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(other)?
            .as_secs(),
    )
    .map_err(|_| InputError::Invalid("system time is out of range").into())
}
fn enroll(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    selected: &str,
    invitation: &Path,
) -> Result<()> {
    use focal_enrollment::{EnrollmentClient, TransportLimits};
    use focal_node::network_join::{ClientInvitation, PendingClientJoin};
    name(selected)?;
    let bundle = ClientInvitation::load(invitation).map_err(other)?;
    let root = settings.data_dir().map_err(other)?;
    super::private_parent(&root)?;
    let root = fs::canonicalize(root)?;
    let mut store = Store::open(&root, true)?.ok_or(CliError::InvalidResponse)?;
    let path = root.join(CATALOG).join(format!("enrollment-{selected}"));
    let profile = Profile::Enrolled {
        enrollment: path.clone(),
        session: None,
    };
    if let Some(old) = store
        .catalog
        .profiles
        .get(selected)
        .or_else(|| store.catalog.removed.get(selected))
    {
        if old != &profile {
            return Err(
                InputError::Invalid("context name is bound to a different connection").into(),
            );
        }
    } else if store
        .catalog
        .profiles
        .len()
        .saturating_add(store.catalog.removed.len())
        >= 64
    {
        return Err(InputError::Capacity.into());
    }
    let pending = PendingClientJoin::open(path, bundle).map_err(other)?;
    let history = root.join(CATALOG).join(format!("history-{selected}"));
    if (store.catalog.profiles.contains_key(selected)
        || store.catalog.removed.contains_key(selected))
        && !exists(&history)?
    {
        return Err(InputError::Invalid("client context request history is missing").into());
    }
    super::private_parent(&history)?;
    store.catalog.removed.remove(selected);
    store.catalog.profiles.insert(selected.into(), profile);
    store.save()?;
    drop(store);
    let receipt = runtime.block_on(async {
        let address = if pending
            .invitation()
            .invitation()
            .trust()
            .endpoint
            .starts_with('[')
        {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        }
        .parse()
        .map_err(other)?;
        let client = EnrollmentClient::bind(address, TransportLimits::default()).map_err(other)?;
        let result = pending.redeem(&client, now()?).await.map_err(other);
        client.close();
        result
    })?;
    super::super::print_json(&serde_json::json!({"condition":"Enrolled","name":selected,"principal":ParticipantId(receipt.identity.principal),"request_id":pending.request_id(),"selected":false})).map_err(CliError::Other)
}
/// A secret credential (a private key) must be reachable only by its owner:
/// mode `& 0o077 == 0` on Unix; owned by the current user on Windows, where an
/// owner-only DACL is the access control.
fn credential_is_owner_private(path: &Path, metadata: &fs::Metadata) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let _ = path;
        Ok(metadata.mode() & 0o077 == 0)
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Ok(focal_platform::fs::owner_at(path)? == focal_platform::fs::current_owner()?)
    }
}
fn read_credential(path: &Path, secret: bool) -> Result<Vec<u8>> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file()
        || before.len() > 64 * 1024
        || before.len() == 0
        || focal_platform::fs::path_hard_link_count(path)? != 1
        || (secret && !credential_is_owner_private(path, &before)?)
    {
        return Err(InputError::Invalid(
            "credential must be a bounded regular file; keys must be owner-private",
        )
        .into());
    }
    let mut file = File::open(path)?;
    let opened = file.metadata()?;
    if focal_platform::fs::path_identity(path)? != focal_platform::fs::file_identity(&file)? {
        return Err(InputError::Invalid("credential changed during open").into());
    }
    let length = usize::try_from(opened.len()).map_err(|_| InputError::Capacity)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| InputError::Capacity)?;
    Read::by_ref(&mut file)
        .take(64 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() != length {
        return Err(InputError::Invalid("credential changed during read").into());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn settings() -> (tempfile::TempDir, Settings) {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.path().into());
        (root, settings)
    }
    #[test]
    fn absent_default_is_read_only_and_unknown_names_do_not_create_connections() {
        let (root, settings) = settings();
        assert!(selected(&settings, None).unwrap().is_none());
        assert!(selected(&settings, Some("missing")).is_err());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
        assert!(selected(&settings, Some("../escape")).is_err());
        assert!(name("local").is_err());
    }
    #[test]
    fn selected_context_is_pinned_and_missing_catalogue_or_history_never_recreated() {
        let (root, settings) = settings();
        let mut store = Store::open(root.path(), true).unwrap().unwrap();
        store.catalog.profiles.insert(
            "alice".into(),
            Profile::Unix {
                node_data_dir: root.path().into(),
                tenant: None,
                session: None,
            },
        );
        store.catalog.selected = Some("alice".into());
        let history = root.path().join(CATALOG).join("history-alice");
        super::super::private_parent(&history).unwrap();
        store.save().unwrap();
        drop(store);
        assert_eq!(selected(&settings, None).unwrap().unwrap().1, history);
        assert!(selected(&settings, Some("local")).unwrap().is_none());
        fs::remove_dir(&history).unwrap();
        assert!(selected(&settings, None).is_err());
        assert!(!history.exists());
        fs::remove_dir_all(root.path().join(CATALOG)).unwrap();
        assert!(Store::open(root.path(), true).is_err());
        assert!(!root.path().join(CATALOG).exists());
    }
    #[test]
    fn removed_names_remain_bound_and_remote_metadata_never_discloses_key_paths() {
        let (root, _) = settings();
        let profile = Profile::Quic(Box::new(QuicProfile {
            endpoint: "127.0.0.1:4433".into(),
            server_name: "focal.local".into(),
            cluster: "11".repeat(16),
            tenant: "22".repeat(16),
            ledger: "33".repeat(16),
            principal: "44".repeat(16),
            root: "55".repeat(16),
            ca: vec![root.path().join("ca.der")],
            certificates: vec![root.path().join("certificate.der")],
            private_key: root.path().join("secret-key.der"),
        }));
        profile.validate().unwrap();
        assert!(!profile.redacted().to_string().contains("secret-key"));
        let mut store = Store::open(root.path(), true).unwrap().unwrap();
        store
            .catalog
            .removed
            .insert("alice".into(), profile.clone());
        store.save().unwrap();
        drop(store);
        assert!(
            Store::open(root.path(), false)
                .unwrap()
                .unwrap()
                .catalog
                .removed
                .get("alice")
                == Some(&profile)
        );
        let forged = br#"{"transport":"unix","node_data_dir":"/tmp","runtime":true}"#;
        assert!(parse_document::<Profile>(forged, InputFormat::Json).is_err());
    }
    #[test]
    fn credential_loading_rejects_symlinks_hardlinks_oversize_and_public_keys() {
        let (root, _) = settings();
        let path = root.path().join("key");
        fs::write(&path, b"key").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_credential(&path, true).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_credential(&path, true).unwrap(), b"key");
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(read_credential(&link, true).is_err());
        fs::remove_file(&link).unwrap();
        fs::hard_link(&path, &link).unwrap();
        assert!(read_credential(&path, true).is_err());
        fs::remove_file(&link).unwrap();
        fs::write(&path, vec![0; 65537]).unwrap();
        assert!(read_credential(&path, true).is_err());
    }
    #[test]
    fn administration_resolves_selected_unix_node_and_rejects_remote_fallback() {
        let (root, settings) = settings();
        assert_eq!(admin_settings(&settings, None).unwrap(), settings);
        let mut store = Store::open(root.path(), true).unwrap().unwrap();
        let node = root.path().join("another-node");
        let history = root.path().join(CATALOG).join("history-near");
        super::super::private_parent(&history).unwrap();
        store.catalog.profiles.insert(
            "near".into(),
            Profile::Unix {
                node_data_dir: node.clone(),
                tenant: None,
                session: None,
            },
        );
        store.catalog.selected = Some("near".into());
        let remote_history = root.path().join(CATALOG).join("history-far");
        super::super::private_parent(&remote_history).unwrap();
        store.catalog.profiles.insert(
            "far".into(),
            Profile::Enrolled {
                enrollment: root.path().join("private-enrollment"),
                session: None,
            },
        );
        store.save().unwrap();
        drop(store);
        assert_eq!(
            admin_settings(&settings, None).unwrap().data_dir().unwrap(),
            node
        );
        assert_eq!(admin_settings(&settings, Some("local")).unwrap(), settings);
        assert!(admin_settings(&settings, Some("far")).is_err());
        assert!(admin_settings(&settings, Some("unknown")).is_err());
    }

    #[test]
    fn recovery_command_preserves_context_and_literal_shell_path_bytes() {
        let path = Path::new("/private/tmp/client 'quoted' $(printf unexpected) `true`");
        let invocation = Context::recovery_invocation(path, "near").unwrap();
        let output = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!(
                "focal() {{ printf '%s\\n' \"$@\"; }}\n{invocation}"
            ))
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!("--data-dir\n{}\n--client-context\nnear\n", path.display())
        );
        assert!(Context::recovery_invocation(Path::new("/tmp/line\nbreak"), "near").is_none());
        assert!(
            Context::recovery_invocation(Path::new("relative"), "local")
                .unwrap()
                .contains("--client-context 'local'")
        );
    }
}

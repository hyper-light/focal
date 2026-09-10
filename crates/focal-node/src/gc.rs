//! The collector agent (26 §5). Every node runs one: each pass it gathers
//! the roots this node can know — every artifact payload and every
//! continuation's bundle named by the committed rows of the replicas it
//! hosts, and the proof each bundle's header names — marks the domains it
//! holds copies for without a core as opaque, installs that protection set
//! in the content store, and drives the store's bounded collector and every
//! hosted replica's seed sweep to completion, one bounded step at a time.
//! The last pass's report is published for the operator. Nothing here
//! decides what is proof: the rows do, and what the rows do not name has
//! stood untouched for the grace before it moves.
use crate::network_service::NetworkHandles;
use focal_client::admin::{
    AdminCollectorReport, AdminGc, AdminGcConfig, AdminGcPass, AdminSeedReport,
};
use focal_core::native::ContentRoot;
use focal_core::native::record_codec::{InspectionLimits, StructuralArchive};
use focal_evidence::{CollectorConfig, ProtectionSet};
use focal_model::{ContentClass, ContentDomainId, ContentHash, ContentRef, LedgerId};
use focal_wire::AccessError;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

/// Milliseconds between passes; one minute by default.
pub const INTERVAL_ENV: &str = "FOCAL_GC_INTERVAL_MS";
/// Milliseconds a file must have stood untouched before it is a candidate.
pub const GRACE_ENV: &str = "FOCAL_GC_GRACE_MS";
/// Milliseconds a quarantined round waits before deletion.
pub const QUARANTINE_ENV: &str = "FOCAL_GC_QUARANTINE_MS";
/// Milliseconds a finished upload's identity stays fenced.
pub const TERMINAL_ENV: &str = "FOCAL_GC_TERMINAL_MS";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
/// Rows one root page visits, and file visits one collector step takes.
const PAGE: usize = 4096;
/// Collector steps one pass runs before it yields to the next tick.
const MAX_STEPS: usize = 256;
/// Bundles whose content roots are remembered between passes.
const MAX_BUNDLES: usize = 65_536;
/// The most bytes a bundle read for its header may take (26 §4).
const MAX_BUNDLE_BYTES: usize = 6 << 20;
const BUNDLE_INSPECTION: InspectionLimits = InspectionLimits {
    bytes: 6 << 20,
    visits: 1 << 30,
    rows: 100_000,
    row_bytes: 6 << 20,
};

/// The operator's view of the collector on this node.
#[derive(Clone)]
pub struct GcHandle(watch::Receiver<AdminGc>);
impl GcHandle {
    pub fn status(&self) -> AdminGc {
        self.0.borrow().clone()
    }
}

pub struct GcAgent {
    node: u64,
    interval: Duration,
    config: CollectorConfig,
    /// A pass whose collector steps did not complete within their bound
    /// continues at the next tick under the same protection set.
    collecting: bool,
    passes: u64,
    last: Option<AdminGcPass>,
    /// What each bundle's header names — the roots of content objects and
    /// of the sealed inline objects — by bundle root.
    bundles: BTreeMap<ContentHash, (Vec<ContentHash>, Vec<ContentHash>)>,
    status: watch::Sender<AdminGc>,
}

fn now_ms() -> Result<u64, AccessError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AccessError::Unavailable)?
            .as_millis(),
    )
    .map_err(|_| AccessError::Unavailable)
}
fn admin_config(interval: Duration, config: CollectorConfig) -> AdminGcConfig {
    AdminGcConfig {
        interval_ms: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
        grace_ms: config.grace_ms,
        quarantine_ms: config.quarantine_ms,
        terminal_grace_ms: config.terminal_grace_ms,
        keep_records: u64::try_from(config.keep_records).unwrap_or(u64::MAX),
        max_marks: u64::try_from(config.max_marks).unwrap_or(u64::MAX),
    }
}
fn admin_report(report: focal_evidence::CollectorReport) -> AdminCollectorReport {
    AdminCollectorReport {
        visited: report.visited,
        uploads_expired: report.uploads_expired,
        terminals_released: report.terminals_released,
        objects_quarantined: report.objects_quarantined,
        chunks_quarantined: report.chunks_quarantined,
        chunks_deferred: report.chunks_deferred,
        records_quarantined: report.records_quarantined,
        receipts_quarantined: report.receipts_quarantined,
        deleted: report.deleted,
        bytes_deleted: report.bytes_deleted,
        opaque_domains: report.opaque_domains,
        complete: report.complete,
    }
}

impl GcAgent {
    pub fn new(node: u64, interval: Duration, config: CollectorConfig) -> (Self, GcHandle) {
        let interval = interval.max(Duration::from_millis(10));
        let (status, receiver) = watch::channel(AdminGc {
            node,
            config: admin_config(interval, config),
            passes: 0,
            collecting: false,
            last: None,
        });
        (
            Self {
                node,
                interval,
                config,
                collecting: false,
                passes: 0,
                last: None,
                bundles: BTreeMap::new(),
                status,
            },
            GcHandle(receiver),
        )
    }
    /// The interval and the collector's settings from the environment, else
    /// the defaults.
    pub fn from_env(node: u64) -> (Self, GcHandle) {
        let millis = |name: &str| {
            std::env::var_os(name).and_then(|value| {
                value
                    .to_str()
                    .and_then(|text| text.trim().parse::<u64>().ok())
            })
        };
        let interval = millis(INTERVAL_ENV)
            .filter(|millis| *millis > 0)
            .map_or(DEFAULT_INTERVAL, Duration::from_millis);
        let mut config = CollectorConfig::default();
        if let Some(grace) = millis(GRACE_ENV) {
            config.grace_ms = grace;
        }
        if let Some(quarantine) = millis(QUARANTINE_ENV) {
            config.quarantine_ms = quarantine;
        }
        if let Some(terminal) = millis(TERMINAL_ENV) {
            config.terminal_grace_ms = terminal;
        }
        Self::new(node, interval, config)
    }
    pub async fn run(mut self, handles: &NetworkHandles) -> Result<(), AccessError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AccessError::Unavailable);
        }
        loop {
            tokio::time::sleep(self.interval).await;
            // A pass that cannot run now (a store that refuses, a replica
            // that is stopping) waits for the next tick; nothing is inferred.
            let _ = self.pass(handles).await;
        }
    }
    fn publish(&self) {
        let _ = self.status.send(AdminGc {
            node: self.node,
            config: admin_config(self.interval, self.config),
            passes: self.passes,
            collecting: self.collecting,
            last: self.last,
        });
    }
    /// One pass: the roots, the protection set, the store's collector to
    /// completion or the step bound, then every hosted replica's seeds.
    pub async fn pass(&mut self, handles: &NetworkHandles) -> Result<(), AccessError> {
        let started = now_ms()?;
        let mut pass = AdminGcPass {
            started_ms: started,
            finished_ms: started,
            replicas: 0,
            protected_objects: 0,
            opaque_domains: 0,
            bundles_unreadable: 0,
            content: AdminCollectorReport::default(),
            seeds: AdminSeedReport::default(),
        };
        if !self.collecting {
            let protection = self.roots(handles, &mut pass).await?;
            pass.protected_objects = u64::try_from(protection.objects()).unwrap_or(u64::MAX);
            handles.content.protect(protection).await?;
            self.collecting = true;
            self.publish();
        }
        let mut report = focal_evidence::CollectorReport::default();
        for _ in 0..MAX_STEPS {
            report = handles
                .content
                .collect(self.config, now_ms()?, PAGE)
                .await?;
            if report.complete {
                break;
            }
            tokio::task::yield_now().await;
        }
        pass.content = admin_report(report);
        if !report.complete {
            // The store keeps its place; the next tick continues the pass.
            pass.finished_ms = now_ms()?;
            self.last = Some(pass);
            self.publish();
            return Ok(());
        }
        self.collecting = false;
        pass.seeds = self.seeds(handles).await?;
        pass.finished_ms = now_ms()?;
        self.passes = self.passes.saturating_add(1);
        self.last = Some(pass);
        self.publish();
        Ok(())
    }
    /// Every ledger this node holds content for: the installed custody
    /// policies and the replicas it hosts.
    fn ledgers(handles: &NetworkHandles, policies: Vec<LedgerId>) -> BTreeSet<LedgerId> {
        let mut ledgers: BTreeSet<LedgerId> = policies.into_iter().collect();
        let mut after = None;
        while let Some((ledger, _)) = handles.fleet.next_host(after) {
            after = Some(ledger);
            ledgers.insert(ledger);
        }
        ledgers
    }
    async fn roots(
        &mut self,
        handles: &NetworkHandles,
        pass: &mut AdminGcPass,
    ) -> Result<ProtectionSet, AccessError> {
        let mut protection = ProtectionSet::new();
        let policies = handles.content.policies().await?;
        for ledger in Self::ledgers(handles, policies) {
            let domain = ContentDomainId(ledger.tenant.0);
            let Ok(host) = handles.fleet.current_host(ledger) else {
                protection.opaque_domain(domain).map_err(content)?;
                pass.opaque_domains = pass.opaque_domains.saturating_add(1);
                continue;
            };
            let mut cursor = None;
            let mut complete = false;
            let mut bundles = Vec::new();
            loop {
                let page = match host.content_roots(cursor, PAGE).await {
                    Ok(page) => page,
                    Err(_) => break,
                };
                for root in page.roots {
                    match root {
                        ContentRoot::Artifact(pointer) | ContentRoot::Inline(pointer) => {
                            protection
                                .protect_object(domain, pointer.root)
                                .map_err(content)?;
                        }
                        ContentRoot::Bundle { root, bytes } => {
                            protection.protect_object(domain, root).map_err(content)?;
                            bundles
                                .try_reserve_exact(1)
                                .map_err(|_| AccessError::Capacity)?;
                            bundles.push((root, bytes));
                        }
                    }
                }
                cursor = page.next;
                if cursor.is_none() {
                    complete = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            if !complete {
                // A replica without a native core, or one that stopped mid-walk:
                // its domain's references are not known here.
                protection.opaque_domain(domain).map_err(content)?;
                pass.opaque_domains = pass.opaque_domains.saturating_add(1);
                continue;
            }
            pass.replicas = pass.replicas.saturating_add(1);
            for (root, bytes) in bundles {
                match self
                    .bundle_content(handles, ledger, domain, root, bytes)
                    .await
                {
                    Some((content_roots, inline)) => {
                        for inner in content_roots {
                            protection.protect_object(domain, inner).map_err(content)?;
                        }
                        for inner in inline {
                            protection.protect_object(domain, inner).map_err(content)?;
                        }
                    }
                    None => pass.bundles_unreadable = pass.bundles_unreadable.saturating_add(1),
                }
                tokio::task::yield_now().await;
            }
        }
        protection.finish();
        Ok(protection)
    }
    /// The content roots one bundle's header names, read once from this
    /// node's store and remembered; `None` when the bundle is not local or
    /// fails its structural check.
    async fn bundle_content(
        &mut self,
        handles: &NetworkHandles,
        ledger: LedgerId,
        domain: ContentDomainId,
        root: ContentHash,
        bytes: u64,
    ) -> Option<(Vec<ContentHash>, Vec<ContentHash>)> {
        if let Some(inner) = self.bundles.get(&root) {
            return Some(inner.clone());
        }
        let policy = handles.content.policy(ledger).await.ok()??;
        let reference = ContentRef {
            domain,
            root,
            length: bytes,
            class: ContentClass::Evidence,
        };
        let read = handles
            .content
            .read_bytes(policy.scope(), reference, MAX_BUNDLE_BYTES)
            .await
            .ok()?;
        let archive = StructuralArchive::inspect(read.value(), BUNDLE_INSPECTION).ok()?;
        let inner = (
            archive.header().content.clone(),
            archive.header().inline.clone(),
        );
        if self.bundles.len() >= MAX_BUNDLES {
            self.bundles.clear();
        }
        self.bundles.insert(root, inner.clone());
        Some(inner)
    }
    async fn seeds(&mut self, handles: &NetworkHandles) -> Result<AdminSeedReport, AccessError> {
        let mut report = AdminSeedReport::default();
        let mut after = None;
        while let Some((ledger, host)) = handles.fleet.next_host(after) {
            after = Some(ledger);
            let mut counted = false;
            for _ in 0..MAX_STEPS {
                let Ok(sweep) = host
                    .collect_seeds(self.config.grace_ms, now_ms()?, PAGE)
                    .await
                else {
                    break;
                };
                if !counted {
                    report.replicas = report.replicas.saturating_add(1);
                    counted = true;
                }
                report.visited = report.visited.saturating_add(sweep.visited);
                report.removed = report.removed.saturating_add(sweep.removed);
                report.bytes_removed = report.bytes_removed.saturating_add(sweep.bytes_removed);
                if sweep.complete {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        Ok(report)
    }
}
fn content(_: focal_evidence::ContentError) -> AccessError {
    AccessError::Capacity
}

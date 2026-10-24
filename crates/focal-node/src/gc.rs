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
    AdminCollectorReport, AdminContentStore, AdminGc, AdminGcConfig, AdminGcPass, AdminSeedReport,
    AdminStoreForm,
};
use focal_core::native::ContentRoot;
use focal_core::native::record_codec::{InspectionLimits, StructuralArchive};
use focal_evidence::{CollectorConfig, ProtectionSet};
use focal_model::{ContentClass, ContentDomainId, ContentHash, ContentRef, LedgerId};
use focal_rt::watch;
use focal_wire::AccessError;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Milliseconds between passes; one minute by default.
pub const INTERVAL_ENV: &str = "FOCAL_GC_INTERVAL_MS";
/// Milliseconds a file must have stood untouched before it is a candidate.
pub const GRACE_ENV: &str = "FOCAL_GC_GRACE_MS";
/// Milliseconds a quarantined round waits before deletion.
pub const QUARANTINE_ENV: &str = "FOCAL_GC_QUARANTINE_MS";
/// Milliseconds a finished upload's identity stays fenced.
pub const TERMINAL_ENV: &str = "FOCAL_GC_TERMINAL_MS";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
/// Rows one root page visits, and file visits one collector step takes (and one step of the
/// content store's conversion, `ContentStore::convert_step`).
pub(crate) const PAGE: usize = 4096;
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

/// What the collector's own memory may hold (26 §5): what it read of the hosted replicas' cold
/// data streams, kept between passes, and the windows it reads them through.
pub const GC_MEMORY_BYTES: usize = 128 * 1024 * 1024;

/// What the collector read of one replica's cold data stream (26 §5; doc 31 M2, A2): its bytes
/// from the start, and every content root their frames name, sorted, each once, charged as their
/// buffer grows. A frame never changes once appended, so a pass reads only what was appended
/// since the last. Read whole at every pass, a frame at a time through the session's owner (a
/// whole chunk or two read and hashed for each), the frames dedup'd against a list one by one,
/// the stream held the owner for most of a pass once a few minutes of retirements were behind
/// it.
struct ColdScanned {
    through: u64,
    roots: Vec<ContentHash>,
    charge: focal_memory::Allocation,
    /// Its roots outgrew the collector's memory: its domain stays opaque, read no more.
    overflowed: bool,
}
impl ColdScanned {
    fn new(budget: &focal_memory::MemoryBudget) -> Option<Self> {
        Some(Self {
            through: 0,
            roots: Vec::new(),
            charge: budget
                .reserve(
                    focal_memory::BudgetKind::Index,
                    focal_memory::BudgetLane::Ordinary,
                    0,
                )
                .ok()?
                .commit(),
            overflowed: false,
        })
    }
    /// What `data` appended since `self.through` names, read and joined to the roots, `through`
    /// moved to its end. Refused or failed, it stands where it stood; roots that outgrow the
    /// collector's memory are let go and it is kept opaque from here on (`overflowed`).
    fn read(
        &mut self,
        data: &focal_ledger::ColdData,
        inspection: InspectionLimits,
        budget: &focal_memory::MemoryBudget,
    ) -> Result<(), focal_evidence::ContentError> {
        use focal_evidence::ContentError;
        let mut found = Vec::new();
        let mut charge = budget
            .reserve(
                focal_memory::BudgetKind::Query,
                focal_memory::BudgetLane::Ordinary,
                0,
            )
            .map_err(|_| ContentError::Capacity)?
            .commit();
        let through = data.scan_content(self.through, inspection, budget, &mut |root| {
            if found.len() == found.capacity() {
                let more = found.capacity().max(64);
                charge
                    .grow(more.saturating_mul(size_of::<ContentHash>()))
                    .map_err(|_| ContentError::Capacity)?;
                found
                    .try_reserve_exact(more)
                    .map_err(|_| ContentError::Capacity)?;
            }
            found.push(root);
            Ok(())
        })?;
        if let Err(error) = self.join(found) {
            self.roots = Vec::new();
            let _ = self.charge.shrink_to(0);
            self.overflowed = true;
            return Err(error);
        }
        self.through = through;
        Ok(())
    }
    /// `found` joined to the roots, sorted and each once, the growth charged first: refused
    /// (`Capacity`), the roots are as they were.
    fn join(&mut self, mut found: Vec<ContentHash>) -> Result<(), focal_evidence::ContentError> {
        use focal_evidence::ContentError;
        if found.is_empty() {
            return Ok(());
        }
        let wanted = self
            .roots
            .len()
            .checked_add(found.len())
            .ok_or(ContentError::Capacity)?;
        if wanted > self.roots.capacity() {
            let room = wanted.max(self.roots.capacity().saturating_mul(2));
            let more = room
                .saturating_sub(self.roots.capacity())
                .checked_mul(size_of::<ContentHash>())
                .ok_or(ContentError::Capacity)?;
            self.charge.grow(more).map_err(|_| ContentError::Capacity)?;
            if self
                .roots
                .try_reserve_exact(room.saturating_sub(self.roots.len()))
                .is_err()
            {
                let held = self.charge.bytes().saturating_sub(more);
                let _ = self.charge.shrink_to(held);
                return Err(ContentError::Capacity);
            }
        }
        self.roots.append(&mut found);
        self.roots.sort_unstable();
        self.roots.dedup();
        Ok(())
    }
}

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
    /// What each hosted replica's cold data stream names, as far as it was read; a replica no
    /// longer hosted is let go at the next pass.
    colds: BTreeMap<LedgerId, ColdScanned>,
    /// The collector's own memory ([`GC_MEMORY_BYTES`]).
    budget: focal_memory::MemoryBudget,
    status: watch::Sender<AdminGc>,
}

/// The content store's form and its conversion as the operator reads them (29 §11): published
/// with the collector's state, whose passes over objects wait while the store converts.
pub(crate) fn admin_content(status: focal_evidence::ConversionStatus) -> AdminContentStore {
    let left = status.left();
    AdminContentStore {
        form: match status.form {
            focal_evidence::StoreForm::Clear => AdminStoreForm::Clear,
            focal_evidence::StoreForm::Counting => AdminStoreForm::Counting,
            focal_evidence::StoreForm::Converting => AdminStoreForm::Converting,
            focal_evidence::StoreForm::Sealed => AdminStoreForm::Sealed,
        },
        resumed: status.resumed,
        started_ms: status.started_ms,
        finished_ms: status.finished_ms,
        files_counted: status.counted.files,
        bytes_counted: status.counted.bytes,
        files_converted: status.converted.files,
        bytes_converted: status.converted.bytes,
        files_gone: status.gone.files,
        bytes_gone: status.gone.bytes,
        files_left: left.files,
        bytes_left: left.bytes,
        held: status.held,
        held_steps: status.held_steps,
        stopped: status.stopped.map(|cause| cause.to_string()),
    }
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
    /// Refused at the runtime's cell bound (its status is a watched value).
    pub fn new(
        node: u64,
        interval: Duration,
        config: CollectorConfig,
        budget: focal_memory::MemoryBudget,
    ) -> Result<(Self, GcHandle), focal_rt::RtError> {
        let interval = interval.max(Duration::from_millis(10));
        let (status, receiver) = watch::channel(AdminGc {
            node,
            config: admin_config(interval, config),
            passes: 0,
            collecting: false,
            last: None,
            content: None,
        })?;
        Ok((
            Self {
                node,
                interval,
                config,
                collecting: false,
                passes: 0,
                last: None,
                bundles: BTreeMap::new(),
                colds: BTreeMap::new(),
                budget,
                status,
            },
            GcHandle(receiver),
        ))
    }
    /// The interval and the collector's settings from the environment, else
    /// the defaults.
    pub fn from_env(
        node: u64,
        budget: focal_memory::MemoryBudget,
    ) -> Result<(Self, GcHandle), focal_rt::RtError> {
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
        Self::new(node, interval, config, budget)
    }
    pub async fn run(mut self, handles: &NetworkHandles) -> Result<(), AccessError> {
        if hyper_rt::futures::shard_id().is_none() {
            return Err(AccessError::Unavailable);
        }
        loop {
            focal_rt::time::sleep(self.interval)
                .await
                .map_err(|_| AccessError::Unavailable)?;
            // A pass that cannot run now (a store that refuses, a replica
            // that is stopping) waits for the next tick; nothing is inferred.
            let _ = self.pass(handles).await;
        }
    }
    fn publish(&self) {
        self.status.send(AdminGc {
            node: self.node,
            config: admin_config(self.interval, self.config),
            passes: self.passes,
            collecting: self.collecting,
            last: self.last,
            content: None,
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
            cold_bytes_scanned: 0,
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
            hyper_rt::futures::yield_now().await;
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
        let ledgers = Self::ledgers(handles, policies);
        self.colds.retain(|ledger, _| ledgers.contains(ledger));
        for ledger in ledgers {
            let domain = ContentDomainId(ledger.tenant.0);
            let Ok(host) = handles.fleet.current_host(ledger) else {
                protection.opaque_domain(domain).map_err(content)?;
                pass.opaque_domains = pass.opaque_domains.saturating_add(1);
                continue;
            };
            let mut cursor = None;
            let mut complete = false;
            let mut bundles = Vec::new();
            let mut cold = false;
            loop {
                let page = match host.content_roots(cursor, PAGE).await {
                    Ok(page) => page,
                    Err(_) => break,
                };
                for root in page.roots {
                    match root {
                        ContentRoot::Artifact { pointer, .. }
                        | ContentRoot::Inline { pointer, .. } => {
                            protection
                                .protect_object(domain, pointer.root)
                                .map_err(content)?;
                        }
                        ContentRoot::Bundle { root, bytes, .. } => {
                            protection.protect_object(domain, root).map_err(content)?;
                            bundles
                                .try_reserve_exact(1)
                                .map_err(|_| AccessError::Capacity)?;
                            bundles.push((root, bytes));
                        }
                        // A seal's bundle (F12) names no content of its own.
                        ContentRoot::Seal { root, .. } => {
                            protection.protect_object(domain, root).map_err(content)?;
                        }
                        // A cold family's frame is no content object; what it names of the
                        // ledger's content is read from the replica's cold stream (doc 31 M2,
                        // A2), every frame's at once below.
                        ContentRoot::Cold { .. } => cold = true,
                    }
                }
                cursor = page.next;
                if cursor.is_none() {
                    complete = true;
                    break;
                }
                hyper_rt::futures::yield_now().await;
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
                    None => {
                        // The bundle header is not readable here (absent policy,
                        // bytes not resident on this replica, or a structural
                        // check failure), so this domain's protection set is
                        // provably incomplete — the payload objects the header
                        // names are unknown. Mark the domain opaque and stop
                        // collecting it this pass, exactly as an incomplete roots
                        // walk does; otherwise those payloads, now referenced only
                        // by this bundle, would be reclaimed and the archived
                        // family permanently lost.
                        pass.bundles_unreadable = pass.bundles_unreadable.saturating_add(1);
                        protection.opaque_domain(domain).map_err(content)?;
                        pass.opaque_domains = pass.opaque_domains.saturating_add(1);
                        break;
                    }
                }
                hyper_rt::futures::yield_now().await;
            }
            if cold {
                if self.cold_roots(ledger, &host, pass).await {
                    if let Some(scanned) = self.colds.get(&ledger) {
                        for inner in &scanned.roots {
                            protection.protect_object(domain, *inner).map_err(content)?;
                        }
                    }
                } else {
                    // A stream not readable here, or one whose content outgrew the collector's
                    // memory, leaves this domain's protection provably incomplete: it is opaque
                    // this pass, as an unreadable bundle makes it.
                    pass.bundles_unreadable = pass.bundles_unreadable.saturating_add(1);
                    protection.opaque_domain(domain).map_err(content)?;
                    pass.opaque_domains = pass.opaque_domains.saturating_add(1);
                }
            }
        }
        protection.finish();
        Ok(protection)
    }
    /// Whether every content root `ledger`'s cold data stream names is known
    /// (`self.colds`): what was appended since the last pass read now, front to back, on the
    /// blocking pool's file share, away from the session's owner. False where the stream cannot
    /// be read here, its read failed (read again from where it stood at the next pass), or its
    /// roots outgrew the collector's memory.
    async fn cold_roots(
        &mut self,
        ledger: LedgerId,
        host: &crate::fleet::ReplicaHost,
        pass: &mut AdminGcPass,
    ) -> bool {
        let Ok(Some((data, inspection))) = host.cold_data().await else {
            return false;
        };
        let mut scanned = match self.colds.remove(&ledger) {
            Some(scanned) => scanned,
            None => match ColdScanned::new(&self.budget) {
                Some(scanned) => scanned,
                None => return false,
            },
        };
        if scanned.overflowed {
            self.colds.insert(ledger, scanned);
            return false;
        }
        if scanned.through > data.length() {
            // Not the stream it read (a replica restored anew): read from its start.
            scanned.through = 0;
            scanned.roots.clear();
        }
        let from = scanned.through;
        let budget = self.budget.clone();
        let read = focal_rt::on_pool(&focal_rt::FILES, move || {
            let read = scanned.read(&data, inspection, &budget);
            (scanned, read)
        })
        .await;
        match read {
            Ok((scanned, read)) => {
                let known = read.is_ok();
                if known {
                    pass.cold_bytes_scanned = pass
                        .cold_bytes_scanned
                        .saturating_add(scanned.through.saturating_sub(from));
                }
                // Refused or failed, it stands where it stood and is read again at the next pass.
                self.colds.insert(ledger, scanned);
                known
            }
            // The job unwound on its worker, what it held with it: read from the start next.
            Err(_) => false,
        }
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
        // The conversion's chunk of scratch, made and charged when a job first needs it and let go
        // with the pass.
        let mut scratch: Option<(Vec<u8>, focal_memory::Allocation)> = None;
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
                hyper_rt::futures::yield_now().await;
            }
            // The replica's seeds at rest (29 §12): converted a job at a time, at most MAX_STEPS
            // steps a pass, each job's chunks read, sealed and flushed on the blocking pool, so the
            // replica's owner only lists, tallies and lets names in the clear go. The last job's
            // result goes back before the next replica's turn.
            let mut done = None;
            let mut status = None;
            for _ in 0..MAX_STEPS {
                let Ok(reply) = host
                    .seed_conversion(done.take(), PAGE, focal_evidence::SEED_JOB_CHUNKS)
                    .await
                else {
                    break;
                };
                status = Some(reply.status);
                match reply.next {
                    focal_evidence::SeedStep::Done => break,
                    focal_evidence::SeedStep::Visited => {}
                    focal_evidence::SeedStep::Job(job) => {
                        let Some((buffer, charge)) = scratch.take().or_else(|| self.scratch())
                        else {
                            break;
                        };
                        let ran = focal_rt::on_pool(&focal_rt::FILES, move || {
                            let mut buffer = buffer;
                            let done = job.run(&mut buffer);
                            (done, buffer)
                        })
                        .await;
                        // A job the pool refused or lost leaves its chunks in the clear: the
                        // replica's owner takes them again at its next step.
                        let Ok((result, buffer)) = ran else {
                            break;
                        };
                        scratch = Some((buffer, charge));
                        done = Some(result);
                    }
                }
                hyper_rt::futures::yield_now().await;
            }
            if let Some(done) = done
                && let Ok(reply) = host.seed_conversion(Some(done), 0, 0).await
            {
                status = Some(reply.status);
            }
            if let Some(status) = status {
                add_conversion(&mut report.conversion, &status);
            }
        }
        Ok(report)
    }
    /// A chunk of scratch for the seed conversion's jobs, charged to the agent's budget; none where
    /// the budget has not the room, and the conversion waits for the next pass.
    fn scratch(&self) -> Option<(Vec<u8>, focal_memory::Allocation)> {
        let bytes = focal_evidence::SEED_CHUNK_BYTES;
        let charge = self
            .budget
            .reserve(
                focal_memory::BudgetKind::Payload,
                focal_memory::BudgetLane::Ordinary,
                bytes,
            )
            .ok()?
            .commit();
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(bytes).ok()?;
        buffer.resize(bytes, 0);
        Some((buffer, charge))
    }
}
/// One replica's seed directory at rest added to the node's view (29 §12): its form counted, and
/// what its conversion counted, converted, found gone and has left.
fn add_conversion(
    view: &mut focal_client::admin::AdminSeedConversion,
    status: &focal_evidence::ConversionStatus,
) {
    use focal_evidence::StoreForm;
    let form = match status.form {
        StoreForm::Clear => &mut view.clear,
        StoreForm::Counting => &mut view.counting,
        StoreForm::Converting => &mut view.converting,
        StoreForm::Sealed => &mut view.sealed,
    };
    *form = form.saturating_add(1);
    if status.held {
        view.held = view.held.saturating_add(1);
    }
    if status.stopped.is_some() {
        view.stopped = view.stopped.saturating_add(1);
    }
    let left = status.left();
    view.files_counted = view.files_counted.saturating_add(status.counted.files);
    view.bytes_counted = view.bytes_counted.saturating_add(status.counted.bytes);
    view.files_converted = view.files_converted.saturating_add(status.converted.files);
    view.bytes_converted = view.bytes_converted.saturating_add(status.converted.bytes);
    view.files_gone = view.files_gone.saturating_add(status.gone.files);
    view.bytes_gone = view.bytes_gone.saturating_add(status.gone.bytes);
    view.files_left = view.files_left.saturating_add(left.files);
    view.bytes_left = view.bytes_left.saturating_add(left.bytes);
}
fn content(_: focal_evidence::ContentError) -> AccessError {
    AccessError::Capacity
}

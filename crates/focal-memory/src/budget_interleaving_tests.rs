use super::*;
use std::cell::RefCell;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Step {
    Acquired,
    RefundReady,
    GrowReady,
    TrimReady,
}

struct Hook {
    step: Step,
    source: MemoryBudget,
    reached: Sender<()>,
    resume: Receiver<()>,
}

thread_local! {
    static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

/// One-shot and scoped to this source's identity. Remove the hook before
/// waiting so recursion and allocation drops never retain a RefCell borrow.
/// A failed test releases its worker through Gate::drop; timeouts cannot panic
/// inside an Allocation destructor or strand the test process indefinitely.
pub(super) fn pause(step: Step, budget: &MemoryBudget) {
    let hook = HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|hook| hook.step == step && Arc::ptr_eq(&hook.source.0, &budget.0))
        {
            slot.take()
        } else {
            None
        }
    });
    if let Some(hook) = hook
        && hook.reached.send(()).is_ok()
    {
        let _ = hook.resume.recv_timeout(WAIT);
    }
}

struct Installed;
impl Installed {
    fn new(hook: Hook) -> Self {
        HOOK.with(|slot| assert!(slot.replace(Some(hook)).is_none()));
        Self
    }
}
impl Drop for Installed {
    fn drop(&mut self) {
        HOOK.with(|slot| {
            slot.take();
        });
    }
}

struct Gate {
    reached: Receiver<()>,
    resume: Option<Sender<()>>,
}
impl Gate {
    fn wait(&self) {
        self.reached.recv_timeout(WAIT).unwrap();
    }
    fn resume(mut self) {
        // A timed-out pause closes its receiver, making this test fail rather
        // than silently treating an uncontrolled interleaving as qualification.
        self.resume.take().unwrap().send(()).unwrap();
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        if let Some(resume) = self.resume.take() {
            let _ = resume.send(());
        }
    }
}

fn gate(step: Step, source: &MemoryBudget) -> (Hook, Gate) {
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    (
        Hook {
            step,
            source: source.clone(),
            reached: reached_tx,
            resume: resume_rx,
        },
        Gate {
            reached: reached_rx,
            resume: Some(resume_tx),
        },
    )
}

fn kind(stats: &BudgetStats, kind: BudgetKind) -> usize {
    stats.by_kind[kind as usize]
}

fn balanced(stats: &BudgetStats) {
    assert_eq!(stats.by_kind.iter().sum::<usize>(), stats.used);
    assert!(stats.used <= stats.limit);
    assert!(stats.ordinary_used <= stats.used);
    assert!(stats.ordinary_used <= stats.limit - stats.completion_reserve);
}

#[test]
fn acquired_credit_cannot_be_trimmed_before_usage_or_ancestor_transfer_is_visible() {
    let parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let baseline = parent.stats();
    let mut pool = parent
        .elastic_funded_child(BudgetLane::Ordinary, 128, 64)
        .unwrap();
    let source = pool.budget().clone();
    let backed = parent.stats();
    let unrelated_parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let unrelated = unrelated_parent
        .elastic_funded_child(BudgetLane::Ordinary, 64, 64)
        .unwrap();

    thread::scope(|scope| {
        let (hook, gate) = gate(Step::Acquired, &source);
        let worker = scope.spawn(|| {
            let _installed = Installed::new(hook);
            // The same stage on another elastic source must not consume or
            // trigger the targeted hook.
            drop(
                unrelated
                    .budget()
                    .reserve(BudgetKind::Query, BudgetLane::Completion, 1)
                    .unwrap(),
            );
            source
                .reserve(BudgetKind::Pages, BudgetLane::Completion, 64)
                .unwrap()
                .commit()
        });
        gate.wait();
        assert_eq!(source.stats().used, 0);
        assert_eq!(parent.stats(), backed);
        assert_eq!(pool.available(), 0);
        assert!(pool.trim_unused(64).is_err());
        assert!(
            source
                .reserve(BudgetKind::Payload, BudgetLane::Completion, 1)
                .is_err()
        );
        assert_eq!(pool.funded_capacity(), 64);
        assert_eq!(parent.stats(), backed);
        gate.resume();
        let pages = worker.join().unwrap();
        assert_eq!(source.stats().used, 64);
        assert_eq!(kind(&parent.stats(), BudgetKind::Pages), 64);
        assert_eq!(
            kind(&parent.stats(), BudgetKind::Reserved),
            backed.used - 64
        );
        drop(pages);
    });

    assert_eq!(parent.stats(), backed);
    assert_eq!(pool.available(), 64);
    balanced(&source.stats());
    pool.trim_unused(64).unwrap();
    drop(source);
    drop(pool);
    assert_eq!(parent.stats(), baseline);
    drop(unrelated);
    assert_eq!(unrelated_parent.stats().used, 0);
}

#[test]
fn refunded_categories_do_not_expose_credit_until_the_final_publication() {
    let parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let baseline = parent.stats();
    let mut pool = parent
        .elastic_funded_child(BudgetLane::Ordinary, 128, 64)
        .unwrap();
    let source = pool.budget().clone();
    let backed = parent.stats();
    let pages = source
        .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 64)
        .unwrap()
        .commit();

    thread::scope(|scope| {
        let (hook, gate) = gate(Step::RefundReady, &source);
        let worker = scope.spawn(|| {
            let _installed = Installed::new(hook);
            drop(pages);
            // A second refund on this source must proceed: the hook is one-shot.
            drop(
                source
                    .reserve(BudgetKind::Query, BudgetLane::Completion, 64)
                    .unwrap(),
            );
        });
        gate.wait();
        assert_eq!(source.stats().used, 0);
        assert_eq!(source.stats().ordinary_used, 0);
        assert_eq!(parent.stats(), backed);
        assert_eq!(pool.available(), 0);
        assert!(pool.trim_unused(64).is_err());
        assert!(
            source
                .reserve(BudgetKind::Payload, BudgetLane::Completion, 1)
                .is_err()
        );
        assert_eq!(parent.stats(), backed);
        gate.resume();
        worker.join().unwrap();
    });

    assert_eq!(pool.available(), 64);
    assert_eq!(parent.stats(), backed);
    balanced(&source.stats());
    pool.trim_unused(64).unwrap();
    drop(source);
    drop(pool);
    assert_eq!(parent.stats(), baseline);
}

#[test]
fn growth_holds_parent_capacity_before_any_new_credit_can_be_spent() {
    let parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let baseline = parent.stats();
    let pool = parent
        .elastic_funded_child(BudgetLane::Ordinary, 64, 0)
        .unwrap();
    let source = pool.budget().clone();
    let metadata = parent.stats().used;
    let pressure = parent
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            parent.limit() - metadata - 64,
        )
        .unwrap();

    let mut pool = thread::scope(|scope| {
        let (hook, gate) = gate(Step::GrowReady, &source);
        let worker = scope.spawn(move || {
            let _installed = Installed::new(hook);
            let mut pool = pool;
            pool.grow(64).unwrap();
            pool
        });
        gate.wait();
        assert_eq!(parent.stats().used, parent.limit());
        assert_eq!(parent.stats().ordinary_used, metadata + 64);
        assert_eq!(kind(&parent.stats(), BudgetKind::Reserved), metadata + 64);
        assert_eq!(source.stats().used, 0);
        assert!(
            source
                .reserve(BudgetKind::Pages, BudgetLane::Completion, 1)
                .is_err()
        );
        assert!(
            parent
                .reserve(BudgetKind::Payload, BudgetLane::Completion, 1)
                .is_err()
        );
        gate.resume();
        worker.join().unwrap()
    });

    assert_eq!(pool.funded_capacity(), 64);
    assert_eq!(pool.available(), 64);
    let pages = source
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 64)
        .unwrap();
    assert_eq!(parent.stats().used, parent.limit());
    assert_eq!(kind(&parent.stats(), BudgetKind::Pages), 64);
    drop(pages);
    balanced(&parent.stats());
    pool.trim_unused(64).unwrap();
    let competitor = parent
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 64)
        .unwrap();
    assert_eq!(parent.stats().used, parent.limit());
    drop(competitor);
    drop(pressure);
    drop(source);
    drop(pool);
    assert_eq!(parent.stats(), baseline);
}

#[test]
fn trimmed_credit_is_unspendable_before_the_parent_refund_becomes_available() {
    let parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let baseline = parent.stats();
    let pool = parent
        .elastic_funded_child(BudgetLane::Ordinary, 64, 64)
        .unwrap();
    let source = pool.budget().clone();
    let backed = parent.stats();
    let pressure = parent
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            parent.limit() - backed.used,
        )
        .unwrap();
    let occupied = parent.stats();

    let mut pool = thread::scope(|scope| {
        let (hook, gate) = gate(Step::TrimReady, &source);
        let worker = scope.spawn(move || {
            let _installed = Installed::new(hook);
            let mut pool = pool;
            pool.trim_unused(64).unwrap();
            pool
        });
        gate.wait();
        assert_eq!(parent.stats(), occupied);
        assert_eq!(source.stats().used, 0);
        assert!(
            source
                .reserve(BudgetKind::Pages, BudgetLane::Completion, 1)
                .is_err()
        );
        assert!(
            parent
                .reserve(BudgetKind::Payload, BudgetLane::Completion, 1)
                .is_err()
        );
        gate.resume();
        worker.join().unwrap()
    });

    assert_eq!(pool.funded_capacity(), 0);
    assert_eq!(pool.available(), 0);
    assert_eq!(pool.budget().limit(), 64);
    assert_eq!(parent.stats().used, occupied.used - 64);
    assert_eq!(parent.stats().ordinary_used, occupied.ordinary_used - 64);
    assert_eq!(
        kind(&parent.stats(), BudgetKind::Reserved),
        backed.used - 64
    );
    let competitor = parent
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 64)
        .unwrap();
    let full = parent.stats();
    assert!(pool.grow(64).is_err());
    assert_eq!(parent.stats(), full);
    assert_eq!(pool.funded_capacity(), 0);
    assert_eq!(pool.available(), 0);
    drop(competitor);
    pool.grow(64).unwrap();
    drop(
        source
            .reserve(BudgetKind::Pages, BudgetLane::Completion, 64)
            .unwrap(),
    );
    balanced(&source.stats());
    balanced(&parent.stats());
    drop(pressure);
    drop(source);
    drop(pool);
    assert_eq!(parent.stats(), baseline);
}

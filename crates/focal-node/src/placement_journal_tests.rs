use super::*;
use focal_control::{ControlScope, RetireContactCommand};
use std::collections::BTreeMap;

const NAME: &str = "placement-partition";
/// A node's local client, which names its intents where it leads the owner.
const LOCAL: [u8; 16] = [0x0a; 16];
/// Its enrolled principal, which names them where it follows (24 §16).
const PRINCIPAL: [u8; 16] = [0x0b; 16];

fn target() -> ControlIdentity {
    ControlIdentity {
        cluster: focal_directory::ClusterId([1; 16]),
        group: [2; 16],
        scope: ControlScope::Root,
        genesis: [3; 32],
    }
}
fn intent(client: [u8; 16], sequence: u64) -> ControlRequest {
    ControlRequest {
        id: ControlRequestId { client, sequence },
        acknowledged_through: sequence - 1,
        command: ControlCommand::RetireContact(RetireContactCommand {
            node: 9,
            expected_generation: 1,
        }),
    }
}
/// The journal at `root` as a node left it.
fn left(root: &Path, saved: &Saved) {
    crate::embedded::durable_dir(&root.join("cluster")).unwrap();
    PrivateJournal::open(
        root.join("cluster").join(NAME),
        &focal_seal::test_keys(&root).unwrap(),
    )
    .unwrap()
    .replace(&postcard::to_stdvec(saved).unwrap())
    .unwrap();
}

/// A node that led its partition journaled an intent under its local client
/// and stopped before it resolved; restarted, it follows the partition, whose
/// seated peer leads now, and opens the journal as its principal. The intent
/// keeps the identity it was journaled under, so it resolves through the
/// leader as the design has it, and the journal adopts the principal once it
/// has, the local client's window kept. The journal had been refused, and the
/// node could place nothing in that partition until it led it again.
#[test]
fn an_intent_journaled_under_one_client_resolves_after_a_reopen_under_the_other() {
    let directory = tempfile::tempdir().unwrap();
    let pending = intent(LOCAL, 4);
    left(
        directory.path(),
        &Saved {
            schema: 2,
            target: target(),
            client: LOCAL,
            completed: 3,
            pending: Some(pending.clone()),
            windows: BTreeMap::from([(PRINCIPAL, 7)]),
        },
    );
    let mut journal = IntentJournal::open(
        directory.path(),
        &focal_seal::test_keys(directory.path()).unwrap(),
        NAME,
        target(),
        PRINCIPAL,
    )
    .unwrap();
    assert_eq!(journal.pending(), Some(&pending));
    assert_eq!(journal.completed(), 3);
    // Its receipt arrives: the local client's window stands at the intent,
    // and the journal is the principal's, at the principal's window.
    journal.saved.completed = 4;
    journal.resolved().unwrap();
    assert!(journal.pending().is_none());
    assert_eq!(journal.saved.client, PRINCIPAL);
    assert_eq!(journal.completed(), 7);
    assert_eq!(journal.saved.windows.get(&LOCAL), Some(&4));
    assert!(!journal.saved.windows.contains_key(&PRINCIPAL));
}

/// With nothing pending a journal adopts the client it is opened with at
/// once, and a refused intent, whose sequence stays free, leaves its client's
/// window where it was.
#[test]
fn a_journal_with_nothing_pending_adopts_its_opener_and_a_refusal_frees_its_sequence() {
    let directory = tempfile::tempdir().unwrap();
    left(
        directory.path(),
        &Saved {
            schema: 2,
            target: target(),
            client: LOCAL,
            completed: 3,
            pending: None,
            windows: BTreeMap::new(),
        },
    );
    let journal = IntentJournal::open(
        directory.path(),
        &focal_seal::test_keys(directory.path()).unwrap(),
        NAME,
        target(),
        PRINCIPAL,
    )
    .unwrap();
    assert_eq!(journal.saved.client, PRINCIPAL);
    assert_eq!(journal.completed(), 0);
    assert_eq!(journal.saved.windows.get(&LOCAL), Some(&3));
    drop(journal);
    // Reopened as the local client with its intent refused: the sequence the
    // intent held is free again under the principal's window kept.
    let directory = tempfile::tempdir().unwrap();
    left(
        directory.path(),
        &Saved {
            schema: 2,
            target: target(),
            client: PRINCIPAL,
            completed: 7,
            pending: Some(intent(PRINCIPAL, 8)),
            windows: BTreeMap::from([(LOCAL, 3)]),
        },
    );
    let mut journal = IntentJournal::open(
        directory.path(),
        &focal_seal::test_keys(directory.path()).unwrap(),
        NAME,
        target(),
        LOCAL,
    )
    .unwrap();
    journal.resolved().unwrap();
    assert_eq!(journal.saved.client, LOCAL);
    assert_eq!(journal.completed(), 3);
    assert_eq!(journal.saved.windows.get(&PRINCIPAL), Some(&7));
}

/// What a journal is still refused for: another owner's, an intent that is
/// not its client's next, and a table of clients with no room for the one it
/// is opened with while an intent waits.
#[test]
fn a_journal_of_another_owner_or_out_of_sequence_or_without_room_is_refused() {
    let other = ControlIdentity {
        group: [4; 16],
        ..target()
    };
    let directory = tempfile::tempdir().unwrap();
    left(
        directory.path(),
        &Saved {
            schema: 2,
            target: other,
            client: LOCAL,
            completed: 3,
            pending: None,
            windows: BTreeMap::new(),
        },
    );
    assert!(matches!(
        IntentJournal::open(
            directory.path(),
            &focal_seal::test_keys(directory.path()).unwrap(),
            NAME,
            target(),
            LOCAL
        ),
        Err(IntentError::Identity)
    ));
    let directory = tempfile::tempdir().unwrap();
    left(
        directory.path(),
        &Saved {
            schema: 2,
            target: target(),
            client: LOCAL,
            completed: 3,
            pending: Some(intent(LOCAL, 5)),
            windows: BTreeMap::new(),
        },
    );
    assert!(matches!(
        IntentJournal::open(
            directory.path(),
            &focal_seal::test_keys(directory.path()).unwrap(),
            NAME,
            target(),
            PRINCIPAL
        ),
        Err(IntentError::Identity)
    ));
    let directory = tempfile::tempdir().unwrap();
    left(
        directory.path(),
        &Saved {
            schema: 2,
            target: target(),
            client: LOCAL,
            completed: 3,
            pending: Some(intent(LOCAL, 4)),
            windows: BTreeMap::from([([1; 16], 1), ([2; 16], 1), ([3; 16], 1), ([4; 16], 1)]),
        },
    );
    assert!(matches!(
        IntentJournal::open(
            directory.path(),
            &focal_seal::test_keys(directory.path()).unwrap(),
            NAME,
            target(),
            PRINCIPAL
        ),
        Err(IntentError::Capacity)
    ));
}

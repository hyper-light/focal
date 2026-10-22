# 29. Sealing at rest

Goal condition 5 asks that every byte focal keeps be encrypted at rest with aws-lc-rs and
opened only on access. In transit this already holds (F58: hybrid ML-KEM key exchange only,
256-bit AEADs only). At rest nothing is sealed as of 2026-10-07: `hyper-seal` is vendored and
no focal crate calls it. This document fixes where the root key comes from, the keys below
it, what each store seals with, and the order the stores move in. The construction itself is
`hyper-seal`'s (hyper-raft `docs/seal.md`); nothing here invents cryptography.

## 1. The construction, inherited

From `docs/seal.md`, unchanged:

- **Keys** are random 256-bit keys in a hierarchy, each wrapped by its parent with AES-256 key
  wrap (RFC 3394, `WrappingKey`). A record of a wrapped key is 61 bytes. Every key is random,
  never derived, so a learned child reveals nothing above it, and destroying a parent's only
  wrapped copy erases every child (NIST SP 800-88r1 cryptographic erase).
- **Files written once** are sealed in segments by STREAM (Hoang, Reyhanitabar, Rogaway and
  Vizár, CRYPTO 2015), each segment an AES-256-GCM seal at a nonce counting its segment and
  marking the last, so truncation, reordering and splicing are detected.
- **Appended logs** seal each record under a key per writer session at its file offset, so no
  key seals two records at one offset across crashes. Every frame and header carries a MAC
  under the log's authentication key.
- **Nonces are never random**: each key seals one sequence and its nonce is the position in it
  (SP 800-38D §8.2.1).
- **Post-quantum.** AES-256 keeps 128 bits against Grover's search (CNSA 2.0). A key sent to
  another machine is wrapped to its ML-KEM-1024 key (`recipient`, FIPS 203).

## 2. Where the root key comes from (F60)

**Decision.** The root key is a `hyper_seal::FileSource`: 32 random bytes in a file outside
the data directory, owner-only (0600, or an owner-only DACL on Windows), refused at open if any
other principal can read it. The hardware sources (macOS Keychain or Secure Enclave, TPM 2.0,
Windows DPAPI/CNG, a cloud KMS) implement the same `KeySource` trait and are chosen by
configuration when present (§7).

- **Its path** is `storage.root_key` in the node's configuration. Unset, it is in the platform's
  per-user configuration directory, never the data directory: `~/Library/Application
  Support/Focal Keys/` on macOS, `$XDG_CONFIG_HOME/focal/keys/` (else `~/.config/focal/keys/`) on
  Linux, `%APPDATA%\Focal\keys\` on Windows, the file named for the data directory's root key ID
  (`<root key id>.key`, the ID `SEAL.node` records in the clear), so a directory finds its key
  file from itself before it knows any identity: a joining node seals its join, its private key
  among it, before it has a node ID (2026-10-20). A node whose keys an earlier binary made under
  the file named for its node (`<node>.key`) keeps that file; a client's data directory has keys
  of its own the same way. In a container or Kubernetes the data directory is a volume and the key
  is a mounted secret file named by `storage.root_key`, as CockroachDB's store keys and TiKV's
  master key are; a backup names the root key ID it needs, which is the file's name.
- **A key inside the data directory is refused at start** (typed, before anything opens): a
  copy of the data directory alone, in a backup, an rsync or a detached volume, must never
  carry what opens it.
- **It is made once**, at a node's first start, before any sealed store is created. A data
  directory whose seal record (§3) names a root key ID that the file does not hold is refused,
  typed: a wrong or missing key is never answered by a fresh one.
- **Made once even by makers at once** (2026-10-20). Two processes may make one directory's keys
  together (two command lines in a new client directory, nodes starting under one configured key
  file), and a start may fail or be killed part-way. Every file of the making, the root key file,
  `SEAL.root` and `SEAL.node`, is therefore published where none is and never over one: written
  to a temporary of the maker's own beside its name (`<name>.<16 hex digits>.part`), synced, and
  linked to the name without clobbering (a hard link on Unix, `MoveFileExW` without
  `MOVEFILE_REPLACE_EXISTING` on Windows), which only the first maker's link does. A reader finds
  no file or a whole one (before, a maker could read the configured key file while another was
  writing it, and a `SEAL.node` installed by an atomic replace could stand over the one another
  maker had already sealed under). Once a name stands, its maker removes every temporary of it,
  so what a maker cut short between its write and its link left goes with the name's next
  publication. A directory whose owner names no key file records its root key's ID in
  `SEAL.root` before it makes the key file: every maker, and every start after one cut short,
  makes and opens the one file the intent names, where a fresh ID each attempt left a key file
  nothing named for every start a failing disk cut short.
- **Under a build with test support** (tests only; a release build never has it), the per-user
  directory is the system's temporary one, and a directory made without a key file takes the one
  root key every test's directory shares (`focal_seal::TEST_ROOT`): its node and store keys are
  its own and random, so no directory's records open under another's, and a test run leaves no
  key file behind for each directory it made (one each had left 1,153 files in four hours).
- **Losing the key loses the data**, as with every system that seals at rest. `focal backup`
  states which root key ID a backup needs, and the operator keeps the key file as they keep
  any secret. Rotation is a rewrap (§4); nothing below the node key moves.

**Why a file first.** Each established system ships a file-held master key as its baseline
and adds external key services on top: CockroachDB (store key files named per store), TiKV
(`master-key` of type `file`, or a KMS), MySQL (`keyring_file`), MongoDB (a local keyfile, or
KMIP). Vault, by contrast, needs an unseal ceremony or a KMS before it serves, which a laptop
cannot be asked for. A file works on all eight release targets with no platform service and no
`unsafe`. It protects against a copied, imaged or stolen data volume, a backup read without the
key, and a disk returned or discarded. Against an attacker who reads the whole user account on
one disk it protects nothing, and only a hardware source does. That gap is closed by §7, not by
pretending a file is more than it is.

## 3. The keys below the root

```
root key (FileSource, or a hardware source)
  └─ node key            random; wrapped by the root; SEAL.node in the data directory (61 B record)
      ├─ log parent key  the hyper-log Sealing.parent: every writer session's key wraps under it
      ├─ log auth key    the hyper-log Sealing.auth: every frame's and header's MAC
      ├─ group key       hyper-durable's group files: images, checkpoints, meta
      ├─ content key     the content store: each object a STREAM under a data key it wraps
      └─ journal key     admin, client and MCP journals, catalogues, trust and identity files
```

Each child is a separate random key, wrapped by the node key and kept in `SEAL.node` beside it,
so one store's key reveals nothing of another's and each can be rotated alone. Per-ledger tenant
keys, which would let a ledger be erased cryptographically wherever its bytes lie, go under the
node key in the same way once retention asks for erase (doc 26). They are not taken now.

## 4. Rotation

A new root generation rewraps the node key (one 61-byte record). A rotated node key rewraps its
five children. A rotated child seals new writes, and the old one is kept, never used to seal, to
open what it sealed until compaction or rewrite retires those bytes. A wrapping key's
originator-usage period is at most two years (SP 800-57 Pt 1, Table 1).

## 5. What each store seals with

| Store | Files | Construction | Owner of the code |
|---|---|---|---|
| Node log | `raft/log` | hyper-log `With { sealing: Some(Sealing { parent, auth }) }` | hyper-log (exists) |
| Group files | `raft/groups/*/{image,meta}` | `hyper_seal::sealed_file` over the framed file (magic, version, payload, CRC kept inside), a data key per file wrapped by the node's group-file key, which is unwrapped from the root key file for that write or read and wiped after it (`focal_seal::store_key`): no store key is held between files, so a node's 1,058 groups never reach the locked region's bound | focal-consensus (`group_files`) |
| Content | `content/` | STREAM per object, names keyed per node (`hyper_seal::name`) so a stored name does not reveal the content's hash | focal-evidence |
| Journals and small files | `*.admin`, `CLIENT.contexts`, `MCP.operations`, `catalogue.bin`, `trust-adopted.bin`, `IDENTITY`'s secret half | STREAM per file, written by the existing atomic replace | focal-platform `install` |
| Payload log | `payloads/*.seg` (a cache, emptied at every open) | each record AES-256-GCM at its segment and offset, under a key of the log's own derived from the process's cache key, never written (§12.1) | focal-ledger `native_payloads` |
| Legacy WAL | `wal/` (below the storage level) | not sealed. The conversion to hyper-log seals as it copies, and the WAL is removed after the conversion is verified | focal-consensus `convert` |

A store is sealed from its creation. A store that exists unsealed is sealed by rewriting it once
under the fence that marks the data directory sealed, as the WAL conversion does: resumable from
a crash cut, never mixed within one file.

## 6. Measured before it is the default

`docs/seal.md` §11 states the measurements a log takes before it seals by default. focal takes
them at its own frame sizes, with records of a few hundred bytes and group commit at the measured
flush, and records them here:

- group commit sealed against unsealed: p50, p99 and p999 of the acknowledgement, under the
  machine's own load, interleaved runs;
- the open of one 300 B and one 4 KiB record, warm and cold;
- seal and open throughput per core;
- allocations on the seal and open paths after setup, which must be none.

Sealing is the default regardless. A measured cost above the flush distribution's margin is a
defect to fix in the sealed path, never a reason to ship unsealed.

## 7. Hardware sources, next

The next sources are, in order: macOS Keychain with the Secure Enclave (the key never leaves the
enclave, it wraps and unwraps in place); TPM 2.0 on Linux and Windows; a cloud KMS (AWS KMS, GCP
KMS) for fleets. Each is a `KeySource` in `focal-platform`, chosen by `storage.root_key`'s
scheme, with its monotonic counter offered for rollback protection where it has one (`docs/seal.md`
§5.2).

## 8. Order of work

1. The root key source, `SEAL.node` and the refusals of §2 (focal-platform, focal-consensus).
2. The node log sealed: `open_shell` passes `Sealing`. Measured per §6.
3. Journals and small files: one sealed install path for every atomic replace.
4. Content: STREAM objects and keyed names, with a rewrite pass for existing stores.
5. Group files, through `hyper_seal::sealed_file` (hyper-raft branch `hyper-seal-files`, written for focal, mantle and slates alike).
6. Hardware sources (§7).

Each step carries its own tamper tests: a flipped byte, a truncation, a spliced segment, a swapped
file and a wrong key are each refused, typed, and never served.

## 9. As built (2026-10-07)

- **Steps 1 and 2.** `focal-seal` makes or opens a node's keys:
  - the root key file is made owner-only and durable with its directory, and never made again for a data directory that has keys;
  - `SEAL.node` holds the node key under the root and the five store keys under the node key, ending in a BLAKE3 hash;
  - the process's locked key region is made on first use, sized at one page of slots.

  Every refusal of §2 is typed and tested:
  - a key file inside the data directory, refused before anything is made;
  - a missing key file, never made again;
  - another node's key file, `WrongKey`, from key wrap's integrity check;
  - a damaged `SEAL.node`, `Damaged`, told apart from a wrong key by its hash;
  - a key file others may read, or of the wrong length.
- **The node log is sealed from its creation.** `open_node_storage` takes the root key file (`node.root_key`, or the platform default named for the node), and the shell's log is created and opened with `hyper_log::Sealing`. The conversion creates its log sealed and verifies it sealed. The shell had not shipped, so no unsealed shell log exists to convert.
- **The log's derived configuration is the sealed one** (`node_log::config`, `sealed: true`): its frames carry their MAC and a key record, and its records their tags.
- **Open.** A sealed log opened without its keys is refused, but hyper-log reports it as a foreign log. A typed refusal is asked of hyper-raft. The renderer does not yet mount the root key as a Kubernetes Secret or a systemd credential. It is the next deployment change, with its goldens.
- **Step 5 (group files).** Every group's records and image are sealed before their durable
  install. The framed file of 27 §15.4 becomes the sealed file's plaintext, so its magic, version
  and checksum still refuse another file's bytes under a valid seal (a records file renamed to an
  image opens, then reads as "another file's magic"). Each file has its own data key, under the
  node's group-file key, read from the root key file for that one write or read: images are written
  at the owners' checkpoint cadence, so the unwrap costs microseconds a thousand entries. Tested:
  - a flipped byte at every offset, and a cut tail, refused by the seal;
  - no group file holds its image's bytes, its records' floor or either magic in the clear;
  - another node's keys do not open a group's files;
  - the crash cut at every operation of a rewrite still leaves the old file or the new;
  - the conversion writes and verifies sealed group files.

## 10. The root key in deployments (a blocker for the storage fence)

The default key path (§2) is right on a laptop or a VM, where the user's configuration
directory persists across restarts. In a container it is wrong. A container's home is its writable
layer, which is gone when the pod or container is recreated. The node then refuses to start
(`MissingKey`, correctly), and its sealed data cannot be read. The defect is latent: the node's
capability level (2) is below the storage level (4), so no node reaches the sealed shell yet. The
fence must not open until every rendered deployment gives the node a persistent key.

- **Kubernetes.** The renderer emits:
  - a Secret holding one key per pod (`<pod>.key`), made by a keys script beside the invitations
    script with `focal create root-key`;
  - an init container that copies the pod's key from the Secret mount into a memory-backed
    `emptyDir` (`medium: Memory`) as a file owned by the pod's user, mode 0600;
  - `node.root_key` pointing at that file.

  A Secret volume is owned by root and readable by `fsGroup`, which focal's owner-only check
  refuses for a non-root process. The copy keeps the key off every disk inside the pod, which is
  also how Vault's agent injector hands secrets over. Kubernetes stores Secrets in etcd, encrypted
  at rest where the cluster's `EncryptionConfiguration` enables it.
- **systemd.** The unit takes the key by `LoadCredentialEncrypted=focal-root-key:…`
  (systemd-creds, sealed to the host's TPM 2.0 where it has one), and `node.root_key` names
  `$CREDENTIALS_DIRECTORY/focal-root-key`. systemd makes that file owner-only and keeps it
  in memory.
- **Docker and compose.** The key is a mounted secret (`/run/secrets/…`), copied at entry into an
  owner-only memory file as on Kubernetes.
- **Laptops and VMs** keep §2's default.

The renderer's goldens change with this, and each deployment test (DC01–DC20) restarts a node
across a container's recreation and reads its sealed data back.

## 11. The content store, designed (step 4)

The content store keeps every artifact's bytes in `objects/<domain>/`, one file per chunk named by
its content hash, plus a manifest per object, staging for uploads, and custody records. It is the
largest store a node keeps, and its file names alone reveal the content hashes of what it holds.

- **A key per content domain.** Each content domain (a tenant's) gets a random key, wrapped by
  the node's content key and kept beside its objects as a 61-byte record. Destroying that record
  erases every byte of the domain wherever it lies: on this node, in its backups and archives
  (NIST SP 800-88r1 cryptographic erase; the erase doc 26's retention asks for).
- **Chunks sealed by content** (`FileSealer::content_keyed`, docs/seal.md §4). A chunk's data key
  and file ID derive from its domain key and its 256-bit hash, so the same chunk seals to the same
  bytes. Deduplication within a domain, idempotent re-seals and the store's verify-by-rehash all
  keep working. Two different chunks share a key only if their hashes collide (Bellare,
  Keelveedhi and Ristenpart, EUROCRYPT 2013, message-locked encryption within one parent).
- **Names keyed per domain** (`hyper_seal::name`, §7): a chunk's file name is a keyed hash of its
  content hash, so a directory listing reveals nothing about what the domain holds.
- **Manifests and custody records** are sealed whole under the domain key, as group files are.
- **Keys held.** The store has one owner, the node's exclusive content writer, which holds the
  node's content key for its life (one slot). It keeps a bounded cache of unwrapped domain keys,
  evicted least recently used, sized within the locked region. A miss unwraps the domain's record
  again: microseconds, never a disk read of the root key.
- **Range reads** open only the 64 KiB segments a range covers (`SealedReader`). Every chunk
  delivered is still hashed whole, as now.
- **Transfers stay plaintext inside TLS** (F58): a node seals what it receives under its own keys.
  Replicas therefore hold different ciphertext for the same chunk, and none needs another's keys.
- **Existing stores.** A store written before the storage fence is rewritten once, chunk by chunk,
  when the node crosses the fence, resumable from any crash cut as the log's conversion is. Reads
  during the rewrite find each chunk in exactly one form.

Tests owed with it:
- a flipped byte, a truncation, a chunk swapped between domains, and a renamed file are refused;
- no stored file or name contains a chunk's bytes or hash;
- erasing a domain's key record leaves its chunks unreadable while other domains read;
- a crash cut at every step of the rewrite.

## 12. Every other file focal writes (inventory, 2026-10-20)

Steps 1, 2 and 5 sealed the node log and the group files. Every other file focal writes was
listed from its writers (each `File::create`, `OpenOptions`, `create_private_new`, `fs::write`,
`atomic_replace` and `install` outside tests):

| Store | Files | Holds | Writer | State |
|---|---|---|---|---|
| Payload log | `payloads/*.seg` | spilled rows' bytes: message payloads | focal-ledger `native_payloads` | sealed (§12.1) |
| Checkpoint seeds | `seeds/<ledger>/` | a checkpoint's native rows, 1 MiB chunks | focal-evidence `seeds` | clear |
| Cold stream | `cold/<ledger>/` | retired families' archive frames | focal-evidence `cold` | clear |
| Content | `content/objects/…` | artifacts, manifests, uploads, custody | focal-evidence `store`, `transfer`, `gc`, `upload_terminal` | clear |
| Session backups | a backup's directory | checkpoint, seeds, content, cold | focal-ledger `session_backup` | clear |
| Enrollment | private directories | the cluster CA's key, nodes' and clients' keys, invitations, the signer's journal | focal-enrollment `files::PrivateDirectory` | sealed (§12.2) |
| Embedded node log | the shell's log of a node that is its own cluster (`wal/` before) | every record of its session and its root's control group | focal-node `EmbeddedNode` through `storage_start`, as a networked node | sealed (§12.3) |
| Bus identities | the bus's identity directory | its local CA's and members' keys | focal-bus `pki` | clear |
| Bus part | a part's image, the node's run | streams' floors, producers' sessions | focal-bus `group`, `member` | clear |
| Client stores | a client's data directory | requests, receipts, pending writes, artifact uploads | focal-client `operation_store`, `native_store`, `pending`, `artifact_transfer` | clear |
| Node files | `CLUSTER.admin`, `CLIENT.contexts`, `MCP.operations`, `trust-adopted.bin`, `IDENTITY`, `POLICY`, `STORAGE.level`, `cluster/partitions` | the node's journals, catalogue, trust and identity | focal-node | clear |

They are sealed in this order, the message data first (the payload log, then seeds, cold and
content as step 4 designs it), then the keys (enrollment, the bus's identities), then the journals
and small files of nodes and clients (step 3). A client's data directory has no node to take keys
from: it gets a `SEAL.node` of its own under a root key in the user's configuration directory
(`default_key_file("client")`), one file for every data directory of that user, each directory's
keys its own below it.

### 12.1 The payload log, sealed (2026-10-20)

The payload log is a cache: emptied at every open, refilled from the records and checkpoints. So
its key is the process's alone and never written. `focal_seal::Cache` derives each log's key
(HKDF-SHA-384) from one parent made at random when a cache first asks for a key, held in the
locked region for the process's life, and a random ID of the log's own. The writer's thread, the log
and each reader make their own AES-256-GCM context from it (`hyper_seal::stream::VersionKey`). What
a crash leaves on the volume is ciphertext under a key nothing holds: an erase with nothing left to
destroy (NIST SP 800-88r1, cryptographic erase).

- **A record** is the payload's length (4 bytes), its bytes, and a 16-byte tag. The owner appends
  records in the clear to its window, which serves recent reads as copies; the writer's thread
  seals each record's length and bytes in its I/O buffer, at the nonce of its segment (version)
  and offset (index), and only then writes it. A record that does not seal is not written, and is
  handed again, never written in the clear.
- **No position seals two plaintexts.** Segments are never reused within a log, and a record's
  bytes never change once appended. A write handed again after a failure seals the same bytes at
  the same nonce, which gives the same ciphertext (GCM is deterministic for one key, nonce and
  plaintext), so nothing is learned from the repeat.
- **A read from a file** opens the record at its own place under the log's key, and checks the
  length the reference states, then the reference's CRC as before. A flipped byte, a record moved
  within its file, or another log's file in place of its own is refused as corrupt (tested).
- **The bound.** A log takes payloads no wider than a row spills (`SPILLED_BYTES`, 1,024), so a
  read opens a record in a buffer on its stack.
- **Charged.** Each key's AES-GCM context is aws-lc's `EVP_AEAD_CTX` (584 bytes) with
  `OPENSSL_malloc`'s 8-byte prefix: the log charges its own and its writer's at open, and each
  reader its own while it holds it, in the lane its maker reads for: a checkpoint's frozen image
  in the committed lane, which a session whose share is full still has (a full session must still
  checkpoint, which is what frees it: the two tests that say so refused a reader charged
  ordinarily), a read lease's in the ordinary lane, where a full share refuses it.

### 12.2 The private directories, sealed (2026-10-20)

Every record of an enrollment's private directory (`focal_enrollment::files::PrivateDirectory`:
the cluster CA's key and its server certificate's, a node's and a client's join key and
enrollment, the founder's draft, invitation drafts, the signer's and every agent's journal, a
client's context catalogue) is sealed whole under the journal key of the data directory it lies in
(`Store::Journal`), unwrapped for that one record and dropped after it, through `focal_seal::file`
(`hyper_seal::sealed_file`, a data key of its own per record). The record's framed bytes (magic,
payload, BLAKE3 hash) are the sealed file's plaintext, so another record's bytes under a valid seal
still read as corrupt. Every opener takes the data directory's keys (`focal_seal::DataKeys`), which
its owner resolves once: a node's at its start (`Settings::data_keys`), a client's by its CLI and
focal-load the same way.

- **Converted once, then refused in the clear.** A directory an earlier binary wrote in the clear is
  sealed by its first writer under this one, under the writer's exclusive lock: each record's
  framed bytes are checked and written again sealed through the atomic replace that writes any
  record, and then the directory is marked (`SEALED`). A crash before the mark leaves each record
  whole in one form or the other, and the next writer finishes. A shared reader reads either form
  and seals nothing; once marked, a record in the clear is refused as corrupt (tested: a planted
  plaintext record after conversion).
- **Temporaries.** Every writer's open lets go of the temporaries (`.pending-*`) a crash left before
  their rename, which were never records and were never removed.
- **Bounded.** A conversion visits at most every record and marker the registry's bounds admit
  (`max_entries`: the fixed records, one an invitation or an enrollment under `EnrollmentLimits`'
  defaults, each with its marker, the lock and the seal's marker).
- **Tests:** no record's secret is in any file of its directory; a flipped byte and another data
  directory's keys are refused; a directory laid out in the clear is read by a shared reader as it
  is, sealed by its first writer, and refuses plaintext after; the crash windows of a sealed write
  (a temporary synced and not renamed; a record renamed before its marker).
- **Found and left open:** a quorum enrollment's `invite-<request>` records are never removed, one
  for every invitation name the node ever issued (owed); and the embedded node's log was still
  focal-log's plaintext WAL (§12.3).

### 12.3 The embedded node's log, sealed (2026-10-21)

A node that is its own cluster (`focal start` without a network) opened focal-log's WAL itself,
so every record of its session, its root's control group beside it among them, reached its disk in
the clear, though a networked node's groups had lived on the sealed shell since the storage level.
It now opens its storage as a networked node's start does (`storage_start::open`): its binary's
announced level is its fence, so its groups live on hyper-log's log under the shell, sealed under
the data directory's root key (§2) from its first start, and a directory an earlier binary left on
the WAL is converted at its next start by the conversion every networked node takes at the storage
level. Its root's control group is a member of the same storage (`ControlReplica::open_on_storage`).
The node lets its storage go in the order the networked node's shutdown keeps, the session first
and the data directory's lock last, and its host's stop answers with what the log's close found.

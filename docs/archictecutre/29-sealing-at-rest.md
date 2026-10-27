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
- **Losing the key loses the data**, as with every system that seals at rest. A sealed backup
  names the operator key it needs by its ID (§12.8), in its `ENCRYPTION-INFO`, in what `create
  backup` and `verify backup` report and in a refusal for want of it, and the operator keeps the key
  file as they keep any secret. Rotation is a rewrap (§4); nothing below the node key moves.

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
- **Existing stores.** A store written before the fence is rewritten once, chunk by chunk, when the
  node crosses the fence, resumable from any crash cut as the log's conversion is. Reads during the
  rewrite find each chunk in one form or both. As built (§12.4) the fence is its own, the content
  level, and the rewrite runs while the store serves.

Tests owed with it:
- a flipped byte, a truncation, a chunk swapped between domains, and a renamed file are refused;
- no stored file or name contains a chunk's bytes or hash;
- erasing a domain's key record leaves its chunks unreadable while other domains read;
- a crash cut at every step of the rewrite.

## 12. Every other file focal writes (inventory, 2026-10-20; taken again 2026-10-24)

Steps 1, 2 and 5 sealed the node log and the group files. Every other file focal writes was
listed from its writers (each `File::create`, `OpenOptions`, `create_private_new`, `fs::write`,
`atomic_replace`, `atomic_file` and `install` outside tests), first on 2026-10-20 and again on
2026-10-24, when the at-rest level (`upgrade::AT_REST_LEVEL`, 10) began sealing what was left. The
second listing found two rows of the first out of date: `CLUSTER.admin` and `CLIENT.contexts` are
private journals (`focal_enrollment::PrivateJournal`), sealed with the private directories since
§12.2, and a bus node's log has lived on hyper-log, sealed, since its replicated part did.

| Store | Files | Holds | Writer | State |
|---|---|---|---|---|
| Node log and group files | the shell's log, group images and checkpoints | every record of every group | hyper-log, hyper-durable, focal-consensus `group_files` | sealed (steps 1, 2, 5) |
| Payload log | `payloads/*.seg` | spilled rows' bytes: message payloads | focal-ledger `native_payloads` | sealed (§12.1) |
| Checkpoint seeds | `seeds/<ledger>/` | a checkpoint's native rows, 1 MiB chunks | focal-evidence `seeds` | sealed from the at-rest level (§12.5) |
| Cold stream | `cold/<ledger>/` | retired families' archive frames | focal-evidence `cold`, `record_file` | sealed from the at-rest level (§12.6) |
| Content | `content/objects/…`, `content/quarantine/…` | artifacts' chunks and manifests | focal-evidence `store`, `transfer`, `gc`, `native_artifact` | sealed from the content level (§12.4) |
| Content staging and custody | `content/staging/`, `objects/<domain>/*.part`, `content/{custody,checkpoints,receipts}/` | uploads and transfers being received; custody records | focal-evidence `staging`, `transfer`, `upload_terminal`, `store` (records), `record_file` | sealed from the at-rest level (§12.7) |
| Session backups | a backup's directory | checkpoint, seeds, content, cold | focal-ledger `session_backup`, focal-seal `operator_key` | sealed from the at-rest level under the operator's key file (§12.8) |
| Private directories and journals | `JOIN/`, `cluster/authority/`, `cluster/invitations/`, `cluster/network/`, `CLUSTER.admin/`, `CLIENT.contexts/`, the placement and admission journals (`cluster/<name>/journal.bin`) | the cluster CA's key, nodes' and clients' keys, invitations, the signer's and every agent's journal, the admin's and contexts' catalogues | focal-enrollment `files::PrivateDirectory`, `PrivateJournal` | sealed (§12.2) |
| Embedded node log | the shell's log of a node that is its own cluster (`wal/` before) | every record of its session and its root's control group | focal-node `EmbeddedNode` through `storage_start`, as a networked node | sealed (§12.3) |
| Bus log | a bus node's `bus.log` | its part's records | focal-bus `member` over hyper-log | sealed (its data directory's keys) |
| Bus identities | the bus's identity directory | its local CA's and members' keys | focal-bus `pki` | sealed (§12.9) |
| Bus part | a part's `part.image`, the node's `run` | streams' floors, producers' sessions; the node's run | focal-bus `group`, `member` | sealed (§12.9) |
| Client stores | a client's data directory and each context's history (`client/…`, `CLI.requests`, `CLI.uploads`, `MCP.operations`, `MCP.requests`, `MCP.uploads`, `WATCHES`, an operation journal where a command was told) | requests, receipts, pending writes, artifact uploads | focal-client `operation_store`, `native_store`, `pending`, `artifact_transfer`, `managed_store`, `managed_requests`, `watch` | sealed from the at-rest level (§12.11) |
| Node files | `IDENTITY`, `POLICY`, `NETWORK`, `SESSION.memory`, `trust-adopted.bin`, `cluster/partitions/*.partition`, `cluster/BOOTSTRAP`, `cluster/apply/<plan>/{PLAN,JOURNAL}`, `cluster/pending-retires` | the node's identity, policy, network state, founded allowance, adopted trust, partition records, the root's genesis, deployment journals, retirements to finish | focal-node `node_files` | sealed from the at-rest level (§12.10) |

**In the clear by design**, each for what it is:

- `SEAL.node` and `SEAL.root`: the wrapped store keys and the root key's ID, by which every sealed
  file is opened; what opens the seal cannot lie under it.
- Key records (`DOMAIN.key` of a content domain or a seed directory): each a key wrapped under a
  store key, nothing in the clear.
- `STORAGE.level`: the fence's level, which a start reads to choose every store's form before it
  opens one; a level and its CRC.
- Locks (`LOCK`, `*.lock`) and markers: empty files held for their locks, `*.initialized` files a
  writer leaves to say its store was made (a fixed sentence each), and the empty `SEALING` and
  `SEALED` of a store converting and converted, a content store's `REST_SEALING` and
  `REST_SEALED` for its files beside its objects (§12.7), the empty `FILES.SEALED` of a data
  directory whose node files are sealed (§12.10), and a client store's empty marks (`SEALED`, and
  `<name>.managed-sealed`, `<name>.watch-sealed`, `<name>.initialized-sealed` where several owners
  share a directory) with `FILES.lock`, the lock of a directory's node files, which their
  conversion takes and every change a client's process makes to one (§12.11).
- The fence a client's context recorded in its history (`STORAGE.level`, as a node's), which its
  next command reads to choose its stores' form before it opens one (§12.11).
- What an operator asks for in the clear, outside any data directory: an artifact downloaded to a
  file it names, a trace written where it says, a deployment plan's output, and an invitation
  bundle handed to a joiner, a bearer credential kept owner-only and spent once.
- The bus identity directory's certificates (`root.der`, `node-N.der`, `client-K.der`): public,
  as every handshake shows them (§12.9).
- A sealed backup's `ENCRYPTION-INFO`: the backup's key wrapped under the operator's key file, and
  that key's ID, a hash of it, neither of which opens anything (§12.8).
- focal-log's WAL of a node below the storage level, which a binary below that level reads alone,
  converted to the sealed shell at the level (27 §15.8, §12.3).
- The measurement tools' reports (`tools/load`, `tools/compare`): a run's results, not focal's data.
- Names. No name holds a record or a hash of one: every store that files by a hash (content, seeds,
  cold streams, staging's parts and custody records, a sealed backup) names each file by its
  directory's keyed name of that hash (§12.4 to §12.8), since a hash in a name confirms what it
  names to anyone holding a candidate. The names left in the clear say where a store keeps what, not
  what it holds: the layout's own (`IDENTITY`, `journal.bin`, `catalogue.bin`, `bus.log`, `open.0`,
  …); ordinals and generations (a bus member's `node-N`, a managed store's `<ordinal>.receipt`, a
  managed stream's `<name>.g<generation>`, a payload segment's number); identifiers, a ledger's, a
  group's or a content domain's directory named by its ID, and an operation's, a request's or an
  upload's by the 128 bits its client drew from the operating system's random source (or a hash of
  them); and the labels an operator gave, a context's name (`enrollment-<name>`, `history-<name>`),
  and an invitation's, a watch's and an upload journal's, each named by an unkeyed hash of the label
  with its cluster or its context. That a volume holds an operation, a ledger or a context is not
  kept from its reader, as Vault's threat model leaves out "the leakage of the existence of secret
  material" (a reader of its storage "may observe that secret material exists and is stored, even if
  it is kept confidential"), and as Kubernetes' encryption at rest seals a Secret's value under its
  etcd key, `/registry/secrets/<namespace>/<name>`, which stays in the clear (the etcd dump of its
  guide, "Encrypting Confidential Data at Rest", shows it). The journey of §12.12 scans every name
  for the hashes it made that a store files under a keyed name.
- The root key file (§2) and an operator's backup key file (§12.8): each outside every data
  directory, owner-only, kept where its operator keeps secrets; what opens the seal cannot lie under
  it.

A store's temporaries are written in its form (a sealed store's sealed), and what a crash leaves of
them goes with the store's sweep past its grace or at its next open, as before.

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

### 12.4 The content store, sealed (2026-10-23)

Every chunk and manifest a node's content store installs is sealed under its domain's key and named
by a keyed hash once the fence the node last recorded opens the content level
(`upgrade::CONTENT_LEVEL`, 9; `storage_level::content_form`); an embedded node takes it from the
level it announces, as it does the other rules. Below the level the store writes the layout every
earlier binary reads, each file in the clear and named by its hash, byte for byte, so a node can
still be rolled back while the fence is below it: sealing a store a binary below the level would
open is what the fence forbids.

- **As §11 designs it, with what changed.** A domain's key is random, kept beside its objects as
  its 61-byte record under the node's content key (`DOMAIN.key`). A file is sealed whole by
  `hyper_seal::sealed_file::seal_content` (hyper-raft 5a22c8c): its data key and file ID derive
  from the domain key and the SHA-256 of exactly its bytes, which the writer takes as it seals, so
  equal bytes seal to equal files and deduplication, idempotent installs and verify-by-rehash keep
  working. A name is the domain's keyed name of the BLAKE3 hash it is filed under
  (`hyper_seal::name`, HMAC-SHA-256 under a child of the domain key, 128 bits, 32 hex digits where
  a name in the clear is 64). No cache of unwrapped domain keys is kept: a domain's key is
  unwrapped from its record for the file in hand and dropped after it, as the group files' are, and
  the store's one writer holds the content key for its life; a reader holds none, and unwraps it
  from the data directory's keys as it opens a domain's record. A file's volume bytes are promised
  at its sealed size (`focal_seal::file::sealed_bound`), and a domain's record at its first
  install.
- **The way between the forms.** A store asked to seal that holds files in the clear is marked as
  converting before anything is sealed (`SEALING` in the store's root) and converted by its owner
  as it serves, a file a step whenever no call waits (`ContentStore::convert_step`, driven from the
  content owner's idle turns, and the embedded node's owner's): the file is read, sealed beside
  itself under its keyed name, and only then is its name in the clear removed, each domain's
  directory flushed as the conversion leaves it, and the store marked sealed at the end (`SEALED`).
  So every file lies in one form or both at every moment, and every lookup while the store is not
  marked looks in the clear first and then under the keyed name: a reader that misses a file in
  the clear finds it sealed whatever the conversion does between. A failure in the clear gives way
  to the sealed form where one opens (the writer seals over a damaged file while the store
  converts). The quarantine's rounds are converted first, then the store's own objects, so an object
  restored meanwhile comes back in a form the rest of the pass meets; the collector leaves objects
  alone while the store converts. A conversion once begun finishes whatever form a later start
  asks for, a store marked sealed stays sealed, and a store that holds nothing is sealed as it
  opens. A step whose sealed file the volume has no room for is held and takes the same file first
  when the owner asks again; one that fails ends the conversion for that run and is reported, the
  store reading both forms until its next start resumes it.
- **What an operator reads.** Before it converts a file, the conversion counts what it must convert
  (the chunks and manifests in the clear and their bytes), in its own order, a step at a time; from
  then on each such file is tallied once, converted by a step or gone before the conversion met it
  (with an expired round, or replaced by a restored copy of the same name). A restore while the
  count runs moves files between what the count has passed and what it has not: the count's place
  says whether a moved file was counted where it left and will be where it lands, and a domain whose
  listing the restore touches is listed again, POSIX leaving a listing under change unspecified; the
  collector deletes no round while the store counts. `ContentStore::conversion_status` gives the
  form (`clear`, `counting`, `converting`, `sealed`), the tally, whether a step is held for room and
  how many were, what stopped the conversion, whether the start resumed it, and when it began and
  ended; the node reports it under `inspect node --gc` (26 §5) and on the metrics page
  (`focal_content_form`, `focal_content_conversion_*`), as CockroachDB's stores report gives the
  share of files and bytes under its active key and Redis's `INFO` a load's bytes done of its total.
  The count costs one listing and one metadata read a file, no write: 256 files of 64 KiB counted in
  under a millisecond beside two seconds of conversion on an NVMe btrfs volume (09).
- **Bounded.** A step visits at most the collector's step of entries and converts at most one file,
  read into the owner's chunk of scratch (the embedded owner's charged to its budget while the
  conversion runs); the count holds the names of the rounds and of one tree's domains, nothing a
  file. An entry that is not a file no longer than its kind's bound (a link included) was never an
  object's and is left as it is, uncounted, as are the parts and temporaries a domain's directory
  holds (its parts are converted from the at-rest level, §12.7).
- **Tests:** a store in the clear converted while it serves, every object read at every step by
  the store, a reader made before the conversion and one made then; an object sealed meanwhile; the
  collector holding then collecting; the quarantined object restored; a start in the clear after the
  mark (`a_store_in_the_clear_is_converted_while_it_serves`). A cut at every step, with the names in
  the clear put back beside their sealed forms and a sealed form damaged beside its bytes in the
  clear, each resumed by a start that asks for the clear
  (`a_conversion_cut_at_any_step_resumes_and_loses_nothing`). A volume without room
  (`a_conversion_without_room_holds_and_takes_the_same_file_next`). The clear layout an earlier
  binary reads, and an empty store sealed at once
  (`an_empty_store_is_sealed_at_once_and_one_in_the_clear_stays_so`). §11's: a flipped byte, a cut,
  a file under another chunk's name and a chunk carried into another domain refused; no stored name
  or byte holding a hash or plaintext; a domain erased by its record while another reads
  (`sealing_tests`). The level's rule (`the_content_store_seals_once_the_recorded_fence_opens_the_content_level`).
- **Sealed since, at the at-rest level** (§12.7): parts of chunks a transfer is receiving
  (`objects/<domain>/*.part`), uploads being staged (`staging/`), and the custody records
  (`custody/`, `checkpoints/`, `receipts/`).
- **Owed.** A reader made while its store converted keeps looking in the clear first for its life,
  one missed open a file, until the node's next start. (An operator's view of the conversion,
  owed here at first, is in `inspect node --gc` and on the metrics page since 2026-10-24: "What
  an operator reads" above.)

### 12.5 Checkpoint seeds, sealed (2026-10-24)

A session's checkpoint seeds (25 §5) are sealed once the fence a node last recorded opens the
at-rest level (`upgrade::AT_REST_LEVEL`, 10; `storage_level::at_rest_form`), read as each seed
directory is opened: by a hosted session (`network_service::native_hosting`, the networked and
embedded node alike), by a placement's restore, and by a session opened over a content store
(`NativeSession`, which takes the store's form). Below the level a directory is written byte for
byte as before, so a node can still be rolled back while the fence is below it.

- **As the content store, a directory a domain.** A seed directory has a key of its own, random,
  kept beside its chunks as its record under the data directory's group key (`DOMAIN.key`,
  `focal_seal::Store::Group`: seeds are a checkpoint's rows), made with the directory's first
  sealed chunk. A chunk is sealed whole, keyed by the BLAKE3 hash it is named by
  (`hyper_seal::sealed_file::SealedWriter::content_keyed`, hyper-raft 8c6b4fa): its data key and
  file ID derive from the directory's key and that hash, which the writer takes of exactly the
  bytes it seals (`sealing::Hashed`), so its bytes are passed over once, where a SHA-256 of them
  (`seal_content`) would be a second pass; equal bytes seal to equal files, so a chunk installed
  twice or by a peer is the same file. A name is the directory's keyed name of the hash
  (HMAC-SHA-256 under a child of its key), so no name or byte of a directory holds a hash or a
  chunk's plaintext, two sessions' same chunk lie in two different files, and destroying a
  directory's record erases every chunk of it.
- **A batch's flushes still overlap.** A checkpoint's batch writes each chunk sealed into a file of
  its own as it is added, unflushed (`SealedWriter::finish_unflushed`, hyper-raft 8c6b4fa), and its
  commit flushes them together on its sync threads, renames them and flushes the directory once, as
  before; `finish` would have flushed each in turn on the adding thread.
- **Key slots.** The process's locked region is a hard bound (`focal_seal::key_slots`, 128 slots
  in a 4 KiB page; hyper-seal §8 asks a consumer of many keys to bound its own). A writer or reader
  unwraps a directory's key for the one chunk it writes or reads, and a writer lets it go before it
  writes the chunk's bytes (the sealed writer holds a derived key of its own, outside the region),
  so a node's sessions hold no slot between their writes and a chunk's claim lasts its key's
  derivation. A reader's pass over many chunks' names (which of a checkpoint's chunks are local)
  unwraps it once (`SeedReader::opened`).
- **The way between the forms.** A directory asked to seal that holds chunks in the clear is marked
  converting (`SEALING`) and converted while it serves; one that holds none is sealed as it opens.
  The conversion counts its chunks in the clear first, then hands out jobs of at most
  `SEED_JOB_CHUNKS` chunks (the batch's sync threads, 8) which the node's collector runs on the
  blocking pool (`focal_rt::FILES`), at most `MAX_STEPS` steps a replica a pass: each chunk read,
  sealed beside itself under its keyed name into a temporary of the job's own (`*.<job>.convert`),
  the files flushed together, renamed, the directory flushed once. The session's owner only lists,
  tallies and lets a converted chunk's name in the clear go when the job's result comes back
  (`SeedStore::conversion_job`, `conversion_done`): a checkpoint never waits behind a conversion's
  flushes. A job whose result never comes back (a pass cut short) has its chunks taken again, first;
  if it finishes late it renames the very bytes the next job did, under the same names, and its
  result counts nothing twice. A chunk sealed already when a cut left its name in the clear is
  verified and kept; a damaged sealed form beside its bytes in the clear is sealed over. The pass
  ended with no job out, the directory is flushed and marked sealed (`SEALED`), and nothing in the
  clear is looked for after.
- **The sweep.** The seed sweep keeps a protected chunk under either name (the protected hashes'
  keyed names taken once a call where the directory has its key), removes nothing in the clear while
  the conversion counts, and tallies a chunk in the clear it removes after as gone; it removes a
  job's crash-left temporaries past its grace, as a batch's.
- **What an operator reads.** `inspect node --gc` gives, in the last pass's seeds, the replicas'
  seed directories by form (`clear`, `counting`, `converting`, `sealed`), how many wait for room
  and how many stopped, and what the conversions counted, converted, found gone and have left
  (`AdminSeedConversion`).
- **Bounded.** A job holds at most `SEED_JOB_CHUNKS` chunks' names and the collector one chunk of
  scratch, charged to its budget; the chunks held for room are at most a job's; the conversion's
  state and progress are boxed, a session holding its stores inline (`Session`'s size bound).
- **Measured** (`benches/seeds.rs` on fedora, 32 cores, NVMe btrfs, 16 interleaved rounds of a
  checkpoint's batch of 64 chunks of 1 MiB, ms a MiB, medians): the batch's writing 0.487 in the
  clear and 0.761 sealed (its sealing, its key's derivation, the directory key's unwrap and the
  sealed file's segments, a chunk), its commit 0.937 and 0.956 (the same flushes, renames and
  directory flush), the whole batch 1.424 and 1.717 (+20.6%); a chunk read back verified 0.373 and
  0.540; a directory in the clear converted by jobs at 2.444. On this Mac, under a load of 45 to 93
  from other sessions, the same rounds spread 3.9 times from one run to the next, the commit's full
  flushes most: only the host that holds still measures the difference.
- **Tests:** the batch, a detached commit, and install, read and removal in both forms; the clear
  layout an earlier binary wrote (`the_clear_form_writes_the_layout_earlier_binaries_wrote`); a
  flipped byte, a cut, another chunk's file under a name and a chunk carried into another session's
  directory refused, no name or byte holding a hash or plaintext, a directory erased by its record
  while another reads
  (`a_sealed_seed_is_refused_changed_cut_renamed_or_moved_and_erased_with_its_record`); a directory
  in the clear converted while a batch commits and a peer's chunk is installed, read at every step
  through the store, a reader made before and one made during, a chunk planted in the clear after
  the mark not looked for, an empty directory sealed as it opens
  (`a_directory_in_the_clear_is_converted_while_it_serves`); a cut after every step with a name put
  back and a sealed form damaged (`a_conversion_cut_at_any_step_resumes_and_loses_nothing`); jobs
  run on another thread, a lost job's chunks retaken and its late result, a job held for room
  (`a_job_seals_its_chunks_off_the_owner_and_its_result_removes_their_clear_names`,
  `a_lost_jobs_chunks_are_taken_again_and_its_late_result_counts_nothing_twice`,
  `a_job_without_room_holds_and_its_chunks_go_first_next`); the sweep in both forms and during the
  count (`the_sweep_keeps_what_is_protected_in_either_form_and_holds_the_clear_while_counting`, which
  fails without the count's hold); the level's rule
  (`the_stores_beside_the_log_seal_once_the_recorded_fence_opens_the_at_rest_level`). The real
  binary: a node founded below the level seeds three checkpoints in the clear, each chunk's bytes
  hashing to its name; the fence recorded and the node restarted, its collector converts the
  directory while the session takes writes, every look at `inspect node --gc` agreeing with itself,
  every chunk held sealed after, nothing named in the clear, and a checkpoint after sealing its own
  (`cli_seeds_at_rest`); a placement's hosts compare their seeds by hash, two hosts naming one chunk
  two ways (`placement_binary`, through `SeedReader::chunks`). Every node and ledger test that seeds a
  checkpoint runs sealed: a cluster founded at the compiled level records the at-rest level.

### 12.6 The cold stream, sealed (2026-10-24)

A session's cold streams (doc 31 M2, A2: the data stream of retired families' archive frames, and
the marks stream that finds a record's batch in it) are sealed once the fence a node last recorded
opens the at-rest level (`storage_level::at_rest_form`), read as each stream's directory opens: by
a hosted session and by a placement's restore of a backup's streams; a session opened over a
content store takes the store's form. Below the level each stream is written byte for byte as
before.

- **A directory a domain, as seeds.** Each stream's directory (`cold/<ledger>/data`, `…/marks`)
  has a key of its own, random, kept as its record under the data directory's group key
  (`DOMAIN.key`, `focal_seal::Store::Group`) and made with its first sealed file. A whole chunk and
  a manifest chunk are filed under the directory's keyed name of the BLAKE3 hash the stream names
  them by, so no name holds a hash, and destroying the record erases the stream.
- **The open chunk, a record file.** The stream past its last whole chunk is appended in place and
  never synced on its own (a checkpoint carries its bytes), so it is sealed an append at a time
  (`record_file`): a file is its header (the format's magic and a random 128-bit file ID, in the
  clear) and its records, each a block of its length and a block of its bytes, both sealed with
  AES-256-GCM under the file's key, derived from the directory's key and the file's ID
  (`DomainKey::record_key`), at the nonce of the block's place among the file's blocks. A record
  holds at most 64 KiB, an append longer being several; each is sealed whole in a buffer of its own
  and written with one write, as the clear form writes an append once. A read begins at the
  nearest of the places the writer keeps, one for each 64 KiB of plaintext.
- **A cut writes nothing again.** A whole chunk is its record file renamed as it is under its keyed
  name (`.pending` until a checkpoint makes it durable, as before), checked against its name when it
  is read; a reader tells a record file from a sealed file by its first bytes. A manifest chunk is
  sealed whole keyed by its hash (`SealedWriter::content_keyed`), and so is a chunk installed from
  a peer (its plaintext inside TLS, its hash checked) or converted: there equal bytes are equal
  files.
- **No key and nonce seal two plaintexts.** A record file's key is its own, its ID random; a block's
  nonce is its place, and a writer only appends. A writer that fails fails its stream until the
  stream opens again, and every open removes the open chunk and whatever a crash left unnamed and
  writes the checkpoint's open bytes into a new file under a new ID. A manifest's, an install's or a
  conversion's key derives from its plaintext's hash. Tested across every file a stream writes
  through appends, cuts, a checkpoint and a crash whose reopen appends other bytes at the same
  offsets of the stream: each ID's bytes only ever grow
  (`no_record_files_key_and_nonce_seal_two_plaintexts`, which fails, its keystream showing, when
  an open chunk's ID is derived from its index).
- **Where this departs from the plan.** The plan's step sealed the open chunk under a key of the
  process's (the payload log's `focal_seal::Cache`) and sealed each chunk again whole,
  content-keyed, at its cut. A cut that seals again reads its mebibyte back, opens it and seals it
  again: every byte passed through AES-GCM three times and written twice where it is now once each.
  At the costs measured on the bench's core below (a 4 KiB page's AES-GCM 0.76 µs, its write
  1.85 µs), the two passes and the second write come to about 0.9 ms a MiB more, an estimate that
  takes every row of the table from 2 KiB batches up past the +50% an append is allowed. A key that
  outlives the process lets the cut rename the file instead. What
  a crash leaves is then ciphertext under the directory's key rather than under none, removed at the
  next open as before: the same bytes lie under the same key hierarchy in the checkpoint that
  carries them and in the chunk they become, and destroying the directory's record erases all of
  it.
- **The way between the forms.** As a seed directory's (§12.5), the conversion shared with it
  (`chunk_convert`): a directory asked to seal that holds whole chunks or manifests in the clear is
  marked converting and converted while the stream goes on, counted first, then by jobs of at most
  `SEED_JOB_CHUNKS` chunks the node's collector runs off the owner; its open chunk is sealed as the
  stream opens. One that holds none is sealed as it opens.
- **What an operator reads.** `inspect node --gc` gives the cold directories by form and their
  conversions' tallies beside the seeds' (`AdminSeedReport::cold`).
- **A frame this replica cannot read is unavailable.** A sealed open chunk is checked as a whole
  chunk always was, so a changed byte in it is now refused where the stream is read rather than
  where the frame is parsed. Such a refusal answered a participant's read of a retired family
  `outcome unknown`, which a client retries as it would a write's, until its transport gave up
  (`cli_archive`); a cold frame this replica holds and cannot read back is now `unavailable`, as a
  custody bundle it holds corrupt is, and a read refused for room `capacity`.
- **Bounded and charged.** An idle stream holds, beyond its names, its open chunk's key (aws-lc's
  context, 592 bytes) and where its reads begin (17 places of 16 bytes for a 1 MiB chunk): 864
  bytes, charged (`an_idle_streams_writer_holds_less_than_a_kibibyte`). An append's room promises
  its records' overhead (36 bytes a record) and a sealed manifest chunk's bound, and the append
  spends it exactly (`a_sealed_append_spends_exactly_what_its_room_promised`); a record's buffer,
  at most 64 KiB and its overhead, is held only while it is written. A whole chunk is read by
  opening its records in place in its own buffer, taken once (the chunk and a tag's room: a marks
  chunk is 65,536 records), and a capture reads the open chunk so; a range of the open chunk is
  read a record at a time; each as a read and a capture are charged
  (`a_chunk_of_small_records_is_read_in_place_within_its_charge`).
- **Measured** (`benches/cold.rs` on fedora, an i9-14900HX, NVMe btrfs, pinned to one P-core, 12
  interleaved rounds of 64 MiB of batches with a 16-byte mark each, the thread's CPU time from
  `/proc/thread-self/schedstat`, medians; the host loaded by other work, 7 to 15):

  | Batches | Clear, CPU ms a MiB | Sealed | Sealed / clear | Bytes stored a byte, sealed |
  |---|---|---|---|---|
  | 256 to 768 bytes | 5.118 | 6.472 | 1.265 | 1.1365 |
  | 512 bytes | 6.345 | 7.419 | 1.169 | 1.1364 |
  | 2 to 6 KiB | 1.654 | 1.994 | 1.205 | 1.0176 |
  | 4 KiB | 1.151 | 1.996 | 1.735 | 1.0175 |
  | 32 to 96 KiB | 0.847 | 1.134 | 1.339 | 1.0014 |
  | 64 KiB | 0.648 | 0.874 | 1.348 | 1.0011 |

  Batches whose sizes vary, as a retirement's batch of up to 64 families' encodings does, cost
  +21% to +34%, the most where batches are largest and AES-GCM is most of the work. Batches all
  of one page, which no retirement writes, cost +74%: the clear stream's appends then each fill one
  page exactly, aligned, while a record's 36 bytes put every sealed one across two, and btrfs pays
  its buffered write by the page (on the same core: a page-aligned 4 KiB write 1.85 µs, the same
  with 36 bytes more 3.8 µs, its AES-GCM 0.76 µs). An earlier run of the same code, the host at 26
  to 36, put every ratio within 0.14 of these (512-byte batches the farthest, at 1.301). On this
  Mac (APFS) the fixed sizes cost +16%
  to +34%. A record's two blocks were written apart at first, at 2.94 times the clear's time at
  4 KiB: macOS's `sample` put 98% of a sealed append in `write` and 1% in AES-GCM, so a record is
  now one write.
- **Tests:** every cold test in both forms (the cut alike however the stream is appended, and a
  sealed replica's chunks named as a clear one's, a
  checkpoint's durability and a reopen, an install through the manifests, a state that does not add
  up, any range read back, room promised and spent); no name or byte of a sealed directory holding
  a hash or plaintext, and refused: a flipped byte in a whole chunk's header, length block and
  bytes, a whole chunk under another's name, a flipped byte and two records swapped in the open
  chunk, another stream's open chunk in its place, a directory erased by its record while another
  reads, and what a crash left gone at the next open with the open chunk under a new key
  (`a_sealed_stream_holds_no_plaintext_and_refuses_what_was_changed`); the nonce test above; a
  whole chunk of 65,536 records read in place within its charge; a
  directory in the clear converted while the stream appends and cuts, every range read at every
  step (`a_cold_directory_in_the_clear_is_converted_while_the_stream_goes_on`); a conversion cut
  after every step, resumed by an open that asks for the clear
  (`a_cold_conversion_cut_at_any_step_resumes_and_loses_nothing`); the real binary reading a
  retired family from its own sealed stream and refusing a changed byte in it as `unavailable`
  (`cli_archive`), with the node tests that retire to cold, back up and restore a session's
  streams, and install them from peers (`cli_retention`, `cli_backup`, `cli_gc`, `fleet_quic`).

### 12.7 The content store's staging, parts and custody records, sealed (2026-10-24)

A content store's files beside its objects are sealed once the fence a node last recorded opens the
at-rest level (`storage_level::at_rest_form`): uploads being staged (`staging/`: an upload's part,
its metadata, the terminal record that fences its identity once it is finished), chunks a transfer
is receiving in parts (`objects/<domain>/*.part`), and the custody records (`checkpoints/`,
`custody/`, `receipts/`). The objects themselves are sealed from the content level (§12.4), one
level below, so these files take a form of their own (`ContentStore::open_forms`): between the two
levels the objects are sealed and these files are written byte for byte as every earlier binary
wrote them, so a node can still be rolled back to a binary of the content level while its fence is
below the at-rest level. Their conversion is marked apart from the objects' (`REST_SEALING`,
`REST_SEALED`), and runs with theirs or alone where the objects are sealed already.

- **Directories as domains.** Staging and each kind of record have a key directory of their own,
  as a seed directory is (a record under the content key, made with its first sealed file), so a
  record carried into another kind's directory opens to nothing. Every file is filed under its
  directory's keyed name: an upload's of its identity (a hash of it under a context of its own, so
  the name is no object's), a record's of the hash or the name it is filed by, a part's of its
  chunk's hash under its domain's key, so no name holds an upload's identity or a hash.
- **Parts, sealed as they are written.** An upload's part and a chunk's part are record files
  (§12.6's, of a kind of their own, `FCLPART1`): each append sealed into records at once and written
  with one write, so an interrupted upload or transfer goes on from sealed bytes. A part's key is
  derived from its directory's key, its file's ID and its owner together (the upload, the chunk:
  `DomainKey::part_key`), and its header carries a tag sealed under that key, so a part moved under
  another owner's name is refused at its header. A part whose last append a crash cut short is read
  up to its last whole record and written again whole into a file of a new ID under a new key
  before anything is appended (`RecordWriter::resume`): what the cut append left of its blocks may
  lie on the volume, and its places are never sealed over under the old key, so no key and nonce
  seal two plaintexts. A record that does not open further back than one append is no crash's and
  is refused. An upload's part is opened once a start and read through whole; a chunk's part,
  opened at every part a sender sends, is read by its length blocks alone (`Verify::Lengths`), its
  bytes opened and checked against the chunk's hash once it is whole.
- **Records, sealed whole.** A record named by its hash (a checkpoint, a transfer manifest) and a
  receipt, named for the copy it is about and replaced as that copy's standing changes, are each
  sealed whole and keyed by their own bytes, so a receipt's versions are never sealed under one key.
  A record already there that opens to the same bytes is kept; one that opens to other bytes under
  a name that is its hash is corrupt. Replacing a receipt removes its name in the clear as well,
  since a reader looks in the clear first.
- **Where it departs from the plan.** The plan sealed the custody records through `focal_seal::file`
  under the content key, as the group files and private directories are sealed (§12.2). That path
  makes a sealed file whole in memory before it is installed and opens one from its whole bytes:
  two copies of a record at once, 16 MiB for a checkpoint record of 8 MiB, and one key for every
  kind, so a record carried into another kind's directory would open and be refused only by its
  reader. The records are sealed by the content store's own key path instead, as its objects are:
  each kind a key directory whose record is under the content key, a record sealed as it is
  written to its file and read through one copy of its plaintext, a conversion streamed through
  the owner's chunk of scratch, and a record of one kind refused at the seal in another's
  directory.
- **The way between the forms.** A store asked to seal them while they lie in the clear converts
  them while it serves, after its objects: each record directory (each quarantine round's, then the
  store's own), each chunk's part met in its domain's directory, then staging. A record named by
  its hash is streamed through the owner's chunk of scratch (`DomainKey::install_streamed`: a
  checkpoint record runs to 8 MiB); a part in the clear is sealed whole beside itself; an upload in
  the clear has its part copied into a sealed part a chunk a step, chasing its appends, and once the
  copy has caught up its sealed metadata is installed, the upload goes on in the sealed part, and
  only then do its files in the clear go. A file in the clear goes on in the clear until the
  conversion meets it (a part a transfer appends to, an upload a client sends to); a new one is
  written in the store's form. A crash at any point leaves each file whole in one form or both: an
  open settles an upload with files in both (its clear files stand while its clear part does) and
  removes a part with no metadata beside it.
- **What an operator reads.** The count counts these files with the objects (staging after them),
  and what changes them while the store converts keeps it exact: a part's growth counts as it
  grows, an upload finished in the clear leaves the count (its part and metadata gone, its terminal
  record counted), a released terminal record or a replaced receipt is tallied gone, and a change
  while the count lists its directory lists it again. So `inspect node --gc` and the metrics page
  count and tally them in the content store's conversion, exact as objects are. Between the levels
  a store reads `sealed`: its objects are, and the files beside them lie in the clear by the
  level's rule, as the fence says. The collector sweeps no record while they convert, as it
  collects no object while the objects do; the quarantine's rounds expire as before once the count
  is taken.
- **Measured** (`benches/content.rs`'s uploads alone, `FOCAL_BENCH_UPLOADS`, each form a process
  of its own, 6 interleaved rounds on an idle M4 Max at load 2; each process's CPU from
  `/usr/bin/time -l`, less a process that only makes the bytes; medians): uploads of 8 MiB take
  8.44 ms of CPU a MiB with every file in the clear, 9.88 with the objects sealed (the content
  level) and 10.33 with staging sealed too, +4.5%: two passes of AES-GCM, a part sealed as it is
  written and opened as it is sealed, 1.8 M cycles a MiB by the core's counters. Uploads of 64 KiB
  take 7.48, 8.38 and 9.41 ms each, +12%, most of it in the kernel. Wall time, less making the
  bytes, is the disk's flushes: 205.5 against 204.2 ms an 8 MiB upload (+0.6%), 54.1 against
  52.6 ms a 64 KiB one (+2.8%). Where a small upload's extra millisecond goes is not measured apart: besides a part's
  key, header and records and its sealed metadata and terminal record, an upload looks staging's
  key up from its record four or five times, as an object's install looks its domain's key up for
  each file, and one look an operation is the first lever to measure.
- **Windows.** focal-evidence's suite runs on Windows CI, and these files' writes and renames with
  it.
- **Tests:** the record file's resume (a part opened again goes on under its key; one cut short is
  written again under a new key; damage behind its last append, a part of another kind and one
  holding more than it may are refused); an upload staged sealed and resumed across a restart with
  nothing of staging's in the clear at any point, a part cut short, a changed or moved part or
  metadata refused, a sealed terminal record's crash windows (`staging_tests`); a chunk received in
  parts across a restart with nothing in the clear, a part moved under another chunk's name refused
  and one cut short gone on under a new key, a part in the clear converted and its transfer gone on
  (`transfer::tests`); records sealed under keyed names and read in either form, a changed or
  carried record refused, records converted while one is replaced with the tally exact at every
  step (`record_seal_tests`); staging converted while its upload goes on, the tally agreeing with
  the files in the clear at every step, and staging's conversion cut at any step; the whole of it
  (a staged upload, a finished one, a chunk's part and a record of each kind) cut after every step
  in turn and resumed (`the_files_beside_the_objects_convert_through_a_cut_at_any_step`); and the
  level's rule: between the levels the objects sealed and these files as every earlier binary wrote
  them, at the at-rest level converted alone
  (`below_the_at_rest_level_the_files_beside_the_objects_stay_as_earlier_binaries_wrote_them`). The
  real binary (`cli_content_form`): a node founded below the at-rest level, its store sealed,
  holding beside its objects in the clear 400 uploads' terminal records, an upload half sent, a
  chunk half received and a record of each kind; below the fence they stay in the clear; the fence
  raised, the node started again converts them while `inspect node --gc` and the metrics page are
  read at every step, counting and converting exactly those 406 files, and the upload, the
  transfer and every record go on from their sealed form, every finished upload's identity still
  fenced.

### 12.8 Session backups, sealed under an operator's key file (2026-10-24)

A session's backup (26 §6) leaves the node: its files are copied to other volumes, other machines
and object stores, and restored on nodes with keys of their own. From the at-rest level it is
sealed as CockroachDB seals a backup, an `ENCRYPTION-INFO` file and every other file encrypted
under a key made once for the backup:

- **The key file is the operator's.** `--key-file` names 32 random bytes in an owner-only file
  (`focal create root-key --output FILE` makes one, as it makes a node's root key for a secret
  store), read through the checks a node's root key file passes: owner-only on every platform,
  exactly 32 bytes (`focal_seal::operator_key`). Its ID is a hash of the key under a context of its
  own (BLAKE3's key derivation), so a backup names the key it needs without holding anything that
  opens it, and the key file is the one thing a restore needs beside the backup: no node's keys,
  no data directory.
- **A key made once for the backup.** `create backup` makes a random key under the operator's
  (`WrappingKey::make_child`) and writes `ENCRYPTION-INFO` before any other file: the format's
  magic (`FCLBKEY1`), the backup key's ID, its record wrapped under the operator's key (AES-KW,
  naming the operator key's ID), and a BLAKE3 trailer over the three, which tells a damaged record
  from another key's. It is the backup's one file in the clear and holds nothing of the session's.
- **Every other file sealed whole.** The envelope (`checkpoint`), the `MANIFEST`, each seed, each
  cold chunk and each object's manifest and chunks are sealed through `focal_seal::file`, each
  under a data key of its own wrapped by the backup's key, and each file a hash names is filed under
  the backup's keyed name of that hash (a `Namer` derived from the backup's key): no file holds a
  row, a chunk, a hash or the manifest in the clear, and no name holds a hash. A file's sealed copy
  is held only while it is written or opened, and charged to the backup's budget while it is
  (`BackupForm`, read and written through `BackupFiles`).
- **Refused before anything is read or written.** `verify backup` and `restore session` take the
  key file. A sealed backup asked of without one, or with a key file whose ID is not the one its
  `ENCRYPTION-INFO` names, is refused (`backup_key_required`, `backup_key_mismatch`, each naming
  the key the backup needs) before anything else of it is read: the CLI refuses it before it asks
  the node, and the node refuses it again. A restore verifies every file before it writes anything,
  so a changed byte, two swapped files, or a file carried in from another backup (sealed under
  another key) is refused with the node untouched.
- **The level's rule.** Below the at-rest level a backup is written in the clear, byte for byte as
  before, and a key file is refused: a backup written below the level is one every binary the fence
  admits restores. A backup without `ENCRYPTION-INFO` reads in the clear whatever key file is
  named, so every backup written before the level, and every one written below it, verifies and
  restores as it always did. A write a crash cut goes on under its own key when it is asked again
  with the key file that opens it; a directory holding anything else, or part of a backup sealed
  under another key, is refused.
- **As planned.** The plan named `focal_seal::file`, keyed names and the CockroachDB layout, as
  built; the backup's record names the operator key by its ID as well, so the reports and the
  refusals name the key a backup needs, as §2 asks of a backup.
- **Found on the way.** A restore of a backup that names no content object (a session of claims
  alone) was refused `unavailable`: the content owner's restore counted its first ask, which only
  begins the restore, among the slices the backup's objects take, so with none it stopped one
  short of the answer. The tests that a second backup into a directory, a restore of a session
  already hosted, a tampered backup and an unacknowledged recovery are refused had passed `--format
  json` to commands that take no such flag, so the argument parser refused every one of them before
  the node saw it; they now check each refusal's code, and the repeated restore, which no node had
  refused, answers as the restore it repeats, as designed.
- **Measured** (this Mac, release, a seeded session's backup of 5 files in the clear and 6 sealed,
  24 interleaved rounds, medians; `measure_a_sealed_backup_against_a_clear_one`): written in
  72.5 ms in the clear and 83.8 ms sealed, the difference one more file's flushes
  (`ENCRYPTION-INFO`); verified in 0.348 ms and 0.426 ms, about 16 µs a sealed file opened.
- **Tests:** an operator's key file read again under the same ID, another file under another, what
  one wraps opened by no other, a file others may read or of another length refused (focal-seal);
  a backup sealed and in the clear naming the same files, a scan of the sealed one finding no hash
  in any name or file and no bytes of any clear file, verified under its key file and refused with
  none or another before anything else is read, a flipped chunk, seed, envelope, manifest or
  encryption record, two swapped files and a file carried from another sealed backup each named, a
  write cut before every durable operation leaving no backup and going on under its own key and
  refused under another, and a sealed backup restoring to the prefix a clear one does
  (focal-ledger); the real binary (`cli_restore`): a session backed up sealed under a key file the
  binary made, refused without one, scanned for its rows' text, payload, identities, hashes and the
  manifest's marker, verified without a node under its key file and refused without it or with
  another, restored on a fresh node with its claim and artifact read back, each of a restore
  without the key file, with another, with a flipped chunk and with swapped files refused with the
  node's stores for the session untouched; a session founded below the level backed up in the clear,
  refused a key file, verified with and without one and restored on a fresh node at the binary's
  level; `cli_backup`, the restore runbook through its three cuts, and the laptop journey, whose
  concept index records the backup key as the decision it is (DC20).

### 12.9 The bus's files, sealed (2026-10-25)

A bus node keeps three things of its own beside its log, which its data directory's keys sealed
since its replicated part began: its part's image, its run, and the identity directory its members
and clients share in the bus's standalone mode. Each is now sealed:

- **The image and the run under the data directory's keys.** `part.image` (each stream's floor,
  each identity's producer sessions, the configuration, as of the newest entry the window let go)
  and `run` (the count each start raises before it is used) are sealed whole through
  `focal_seal::file` under the directory's group key (`Store::Group`), unwrapped for each write and
  dropped after, as a group file's key is (`group::ImageFile`, `member::raise_run`). A node whose
  directory is not yet marked takes an image or a run an earlier binary wrote in the clear (the
  image's magic first, the run's eight bytes alone, where a sealed file begins with its header's
  version): the run is raised and written sealed, and the image is written again sealed by the
  part's own image writer, the thread every image goes through, and made durable before the part is
  handed back. What an earlier binary's cut install of its image left, its temporary holding the
  image in the clear, is removed, where with no image to convert nothing would replace it before the
  node's next image. Then the directory is marked `SEALED`, and from the mark either in the clear is
  refused. A cut before the mark leaves each file whole in one form or the other, and the next open
  finishes.
- **The identity directory gets keys of its own.** `focal start bus --pki DIR` reads its local
  authority's and every member's and client's identity from DIR, made once by its first opener. The
  directory now has a `SEAL.node` of its own under the per-user root key file named for its root key,
  as a client's data directory has (`focal_seal::resolve`), and each key file holds a frame sealed
  whole under the directory's journal key (`Store::Journal`): `FCLBUSK1`, its name's length and its
  name, the PKCS #8 key and a BLAKE3 hash over all of it. A key file put in another key's place opens
  to a frame naming another file, and one carried in from another directory opens under no key of
  this one's, so both are refused, as a changed byte is. A directory an earlier binary wrote in the
  clear is converted by its first opener under the directory's lock (`LOCK`), key by key, each through
  the rename every identity file is written by, a key already sealed (a conversion a crash cut) opened
  to be sure of it, and then marked `SEALED`; from the mark a key in the clear is refused, as §12.2's
  private directories refuse one. A directory whose make was cut before its authority was never read
  whole, so nothing in it was served: it is swept of what the cut left (each identity's files and
  staged copies, and the mark) before it is made, so a key an earlier binary filed there in the clear,
  one the new make files no more, does not lie there for good. The certificates are public, every
  handshake shows them, and stay in the clear.
- **As planned.** The step named `Store::Group` for the image and the run, a `SEAL.node` of the
  identity directory's own under the per-user root key file and `Store::Journal` for its keys,
  conversion by the first writer under its lock, and refusal after the mark, as built.
- **Found on the way: a group restarted one node at a time lost its leader.** The step's test
  restarts each node of a group of three in turn under publishing. After the third restart no pair
  was connected and no leader was elected, with the files in either form: a node never dialed a pair
  again once its connection closed, a restarted node dialed only the nodes above it, and a peer of
  a restarted node held its dead connection until its two-minute idle timeout, the restarted
  endpoint drawing a fresh QUIC reset key and connection-ID key at every start, so it could tell no
  peer that its last run's connection was gone. Both causes are fixed: the pair's lesser node dials
  whenever its endpoint tells it the pair has no connection, at once after a connection that lasted
  and after a dial that gave none lasting a back-off from the last dial's start (1 s, RFC 8085
  §3.1.3's initial interval and TCP's initial retransmission timeout, doubled to 3 s, the least
  interval that section allows a sender whose destination answers nothing); and an endpoint keeps a
  key across its restarts (hyper-transport's `RestartKey`, taken from hyper-raft at 698c0da, its
  docs/transport.md §4ah), derived for each bus node from its own private key and the address its
  socket is bound to (BLAKE3's key derivation), so a restarted node answers a packet on an old
  connection with a reset the sender verifies, as a restarted host's TCP answers with an RST, and the
  sender's owner dials it again. With either fix alone the test froze, no batch acknowledged for 30
  s (34 §8). And a node that stopped leading kept delivering to its subscribers, where as a
  follower it could let a stalled subscriber's offsets go: it now ends each subscription with a
  notice naming the leader it knows and the offset it would have delivered next, and the
  subscriber subscribes again there (34 P7, §8).
- **Measured** (this Mac, release, medians of 101 rounds interleaved with the same bytes installed in
  the clear; `measure_a_sealed_image_write`): sealing adds nothing measurable to an image's write,
  whose install's flush takes about 8.5 ms here. The group key's unwrap takes 116 to 122 µs a write
  and the seal 30 µs at an image of 236 bytes (four streams and a producer), 32 µs at 6.2 KB (64 of
  each) and 141 µs at 393 KB (4,096 of each); written sealed 8.49, 8.43 and 9.28 ms, in the clear
  8.50, 8.46 and 8.48 ms. The run is written once a start and the identity directory read once a
  start. Through three restarts under publishing and consuming, each next 20 batches were acknowledged
  and delivered at the rate before any restart (60 batches in 6.6 to 18.9 s before, 20 in 2.3 to
  6.2 s after each, this Mac under other sessions' load).
- **Tests:** an identity directory made sealed and read back; one kept in the clear sealed by its
  first opener, each key what it was, and a conversion cut after two keys with a third staged
  finished by the next opener; one an earlier binary's make cut before its authority, its keys in
  the clear and more of them than asked, one more staged, made with no key left in the clear; a key
  planted in the clear after the mark, a changed byte, a key file put in another's place and one
  carried from another directory, each refused by name (`pki`); a
  part's image sealed and opening only under its directory's group key, one in the clear refused
  after the mark and before it taken and written again sealed at its own point, a changed byte and
  another node's image refused (`group`); a run raised sealed, one in the clear taken and sealed
  before the mark and refused after, a changed byte and another node's run refused; the redial's
  schedule (`member`); and a group of three whose identity directory an earlier binary made in the
  clear, its directory and each node's run and image turned back to the clear before the restarts
  as that binary kept them, restarted one node at a time, each reading its identity from the
  directory as its service does, while a producer publishes and a subscriber following the leader
  consumes: every batch acknowledged once and in order, the producer's session carried through (its
  newest batch sent again recognised), the subscriber delivered every batch from where it began once
  and in order, each restarted node reopened at its image's floor, the directory sealed by the first
  start and again by the first restart with every key what it was, each node's run and image sealed
  after its restart, and a key and an image planted in the clear, changed by a byte or carried from
  another node or directory each refused; a node whose first image install an earlier binary cut
  opened with no temporary of it left (`tests/group.rs`). The transport's own test: a peer
  told its connection closed within a keep-alive of a restart with the kept key, and only at its
  idle timeout with another (hyper-raft `exchange::a_restarted_endpoint_resets_what_its_last_run_held`).

### 12.10 A node's small files, sealed (2026-10-25)

A node keeps a handful of small files beside its stores: its identity (`IDENTITY`), its committed
policy (`POLICY`), its network state (`NETWORK`), the allowance of the session it founded
(`SESSION.memory`), its root's genesis (`cluster/BOOTSTRAP`), each partition it hosts
(`cluster/partitions/*.partition`), each deployment it applied (`cluster/apply/<plan>/PLAN` and
`JOURNAL`), the partitions whose host it has to retire (`cluster/pending-retires`), and the issuers
each client enrollment in its data directory adopted (`CLIENT.contexts/enrollment-*/trust-adopted.bin`).
Each was written in the clear through the atomic replace every node file is written by. Each is now
one of the node's files (`node_files::NodeFiles`), sealed once the fence the directory records opens
the at-rest level:

- **Sealed whole, its name bound.** A node file is sealed whole through `focal_seal::file` under
  the directory's journal key (`Store::Journal`), unwrapped for each write and dropped after (a
  conversion unwraps it once for every file it seals). Its plaintext is a frame: `FCLNFIL1`, its
  name's length and its name relative to the data directory, then the file's bytes as its writer
  made them. A file moved to another's name opens to a frame naming another file, and one carried
  in from another directory opens under no key of this one's, so both are refused, as a changed
  byte is. A sealed file begins with `FCLNSEAL` in the clear, which says what it is and nothing of
  what it holds, so either form is told from the file alone (a sealed file's own header has no
  magic, and some node files, the allowance and the partition records, have none either).
- **The form read at each write.** Below the level a node file is written byte for byte as every
  earlier binary wrote it, so a binary below the level may still be rolled back to. The form is
  read from the fence the directory records (`STORAGE.level`) at each write, not fixed when the
  directory opened: a node founded at the level writes its identity before its fence is recorded,
  and a node whose fence rose while it ran seals what it writes from then on.
- **Converted at the start, under the lock, before the node serves.** `NodeFiles::convert` visits
  every node file: one in the clear is checked by the rule its own reader holds it to, written
  sealed through the atomic replace that writes it, and the directory is then marked
  (`FILES.SEALED`, empty, in the clear by design); one sealed already, by a conversion a crash cut
  or a write since the fence rose, is opened to check it and left. What a cut install left beside a
  file, its temporary holding it in the clear, is removed (`.install`, and an earlier binary's
  `trust-adopted.bin.next`). A cut anywhere leaves each file whole in one form or the other, and the
  next start finishes. It runs where every start opens its directory, under the directory's lock
  (`NodeDirectory`), and where a start first records a fence at the level (`storage_start`), so a
  node founded at the level, or joining a cluster at it, seals its files during its first start.
  The partition records are listed within the partitions a node may host, the enrollments within
  the contexts a data directory keeps; every plan the directory holds is converted, none skipped,
  as one left in the clear would be refused once the directory is marked.
- **Refused in the clear after the mark.** Before the mark a reader takes either form; from it a
  node file in the clear is refused by name (`NodeError::ClearAfterSealed`). A mark or a file that
  cannot be looked for is an error, never its absence: a policy whose file could not be looked for
  would read as no policy, and a mark as no mark.
- **The identity no longer names the keys.** A directory whose keys an earlier binary made under
  the root key file named for its node (`<node>.key`) found that file by reading its identity. The
  identity sealed, the keys would be found by what they open. Before the identity is sealed, the
  key file is linked under the name its root key's ID gives it (`Settings::name_key_file`: one key,
  two names; copied where the file system holds no links), the name every directory made since is
  found by, and a sealed identity names no legacy file.
- **One reader of the identity.** The identity was read by its path in seven places of focal-node's
  tests and two of the load tool's driver (`embedded::decode_identity`), which would have refused
  every node founded at the level. The path reader is gone; every reader opens the directory's node
  files (`node_files::read_identity`), in either form.
- **A client's own data directory.** A data directory that hosts no node records the fence its
  contexts learn from their clusters, and converts the issuers its enrollments adopted at a
  context's open once that fence opens the level (§12.11).
- **Found on the way.** Three node files were read whole with no bound: the founded allowance and
  the pending retirements (`std::fs::read`), and each partition record. Each is read within its
  bound now: 21 bytes; a partition ID for each partition a node may host; and a partition record's
  own fields at their largest (452 bytes, pinned to its types by a test) and its image, a
  partition's checkpoint, which every group's checkpoint holds whole and focal-consensus refuses
  past `IMAGE_BYTES` (8 MiB). The record's bound had cited the root authority's checkpoint bound,
  of the same size and another thing; the root's genesis is held the same way, its fields (85
  bytes) beside the founding bootstrap its control group founds on. A file past its bound was refused as "node identity or
  stored policy is corrupt or incompatible", whichever file it was; it is refused by its path and
  bound now (`NodeError::PastBound`). And the plans a node applied are never pruned: `cluster/apply`
  grows with every apply (`inspect deployment` lists 64 of them, an undrived bound). Open.
- **Measured** (this Mac, release, medians of 101 rounds interleaved with the same bytes installed
  in the clear, under other sessions' load; `measure_node_files`): sealing adds nothing measurable to
  a node file's write, whose install's flushes take about 8.4 ms here, but at a journal's bound. The
  journal key's unwrap takes 64 to 74 µs a write and the seal 21 µs at 21 and 256 bytes, 73 µs at
  64 KiB and 790 µs at 1 MiB; written sealed 8.38, 8.63, 8.54 and 9.75 ms, in the clear 8.61, 8.29,
  8.40 and 8.39 ms. A conversion of a directory holding every kind of node file and a record for
  each partition a node may host, 41 files, takes 357 ms (the median of 11), one install a file,
  once, at the node's first start at the level.

Tests (`node_files.rs`): a file written byte for byte as before below the level and sealed from it;
every file of a directory converted and marked, each read back whole, a second conversion doing
nothing; a conversion cut at every point between its files' sealed writes and its mark (none
sealed, the first, the first two, ..., all and no mark), an install's temporary and an earlier
binary's staged adoption left in the clear beside them, each finished by the next start with
nothing lost; a planted, moved, carried and changed file refused after the mark; a legacy key file
named for its root key before the identity is sealed, and the keys found without the identity
after; a partition record's and a genesis file's own fields at their largest equal to their
derived bounds (`network_directory`, `cluster`). `cli_node_files_at_rest.rs`, the real binary: a node founded below the
level, which applies a deployment committing its home region there, its files in their clear forms
(its identity, policy, network state and founded allowance, and the deployment's plan and
journal; the root's genesis is an embedded root's enrollment's, which the binary does not found
with, and `node_files`'s tests plant it); the fence raised and recorded; started again, every file sealed and the
directory marked before it served, its identity, committed policy and deployment read back
unchanged; a clear identity planted after the mark refused by name, and read again once its sealed
form is back.

### 12.11 A client's stores, sealed (2026-10-25)

A client keeps what it asked and was answered in stores of its own (focal-client): the operation
store (`MCP.operations`) and each operation's pending journal (`client/operations/<id>`, or where a
command was told to keep it), the native journal (`client/native`), managed ownership and its stores
(`CLI.requests`, `MCP.requests`, each watch's), the watches (`WATCHES`), and the upload stores
(`CLI.uploads`, `MCP.uploads`) with each upload's staged bytes. They lie in a node's data directory
when a command runs beside its node, and in a context's history (`CLIENT.contexts/history-<name>`)
otherwise. Each was written in the clear. Each is now kept as the fence its data directory records
says:

- **Sealed whole, its name bound.** A record stays the whole file its store has always written (its
  magic, its length, its bytes and their BLAKE3 hash), sealed whole through `focal_seal::named`, the
  seal node files take (§12.10) with a kind of its own: the frame `FCLCREC1`, the record's path
  relative to its data directory, then the record, under the directory's journal key, behind a clear
  `FCLCSEAL` that says what it is and nothing of what it holds. A record moved to another's name or
  another store, or carried in from another data directory, is refused as a changed byte is. A store
  its operator put outside the data directory (an operation journal at a path a command named) is
  bound to its canonical path whole, and refused moved; a path that is not UTF-8 is named lossily
  and told apart by the hash of its bytes. A bootstrap marker that names its store's context (an
  upload store's `<name>.initialized`, `MCP.operations.initialized`) is kept the same way
  (`at_rest::OwnerFile`).
- **An upload's bytes, a part.** An upload's staged bytes are a part, §12.7's record file
  (`FCLPART1`): each stage appended as one record, sealed as it is written under a key of the part's
  own (HKDF-SHA-384 from the journal key, the part's file ID and the hash of its bound name), so an
  upload resumed across a restart reads its bytes from sealed records. No key and nonce seal two
  plaintexts: a part whose last stage a crash cut short is written again whole under a new key
  (`RecordWriter::resume_in`), and one holding a stage its state never recorded (the crash between
  the stage's bytes and its record) is cut back to its recorded bytes the same way
  (`RecordWriter::cut`). A part's files are made and opened owner-private and checked, as every file
  of a client's store is: record files take their files from a policy (`RecordFiles`), the content
  store's as before (`Plain`).
- **The form from the fence.** Below the at-rest level every store is written byte for byte as
  every earlier binary wrote it. A store in a node's data directory follows the fence the node
  records (`STORAGE.level`). A context's history follows the higher of its data directory's fence
  and the one the context learned from the cluster it addresses, which it records beside its stores;
  a client's own data directory, which hosts no node (it holds no node identity), records the fence
  its contexts learn as a node records its own (`storage_level::client_level`, `client_at_rest`). A
  marked store seals what it writes whatever form it is asked for, and refuses to open without its
  keys.
- **The fence learned from the cluster.** A node answers `Operation::Fence` (tag 35, a participant's
  exchange answered by the node from no session) with the fence it last recorded. A context that
  addresses its node through the node's socket reads it from the node's own directory. A remote
  context asks it once as it enrolls, and beside the first request a command sends while the fence it
  knows is below the level (`Remote::fence`: the two in flight together, so the asking costs no
  round trip of its own). A node below the level that added the query refuses its kind on that
  request's own stream (hyper-transport's `Refusal::Kind`; the connection is kept), and the context
  keeps the level it knew. Learned, the fence takes effect at the next open, as a node's takes effect
  at its next start.
- **Converted at the next open, under the store's lock.** Each store converts at its open once its
  form seals and it is not marked: each record in the clear read and checked as its reader checks
  it, written sealed through the replace that writes it, then the store marked (`SEALED`, or
  `<name>.managed-sealed`, `<name>.watch-sealed`, `<name>.initialized-sealed` where several owners
  share a directory), what it holds named by its own authority (its catalogue, or its state), within
  the bound its limits set. An upload's payload in the clear is copied into a new part a chunk at a
  time, the prefix its state vouches for hashed as it is copied and checked before anything in the
  clear goes; then its state is sealed, the payload in the clear removed, and the upload marked. A
  cut at any step leaves each record whole in one form or the other (a part beside a payload in the
  clear is a cut conversion's, made again), and the next open finishes. A store made from the level
  is marked as it is made, so a record in the clear is refused from its first. Each store converts
  under its own lock at its own open: a watch a running process holds converts when a process opens
  it again. A client's own data directory converts its node files (the issuers its enrollments
  adopted, §12.10) at a context's open once its fence opens the level, under `FILES.lock`, since no
  node holds its lock.
- **Refused in the clear after the mark.** Before a store's mark a reader takes either form; from it
  a record in the clear is refused by name (`AtRestError::ClearAfterSealed`).
- **Every outcome known once.** A request pending across the conversion is retried under its
  identity from its sealed journal and answered once: a command committed with its reply lost is
  answered as the duplicate it is, a prepared managed request is sent as prepared and never expanded
  again, and an upload's pending append goes out as it was.
- **Found on the way.** A node's conversion of its files (§12.10) took every name in
  `CLIENT.contexts` that began `enrollment-` for an enrollment's directory, the marker a client's join
  leaves beside it (`enrollment-<name>.client-initialized`) among them, and failed on that file
  (`ENOTDIR`): a node data directory holding a client enrollment would have failed its start at the
  level, as a client's own directory failed its first open in the real-binary test. Only directories
  are visited now, and `node_files`' tests plant the marker. A marked store asked for the clear form
  would have written records its own reader then refused; a marked store seals whatever form it is
  asked for. And two commands of one client changing a node file at once raced: an adoption reads
  the issuers adopted, adds one and writes them back, and a context records the fence it learned
  unless one as high is recorded, each through the atomic replace's one temporary
  (`<name>.install`). Two adopting at once each wrote over the other's adoption, and two recording
  at once could leave the lower fence, or one renamed the temporary away under the other, whose
  write failed (`ENOENT`), or installed a file the other was still writing into. Every change a
  client's process makes to a node file is now read, changed and written under the node files' lock
  (`FILES.lock`, `NodeFiles::lock`), which their conversion takes too, a node's at its start among
  them, so no two write through one temporary and none writes over a change it did not read
  (`storage_level::record_shared`). Tests, threads standing for the processes (a file lock taken
  through two opens of its file is two owners within a process as across processes): four issuers
  adopted at once, all four kept, over eight rounds (without the lock, three of the four writes
  failed in the first round); eight fences recorded at once, the highest left (without it, seven of
  the eight failed).
- **Names.** A store's directory names hold operations', uploads' and watches' identities in the
  clear, as before: identifiers and labels, never a record or a hash of one (§12's names, clear by
  design).

- **Measured** (this Mac, release, under other sessions' load at a load average of 8 to 17;
  `measure_record_seal`, `measure_upload_staging`, `measure_store_conversion`): a record sealed whole,
  the journal key unwrapped for it as a write unwraps it, takes 54 µs at 256 B, 52 µs at 4 KiB,
  140 µs at 64 KiB and 1.23 ms at 1 MiB, a journal's bound, and opens in 50, 44, 91 and 716 µs (the
  medians of 101); its write beside that is its replace's flushes, about 8 ms here (§12.10). An
  upload staged 64 KiB a stage, each stage its bytes written and synced and then its state installed,
  takes 13.73 ms a stage sealed and 13.71 ms in the clear, 1.880 s and 1.869 s for 8 MiB (+0.6%, the
  medians of 5 interleaved rounds): a part's seal is lost in a stage's two flushes. A conversion,
  once: an upload of 8 MiB in 76 ms; a store of 32 answered operations in 1.06 s (the median of
  11), its 65 records each written sealed through a replace and its 33 marks made, each with its
  flushes.

Tests (focal-client): for each store (a pending journal, the operation store, the native journal, a
managed store, managed ownership, the watches, an upload and an upload store), a store below the
level holding answered and pending work converted at its open and cut after each of its steps in
turn (`at_rest::cut`), the next open finishing, every record sealed and nothing of them in the clear,
answers kept and pending work answered once; and the tampering battery on each store's sealed
records, a changed byte, a record swapped with another's or carried from another data directory, and
one planted in the clear after the mark, each refused and the store opening once it is put back. An
upload cut between a stage's bytes and its state goes on under a new key (focal-evidence: a part cut
to an earlier append, written again under a new ID). The MCP store's marker converted with its store,
still binding its context (focal-node). The fence query on the wire (focal-wire): an actor's exchange
in every profile, refused to a node, answered by its level alone. `cli_client_stores_at_rest.rs`, the
real binary: a node founded below the level; through its directory, answered requests, a claim whose
outcome stayed unknown (the node down) and an artifact whose upload stayed pending, its bytes owned by
the client; a client enrolled from a directory of its own, which records the cluster's fence as it
enrolls, with an answered claim; every record in the clear below the fence; the fence raised and the
node restarted; then the pending claim retried from its converted journal and committed once, the
artifact committed once and read back exactly, the enrolled client learning the risen fence beside a
request and converting its history and its directory's files at its next open, and no record, report
or claim's words of either client in the clear.

### 12.12 Every store at once, end to end (2026-10-26)

`crates/focal-node/tests/cli_at_rest.rs` holds the whole of §12 on the real binary at once, across
the fence (Unix-only, as the other real-binary at-rest tests: the data directories are kept private
by their mode bits):

- **Founded below the level, every store filled.** A node founded at the level below seeds its
  checkpoints (an inline bound of 64 bytes, so a checkpoint's rows go to seeds), carries a two-party
  cycle to its release so its family retires to the cold stream, takes claims, and backs its session
  up in the clear. A client enrolled from a directory of its own records the cluster's fence as it
  enrolls and claims. A claim through the node's directory is cut after its commit and before its
  reply (`FOCAL_FAULT`), its outcome unknown. With the node down, its content store's own writer
  lays out an upload half sent, a chunk half received from a peer and a custody record of each kind.
  A bus group of three publishes. Below the fence every store holds what the journey wrote in the
  clear, which the scan would find: the claims' words in the seeds and the cold stream, the clients'
  words in their journals, the upload's, the chunk's and the records' bytes beside the objects, the
  seeds named by their hashes, and six stores' clear forms by their magic (the node's identity and
  network state, the client's native catalogue, journals and prepared requests, a custody receipt).
- **The fence raised; every store converted while it serves.** The fence rises to the at-rest level
  and the node records it. Started again, the node converts its files before it serves (§12.10) and
  its seeds, cold stream and the files beside its objects as it serves (§12.5 to §12.7), while its
  claims and its retired family read back at every look. The claim cut after its commit is retried
  once from its converted journal and answered as the duplicate it is (§12.11). The enrolled client
  learns the risen fence beside a request and converts its history and its directory's files at its
  next open. A backup taken at the level is sealed under an operator's key file: a fresh node
  refuses it without the key file (`backup_key_required`) and restores it with it at the source's
  prefix (§12.8). The bus group opens its sealed files again (§12.9). The upload, the chunk and the
  records go on from their sealed forms. The backup taken in the clear below the fence still
  verifies, as a binary below the level may restore it.
- **The scan.** Every file under every data directory the journey made: the node's, the client's,
  the fresh node's, the bus group's with its root key files beside them, and the sealed backup. A
  file §12 lists clear by design by its name (`SEAL.node`, `SEAL.root`, `STORAGE.level`, the locks,
  the marks and markers, the key records) holds no more than a page: each is empty, a fixed
  sentence, a wrapped key, or a level and its CRC. No other file holds a marker the journey wrote
  (the claims' words, the enrolled client's, the pending request's, a proof's, the staged upload's
  bytes, the received chunk's, a custody record's, a bus record's) or the magic any store's clear
  form begins with (fifty of them, every eight-byte magic the store crates declare but the clear
  headers of the sealed forms themselves), so a store left in the clear is found even where the
  journey wrote no marker into it. No path names a hash the journey made that a store files under a
  keyed name (each seed's, the artifact's, the staged upload's identity, the chunk's root, the
  custody record's). The names that stay (traced from a run: the layout's own, identifiers and
  labels) are §12's names, clear by design. The invitation bundle is the operator's, kept outside
  every data directory.
- **Under load.** `scripts/at-rest-loop.sh` runs the journey again and again with a busy loop on
  each of two CPUs, its odd runs on one CPU and its even runs on both (`taskset`), on GitHub's Linux
  runners (`.github/workflows/at-rest.yml`, nightly and on any `at-rest-loop/*` branch). Twenty
  runs on an `ubuntu-24.04` runner (four vCPUs) all passed, each in 25 to 28 s, on one CPU and on
  two alike: the journey waits on its own steps in turn (a process started, a node ready, a
  request answered), each of which the busy loops slow alike. The scan first read every file once
  for each marker, which held each run at 377 to 383 s on one CPU and on two (the search ran at half
  of one CPU either way); it reads each file once and searches every marker together now.
- **Found on the way.** §12.11's open defect, two commands of one client adopting an issuer at
  once through one temporary, closed with the race beside it, two recording the fence at once
  (§12.11's last finding).

Measured: the journey takes 25 s on this Mac (debug, under other sessions' load) and 23 s on fedora
on two CPUs of its own; on the same two CPUs with a busy loop on each, 33 s, and on one of them
with a busy loop, 36 s.

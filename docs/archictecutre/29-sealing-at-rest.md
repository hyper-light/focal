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

- **Its path** is `storage.root_key` in the node's configuration. Unset, it is the platform's
  per-user configuration directory, never the data directory: `~/Library/Application
  Support/Focal Keys/<node>.key` on macOS, `$XDG_CONFIG_HOME/focal/keys/<node>.key` (else
  `~/.config/focal/keys/`) on Linux, `%APPDATA%\Focal\keys\<node>.key` on Windows. In a
  container or Kubernetes the data directory is a volume and the key is a mounted secret file
  named by `storage.root_key`, as CockroachDB's store keys and TiKV's master key are.
- **A key inside the data directory is refused at start** (typed, before anything opens): a
  copy of the data directory alone, in a backup, an rsync or a detached volume, must never
  carry what opens it.
- **It is made once**, at a node's first start, by `create_private_new` and a durable
  directory sync, before any sealed store is created. A data directory whose seal record (§3)
  names a root key ID that the file does not hold is refused, typed: a wrong or missing key is
  never answered by a fresh one.
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

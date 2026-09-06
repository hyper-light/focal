# Cluster enrollment

This crate adds network identity only when a deployment enables networking. Embedded and Unix-domain use require no CA, invitation, or join service.

`BootstrapAuthority::open_or_create` holds an exclusive lock on a dedicated private directory. The parent directory must already exist. The authority creates a cluster CA and a bootstrap server certificate, writes both keys atomically with mode `0600` inside a `0700` directory, flushes files and directory entries, and records initialization markers. Reopening with a different cluster fails. Missing previously initialized key material, checksum damage, unsafe permissions and multiple writers fail closed. A joining node separately uses `JoinKey::open_or_create` to persist its own private key, signed CSR and stable request ID before transmission. Private keys never cross the enrollment protocol.

The initial CA lifetime is ten years; bootstrap server lifetime is one year. Enrolled credential lifetime defaults to thirty days and is bounded to one year. Renewal and CA rotation require an explicit future committed policy; this crate does not silently replace established trust. Automatic operator-facing renewal is not implemented. The local persistence adapter currently requires Unix ownership and permission semantics.

## Metadata commit contract

1. Construct `EnrollmentRegistry` with the cluster ID, public CA certificate, first unassigned node number and explicit `EnrollmentLimits`. These limits bound invitation count, enrollment count, token lifetime, credential lifetime and aggregate metadata bytes. Retained revoked/used entries count against capacity; there is no unsafe automatic deletion of retry receipts.
2. `prepare_invitation` creates OS-random identifiers and a 256-bit invitation secret. `InvitationDraft::command` contains only a domain-separated secret hash, cluster/role binding, expiry and trust fingerprint. Persist it through the metadata authority, then publish its prepared registry update. `InvitationDraft::release` refuses to expose an uncommitted invitation.
3. A node verifies the bootstrap server and transmits its `JoinRequest`. `prepare_join` verifies the invitation and signed CSR, assigns one identity, and builds an `EnrollmentCommand` containing the exact public certificate. It cannot promote a Raft voter. Requested CSR names, subjects, CA bits and usages are discarded. Node certificates receive only the assigned cluster/node name with client/server usages; client certificates receive client usage only.
4. Reserve `charged_bytes()` for the current state and the replacement before `prepare_command`. This validates and constructs `PreparedEnrollmentUpdate` before consensus proposal. Commit the command through the metadata authority, then call `publish(update, committed_index)`. Publication validates owner lineage and base revision/index and swaps the prepared state without allocation, parsing or signature verification. Recovery may use deterministic `apply_committed` with the actual committed log index. A conflicting prepared command cannot assign a second identity.
5. Only `registry.release(request, now)` or an `Existing` result supplies a committed enrollment receipt. `JoinKey::complete` verifies the receipt against the saved CSR/key and invited CA, then durably installs it before returning TLS credential material. It recovers the exact certificate after process restart.

Metadata commands and checkpoints contain no invitation secrets or private keys. `EnrollmentCommand::encode/decode` and `EnrollmentRegistry::checkpoint/restore` provide bounded persistence formats. The owning node must persist commands in its existing replicated metadata log; this crate introduces no independent authority WAL. Tests exercise this seam with the real physical WAL, including an ambiguous fsync failure before publication.

Registry time is trusted ingress Unix time, not a client timestamp. Committed metadata carries its decision time, and a persisted time floor rejects clock rollback below that fence. This is an invitation/credential expiry policy, not a consensus read lease.

Invitation expiry is the deadline for **new admission**. An exact previously committed request/CSR can retrieve its same public certificate afterwards, while its credential is still valid. Secret proof, signed CSR, cluster/role binding and revocation checks still apply. This closes the lost-reply window without admitting another identity or extending certificate validity. A fresh admission using an expired invitation always fails.

`prepare_revoke` commits revocation of an invitation and any associated enrollment. `authorize_certificate` must be consulted when installing/rechecking transport grants; a cryptographically valid certificate alone does not bypass a committed revocation. Caller wiring must propagate those changes to its peer registry. Existing public-key enrollment cannot allocate another identity through a different invitation. Membership admission, learner catch-up, quorum policy and voter promotion remain separate committed operations.

## Bootstrap transport

`EnrollmentServer::bind(address, identity, limits)` and `serve(handler)` with an owned `JoinHandler + Clone` run an actual Quinn TLS 1.3 endpoint with ALPN `focal-enroll/1`. Bootstrap authentication is server-only TLS because a joining node has no issued client certificate yet. Invitation possession plus CSR proof authenticates enrollment at the metadata handler. The handler must prepare, commit, publish and release before returning `JoinResponse::Enrolled`; prepared certificates are never successful responses.

`EnrollmentClient::redeem` uses the operator invitation's CA and TLS server name with ordinary rustls verification. It also checks the exact invited server leaf fingerprint **before opening an application stream or building a token-bearing request**. It never enables 0-RTT or a permissive certificate verifier. The trust bundle and secret are exposed only by the explicit `Invitation::expose_token` operation; `Debug` and errors redact secrets. Operator invitations must be delivered through a trusted channel.

One connection admits one request and one response. Fixed-size frame headers validate magic, version, kind and the 16 KiB cap before payload allocation; the request must end before handler dispatch. QUIC stream/connection flow control, an active-connection semaphore and a whole-exchange timeout bound retained work. The client also bounds concurrent redemptions. An ambiguous send/reply returns `OutcomeUnknown`; retry with the same persisted `JoinKey` and invitation. Cancellation never retracts a metadata command.

Tests cover real TLS and QUIC handshakes, rogue CAs, a valid CA/name with the wrong exact pin, bounded framing, metadata commit before delivery, unknown-outcome retry, concurrent invitation consumption, changed cluster/role/key, CSR privilege requests, expiry/revocation, metadata budgets, key permissions, owner locks and restart.

Cryptography uses [`rcgen` CSR signature verification](https://docs.rs/rcgen/0.14.10/rcgen/struct.CertificateSigningRequestParams.html), [`rustls` certificate verification](https://docs.rs/rustls/latest/rustls/client/struct.WebPkiServerVerifier.html), OS randomness, and BLAKE3 domain-separated hashes. There is no custom TLS verifier, signature algorithm or encryption scheme.

`PrivateJournal` provides one owner-only atomic retry record for a composed signer, with an exclusive lock, checksum, file/directory sync and initialization marker. Replacement validates the existing record first and fails closed after an ambiguous write. Durable parent invitation records detect loss of entire draft directories. The node's quorum enrollment owner uses this primitive for exact public proposal replay and the existing `PendingInvitation` for secret custody; these files confer no metadata authority. `prepare_pending_invitation` rebases the same secret-bearing draft only after the prior proposal has received a definitive metadata comparison rejection. Unknown outcomes must be reconciled first.

## Founding identity and scoped node statements

`FoundingEnrollmentDraft::open_or_create` prepares only a **new** root genesis.
It binds the first Node certificate to the existing local node ID/principal and
the saved `JoinKey` CSR. Its constructor accepts no established registry to
modify. The owner-only draft persists the exact public registry and certificate;
reopening with another node, principal, key or CA fails. An initialized draft
whose file disappears fails closed. This helper is never a way to replace an
established root registry.

The founding receipt is provisional. The root owner must durably install the
exact public genesis and establish the required current-term quorum barrier
before granting its certificate or completing the key. On restart it must check
the recovered **current** registry, including revocation and expiry, rather than
trusting the saved original draft. The founding certificate has a CA-signed,
explicit genesis-principal subject marker, accepted only for the first Node
receipt. Ordinary CSR issuance discards requested subjects and cannot request
that marker. The initial receipt confers Node identity, not a Raft membership
change.

`CredentialMaterial::sign_node_statement` signs a bounded, domain-separated
statement using rcgen's P-256 signing primitive. `EnrollmentRegistry::
verify_node_statement` checks cluster and payload binding, active committed Node
enrollment, certificate identity, expiry/revocation and the
[ring ECDSA P-256/SHA-256 ASN.1 signature](https://docs.rs/ring/latest/ring/signature/static.ECDSA_P256_SHA256_ASN1.html).
Client and bootstrap server certificates cannot act as node attesters. This
proves which enrolled key made the statement; the directory's separately
committed assignments determine whether it has the claimed topology, replica or
quorum authority. No signature grants authority to its own supplied voter list.

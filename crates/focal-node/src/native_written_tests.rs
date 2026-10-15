//! Continuations belong to their original publication, never to a retry's later root.
use super::*;

#[test]
fn a_retry_after_later_publication_and_restart_keeps_its_receipt_without_written_state() {
    focal_rt::block_on(async move {
        let root = tempfile::tempdir().unwrap();
        native_activation::activate_local(
            &settings(root.path()),
            NativeContentProfile::ProjectionOnly,
        )
        .unwrap();
        let node = EmbeddedNode::open(&settings(root.path())).unwrap();
        let ledger = node.identity.ledger;
        let issuer = node.identity.issuer;
        let actor = peer(&node, issuer, PeerRole::Actor);
        let original = envelope(
            &node,
            NATIVE_WRITTEN_PROTOCOL_VERSION,
            1,
            Operation::Native {
                frame: frame(ledger, &create(&node, 1, 100)),
            },
        );
        let post = NativeInput {
            request: RequestKey {
                principal: issuer,
                epoch: RequestEpoch(1),
                id: RequestId::from_u128(2),
            },
            command: NativeCommand::Post {
                expected: binding(ledger, 100),
            },
        };
        let post = envelope(
            &node,
            NATIVE_WRITTEN_PROTOCOL_VERSION,
            2,
            Operation::Native {
                frame: frame(ledger, &post),
            },
        );
        let current = envelope(
            &node,
            NATIVE_PROTOCOL_VERSION,
            3,
            read(NativeReadQuery::Objects(vec![NativeObjectRef::Claim(
                ClaimId::from_u128(100),
            )])),
        );
        let (host, owner) = LocalHost::spawn(node, WireLimits::default()).unwrap();
        let Response::Native(NativeMutationReply::CommittedWithWritten { receipt, written }) =
            call(&host, actor.clone(), original.clone()).await
        else {
            panic!("fresh commit carries its own state")
        };
        assert_eq!(written.sequence, receipt.sequence);
        let generated = written.claim.unwrap();
        assert_eq!(generated.status, ClaimStatus::Generated);
        assert_eq!(generated.binding.revision, ObjectRevision(1));
        let Response::Native(NativeMutationReply::CommittedWithWritten {
            written: posted, ..
        }) = call(&host, actor.clone(), post).await
        else {
            panic!("post carries its own state")
        };
        assert_eq!(posted.claim.unwrap().status, ClaimStatus::Posted);
        assert_eq!(posted.claim.unwrap().binding.revision, ObjectRevision(2));
        assert_eq!(
            written.claim.unwrap(),
            generated,
            "later writes cannot alter the first projection"
        );
        let stable = Response::Native(NativeMutationReply::Committed(receipt));
        assert_eq!(call(&host, actor.clone(), original.clone()).await, stable);
        let mut old_protocol = original.clone();
        old_protocol.protocol = NATIVE_PROTOCOL_VERSION;
        assert_eq!(call(&host, actor.clone(), old_protocol).await, stable);
        host.stop().await.unwrap();
        owner.join_async().await.unwrap();

        let node = EmbeddedNode::open(&settings(root.path())).unwrap();
        let (host, owner) = LocalHost::spawn(node, WireLimits::default()).unwrap();
        assert_eq!(call(&host, actor.clone(), original).await, stable);
        let Response::NativeRead(page) = call(&host, actor, current).await else {
            panic!("current read")
        };
        let NativeObject::Claim(claim) = &page.objects[0] else {
            panic!("claim")
        };
        assert_eq!(claim.status, ClaimStatus::Posted);
        assert_eq!(claim.binding.revision, ObjectRevision(2));
        host.stop().await.unwrap();
        owner.join_async().await.unwrap();
    })
    .unwrap();
}

use super::*;

fn claim_buffers(content: &ClaimDescriptor) -> Vec<*const ()> {
    let mut buffers = vec![
        content.description().as_ptr().cast(),
        content.relations().as_ptr().cast(),
        content.requirements().as_ptr().cast(),
    ];
    buffers.extend(content.scopes().map(|scope| scope.key.as_ptr().cast()));
    buffers.extend(content.slots().map(|slot| slot.checks.as_ptr().cast()));
    buffers
}

fn validation_buffers(descriptor: &ValidationDescriptor) -> Vec<*const ()> {
    let mut buffers = vec![
        descriptor.description().as_ptr().cast(),
        descriptor.contributed_by().as_ptr().cast(),
    ];
    if let Some(quality_bar) = descriptor.quality_bar() {
        buffers.push(quality_bar.as_ptr().cast());
    }
    buffers
}

struct AuthoredSnapshot {
    content: ClaimDescriptor,
    descriptor: ValidationDescriptor,
    claim_buffers: Vec<*const ()>,
    validation_buffers: Vec<*const ()>,
    resolution: [NativeCreatedObject; 2],
}

impl AuthoredSnapshot {
    fn capture(proposal: &NativeAuthoredProposal) -> Self {
        assert_eq!(proposal.declarations.len(), 1);
        let descriptor = proposal.declarations.first().unwrap();
        assert_eq!(
            descriptor.declaration().target(),
            validation::TargetDeclaration::Delivery
        );
        Self {
            content: proposal
                .content
                .try_copy(proposal.content.retained_bytes().unwrap())
                .unwrap(),
            descriptor: descriptor
                .try_copy(descriptor.retained_bytes().unwrap())
                .unwrap(),
            claim_buffers: claim_buffers(&proposal.content),
            validation_buffers: validation_buffers(descriptor),
            resolution: [
                NativeCreatedObject {
                    ordinal: 0,
                    family: NativeCreatedFamily::Claim,
                    schema: proposal.content.schema(),
                    content: proposal.content.content_hash(),
                    requested: proposal.content.binding().object,
                    resolved: proposal.content.binding().object,
                },
                NativeCreatedObject {
                    ordinal: 1,
                    family: NativeCreatedFamily::Validation,
                    schema: descriptor.schema(),
                    content: descriptor.content_hash(),
                    requested: descriptor.binding().object,
                    resolved: descriptor.binding().object,
                },
            ],
        }
    }

    fn claim(&self) -> ClaimId {
        self.content.id()
    }

    fn validation(&self) -> ValidationId {
        ValidationId(self.descriptor.binding().object.0)
    }

    fn check_claim(&self, content: &ClaimDescriptor) {
        assert_eq!(content, &self.content);
        assert_eq!(claim_buffers(content), self.claim_buffers);
    }

    fn check_validation(&self, descriptor: &ValidationDescriptor) {
        assert_eq!(descriptor.schema(), self.descriptor.schema());
        assert_eq!(descriptor.binding(), self.descriptor.binding());
        assert_eq!(descriptor.content_hash(), self.descriptor.content_hash());
        assert_eq!(
            descriptor.specification_hash(),
            self.descriptor.specification_hash()
        );
        assert_eq!(descriptor.description(), self.descriptor.description());
        assert_eq!(descriptor.quality_bar(), self.descriptor.quality_bar());
        assert_eq!(
            descriptor.contributed_by(),
            self.descriptor.contributed_by()
        );
        assert_eq!(
            descriptor.policy_revision(),
            self.descriptor.policy_revision()
        );
        assert_eq!(
            descriptor.declaration().intent_fingerprint(),
            self.descriptor.declaration().intent_fingerprint()
        );
        assert_eq!(validation_buffers(descriptor), self.validation_buffers);
    }

    fn check_view(&self, view: &NativeView<'_>, request: u128, status: ClaimStatus) {
        assert_eq!(view.content_profile(), NativeContentProfile::AuthoredV1);
        let claim = view.claim(self.claim()).unwrap();
        assert_eq!(claim.status(), status);
        assert_eq!(claim.binding().content, self.content.content_hash());
        self.check_claim(view.claim_content(self.claim()).unwrap());
        let descriptor = view.validation_descriptor(self.validation()).unwrap();
        self.check_validation(descriptor);
        assert!(std::ptr::eq(
            descriptor.declaration(),
            view.definition(self.validation()).unwrap()
        ));
        assert_eq!(
            view.creation_result(key(request)).unwrap().entries(),
            self.resolution
        );
    }

    fn check_pinned(&self, read: &NativeRead, request: u128) {
        assert_eq!(
            read.with_authored_claim(self.claim(), 0, |content, state| {
                self.check_claim(content);
                assert_eq!(state.binding(), self.content.binding());
                state.status()
            })
            .unwrap(),
            Some(ClaimStatus::Generated)
        );
        assert_eq!(
            read.with_validation_descriptor(self.validation(), 0, |descriptor| {
                self.check_validation(descriptor);
                descriptor.content_hash()
            })
            .unwrap(),
            Some(self.descriptor.content_hash())
        );
        assert_eq!(
            read.with_creation_result(key(request), 0, |result| {
                assert_eq!(result.entries(), self.resolution);
                result.entries().len()
            })
            .unwrap(),
            Some(2)
        );
    }
}

fn stage(owner: &mut NativeOwner, input: NativeInput) -> (NativeCandidate, NativeOutcome) {
    match owner.prepare(context(ISSUER), input, None).unwrap() {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        NativeStaging::Existing { .. } => panic!("expected fresh authored lifecycle candidate"),
    }
}

#[test]
fn authored_owner_keeps_full_bodies_and_pinned_creation_through_post_and_cancel() {
    let proposed = proposal(100, 200);
    let snapshot = AuthoredSnapshot::capture(&proposed);
    let mut owner = NativeOwner::new(core()).unwrap();
    let (created, creation) = stage(&mut owner, create(1, vec![proposed]));
    assert_eq!(owner.committed().sequence(), SessionSeq(0));
    assert!(owner.committed().claim(snapshot.claim()).is_none());
    assert!(owner.committed().claim_content(snapshot.claim()).is_none());
    assert!(
        owner
            .committed()
            .validation_descriptor(snapshot.validation())
            .is_none()
    );
    assert!(owner.committed().creation_result(key(1)).is_none());
    assert_eq!(owner.effective().sequence(), creation.sequence);
    snapshot.check_view(&owner.effective(), 1, ClaimStatus::Generated);
    snapshot.check_view(
        &owner.candidate(created).unwrap(),
        1,
        ClaimStatus::Generated,
    );
    assert_eq!(owner.publish_after_durable(created).unwrap(), creation);
    snapshot.check_view(&owner.committed(), 1, ClaimStatus::Generated);

    let pinned = owner.pin(0, 100).unwrap();
    assert_eq!(pinned.sequence(), creation.sequence);
    snapshot.check_pinned(&pinned, 1);

    let expected = owner.effective().claim(snapshot.claim()).unwrap().binding();
    let (posted, posting) = stage(&mut owner, input(2, NativeCommand::Post { expected }));
    snapshot.check_view(&owner.effective(), 1, ClaimStatus::Posted);
    snapshot.check_view(&owner.committed(), 1, ClaimStatus::Generated);
    snapshot.check_pinned(&pinned, 1);
    assert_eq!(owner.publish_after_durable(posted).unwrap(), posting);
    snapshot.check_view(&owner.committed(), 1, ClaimStatus::Posted);

    let expected = owner.effective().claim(snapshot.claim()).unwrap().binding();
    let (cancelled, cancellation) = stage(&mut owner, input(3, NativeCommand::Cancel { expected }));
    snapshot.check_view(&owner.effective(), 1, ClaimStatus::Cancelled);
    snapshot.check_view(&owner.committed(), 1, ClaimStatus::Posted);
    snapshot.check_pinned(&pinned, 1);
    assert_eq!(
        owner.publish_after_durable(cancelled).unwrap(),
        cancellation
    );
    snapshot.check_view(&owner.effective(), 1, ClaimStatus::Cancelled);
    snapshot.check_view(&owner.committed(), 1, ClaimStatus::Cancelled);
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(pinned.sequence(), creation.sequence);
    snapshot.check_pinned(&pinned, 1);
    owner.release(&pinned).unwrap();
}

#[test]
fn self_work_draft_is_retained_but_post_refuses_without_mutating_the_owner() {
    let proposed = proposal_with(300, 400, ISSUER, ActionType::Work, &[]);
    let snapshot = AuthoredSnapshot::capture(&proposed);
    let mut owner = NativeOwner::new(core()).unwrap();
    let (candidate, creation) = stage(&mut owner, create(10, vec![proposed]));
    snapshot.check_view(&owner.effective(), 10, ClaimStatus::Generated);
    assert_eq!(owner.publish_after_durable(candidate).unwrap(), creation);
    snapshot.check_view(&owner.committed(), 10, ClaimStatus::Generated);
    let expected = owner.committed().claim(snapshot.claim()).unwrap().binding();
    let budget = owner.budget_stats();
    let range = owner.range_stats();

    assert!(matches!(
        owner.prepare(
            context(ISSUER),
            input(11, NativeCommand::Post { expected }),
            None
        ),
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::InvalidPolicy
        )))
    ));
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(owner.budget_stats(), budget);
    assert_eq!(owner.range_stats(), range);
    assert_eq!(owner.effective().sequence(), creation.sequence);
    assert_eq!(owner.committed().sequence(), creation.sequence);
    assert_eq!(
        owner.effective().claim(snapshot.claim()).unwrap().binding(),
        expected
    );
    assert!(owner.effective().recorded(key(11)).is_none());
    assert!(owner.effective().creation_result(key(11)).is_none());
    snapshot.check_view(&owner.effective(), 10, ClaimStatus::Generated);
    snapshot.check_view(&owner.committed(), 10, ClaimStatus::Generated);
}

use super::*;
impl ManagedRequests {
    /// Return the exact saved wire action, with every private lock released.
    /// None means ownership is ready and no delivered prefix needs acknowledgment.
    pub fn maintenance(
        &self,
        ids: &mut impl IdGenerator,
    ) -> Result<Option<RequestEnvelope>, ManagedRequestsError> {
        let (directory, mut state) = self.load()?;
        if let Some(request) = &state.pending {
            return Ok(Some(request.as_ref().clone()));
        }
        if let Phase::Initialize(initial) = &state.phase {
            ManagedOperationStore::finish_initialization(
                &self.child_path(&state),
                self.context,
                self.limits,
                &initial.input,
                &initial.receipt,
            )?;
            state.phase = Phase::Ready;
            self.save(&directory, &state, true)?;
        }
        if let Phase::Observed { slot, generation } = state.phase {
            let id = fresh(ids)?;
            let owner = fresh(ids)?;
            let input = RequestStreamControlInput {
                cluster: self.context.cluster,
                ledger: self.context.ledger,
                principal: self.context.principal,
                id,
                command: RequestStreamCommand::Register {
                    slot,
                    expected_generation: generation,
                    owner,
                    window: self.limits.window,
                },
            };
            state.phase = Phase::Register(Box::new(Registration {
                input,
                probe: false,
            }));
        }
        let request = match &state.phase {
            Phase::Scan { slot } => self.envelope(
                fresh(ids)?,
                Operation::RequestStreamRead {
                    cluster: self.context.cluster,
                    query: RequestStreamQuery::Slot { slot: *slot },
                },
            ),
            Phase::Register(r) if r.probe => {
                let RequestStreamCommand::Register { slot, .. } = r.input.command else {
                    return Err(ManagedRequestsError::Corrupt);
                };
                self.envelope(
                    fresh(ids)?,
                    Operation::RequestStreamRead {
                        cluster: self.context.cluster,
                        query: RequestStreamQuery::Slot { slot },
                    },
                )
            }
            Phase::Register(r) => self.control(&r.input),
            Phase::Ready => {
                let store = self.ready(&state)?;
                let pruned = self.prune(&store, &mut state)?;
                let status = store.status()?;
                if status.closed {
                    // The closed generation is fully resolved and retired:
                    // rotate to the next generation on the same slot. The
                    // record now names the next phase, so the fresh load
                    // below continues with its registration.
                    self.begin_rotation(&directory, &mut state, status.stream)?;
                    drop(directory);
                    return self.maintenance(ids);
                }
                let control = if let Some(control) = store.pending_control()? {
                    Some(control)
                } else {
                    let mut next = status.retired_through;
                    let mut count = 0u32;
                    for mark in &state.delivered {
                        let Some(expected) = next.checked_add(1) else {
                            break;
                        };
                        if mark.key.ordinal != expected {
                            break;
                        }
                        next = expected;
                        count = count.checked_add(1).ok_or(ManagedRequestsError::Corrupt)?;
                    }
                    if count > 0 {
                        Some(store.prepare_acknowledgment(fresh(ids)?, count)?)
                    } else if store.stop_if_drained(state.rotation)? {
                        // Every ordinal of a full generation is retired:
                        // stop issuance durably and close it exactly.
                        Some(store.prepare_close(fresh(ids)?)?)
                    } else {
                        None
                    }
                };
                let Some(control) = control else {
                    if pruned {
                        self.save(&directory, &state, true)?;
                    }
                    return Ok(None);
                };
                self.control(&control)
            }
            Phase::Exhausted => {
                state.phase = Phase::Scan { slot: 0 };
                self.save(&directory, &state, true)?;
                return Err(ManagedRequestsError::Exhausted);
            }
            Phase::Initialize(_) | Phase::Observed { .. } => {
                return Err(ManagedRequestsError::Corrupt);
            }
        };
        state.pending = Some(Box::new(request.clone()));
        self.save(&directory, &state, true)?;
        Ok(Some(request))
    }
    /// Client routing may change only the physical route epoch. Receipt scope,
    /// request ID, payload and principal remain bound to the saved action.
    pub fn accept_maintenance(
        &self,
        request: &RequestEnvelope,
        response: ResponseEnvelope,
    ) -> Result<(), ManagedRequestsError> {
        let (directory, mut state) = self.load()?;
        let input_hash = request_digest(request)?;
        let response_hash = digest(&response)?;
        if state.accepted == Some((input_hash, response_hash)) {
            return Ok(());
        }
        let saved = state
            .pending
            .as_deref()
            .ok_or(ManagedRequestsError::Stale)?;
        if !same_request(saved, request) {
            return Err(ManagedRequestsError::Stale);
        }
        let mut routed = request.clone();
        routed.route_epoch = response.route_epoch;
        focal_wire::validate_response(
            &routed,
            &response,
            Some(self.context.principal),
            &focal_wire::WireLimits::default(),
        )
        .map_err(|_| ManagedRequestsError::InvalidResponse)?;
        if let Response::Error(error) = response.result {
            if matches!(error, AccessError::ManagedConflict)
                && let Phase::Register(registration) = &mut state.phase
                && !registration.probe
            {
                registration.probe = true;
                state.pending = None;
                self.save(&directory, &state, true)?;
                return Ok(());
            }
            return Err(ManagedRequestsError::Remote(error));
        }
        match (&mut state.phase, response.result) {
            (Phase::Scan { slot }, Response::RequestStreamRead(reply)) => {
                let RequestStreamReadResult::Slot(observed) = reply.page.result else {
                    return Err(ManagedRequestsError::InvalidResponse);
                };
                // First fit keeps the normal laptop path to one read + register.
                match observed {
                    RequestStreamState::Vacant { generation, .. } if generation < u64::MAX => {
                        // Entropy is supplied only to maintenance. Persist the
                        // observation now; no Register can escape without IDs.
                        state.phase = Phase::Observed {
                            slot: *slot,
                            generation,
                        };
                    }
                    _ => {
                        state.phase = slot
                            .checked_add(1)
                            .filter(|next| *next < SCAN)
                            .map_or(Phase::Exhausted, |slot| Phase::Scan { slot })
                    }
                }
            }
            (Phase::Register(registration), Response::RequestStreamRead(reply)) => {
                let RequestStreamReadResult::Slot(observed) = reply.page.result else {
                    return Err(ManagedRequestsError::InvalidResponse);
                };
                let RequestStreamCommand::Register {
                    slot,
                    expected_generation,
                    owner,
                    window,
                } = registration.input.command
                else {
                    return Err(ManagedRequestsError::Corrupt);
                };
                let generation = match observed {
                    RequestStreamState::Vacant { generation, .. } => generation,
                    RequestStreamState::Active { stream, .. } => stream.generation,
                };
                // An owned registration whose reply was lost is recognized
                // by its nonce at exactly the generation it was assigned.
                let ours = matches!(observed,RequestStreamState::Active{stream,owner:actual,window:bound,..} if expected_generation.checked_add(1)==Some(stream.generation) && owner==actual && window==bound);
                if generation > expected_generation && !ours {
                    state.phase = Phase::Scan { slot };
                } else {
                    registration.probe = false;
                }
            }
            (Phase::Register(registration), Response::RequestStreamControlled(reply)) => {
                state.phase = Phase::Initialize(Box::new(Initialization {
                    input: registration.input.clone(),
                    receipt: reply.receipt,
                }));
            }
            (Phase::Ready, Response::RequestStreamControlled(reply)) => {
                let store = self.ready(&state)?;
                store.record_control(reply.receipt)?;
                self.prune(&store, &mut state)?;
            }
            _ => return Err(ManagedRequestsError::InvalidResponse),
        }
        state.pending = None;
        state.accepted = Some((input_hash, response_hash));
        self.save(&directory, &state, true)
    }
    fn envelope(&self, id: RequestId, operation: Operation) -> RequestEnvelope {
        RequestEnvelope {
            protocol: focal_wire::MANAGED_PROTOCOL_VERSION,
            ledger: self.context.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: id,
            operation,
        }
    }
    fn control(&self, input: &RequestStreamControlInput) -> RequestEnvelope {
        self.envelope(
            input.id,
            Operation::RequestStreamControl {
                cluster: input.cluster,
                command: input.command.clone(),
            },
        )
    }
}

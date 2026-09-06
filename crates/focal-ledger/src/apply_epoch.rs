impl Session {
    fn apply_config(&self) -> EpochLimits {
        let mut config = self.limits.apply;
        config.max_bytes = config
            .max_bytes
            .min(self.limits.memory_bytes.checked_div(4).unwrap_or(0))
            .min(
                self.limits
                    .completion_reserve_bytes
                    .checked_div(2)
                    .unwrap_or(0),
            );
        if config.max_bytes < 4 * 1024 * 1024 {
            config.max_workers = 1;
            config.worker_stack_bytes = config.worker_stack_bytes.min(256 * 1024);
        }
        config
    }
    fn ensure_apply_workspace(&mut self, lane: BudgetLane) -> Result<(), LedgerError> {
        if self.apply_workspace.is_none() {
            self.apply_workspace = Some(
                self.budget
                    .reserve(BudgetKind::Pending, lane, self.apply_config().max_bytes)?
                    .commit(),
            );
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn reserve_candidate<'a>(
        &self,
        prepared: &PreparedMutation,
        entry_hash: ContentHash,
        before: CoreView<'_>,
        patch: RowPatch<'_>,
        result: &ApplyResult,
        prior: impl Iterator<Item = &'a Candidate> + Clone,
        core_bytes: usize,
        pending: bool,
    ) -> Result<Candidate, LedgerError> {
        let lane = if pending {
            mutation_lane(&prepared.input.command)
        } else {
            BudgetLane::Completion
        };
        let previous_bytes = reference_charge(&before)?;
        let core_growth = self
            .budget
            .reserve(
                BudgetKind::Payload,
                lane,
                core_bytes.saturating_sub(previous_bytes),
            )?
            .commit();
        let rows_charge = self
            .budget
            .reserve(
                BudgetKind::Payload,
                lane,
                if pending {
                    reference_charge(&patch)?
                } else {
                    0
                },
            )?
            .commit();
        let intent_charge = self
            .budget
            .reserve(BudgetKind::Pending, lane, reference_charge(prepared)?)?
            .commit();
        let result_charge = self
            .budget
            .reserve(BudgetKind::Pending, lane, reference_charge(result)?)?
            .commit();
        let graph = self.graph.prepare_patch(
            before,
            patch,
            prior.clone().last().map(|candidate| &candidate.graph),
            lane,
        )?;
        let charge = self
            .budget
            .reserve(
                BudgetKind::Pending,
                lane,
                size_of::<Candidate>()
                    .checked_add(
                        result
                            .deltas
                            .len()
                            .checked_mul(size_of::<RetainedDelta>())
                            .ok_or(LedgerError::Capacity)?,
                    )
                    .ok_or(LedgerError::Capacity)?,
            )?
            .commit();
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(result.deltas.len())
            .map_err(|_| LedgerError::Capacity)?;
        for delta in &result.deltas {
            let allocation = self
                .budget
                .reserve(BudgetKind::Payload, lane, reference_charge(delta)?)?
                .commit();
            retained.push(RetainedDelta {
                bytes: postcard::experimental::serialized_size(delta)?,
                delta: delta.clone(),
                _charge: allocation,
            });
        }
        let retire_through = self.retention_forecast_from(&retained, patch.sequence(), prior)?;
        Ok(Candidate {
            sequence: patch.sequence(),
            entry_hash,
            prepared: prepared.clone(),
            core_bytes,
            graph,
            result: result.clone(),
            retained,
            retire_through,
            core_growth,
            _rows_charge: rows_charge,
            _intent_charge: intent_charge,
            result_charge,
            _charge: charge,
        })
    }
    fn apply_domain_epoch(
        &mut self,
        entries: &[focal_consensus::CommittedEntry],
        events: &mut SessionEvents,
    ) -> Result<usize, LedgerError> {
        if entries.is_empty() {
            return Err(LedgerError::Corrupt);
        }
        self.ensure_delta_slots()?;
        self.ensure_apply_workspace(BudgetLane::Completion)?;
        self.pending_cursor = None;
        self.pending_maintenance = None;
        let mut count = entries.len();
        loop {
            let selected = entries.get(..count).ok_or(LedgerError::Corrupt)?;
            let attempt = self.prepare_committed_epoch(selected);
            let (output, mut candidates, from_pending) = match attempt {
                Ok(value) => value,
                Err(error) if count > 1 && apply_capacity(&error) => {
                    count = count.checked_div(2).ok_or(LedgerError::Capacity)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let checked = self.preflight_epoch(&output, &mut candidates, from_pending);
            let (final_sequence, floor, bytes, items, target) = match checked {
                Ok(value) => value,
                Err(error) => {
                    self.clear_pending();
                    return Err(error);
                }
            };
            let report = output.report();
            // All domain, page-chain, delta-ring and allowance checks precede
            // the first mutation. No external observer can enter this owner.
            let results = self.core.publish_epoch(output)?;
            if from_pending {
                self.pending_rows.drop_prefix(count, &self.core)?;
            }
            self.retire_deltas(floor)?;
            for (candidate, result) in candidates.into_iter().zip(results) {
                self.graph.publish(candidate.graph)?;
                for delta in candidate.retained {
                    if delta.delta.id.sequence > floor {
                        self.deltas.push_back(delta);
                    }
                }
                drop(candidate.result);
                events.committed.push(result);
                events._charges.push(candidate.result_charge);
            }
            self.delta_bytes = bytes;
            if self.deltas.len() != items || self.core.sequence() != final_sequence {
                return Err(LedgerError::Corrupt);
            }
            self.shrink_core_charge(target)?;
            self.applied_raft = selected.last().ok_or(LedgerError::Corrupt)?.index;
            self.last_epoch = Some(report);
            return Ok(count);
        }
    }
    fn preflight_epoch(
        &mut self,
        output: &EpochOutput,
        candidates: &mut [Candidate],
        from_pending: bool,
    ) -> Result<(SessionSeq, SessionSeq, usize, usize, usize), LedgerError> {
        let final_sequence = self.core.validate_epoch(output)?;
        self.graph
            .validate_publication(candidates.iter().map(|candidate| &candidate.graph))?;
        let (floor, bytes, items) = self.validate_epoch_deltas(candidates)?;
        let target = reference_charge(&output.view_before(&self.core, candidates.len())?)?;
        for (index, candidate) in candidates.iter_mut().enumerate() {
            if candidate.core_bytes
                != reference_charge(&output.view_before(
                    &self.core,
                    index.checked_add(1).ok_or(LedgerError::Capacity)?,
                )?)?
            {
                return Err(LedgerError::Corrupt);
            }
            match mutation_lane(&candidate.prepared.input.command) {
                BudgetLane::Ordinary if from_pending => {
                    self.core_charge.absorb(&mut candidate.core_growth)?
                }
                _ => self
                    .core_completion_charge
                    .absorb(&mut candidate.core_growth)?,
            }
        }
        let total = self
            .core_charge
            .bytes()
            .checked_add(self.core_completion_charge.bytes())
            .ok_or(LedgerError::Corrupt)?;
        if total < target {
            return Err(LedgerError::Corrupt);
        }
        Ok((final_sequence, floor, bytes, items, target))
    }
    fn prepare_committed_epoch(
        &mut self,
        entries: &[focal_consensus::CommittedEntry],
    ) -> Result<(EpochOutput, Vec<Candidate>, bool), LedgerError> {
        let decode_bytes = entries
            .iter()
            .try_fold(0usize, |total, entry| {
                total.checked_add(entry.data.len().checked_mul(64)?)
            })
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(LedgerError::Capacity)?;
        let _decode =
            self.budget
                .reserve(BudgetKind::Recovery, BudgetLane::Completion, decode_bytes)?;
        let mut inputs = Vec::new();
        inputs
            .try_reserve_exact(entries.len())
            .map_err(|_| LedgerError::Capacity)?;
        let mut previous = self.applied_raft;
        for entry in entries {
            if entry.index <= previous {
                return Err(LedgerError::Corrupt);
            }
            previous = entry.index;
            inputs.push(postcard::from_bytes::<PreparedMutation>(
                entry
                    .data
                    .strip_prefix(ENTRY_MAGIC)
                    .ok_or(LedgerError::Corrupt)?,
            )?);
        }
        let plan = self.core.plan_epoch(inputs.clone(), self.apply_config())?;
        #[cfg(test)]
        let mut plan = plan;
        #[cfg(test)]
        if self.omit_epoch_declarations {
            for index in 0..plan.len() {
                let mut declaration = plan.accesses(index).ok_or(LedgerError::Corrupt)?.clone();
                declaration.reads.clear();
                declaration.writes.clear();
                plan.declare(index, declaration)?;
            }
        }
        let output = plan.execute(&self.core)?;
        self.core.validate_epoch(&output)?;
        let from_pending = self.pending.len() >= entries.len()
            && self.pending_rows.matches_epoch(&output)
            && self
                .pending
                .iter()
                .zip(entries)
                .zip(&inputs)
                .zip(output.results())
                .all(|(((candidate, entry), prepared), result)| {
                    candidate.sequence == result.receipt.sequence
                        && candidate.entry_hash
                            == ContentHash(*blake3::hash(&entry.data).as_bytes())
                        && candidate.prepared == *prepared
                        && candidate.result == *result
                });
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(entries.len())
            .map_err(|_| LedgerError::Capacity)?;
        if from_pending {
            self.graph.validate_publication(
                self.pending
                    .iter()
                    .take(entries.len())
                    .map(|candidate| &candidate.graph),
            )?;
            // No remaining fallible allocation after removing these reservations.
            for _ in entries {
                candidates.push(self.pending.pop_front().ok_or(LedgerError::Corrupt)?);
            }
        } else {
            self.clear_pending();
            for (index, (prepared, result)) in inputs.iter().zip(output.results()).enumerate() {
                let before = output.view_before(&self.core, index)?;
                let after = output.view_before(
                    &self.core,
                    index.checked_add(1).ok_or(LedgerError::Capacity)?,
                )?;
                let patch = output.patch(index).ok_or(LedgerError::Corrupt)?;
                let candidate = self.reserve_candidate(
                    prepared,
                    ContentHash(
                        *blake3::hash(&entries.get(index).ok_or(LedgerError::Corrupt)?.data)
                            .as_bytes(),
                    ),
                    before,
                    patch,
                    result,
                    candidates.iter(),
                    reference_charge(&after)?,
                    false,
                )?;
                candidates.push(candidate);
            }
        }
        Ok((output, candidates, from_pending))
    }
    fn validate_epoch_deltas(
        &self,
        candidates: &[Candidate],
    ) -> Result<(SessionSeq, usize, usize), LedgerError> {
        let current_bytes = self
            .deltas
            .iter()
            .try_fold(0usize, |bytes, delta| bytes.checked_add(delta.bytes))
            .ok_or(LedgerError::Corrupt)?;
        if current_bytes != self.delta_bytes {
            return Err(LedgerError::Corrupt);
        }
        for candidate in candidates {
            if candidate.retained.len() != candidate.result.deltas.len()
                || candidate
                    .retained
                    .iter()
                    .zip(&candidate.result.deltas)
                    .any(|(a, b)| a.delta != *b)
            {
                return Err(LedgerError::Corrupt);
            }
        }
        let floor = self.delta_floor.max(
            candidates
                .last()
                .ok_or(LedgerError::Corrupt)?
                .retire_through,
        );
        let mut bytes = 0usize;
        let mut items = 0usize;
        for delta in self
            .deltas
            .iter()
            .chain(candidates.iter().flat_map(|candidate| &candidate.retained))
            .filter(|delta| delta.delta.id.sequence > floor)
        {
            if delta.bytes != postcard::experimental::serialized_size(&delta.delta)? {
                return Err(LedgerError::Corrupt);
            }
            bytes = bytes
                .checked_add(delta.bytes)
                .ok_or(LedgerError::Capacity)?;
            items = items.checked_add(1).ok_or(LedgerError::Capacity)?;
        }
        if bytes > self.limits.delta_bytes
            || items > self.limits.delta_items
            || items > self.deltas.capacity()
        {
            return Err(LedgerError::Capacity);
        }
        Ok((floor, bytes, items))
    }
    fn shrink_core_charge(&mut self, target: usize) -> Result<(), LedgerError> {
        let total = self
            .core_charge
            .bytes()
            .checked_add(self.core_completion_charge.bytes())
            .ok_or(LedgerError::Corrupt)?;
        let excess = total.checked_sub(target).ok_or(LedgerError::Corrupt)?;
        let completion = excess.min(self.core_completion_charge.bytes());
        self.core_completion_charge.shrink_to(
            self.core_completion_charge
                .bytes()
                .checked_sub(completion)
                .ok_or(LedgerError::Corrupt)?,
        )?;
        let ordinary = excess.checked_sub(completion).ok_or(LedgerError::Corrupt)?;
        self.core_charge.shrink_to(
            self.core_charge
                .bytes()
                .checked_sub(ordinary)
                .ok_or(LedgerError::Corrupt)?,
        )?;
        Ok(())
    }
}
fn apply_capacity(error: &LedgerError) -> bool {
    matches!(
        error,
        LedgerError::Capacity
            | LedgerError::Epoch(EpochError::Capacity(_))
            | LedgerError::Memory(MemoryError::Capacity { .. })
            | LedgerError::Graph(GraphError::Memory(MemoryError::Capacity { .. }))
    )
}

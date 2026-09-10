use super::*;

impl NativePrepared {
    pub fn result_testament(&self, id: TestamentId) -> Option<&NativeResultTestament> {
        as_result_testament(self.fragments.get(&Key::ResultTestament(id)))
    }
    pub fn claim_result_testament(&self, claim: ClaimId) -> Option<&NativeResultTestament> {
        let id = index(self.fragments.get(&Key::ClaimResultTestament(claim)))?;
        self.result_testament(id)
            .filter(|row| row.testament().claim() == claim)
    }
}
impl Core<NativeState> {
    pub fn native_result_testament(&self, id: TestamentId) -> Option<&NativeResultTestament> {
        as_result_testament(self.state.rows.get(&Key::ResultTestament(id)))
    }
    pub fn native_claim_result_testament(&self, claim: ClaimId) -> Option<&NativeResultTestament> {
        let id = index(self.state.rows.get(&Key::ClaimResultTestament(claim)))?;
        self.native_result_testament(id)
            .filter(|row| row.testament().claim() == claim)
    }
}
impl NativeRead {
    pub fn with_result_testament<T>(
        &self,
        id: TestamentId,
        now: u64,
        project: impl FnOnce(&NativeResultTestament) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::ResultTestament(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_result_testament(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}

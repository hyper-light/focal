//! Borrowed projections of independent work, diagnostics and responses. Snapshot
//! callbacks preserve the existing expiring lease boundary without cloning rows.
use super::*;

pub(super) fn as_registrations(row: Option<&Row>) -> Option<&RegistrationSet> {
    match row {
        Some(Row::Claim(value)) => value.registrations(),
        _ => None,
    }
}

pub(super) fn as_missing(row: Option<&Row>) -> Option<&NativeMissingResult> {
    match row {
        Some(Row::MissingResult(value)) => value.get(),
        _ => None,
    }
}

pub(super) fn as_delivery(row: Option<&Row>) -> Option<&NativeDeliveryResult> {
    match row {
        Some(Row::DeliveryResult(value)) => value.get(),
        _ => None,
    }
}

pub(super) fn as_work(row: Option<&Row>) -> Option<&NativeWork> {
    match row {
        Some(Row::Work(value)) => value.get(),
        _ => None,
    }
}

pub(super) fn as_diagnostic(row: Option<&Row>) -> Option<&NativeDiagnostic> {
    match row {
        Some(Row::Diagnostic(value)) => value.get(),
        _ => None,
    }
}

pub(super) fn as_response(row: Option<&Row>) -> Option<&Response> {
    match row {
        Some(Row::Response(value)) => value.get(),
        _ => None,
    }
}

pub(super) fn as_response_record(row: Option<&Row>) -> Option<&NativeResponseRecord> {
    match row {
        Some(Row::Response(value)) => value.record(),
        _ => None,
    }
}

impl NativePrepared {
    pub fn missing_result(&self, key: NativeResultKey) -> Option<&NativeMissingResult> {
        as_missing(self.range.get(&Key::MissingResult(key)))
    }
    pub fn response_record(&self, id: TestamentId) -> Option<&NativeResponseRecord> {
        as_response_record(self.range.get(&Key::Response(id)))
    }
    pub fn registrations(&self, id: ClaimId) -> Option<&RegistrationSet> {
        as_registrations(self.range.get(&Key::Claim(id)))
    }
    pub fn delivery_result(&self, key: NativeResultKey) -> Option<&NativeDeliveryResult> {
        as_delivery(self.range.get(&Key::DeliveryResult(key)))
    }
    pub fn work(&self, id: ArtifactId) -> Option<&NativeWork> {
        as_work(self.range.get(&Key::Work(id)))
    }
    pub fn diagnostic(&self, id: ArtifactId) -> Option<&NativeDiagnostic> {
        as_diagnostic(self.range.get(&Key::Diagnostic(id)))
    }
    pub fn response(&self, id: TestamentId) -> Option<&Response> {
        as_response(self.range.get(&Key::Response(id)))
    }
}

impl Core<NativeState> {
    pub fn native_missing_result(&self, key: NativeResultKey) -> Option<&NativeMissingResult> {
        as_missing(self.state.rows.get(&Key::MissingResult(key)))
    }
    pub fn native_response_record(&self, id: TestamentId) -> Option<&NativeResponseRecord> {
        as_response_record(self.state.rows.get(&Key::Response(id)))
    }
    pub fn native_registrations(&self, id: ClaimId) -> Option<&RegistrationSet> {
        as_registrations(self.state.rows.get(&Key::Claim(id)))
    }
    pub fn native_delivery_result(&self, key: NativeResultKey) -> Option<&NativeDeliveryResult> {
        as_delivery(self.state.rows.get(&Key::DeliveryResult(key)))
    }
    pub fn native_work(&self, id: ArtifactId) -> Option<&NativeWork> {
        as_work(self.state.rows.get(&Key::Work(id)))
    }
    pub fn native_diagnostic(&self, id: ArtifactId) -> Option<&NativeDiagnostic> {
        as_diagnostic(self.state.rows.get(&Key::Diagnostic(id)))
    }
    pub fn native_response(&self, id: TestamentId) -> Option<&Response> {
        as_response(self.state.rows.get(&Key::Response(id)))
    }
}

impl NativeRead {
    pub fn with_missing_result<T>(
        &self,
        key: NativeResultKey,
        now: u64,
        project: impl FnOnce(&NativeMissingResult) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::MissingResult(key);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_missing(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_response_record<T>(
        &self,
        id: TestamentId,
        now: u64,
        project: impl FnOnce(&NativeResponseRecord) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Response(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_response_record(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_registrations<T>(
        &self,
        id: ClaimId,
        now: u64,
        project: impl FnOnce(&RegistrationSet) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Claim(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_registrations(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_delivery_result<T>(
        &self,
        key: NativeResultKey,
        now: u64,
        project: impl FnOnce(&NativeDeliveryResult) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::DeliveryResult(key);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_delivery(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_work<T>(
        &self,
        id: ArtifactId,
        now: u64,
        project: impl FnOnce(&NativeWork) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Work(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_work(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }

    pub fn with_diagnostic<T>(
        &self,
        id: ArtifactId,
        now: u64,
        project: impl FnOnce(&NativeDiagnostic) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Diagnostic(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_diagnostic(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }

    pub fn with_response<T>(
        &self,
        id: TestamentId,
        now: u64,
        project: impl FnOnce(&Response) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Response(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    as_response(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}

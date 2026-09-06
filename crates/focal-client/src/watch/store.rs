use super::*;
use crate::{operation_store::files::Directory, pending::OperationContext};
use std::path::{Path, PathBuf};
const CATALOGUE: &str = "WATCHES.watch-owner";
#[derive(Serialize, Deserialize)]
struct Catalogue {
    schema: u16,
    context: OperationContext,
    entries: Vec<Entry>,
}
#[derive(Serialize, Deserialize)]
struct Entry {
    name: String,
    options: WatchOptions,
    ready: bool,
}
pub struct WatchStore {
    parent: PathBuf,
    context: OperationContext,
}
impl WatchStore {
    pub fn open(parent: impl AsRef<Path>, context: OperationContext) -> Result<Self, WatchError> {
        if context.cluster == [0; 16]
            || context.principal.is_zero()
            || context.ledger.tenant.is_zero()
            || context.ledger.session.is_zero()
        {
            return Err(WatchError::Invalid);
        }
        let this = Self {
            parent: parent.as_ref().into(),
            context,
        };
        let (directory, initialized) = Directory::watch(&this.parent, "WATCHES", true)?;
        if !directory.exists(CATALOGUE)? {
            if initialized {
                return Err(WatchError::Corrupt);
            }
            directory.write(
                CATALOGUE,
                MAGIC,
                &encode(&Catalogue {
                    schema: 1,
                    context,
                    entries: vec![],
                })?,
                false,
            )?;
        }
        let state = this.read(&directory)?;
        if !initialized {
            if !state.entries.is_empty() {
                return Err(WatchError::Corrupt);
            }
            directory.finish_coordinator()?;
        }
        Ok(this)
    }
    pub fn context(&self) -> OperationContext {
        self.context
    }
    pub fn names(&self) -> Result<Vec<String>, WatchError> {
        let (directory, _) = Directory::watch(&self.parent, "WATCHES", false)?;
        Ok(self
            .read(&directory)?
            .entries
            .into_iter()
            .map(|e| e.name)
            .collect())
    }
    pub fn create(&self, name: &str, options: WatchOptions) -> Result<WatchJournal, WatchError> {
        if !valid_name(name) || name.len() > 64 {
            return Err(WatchError::Invalid);
        }
        options.validate()?;
        let (directory, _) = Directory::watch(&self.parent, "WATCHES", false)?;
        let mut state = self.read(&directory)?;
        let index = match state.entries.iter().position(|e| e.name == name) {
            Some(index) => {
                if state
                    .entries
                    .get(index)
                    .is_none_or(|e| e.options != options)
                {
                    return Err(WatchError::Conflict);
                }
                index
            }
            None => {
                if state.entries.len() >= MAX_WATCHES {
                    return Err(WatchError::Capacity);
                }
                state
                    .entries
                    .try_reserve(1)
                    .map_err(|_| WatchError::Capacity)?;
                let index = state.entries.len();
                state.entries.push(Entry {
                    name: name.into(),
                    options: options.clone(),
                    ready: false,
                });
                directory.write(CATALOGUE, MAGIC, &encode(&state)?, true)?;
                index
            }
        };
        let entry = state.entries.get(index).ok_or(WatchError::Corrupt)?;
        let journal = WatchJournal::open(&self.parent, name, self.context, options, !entry.ready)?;
        if !entry.ready {
            state
                .entries
                .get_mut(index)
                .ok_or(WatchError::Corrupt)?
                .ready = true;
            directory.write(CATALOGUE, MAGIC, &encode(&state)?, true)?;
        }
        Ok(journal)
    }
    pub fn resume(&self, name: &str) -> Result<WatchJournal, WatchError> {
        let (directory, _) = Directory::watch(&self.parent, "WATCHES", false)?;
        let state = self.read(&directory)?;
        let entry = state
            .entries
            .into_iter()
            .find(|entry| entry.name == name)
            .ok_or(WatchError::Missing)?;
        drop(directory);
        self.create(name, entry.options)
    }
    fn read(&self, directory: &Directory) -> Result<Catalogue, WatchError> {
        let bytes = directory.read(CATALOGUE, MAGIC, RECORD_BYTES)?;
        let (state, rest): (Catalogue, &[u8]) =
            postcard::take_from_bytes(&bytes).map_err(|_| WatchError::Corrupt)?;
        if !rest.is_empty() || encode(&state)? != bytes {
            return Err(WatchError::Corrupt);
        }
        if state.schema != 1 || state.context != self.context || state.entries.len() > MAX_WATCHES {
            return Err(WatchError::Corrupt);
        }
        for (index, entry) in state.entries.iter().enumerate() {
            entry.options.validate()?;
            if !valid_name(&entry.name)
                || entry.name.len() > 64
                || state
                    .entries
                    .iter()
                    .take(index)
                    .any(|old| old.name == entry.name)
            {
                return Err(WatchError::Corrupt);
            }
        }
        Ok(state)
    }
}

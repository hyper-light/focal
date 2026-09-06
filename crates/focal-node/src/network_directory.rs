//! The single initial partition has one tracked physical owner. Its authority
//! comes from the root journal; its namespace never contains a global session
//! table. General partition scheduling will reuse grouped metadata ownership.
use super::*;
use crate::directory_bootstrap::{DirectoryBootstrapError, FirstDirectoryPlan};
use tokio::sync::watch;

#[derive(Clone)]
pub struct DirectoryHandle {
    plan: FirstDirectoryPlan,
    state: watch::Receiver<Option<ControlHost>>,
}
impl DirectoryHandle {
    pub fn namespace(&self) -> LedgerId {
        self.plan.namespace()
    }
    pub fn group(&self) -> [u8; 16] {
        self.plan.group().0
    }
    /// Trusted local lookup. Clone one physical handle before any await; never
    /// retain the watch borrow while a request or disk recovery is running.
    pub fn host(&self) -> Option<ControlHost> {
        self.state.borrow().clone()
    }
}

pub(super) struct DirectoryStartup {
    plan: FirstDirectoryPlan,
    state: watch::Sender<Option<ControlHost>>,
    wal: SharedWal,
    budget: MemoryBudget,
}
impl DirectoryStartup {
    pub(super) fn new(
        assigned: bool,
        cluster: [u8; 16],
        founder: u64,
        wal: SharedWal,
        budget: MemoryBudget,
    ) -> Result<(DirectoryHandle, Option<Self>), DirectoryBootstrapError> {
        let plan = FirstDirectoryPlan::derive(cluster, founder)?;
        let (state, receiver) = watch::channel(None);
        let handle = DirectoryHandle {
            plan,
            state: receiver,
        };
        let startup = assigned.then_some(Self {
            plan,
            state,
            wal,
            budget,
        });
        Ok((handle, startup))
    }

    pub(super) async fn run(
        self,
        root: &ControlHost,
        pool: &PeerConnectionPool,
        owners: &OwnerGate,
    ) -> Result<(), ServiceError> {
        let permit = loop {
            if root.progress().stopped {
                return Err(ServiceError::Owner("root owner ended"));
            }
            match root.prepare_directory(self.plan).await {
                Ok(permit) => break permit,
                Err(
                    DirectoryBootstrapError::Unauthorized
                    | DirectoryBootstrapError::Unavailable
                    | DirectoryBootstrapError::NotReady
                    | DirectoryBootstrapError::Capacity,
                ) => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        // Spawning, registering, and publishing are synchronous in this poll.
        // Cancellation cannot strand an untracked disk owner between awaits.
        let installed_index = permit.root_index();
        let expires_at = permit.expires_at();
        let (host, owner, output) = ControlHost::spawn_directory(permit, self.wal, self.budget)?;
        owners.register(PhysicalOwner::Control(owner))?;
        self.state.send_replace(Some(host.clone()));
        let replication = crate::replication::drive_directory_replication(output, pool, 16);
        let refresh = refresh_authority(self.plan, root, &host, installed_index, expires_at);
        tokio::pin!(replication, refresh);
        tokio::select! {
            result = &mut replication => result.map_err(ServiceError::from).and(Err(ServiceError::Owner("directory egress ended"))),
            result = &mut refresh => result,
        }
    }
}


async fn refresh_authority(
    plan: FirstDirectoryPlan,
    root: &ControlHost,
    directory: &ControlHost,
    mut installed_index: u64,
    mut expires_at: i64,
) -> Result<(), ServiceError> {
    loop {
        if root.progress().stopped || directory.progress().stopped {
            return Err(ServiceError::Owner("directory authority owner ended"));
        }
        if root.progress().applied_index > installed_index || unix_time()? >= expires_at {
            // A new permit comes from a fresh root quorum. Existing valid data
            // routes remain usable while metadata changes await that quorum.
            let outcome = match root.prepare_directory(plan).await {
                Ok(permit) => {
                    let valid_until = permit.expires_at();
                    directory.refresh_directory(permit).await.map(|receipt| (receipt, valid_until))
                }
                Err(error) => Err(error),
            };
            match outcome {
                Ok((receipt, valid_until)) => {
                    if receipt.root != root.progress().identity || receipt.root_index < installed_index {
                        return Err(DirectoryBootstrapError::Inconsistent.into());
                    }
                    installed_index = receipt.root_index;
                    expires_at = valid_until;
                }
                Err(DirectoryBootstrapError::Unauthorized | DirectoryBootstrapError::Unavailable
                    | DirectoryBootstrapError::NotReady | DirectoryBootstrapError::Capacity) => {}
                Err(error) => return Err(error.into()),
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

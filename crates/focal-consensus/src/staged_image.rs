//! A group's image written off its owner (Ongaro's thesis §5.1: the state machine goes on while
//! its snapshot is written). The owner hands an [`ImageStager`] to a thread of its own, which
//! writes the image whole and durable under a name of its own; the owner checks the point is
//! still one it may checkpoint at and adopts the image with a rename, no write and no sync; the
//! thread makes the directory durable; the owner then takes the image as the group's durable
//! one and compacts the log behind it (hyper-durable's I8). The owner never waits on the disk.
use std::path::PathBuf;

use focal_platform::fs::FileMedium;

use crate::group_files::{self, GroupSeal, ImagePoint};
use crate::{CheckpointPoint, ConsensusError};

/// Where one group's image is staged: owned, so a thread may hold it while the owner goes on.
#[derive(Debug, Clone)]
pub struct ImageStager {
    dir: PathBuf,
    seal: GroupSeal,
    bound: usize,
}

/// An image written whole and durable as the group's next, at its point, waiting for the owner
/// to adopt it ([`crate::DurableNode::adopt_staged_checkpoint`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedImage {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) bytes: u64,
}

impl StagedImage {
    /// The image's length.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl ImageStager {
    pub(crate) fn new(dir: PathBuf, seal: GroupSeal, bound: usize) -> Self {
        Self { dir, seal, bound }
    }

    /// Writes `image`, of `point`, whole and durable as the group's next image. Its name is not
    /// the group's image until the owner adopts it.
    pub fn stage(
        &self,
        point: &CheckpointPoint,
        image: &[u8],
    ) -> Result<StagedImage, ConsensusError> {
        let stored = ImagePoint {
            index: point.index,
            term: point.term,
            configuration: point.configuration.clone(),
        };
        group_files::stage_image(
            &mut FileMedium,
            &self.dir,
            &stored,
            image,
            self.bound,
            &self.seal,
        )
        .map_err(crate::shell_node::file_error)?;
        Ok(StagedImage {
            index: point.index,
            term: point.term,
            bytes: u64::try_from(image.len()).map_err(|_| ConsensusError::Capacity)?,
        })
    }

    /// Makes the group's directory durable, and with it the name of an image the owner adopted.
    pub fn settle(&self) -> Result<(), ConsensusError> {
        group_files::settle_images(&mut FileMedium, &self.dir)
            .map_err(crate::shell_node::file_error)
    }
}

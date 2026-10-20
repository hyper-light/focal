//! The inbox reads a local host holds until they have their page (`Operation::NativeWait`), by
//! recipient: served again when a delivery to their recipient applies (`Session::
//! take_native_delivered`, after each of the owner's polls), and answered with their page as it
//! stands once their wait ends. A commit looks only at the inboxes it delivered to. Each read holds
//! its request's charge while it waits, so the host's budget bounds them as it bounds every
//! request (`LocalHost::handle` reserves it before the request is queued).
use crate::embedded::EmbeddedNode;
use focal_memory::Allocation;
use focal_model::{ParticipantId, RouteEpoch};
use focal_wire::*;
use hyper_rt::sync::OneshotSender;
use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

/// An inbox read whose page was empty when it came: what it waits on and until when.
pub(crate) struct InboxWait {
    pub(crate) recipient: ParticipantId,
    pub(crate) until: Instant,
    pub(crate) peer: AuthenticatedPeer,
    pub(crate) list: NativeListRequest,
    pub(crate) route: RouteEpoch,
}

/// A parked inbox read: its wait, its reply's header, its caller's answer and its charge.
struct Parked {
    header: ResponseEnvelope,
    wait: InboxWait,
    response: OneshotSender<OwnedResponse>,
    charge: Allocation,
}

impl Parked {
    fn finish(self, result: Response, limits: &WireLimits) {
        let mut header = self.header;
        header.result = result;
        let _ = self.response.send(crate::host::finish_response(
            crate::host::bounded(header, limits),
            self.charge,
        ));
    }
}

#[derive(Default)]
pub(crate) struct Inboxes {
    by_recipient: BTreeMap<ParticipantId, VecDeque<Parked>>,
    /// No parked read's wait ends earlier: a bound from below, lowered as each parks and made
    /// exact only by the sweep of the waits that have ended, so a delivery's wake looks at the
    /// inboxes it reached and no others.
    earliest: Option<Instant>,
}

impl Inboxes {
    pub(crate) fn park(
        &mut self,
        header: ResponseEnvelope,
        wait: InboxWait,
        response: OneshotSender<OwnedResponse>,
        charge: Allocation,
    ) {
        self.earliest = Some(self.earliest.map_or(wait.until, |at| at.min(wait.until)));
        self.by_recipient
            .entry(wait.recipient)
            .or_default()
            .push_back(Parked {
                header,
                wait,
                response,
                charge,
            });
    }
    /// When the owner must look again even with nothing to take: the earliest wait's end.
    pub(crate) fn earliest(&self) -> Option<Instant> {
        self.earliest
    }
    /// Serves again the reads waiting on the inboxes the session delivered to since it was last
    /// asked, and answers those whose wait has ended by `now`.
    pub(crate) fn wake(
        &mut self,
        node: &mut EmbeddedNode,
        views: &mut crate::reads::ReadViews,
        limits: &WireLimits,
        now: Instant,
    ) {
        if self.by_recipient.is_empty() {
            // Nothing waits: the notes are let go, and the next are exact again.
            let _ = node.session.take_native_delivered();
            return;
        }
        let delivered = node.session.take_native_delivered();
        let due = self.earliest.is_some_and(|at| at <= now);
        if delivered.is_empty() && !due {
            return;
        }
        if delivered.all || due {
            let inboxes = std::mem::take(&mut self.by_recipient);
            for (recipient, waits) in inboxes {
                let reached = delivered.reaches(recipient);
                self.serve(recipient, waits, (node, views, limits), reached, now);
            }
            self.earliest = self
                .by_recipient
                .values()
                .flatten()
                .map(|parked| parked.wait.until)
                .min();
        } else {
            for &recipient in &delivered.recipients {
                if let Some(waits) = self.by_recipient.remove(&recipient) {
                    self.serve(recipient, waits, (node, views, limits), true, now);
                }
            }
        }
    }
    /// Answers each of `recipient`'s reads whose page now holds something (looked at only where
    /// a delivery `reached` it), whose wait has ended by `now`, or whose caller left; parks the
    /// rest again.
    fn serve(
        &mut self,
        recipient: ParticipantId,
        waits: VecDeque<Parked>,
        (node, views, limits): (&mut EmbeddedNode, &mut crate::reads::ReadViews, &WireLimits),
        reached: bool,
        now: Instant,
    ) {
        let mut left = VecDeque::new();
        for mut parked in waits {
            if parked.response.is_closed() {
                continue;
            }
            let ended = parked.wait.until <= now;
            if !reached && !ended {
                left.push_back(parked);
                continue;
            }
            let served = crate::native_lists::local(
                &mut node.session,
                views,
                &parked.wait.peer,
                &parked.wait.list,
                parked.wait.route,
                limits,
                &mut parked.charge,
            );
            match served {
                Ok(page) if ended || !page.objects.is_empty() || page.next.is_some() => {
                    parked.finish(Response::NativeListed(page), limits);
                }
                Ok(_) => left.push_back(parked),
                Err(error) => parked.finish(Response::Error(error), limits),
            }
        }
        if !left.is_empty() {
            self.by_recipient.insert(recipient, left);
        }
    }
    /// Every parked read answered: the host is stopping, and a read is asked again of the node
    /// once it serves.
    pub(crate) fn close(&mut self, limits: &WireLimits) {
        for (_, waits) in std::mem::take(&mut self.by_recipient) {
            for parked in waits {
                parked.finish(Response::Error(AccessError::Unavailable), limits);
            }
        }
        self.earliest = None;
    }
}

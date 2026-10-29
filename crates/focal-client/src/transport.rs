use focal_wire::*;
use std::{future::Future, pin::Pin};

pub type TransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ResponseEnvelope, WireError>> + Send + 'a>>;
pub trait ClientTransport: Send + Sync {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a>;
    /// The routes this transport was given to its cluster, its first route first (where a
    /// request goes when the client knows no other): each asked in turn once the route a request
    /// was sent on gave no answer, as a Kafka client's bootstrap servers or a NATS client's
    /// server list are asked for the cluster when the server it held is gone. Empty for a
    /// transport that reaches one node whatever the route.
    fn routes(&self) -> &[RouteHint] {
        &[]
    }
}

pub struct EmbeddedTransport<H> {
    peer: AuthenticatedPeer,
    handler: H,
    limits: WireLimits,
}
impl<H: RequestHandler> EmbeddedTransport<H> {
    pub fn new(peer: AuthenticatedPeer, handler: H, limits: WireLimits) -> Result<Self, WireError> {
        limits.validate()?;
        Ok(Self {
            peer,
            handler,
            limits,
        })
    }
}
impl<H: RequestHandler> ClientTransport for EmbeddedTransport<H> {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            if request.protocol == MANAGED_PROTOCOL_VERSION
                && !self.handler.supports_managed_requests()
            {
                return Err(WireError::Access(AccessError::UnsupportedProtocol));
            }
            if request.protocol == PEER_PROTOCOL_VERSION
                && (!self.handler.supports_managed_requests()
                    || !self.handler.supports_participant_requests())
            {
                return Err(WireError::Access(AccessError::UnsupportedProtocol));
            }
            if request.protocol == NATIVE_PROTOCOL_VERSION
                && (!self.handler.supports_managed_requests()
                    || !self.handler.supports_participant_requests()
                    || !self.handler.supports_native_requests())
            {
                return Err(WireError::Access(AccessError::UnsupportedProtocol));
            }
            // Exercise the identical versioned codec, limits, and authorization seam.
            let bytes = encode_payload(request, self.limits.max_frame_bytes)?;
            let request = decode_payload(&bytes)?;
            let response = dispatch(&self.handler, self.peer.clone(), request, &self.limits).await;
            decode_payload(&encode_payload(&response, self.limits.max_frame_bytes)?)
        })
    }
}

/// The node's local socket: `AF_UNIX` on every OS, its peer named by the kernel.
pub struct LocalTransport {
    remote: LocalRemote,
}
impl LocalTransport {
    pub fn connect(
        path: impl AsRef<std::path::Path>,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        Ok(Self {
            remote: LocalRemote::new(path, limits)?,
        })
    }
}
impl ClientTransport for LocalTransport {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        // The local socket reaches this node alone: a redirect is followed by
        // resending at the hinted epoch here, and the node answers again with
        // the same hint when its log leads elsewhere, which the client then
        // reports as the route change it is.
        let _ = route;
        Box::pin(async move { self.remote.request(request).await })
    }
}

/// The participant's QUIC transport: one connection per route, dialed once
/// however many calls arrive cold (`RouteConnections`, the audit's F60).
pub struct QuicTransport {
    routes: RouteConnections,
    /// The routes it was given, its first route first (`ClientTransport::routes`): one at
    /// least, and no more than it holds connections.
    given: Vec<RouteHint>,
}
impl QuicTransport {
    pub fn new(
        connector: QuicConnector,
        initial: RouteHint,
        max_connections: usize,
    ) -> Result<Self, WireError> {
        Self::with_routes(connector, vec![initial], max_connections)
    }
    /// A transport given several routes to its cluster, the first its first route: each a node
    /// a request is sent to in turn once the route it was sent on gave no answer. One at least,
    /// and no more than the connections it holds.
    pub fn with_routes(
        connector: QuicConnector,
        given: Vec<RouteHint>,
        max_connections: usize,
    ) -> Result<Self, WireError> {
        if given.is_empty()
            || given.len() > max_connections
            || given
                .iter()
                .any(|route| route.endpoint.len() > 512 || route.server_name.len() > 253)
        {
            return Err(WireError::Limit);
        }
        Ok(Self {
            routes: RouteConnections::new(connector, max_connections)?,
            given,
        })
    }
    /// Physical dials this transport has started.
    pub async fn dials(&self) -> u64 {
        self.routes.dials().await
    }
}
impl ClientTransport for QuicTransport {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let route = route
                .or_else(|| self.given.first())
                .ok_or(WireError::Limit)?;
            // A dial the listener refused at admission is its answer
            // (`Access(Capacity)`), resent as the refusal it is; the
            // connection is forgotten only when it is what failed.
            self.routes
                .request(&route.endpoint, &route.server_name, request)
                .await
        })
    }
    fn routes(&self) -> &[RouteHint] {
        &self.given
    }
}

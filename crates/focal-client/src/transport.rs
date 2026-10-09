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

pub struct UnixTransport {
    remote: UnixRemote,
}
impl UnixTransport {
    pub fn connect(
        path: impl AsRef<std::path::Path>,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        Ok(Self {
            remote: UnixRemote::new(path, limits)?,
        })
    }
}
impl ClientTransport for UnixTransport {
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
    initial: RouteHint,
}
impl QuicTransport {
    pub fn new(
        connector: QuicConnector,
        initial: RouteHint,
        max_connections: usize,
    ) -> Result<Self, WireError> {
        if initial.endpoint.len() > 512 || initial.server_name.len() > 253 {
            return Err(WireError::Limit);
        }
        Ok(Self {
            routes: RouteConnections::new(connector, max_connections)?,
            initial,
        })
    }
    /// Physical dials this transport has started.
    pub fn dials(&self) -> u64 {
        self.routes.dials()
    }
}
impl ClientTransport for QuicTransport {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let route = route.unwrap_or(&self.initial);
            // A listener that refused the connection at admission answered
            // before anything was sent (`Access(Capacity)`): the client
            // resends it as the refusal it is. Any other dial failure is
            // reported as it was.
            let connected = self
                .routes
                .connect(&route.endpoint, &route.server_name)
                .await?;
            let result = connected.remote.request(request).await;
            if result.is_err() {
                // The connection that failed leaves the cache — unless a
                // newer one took its place meanwhile.
                self.routes
                    .forget(&route.endpoint, &route.server_name, connected.generation);
            }
            result
        })
    }
}

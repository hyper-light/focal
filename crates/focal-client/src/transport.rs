use focal_wire::*;
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Mutex};

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

pub struct QuicTransport {
    connector: QuicConnector,
    initial: RouteHint,
    connections: Mutex<Connections>,
    max_connections: usize,
}

/// A cached QUIC connection with the logical clock value of its last use, so the
/// cache evicts the least-recently-used connection under pressure rather than
/// the lexicographically smallest key.
struct Cached {
    remote: QuicRemote,
    used: u64,
}

#[derive(Default)]
struct Connections {
    entries: BTreeMap<(String, String), Cached>,
    clock: u64,
}
impl QuicTransport {
    pub fn new(
        connector: QuicConnector,
        initial: RouteHint,
        max_connections: usize,
    ) -> Result<Self, WireError> {
        if max_connections == 0
            || max_connections > 1024
            || initial.endpoint.len() > 512
            || initial.server_name.len() > 253
        {
            return Err(WireError::Limit);
        }
        Ok(Self {
            connector,
            initial,
            connections: Mutex::new(Connections::default()),
            max_connections,
        })
    }
    async fn connection(&self, route: &RouteHint) -> Result<QuicRemote, WireError> {
        tokio::runtime::Handle::try_current().map_err(|_| WireError::Connection)?;
        let key = (route.endpoint.clone(), route.server_name.clone());
        {
            let mut guard = self.connections.lock().map_err(|_| WireError::Connection)?;
            guard.clock = guard.clock.saturating_add(1);
            let clock = guard.clock;
            if let Some(entry) = guard.entries.get_mut(&key) {
                entry.used = clock;
                return Ok(entry.remote.clone());
            }
        }
        let mut addresses = tokio::net::lookup_host(route.endpoint.as_str())
            .await
            .map_err(|_| WireError::Connection)?;
        let address = addresses.next().ok_or(WireError::Connection)?;
        let connection = self.connector.connect(address, &route.server_name).await?;
        let mut guard = self.connections.lock().map_err(|_| WireError::Connection)?;
        guard.clock = guard.clock.saturating_add(1);
        let clock = guard.clock;
        // Evict the least-recently-used connection only when admitting a new key
        // at capacity; a re-inserted key just refreshes its clock.
        if !guard.entries.contains_key(&key)
            && guard.entries.len() >= self.max_connections
            && let Some(evict) = guard
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.used)
                .map(|(evict_key, _)| evict_key.clone())
        {
            guard.entries.remove(&evict);
        }
        guard.entries.insert(
            key,
            Cached {
                remote: connection.clone(),
                used: clock,
            },
        );
        Ok(connection)
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
            let connection = self.connection(route).await?;
            let result = connection.request(request).await;
            if result.is_err() {
                self.connections
                    .lock()
                    .map_err(|_| WireError::Connection)?
                    .entries
                    .remove(&(route.endpoint.clone(), route.server_name.clone()));
            }
            result
        })
    }
}

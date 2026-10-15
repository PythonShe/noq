//! The sender of a connection: one socket, or several behind a router.

use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tracing::trace;
use udp::Transmit;

use crate::runtime::{SocketId, SocketRouter, UdpSender};

/// Sends on the socket a transmit is routed to
///
/// A single-socket endpoint has one sender, under [`SocketId::SINGLE`]. A multi-socket
/// endpoint creates a sender per socket through its router, the first time a transmit is
/// routed to that socket. A connection therefore only holds senders for sockets it uses.
pub(crate) struct ConnectionSender {
    /// `None` for a single-socket endpoint
    router: Option<Arc<dyn SocketRouter>>,
    senders: Vec<(SocketId, Pin<Box<dyn UdpSender>>)>,
}

impl ConnectionSender {
    /// Creates the sender of a single-socket endpoint
    pub(crate) fn single(sender: Pin<Box<dyn UdpSender>>) -> Self {
        Self {
            router: None,
            senders: vec![(SocketId::SINGLE, sender)],
        }
    }

    /// Creates a sender that routes through `router`
    pub(crate) fn routed(router: Arc<dyn SocketRouter>) -> Self {
        Self {
            router: Some(router),
            senders: Vec::new(),
        }
    }

    /// Returns the socket for a destination and the segment limit of its sender
    ///
    /// Creates the socket's sender on first use. Returns `None` if the router drops the
    /// destination.
    pub(crate) fn route(
        &mut self,
        destination: SocketAddr,
        src_ip: Option<IpAddr>,
    ) -> Option<(SocketId, NonZeroUsize)> {
        let socket = match &self.router {
            None => SocketId::SINGLE,
            Some(router) => router.route(destination, src_ip)?,
        };
        let sender = match self.senders.iter().find(|(id, _)| *id == socket) {
            Some((_, sender)) => sender,
            None => {
                let router = self
                    .router
                    .as_ref()
                    .expect("a single sender is always present");
                self.senders.push((socket, router.create_sender(socket)));
                &self.senders.last().expect("just pushed").1
            }
        };
        Some((socket, sender.max_transmit_segments()))
    }

    /// Sends `transmit` on the sender of `socket`, which [`route`] returned
    ///
    /// [`route`]: Self::route
    pub(crate) fn poll_send_on(
        &mut self,
        socket: SocketId,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        match self.senders.iter_mut().find(|(id, _)| *id == socket) {
            Some((_, sender)) => sender.as_mut().poll_send(transmit, cx),
            // Only possible for a transmit blocked before a rebind replaced the senders.
            None => {
                trace!(destination = %transmit.destination, "socket gone, dropping transmit");
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl fmt::Debug for ConnectionSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionSender")
            .field("router", &self.router)
            .field("sockets", &self.senders.len())
            .finish()
    }
}

/// Routes, then sends, for senders used outside a connection
impl UdpSender for ConnectionSender {
    fn poll_send(
        self: Pin<&mut Self>,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some((socket, _)) = this.route(transmit.destination, transmit.src_ip) else {
            // Like a failed sendmsg, this counts as loss and is left to QUIC to recover from.
            trace!(destination = %transmit.destination, "no socket for destination, dropping transmit");
            return Poll::Ready(Ok(()));
        };
        this.poll_send_on(socket, transmit, cx)
    }
}

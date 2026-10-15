//! Range routing for [`Runtime::wrap_udp_sockets`].
//!
//! [`Runtime::wrap_udp_sockets`]: super::Runtime::wrap_udp_sockets

use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use ipnet::IpNet;
use udp::Transmit;

use super::{AsyncUdpSocket, SocketId, SocketRouter, UdpSender};

/// A socket to receive on, and a second handle to the same socket for creating senders
pub(super) type SocketPair = (Box<dyn AsyncUdpSocket>, Box<dyn AsyncUdpSocket>);

/// Wraps `sockets` with `wrap` and builds a [`RangeRouter`] over them
///
/// Returns the sockets to receive on, in the order given.
#[allow(clippy::type_complexity)] // The return type of `Runtime::wrap_udp_sockets`.
pub(super) fn range_router(
    sockets: Vec<(std::net::UdpSocket, Vec<IpNet>)>,
    mut wrap: impl FnMut(std::net::UdpSocket) -> io::Result<SocketPair>,
) -> io::Result<(Vec<Box<dyn AsyncUdpSocket>>, Arc<dyn SocketRouter>)> {
    let mut receivers = Vec::with_capacity(sockets.len());
    let mut senders = Vec::with_capacity(sockets.len());
    let mut routes = Vec::new();
    for (socket, ranges) in sockets {
        let (receiver, sender) = wrap(socket)?;
        let id = SocketId::new();
        routes.extend(ranges.into_iter().map(|range| (range, id)));
        senders.push((id, sender));
        receivers.push(receiver);
    }
    // Stable sort keeps the socket order for equal prefix lengths.
    routes.sort_by_key(|(range, _)| std::cmp::Reverse(range.prefix_len()));
    let router = RangeRouter {
        routes: routes.into(),
        senders: senders.into(),
    };
    Ok((receivers, Arc::new(router)))
}

/// Routes by destination range: the longest matching range wins
#[derive(Debug)]
struct RangeRouter {
    /// Sorted by prefix length, longest first
    routes: Box<[(IpNet, SocketId)]>,
    /// Handles to the sockets, only used to create senders
    senders: Box<[(SocketId, Box<dyn AsyncUdpSocket>)]>,
}

impl SocketRouter for RangeRouter {
    fn route(&self, destination: SocketAddr, _src_ip: Option<IpAddr>) -> Option<SocketId> {
        self.routes
            .iter()
            .find(|(range, _)| route_contains(range, destination.ip()))
            .map(|(_, id)| *id)
    }

    fn create_sender(&self, socket: SocketId) -> Pin<Box<dyn UdpSender>> {
        let (_, socket) = self
            .senders
            .iter()
            .find(|(id, _)| *id == socket)
            .expect("only ids returned by route are passed to create_sender");
        let sender = socket.create_sender();
        match socket.local_addr() {
            Ok(addr) if addr.is_ipv4() => Box::pin(Ipv4Sender(sender)),
            _ => sender,
        }
    }
}

/// Returns whether `ip` is in `range`, treating IPv4-mapped addresses as IPv4
///
/// An IPv4-mapped address matches IPv4 ranges, and IPv6 ranges only if they lie within
/// `::ffff:0:0/96`. A plain `::/0` socket is therefore never asked to send IPv4, which only a
/// dual-stack socket can do, and such a socket lists that range.
fn route_contains(range: &IpNet, ip: IpAddr) -> bool {
    match (range, ip) {
        (IpNet::V4(range), IpAddr::V6(ip)) => {
            ip.to_ipv4_mapped().is_some_and(|ip| range.contains(&ip))
        }
        (IpNet::V6(range), IpAddr::V6(ip)) if ip.to_ipv4_mapped().is_some() => {
            range.prefix_len() >= 96 && range.contains(&ip)
        }
        (range, ip) => range.contains(&ip),
    }
}

/// Sends on an IPv4 socket, mapping IPv4-mapped addresses back to IPv4
struct Ipv4Sender(Pin<Box<dyn UdpSender>>);

impl fmt::Debug for Ipv4Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ipv4Sender").field(&self.0).finish()
    }
}

impl UdpSender for Ipv4Sender {
    fn poll_send(
        self: Pin<&mut Self>,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let unmap = |ip: IpAddr| ip.to_canonical();
        let transmit = Transmit {
            destination: SocketAddr::new(
                unmap(transmit.destination.ip()),
                transmit.destination.port(),
            ),
            src_ip: transmit.src_ip.map(unmap),
            ..transmit.clone()
        };
        self.get_mut().0.as_mut().poll_send(&transmit, cx)
    }

    fn max_transmit_segments(&self) -> NonZeroUsize {
        self.0.max_transmit_segments()
    }
}

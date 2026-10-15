use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use std::{io, sync::Arc, task::ready};

use async_io::Async;
use async_io::Timer;

use super::AsyncTimer;
use super::{
    AsyncUdpSocket, Runtime, SocketRouter, UdpSender, UdpSenderHelper, UdpSenderHelperSocket,
    router,
};

/// A noq runtime for smol
#[derive(Debug)]
pub struct SmolRuntime;

impl Runtime for SmolRuntime {
    fn new_timer(&self, t: Instant) -> Pin<Box<dyn AsyncTimer>> {
        Box::pin(Timer::at(t))
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        ::smol::spawn(future).detach();
    }

    fn wrap_udp_socket(&self, sock: std::net::UdpSocket) -> io::Result<Box<dyn AsyncUdpSocket>> {
        Ok(Box::new(UdpSocket::new(sock)?))
    }

    #[allow(clippy::type_complexity)] // Same signature as the trait method.
    fn wrap_udp_sockets(
        &self,
        sockets: Vec<(std::net::UdpSocket, Vec<ipnet::IpNet>)>,
    ) -> io::Result<(Vec<Box<dyn AsyncUdpSocket>>, Arc<dyn SocketRouter>)> {
        // Clones share the socket, so senders need no second file descriptor.
        router::range_router(sockets, |socket| {
            let socket = UdpSocket::new(socket)?;
            Ok((Box::new(socket.clone()), Box::new(socket)))
        })
    }
}

impl AsyncTimer for Timer {
    fn reset(mut self: Pin<&mut Self>, t: Instant) {
        self.set_at(t)
    }

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Future::poll(self, cx).map(|_| ())
    }
}

#[derive(Debug, Clone)]
struct UdpSocket {
    io: Arc<Async<std::net::UdpSocket>>,
    inner: Arc<udp::UdpSocketState>,
}

impl UdpSocket {
    fn new(sock: std::net::UdpSocket) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(udp::UdpSocketState::new((&sock).into())?),
            io: Arc::new(Async::new_nonblocking(sock)?),
        })
    }
}

impl UdpSenderHelperSocket for UdpSocket {
    fn max_transmit_segments(&self) -> NonZeroUsize {
        self.inner.max_gso_segments()
    }

    fn try_send(&self, transmit: &udp::Transmit<'_>) -> io::Result<()> {
        self.inner.try_send((&self.io).into(), transmit)?;

        Ok(())
    }
}

impl AsyncUdpSocket for UdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(UdpSenderHelper::new(self.clone(), |socket: &Self| {
            let socket = socket.clone();
            async move { socket.io.writable().await }
        }))
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.io.poll_readable(cx))?;
            match self.inner.recv((&self.io).into(), bufs, meta) {
                Ok(res) => return Poll::Ready(Ok(res)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.io.as_ref().as_ref().local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }

    fn max_receive_segments(&self) -> NonZeroUsize {
        self.inner.gro_segments()
    }
}

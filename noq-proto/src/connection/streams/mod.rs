use std::{
    collections::hash_map,
    io,
    ops::{Deref, DerefMut},
};

use bytes::Bytes;
use rustc_hash::FxHashMap;
use thiserror::Error;
use tracing::trace;

use super::spaces::Retransmits;
use crate::{
    Dir, PathId, StreamId, VarInt,
    connection::streams::state::{StreamRecv, get_or_insert_recv, get_or_insert_send},
    frame,
};

mod recv;
use recv::Recv;
pub use recv::{Chunks, ReadError, ReadableError};

mod send;
pub(crate) use send::{ByteSlice, BytesArray, Written};
use send::{BytesSource, Send, SendState};
pub use send::{FinishError, WriteError};

mod state;

#[allow(unreachable_pub)] // fuzzing only
pub use state::StreamsState;

/// Access to streams
pub struct Streams<'a> {
    pub(super) state: &'a mut StreamsState,
    pub(super) conn_state: &'a super::State,
}

#[allow(clippy::needless_lifetimes)] // Needed for cfg(fuzzing)
impl<'a> Streams<'a> {
    #[cfg(fuzzing)]
    pub fn new(state: &'a mut StreamsState, conn_state: &'a super::State) -> Self {
        Self { state, conn_state }
    }

    /// Open a single stream if possible
    ///
    /// Returns `None` if the streams in the given direction are currently exhausted.
    pub fn open(&mut self, dir: Dir) -> Option<StreamId> {
        if self.conn_state.is_closed() {
            return None;
        }

        if self.state.next[dir as usize] >= self.state.max[dir as usize] {
            self.state.streams_blocked[dir as usize] = true;
            return None;
        }

        self.state.next[dir as usize] += 1;
        let id = StreamId::new(self.state.side, dir, self.state.next[dir as usize] - 1);
        self.state.insert_local(id);
        self.state.send_streams += 1;
        Some(id)
    }

    /// Accept a remotely initiated stream of a certain directionality, if possible
    ///
    /// Returns `None` if there are no new incoming streams for this connection.
    /// Has no impact on the data flow-control or stream concurrency limits.
    pub fn accept(&mut self, dir: Dir) -> Option<StreamId> {
        if self.state.next_remote[dir as usize] == self.state.next_reported_remote[dir as usize] {
            return None;
        }

        let x = self.state.next_reported_remote[dir as usize];
        self.state.next_reported_remote[dir as usize] = x + 1;
        if dir == Dir::Bi {
            self.state.send_streams += 1;
        }

        Some(StreamId::new(!self.state.side, dir, x))
    }

    #[cfg(fuzzing)]
    pub fn state(&mut self) -> &mut StreamsState {
        self.state
    }

    /// The number of streams that may have unacknowledged data.
    pub fn send_streams(&self) -> usize {
        self.state.send_streams
    }

    /// The number of remotely initiated open streams of a certain directionality.
    ///
    /// Includes remotely initiated streams, which have not been accepted via
    /// [`accept`](Self::accept). These streams count against the respective concurrency limit
    /// reported by
    /// [`Connection::max_concurrent_streams`](super::Connection::max_concurrent_streams).
    pub fn remote_open_streams(&self, dir: Dir) -> u64 {
        // total opened - total closed = total opened - (total permitted - total permitted unclosed)
        self.state.next_remote[dir as usize]
            - (self.state.max_remote[dir as usize]
                - self.state.allocated_remote_count[dir as usize])
    }
}

/// Access to streams
pub struct RecvStream<'a> {
    pub(super) id: StreamId,
    pub(super) state: &'a mut StreamsState,
    pub(super) pending: &'a mut Retransmits,
}

impl RecvStream<'_> {
    /// Whether this stream is still in ordered read mode.
    ///
    /// A stream switches permanently to unordered mode when [`Self::read`] is called with
    /// `ordered` set to `false`.
    pub fn is_ordered(&self) -> Result<bool, ClosedStream> {
        let Some(stream) = self.state.recv.get(&self.id) else {
            return Err(ClosedStream { _private: () });
        };
        let Some(stream) = stream.as_ref().and_then(StreamRecv::as_open_recv) else {
            return Ok(true);
        };
        if stream.stopped {
            return Err(ClosedStream { _private: () });
        }
        Ok(stream.assembler.is_ordered())
    }

    /// Read from the given recv stream
    ///
    /// `max_length` limits the maximum size of the returned `Bytes` value; passing `usize::MAX`
    /// will yield the best performance. `ordered` will make sure the returned chunk's offset will
    /// have an offset exactly equal to the previously returned offset plus the previously returned
    /// bytes' length.
    ///
    /// Yields `Ok(None)` if the stream was finished. Otherwise, yields a segment of data and its
    /// offset in the stream. If `ordered` is `false`, segments may be received in any order, and
    /// the `Chunk`'s `offset` field can be used to determine ordering in the caller.
    ///
    /// While most applications will prefer to consume stream data in order, unordered reads can
    /// improve performance when packet loss occurs and data cannot be retransmitted before the flow
    /// control window is filled. On any given stream, you can switch from ordered to unordered
    /// reads, but ordered reads on streams that have seen previous unordered reads will return
    /// `ReadError::IllegalOrderedRead`.
    pub fn read(&mut self, ordered: bool) -> Result<Chunks<'_>, ReadableError> {
        Chunks::new(self.id, ordered, self.state, self.pending)
    }

    /// Stop accepting data on the given receive stream
    ///
    /// Discards unread data and notifies the peer to stop transmitting. Once stopped, further
    /// attempts to operate on a stream will yield `ClosedStream` errors.
    pub fn stop(&mut self, error_code: VarInt) -> Result<(), ClosedStream> {
        let mut entry = match self.state.recv.entry(self.id) {
            hash_map::Entry::Occupied(s) => s,
            hash_map::Entry::Vacant(_) => return Err(ClosedStream { _private: () }),
        };
        let stream = get_or_insert_recv(self.state.stream_receive_window)(entry.get_mut());

        let (read_credits, stop_sending) = stream.stop()?;
        if stop_sending.should_transmit() {
            self.pending.stop_sending.push(frame::StopSending {
                id: self.id,
                error_code,
            });
        }

        // We need to keep stopped streams around until they're finished or reset so we can update
        // connection-level flow control to account for discarded data. Otherwise, we can discard
        // state immediately.
        if !stream.final_offset_unknown() {
            let recv = entry.remove().expect("must have recv when stopping");
            self.state.stream_recv_freed(self.id, recv);
        }

        if self.state.add_read_credits(read_credits).should_transmit() {
            self.pending.max_data = true;
        }

        Ok(())
    }

    /// Returns the number of bytes read from this stream.
    ///
    /// This is the offset of the next byte to be read, i.e. the length of the contiguous
    /// prefix of the stream consumed by the application.
    pub fn bytes_read(&self) -> Result<u64, ClosedStream> {
        let recv = self
            .state
            .recv
            .get(&self.id)
            .and_then(|s| s.as_ref())
            .and_then(|s| s.as_open_recv())
            .ok_or(ClosedStream { _private: () })?;
        Ok(recv.assembler.bytes_read())
    }

    /// Check whether this stream has been reset by the peer, returning the reset error code if so
    ///
    /// After returning `Ok(Some(_))` once, stream state will be discarded and all future calls will
    /// return `Err(ClosedStream)`.
    pub fn received_reset(&mut self) -> Result<Option<VarInt>, ClosedStream> {
        let hash_map::Entry::Occupied(entry) = self.state.recv.entry(self.id) else {
            return Err(ClosedStream { _private: () });
        };
        let Some(s) = entry.get().as_ref().and_then(|s| s.as_open_recv()) else {
            return Ok(None);
        };
        if s.stopped {
            return Err(ClosedStream { _private: () });
        }
        let Some(code) = s.reset_code() else {
            return Ok(None);
        };

        // Clean up state after application observes the reset, since there's no reason for the
        // application to attempt to read or stop the stream once it knows it's reset
        let (_, recv) = entry.remove_entry();
        self.state
            .stream_recv_freed(self.id, recv.expect("must have recv on reset"));
        self.state.queue_max_stream_id(self.pending);

        Ok(Some(code))
    }
}

/// Access to streams
pub struct SendStream<'a> {
    pub(super) id: StreamId,
    pub(super) state: &'a mut StreamsState,
    pub(super) pending: &'a mut Retransmits,
    pub(super) conn_state: &'a super::State,
}

#[allow(clippy::needless_lifetimes)] // Needed for cfg(fuzzing)
impl<'a> SendStream<'a> {
    #[cfg(fuzzing)]
    pub fn new(
        id: StreamId,
        state: &'a mut StreamsState,
        pending: &'a mut Retransmits,
        conn_state: &'a super::State,
    ) -> Self {
        Self {
            id,
            state,
            pending,
            conn_state,
        }
    }

    /// Send data on the given stream
    ///
    /// Returns the number of bytes successfully written.
    pub fn write(&mut self, data: &[u8]) -> Result<usize, WriteError> {
        Ok(self.write_source(&mut ByteSlice::from_slice(data))?.bytes)
    }

    /// Send data on the given stream
    ///
    /// Returns the number of bytes written and advances the provided `Bytes`
    /// slice, removing all completely written chunks.
    ///
    /// Note that this method might also write a partial chunk. In this case
    /// the chunk will be advanced and contain only non-written data after the call.
    pub fn write_chunks(&mut self, data: &mut &mut [Bytes]) -> Result<usize, WriteError> {
        let written = self.write_source(&mut BytesArray::from_chunks(data))?;
        *data = &mut std::mem::take(data)[written.chunks..];
        Ok(written.bytes)
    }

    fn write_source<'b, B: BytesSource<'b>>(
        &mut self,
        source: &'b mut B,
    ) -> Result<Written, WriteError> {
        if self.conn_state.is_closed() {
            trace!(%self.id, "write blocked; connection draining");
            return Err(WriteError::Blocked);
        }

        let limit = self.state.write_limit();

        let max_send_data = self.state.max_send_data(self.id);

        let stream = self
            .state
            .send
            .get_mut(&self.id)
            .map(get_or_insert_send(max_send_data))
            .ok_or(WriteError::ClosedStream)?;

        if limit == 0 {
            trace!(
                stream = %self.id, max_data = self.state.max_data, data_sent = self.state.data_sent,
                "write blocked by connection-level flow control or send window"
            );
            if !stream.connection_blocked {
                stream.connection_blocked = true;
                self.state.connection_blocked.push(self.id);
            }
            return Err(WriteError::Blocked);
        }

        let was_pending = stream.is_pending();
        let written = stream.write(source, limit)?;
        self.state.data_sent += written.bytes as u64;
        self.state.unacked_data += written.bytes as u64;
        trace!(stream = %self.id, "wrote {} bytes", written.bytes);
        if !was_pending {
            self.state
                .pending
                .push_pending(self.id, stream.priority, stream.affinity);
        }
        Ok(written)
    }

    /// Check if this stream was stopped, get the reason if it was
    pub fn stopped(&self) -> Result<Option<VarInt>, ClosedStream> {
        match self.state.send.get(&self.id).as_ref() {
            Some(Some(s)) => Ok(s.stop_reason),
            Some(None) => Ok(None),
            None => Err(ClosedStream { _private: () }),
        }
    }

    /// Finish a send stream, signalling that no more data will be sent.
    ///
    /// If this fails, no [`StreamEvent::Finished`] will be generated.
    ///
    /// [`StreamEvent::Finished`]: crate::StreamEvent::Finished
    pub fn finish(&mut self) -> Result<(), FinishError> {
        let max_send_data = self.state.max_send_data(self.id);
        let stream = self
            .state
            .send
            .get_mut(&self.id)
            .map(get_or_insert_send(max_send_data))
            .ok_or(FinishError::ClosedStream)?;

        let was_pending = stream.is_pending();
        stream.finish()?;
        if !was_pending {
            self.state
                .pending
                .push_pending(self.id, stream.priority, stream.affinity);
        }

        Ok(())
    }

    /// Abandon transmitting data on a stream
    ///
    /// # Panics
    /// - when applied to a receive stream
    pub fn reset(&mut self, error_code: VarInt) -> Result<(), ClosedStream> {
        let max_send_data = self.state.max_send_data(self.id);
        let stream = self
            .state
            .send
            .get_mut(&self.id)
            .map(get_or_insert_send(max_send_data))
            .ok_or(ClosedStream { _private: () })?;

        if matches!(stream.state, SendState::ResetSent) {
            // Redundant reset call
            return Err(ClosedStream { _private: () });
        }

        // Restore the portion of the send window consumed by the data that we aren't about to
        // send. We leave flow control alone because the peer's responsible for issuing additional
        // credit based on the final offset communicated in the RESET_STREAM frame we send.
        self.state.unacked_data -= stream.pending.unacked();
        stream.reset();
        self.state.pending.remove(self.id, stream.affinity);
        self.pending.reset_stream.push((self.id, error_code));

        // Don't reopen an already-closed stream we haven't forgotten yet
        Ok(())
    }

    /// Set the priority of a stream
    ///
    /// # Panics
    /// - when applied to a receive stream
    pub fn set_priority(&mut self, priority: i32) -> Result<(), ClosedStream> {
        let max_send_data = self.state.max_send_data(self.id);
        let stream = self
            .state
            .send
            .get_mut(&self.id)
            .map(get_or_insert_send(max_send_data))
            .ok_or(ClosedStream { _private: () })?;

        stream.priority = priority;
        Ok(())
    }

    /// Sets the path affinity of a stream.
    ///
    /// Applies to subsequently packetized STREAM frames, including retransmissions and FINs.
    /// Does not affect stream control frames or packets already built for transmission.
    /// Unblocking rejoins the back of the stream's priority group.
    ///
    /// # Errors
    ///
    /// Returns [`ClosedStream`] if the stream no longer exists.
    ///
    /// # Panics
    ///
    /// Panics when applied to a receive-only stream.
    pub fn set_affinity(&mut self, affinity: StreamAffinity) -> Result<(), ClosedStream> {
        let max_send_data = self.state.max_send_data(self.id);
        let stream = self
            .state
            .send
            .get_mut(&self.id)
            .map(get_or_insert_send(max_send_data))
            .ok_or(ClosedStream { _private: () })?;

        if stream.affinity == affinity {
            return Ok(());
        }
        match (stream.affinity, affinity) {
            (StreamAffinity::Block, _) => {
                if !stream.is_reset() && stream.is_pending() {
                    self.state
                        .pending
                        .push_pending(self.id, stream.priority, affinity);
                }
            }
            (_, StreamAffinity::Block) => self.state.pending.remove(self.id, stream.affinity),
            (StreamAffinity::Any, StreamAffinity::Any) => {}
            (StreamAffinity::Any, StreamAffinity::Sticky(new)) => {
                self.state.pending.set_affinity(self.id, None, Some(new));
            }
            (StreamAffinity::Sticky(old), StreamAffinity::Any) => {
                self.state.pending.set_affinity(self.id, Some(old), None);
            }
            (StreamAffinity::Sticky(old), StreamAffinity::Sticky(new)) => {
                self.state
                    .pending
                    .set_affinity(self.id, Some(old), Some(new));
            }
        }
        stream.affinity = affinity;
        Ok(())
    }

    /// Get the priority of a stream
    ///
    /// # Panics
    /// - when applied to a receive stream
    pub fn priority(&self) -> Result<i32, ClosedStream> {
        let stream = self
            .state
            .send
            .get(&self.id)
            .ok_or(ClosedStream { _private: () })?;

        Ok(stream.as_ref().map(|s| s.priority).unwrap_or_default())
    }
}

/// Restricts which paths may carry a stream's STREAM frames.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum StreamAffinity {
    /// Uses the connection's available/backup scheduling policy.
    #[default]
    Any,
    /// Uses only this path, overriding available/backup preference but not transport limits.
    Sticky(PathId),
    /// Prevents transmission of STREAM frames for this stream.
    Block,
}

/// Pending streams partitioned by affinity, with shared priority and fairness ordering.
struct PendingStreamsQueue {
    any: IndexedStreamsQueue,
    sticky_by_path: FxHashMap<PathId, IndexedStreamsQueue>,
    /// A monotonically decreasing counter, used to implement round-robin scheduling for streams of
    /// the same priority. Underflowing is not a practical concern, as it is initialized to
    /// u64::MAX and only decremented by 1 in `push_pending`
    recency: u64,
}

impl PendingStreamsQueue {
    fn new() -> Self {
        Self {
            any: IndexedStreamsQueue::default(),
            sticky_by_path: FxHashMap::default(),
            recency: u64::MAX,
        }
    }

    /// Push a pending stream ID with the given priority, queued after any already-queued streams
    /// for the priority
    fn push_pending(&mut self, id: StreamId, priority: i32, affinity: StreamAffinity) {
        let sticky = match affinity {
            StreamAffinity::Any => None,
            StreamAffinity::Sticky(path) => Some(path),
            StreamAffinity::Block => return,
        };
        // As the recency counter is monotonically decreasing, we know that using its value to sort
        // this stream will queue it after all other queued streams of the same priority.
        // This is enough to implement round-robin scheduling for streams that are still pending
        // even after being handled, as in that case they are removed from the queue,
        // handled, and then immediately reinserted.
        self.recency -= 1;
        let recency = self.recency;
        self.queue_mut(sticky).push(PendingStream {
            priority,
            recency,
            id,
        });
    }

    fn pop(&mut self, path: PathId, status_allows_data: bool) -> Option<PendingStream> {
        let any = status_allows_data.then(|| self.any.peek()).flatten();
        let sticky = self.sticky_by_path.get(&path).and_then(|q| q.peek());
        if any > sticky {
            self.queue_mut(None).pop()
        } else {
            sticky?; // skip looking at the sticky map, if none exists.
            self.queue_mut(Some(path)).pop()
        }
    }

    fn set_affinity(&mut self, id: StreamId, old: Option<PathId>, new: Option<PathId>) {
        let moved = self.queue_mut(old).remove(id);
        if let Some(pending) = moved {
            self.queue_mut(new).push(pending);
        }
    }

    fn queue_mut(&mut self, sticky: Option<PathId>) -> PendingQueueMut<'_> {
        match sticky {
            None => PendingQueueMut::Any(&mut self.any),
            Some(path) => {
                let entry = match self.sticky_by_path.entry(path) {
                    hash_map::Entry::Occupied(entry) => entry,
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert_entry(IndexedStreamsQueue::default())
                    }
                };
                PendingQueueMut::Sticky(Some(entry))
            }
        }
    }

    fn remove(&mut self, id: StreamId, affinity: StreamAffinity) {
        let sticky = match affinity {
            StreamAffinity::Any => None,
            StreamAffinity::Sticky(path) => Some(path),
            StreamAffinity::Block => return,
        };
        // Abandoned-path queues might never be selected again.
        self.queue_mut(sticky).remove(id);
    }

    fn clear(&mut self) {
        self.any.clear();
        self.sticky_by_path.clear();
    }

    fn iter(&self, path: PathId, status_allows_data: bool) -> impl Iterator<Item = &PendingStream> {
        status_allows_data
            .then_some(&self.any)
            .into_iter()
            .flat_map(|q| q.iter())
            .chain(
                self.sticky_by_path
                    .get(&path)
                    .into_iter()
                    .flat_map(|q| q.iter()),
            )
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.any.len() + self.sticky_by_path.values().map(|q| q.len()).sum::<usize>()
    }
}

enum PendingQueueMut<'a> {
    Any(&'a mut IndexedStreamsQueue),
    // The entry is present until Drop takes ownership to remove an empty bucket.
    Sticky(Option<hash_map::OccupiedEntry<'a, PathId, IndexedStreamsQueue>>),
}

impl Deref for PendingQueueMut<'_> {
    type Target = IndexedStreamsQueue;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Any(queue) => queue,
            Self::Sticky(entry) => entry.as_ref().expect("entry is present until drop").get(),
        }
    }
}

impl DerefMut for PendingQueueMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Any(queue) => queue,
            Self::Sticky(entry) => entry
                .as_mut()
                .expect("entry is present until drop")
                .get_mut(),
        }
    }
}

impl Drop for PendingQueueMut<'_> {
    fn drop(&mut self) {
        if let Self::Sticky(entry) = self {
            let entry = entry.take().expect("entry is present until drop");
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }
}

#[derive(Default)]
struct IndexedStreamsQueue {
    queue: Vec<PendingStream>,
    locations: FxHashMap<StreamId, usize>,
}

impl IndexedStreamsQueue {
    fn push(&mut self, pending: PendingStream) {
        // Late ACKs can clear pending work without removing the queue entry. A subsequent
        // enqueue replaces its metadata rather than creating a second entry for the stream.
        if let Some(&index) = self.locations.get(&pending.id) {
            self.queue[index] = pending;
            self.repair(index);
        } else {
            let index = self.queue.len();
            self.locations.insert(pending.id, index);
            self.queue.push(pending);
            self.sift_up(index);
        }
    }

    fn peek(&self) -> Option<&PendingStream> {
        self.queue.first()
    }

    fn pop(&mut self) -> Option<PendingStream> {
        self.remove(self.peek()?.id)
    }

    fn remove(&mut self, id: StreamId) -> Option<PendingStream> {
        let index = self.locations.remove(&id)?;
        let removed = self.queue.swap_remove(index);
        if index < self.queue.len() {
            self.locations.insert(self.queue[index].id, index);
            self.repair(index);
        }
        Some(removed)
    }

    fn clear(&mut self) {
        self.queue.clear();
        self.locations.clear();
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = &PendingStream> {
        self.queue.iter()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.queue.len()
    }

    fn repair(&mut self, index: usize) {
        if index > 0 && self.queue[index] > self.queue[(index - 1) / 2] {
            self.sift_up(index);
        } else {
            self.sift_down(index);
        }
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.queue[parent] >= self.queue[index] {
                break;
            }
            self.swap(parent, index);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let mut child = 2 * index + 1;
            if child >= self.queue.len() {
                break;
            }
            let right = child + 1;
            if right < self.queue.len() && self.queue[right] > self.queue[child] {
                child = right;
            }
            if self.queue[index] >= self.queue[child] {
                break;
            }
            self.swap(index, child);
            index = child;
        }
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.queue.swap(a, b);
        self.locations.insert(self.queue[a].id, a);
        self.locations.insert(self.queue[b].id, b);
    }
}

/// The [`StreamId`] of a stream with pending data queued, ordered by its priority and recency
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingStream {
    /// The priority of the stream
    // Note that this field should be kept above the `recency` field, in order for the `Ord` derive
    // to be correct (See https://doc.rust-lang.org/stable/std/cmp/trait.Ord.html#derivable)
    priority: i32,
    /// A tie-breaker for streams of the same priority, used to improve fairness by implementing
    /// round-robin scheduling: Larger values are prioritized, so it is initialised to
    /// `u64::MAX`, and when a stream writes data, we know that it currently has the highest
    /// recency value, so it is deprioritized by setting its recency to 1 less than the
    /// previous lowest recency value, such that all other streams of this priority will get
    /// processed once before we get back round to this one
    recency: u64,
    /// The ID of the stream
    // The way this type is used ensures that every instance has a unique `recency` value, so this
    // field should be kept below the `priority` and `recency` fields, so that it does not
    // interfere with the behaviour of the `Ord` derive
    id: StreamId,
}

/// Application events about streams
#[derive(Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// One or more new streams has been opened and might be readable
    Opened {
        /// Directionality for which streams have been opened
        dir: Dir,
    },
    /// A currently open stream likely has data or errors waiting to be read
    Readable {
        /// Which stream is now readable
        id: StreamId,
    },
    /// A formerly write-blocked stream might be ready for a write or have been stopped
    ///
    /// Only generated for streams that are currently open.
    Writable {
        /// Which stream is now writable
        id: StreamId,
    },
    /// A finished stream has been fully acknowledged or stopped
    Finished {
        /// Which stream has been finished
        id: StreamId,
    },
    /// The peer asked us to stop sending on an outgoing stream
    Stopped {
        /// Which stream has been stopped
        id: StreamId,
        /// Error code supplied by the peer
        error_code: VarInt,
    },
    /// At least one new stream of a certain directionality may be opened
    Available {
        /// Directionality for which streams are newly available
        dir: Dir,
    },
}

/// Indicates whether a frame needs to be transmitted
///
/// This type wraps around bool and uses the `#[must_use]` attribute in order
/// to prevent accidental loss of the frame transmission requirement.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[must_use = "A frame might need to be enqueued"]
pub struct ShouldTransmit(bool);

impl ShouldTransmit {
    /// Returns whether a frame should be transmitted
    pub fn should_transmit(self) -> bool {
        self.0
    }
}

/// Error indicating that a stream has not been opened or has already been finished or reset
#[derive(Debug, Default, Error, Clone, PartialEq, Eq)]
#[error("closed stream")]
pub struct ClosedStream {
    _private: (),
}

impl From<ClosedStream> for io::Error {
    fn from(x: ClosedStream) -> Self {
        Self::new(io::ErrorKind::NotConnected, x)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum StreamHalf {
    Send,
    Recv,
}

/// A helper trait to unify Bytes, `Vec<u8>` and `&[u8]` as sources of bytes
pub(super) trait BytesOrSlice<'a>: AsRef<[u8]> + 'a {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }
    fn into_bytes(self) -> Bytes;
}

impl BytesOrSlice<'_> for Bytes {
    fn into_bytes(self) -> Bytes {
        self
    }
}

impl<'a> BytesOrSlice<'a> for &'a [u8] {
    fn into_bytes(self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod proptests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use test_strategy::{Arbitrary, proptest};

    use super::{IndexedStreamsQueue, PendingStream};
    use crate::{Dir, Side, StreamId};

    #[proptest]
    fn indexed_queue_matches_reference(
        #[strategy(proptest::collection::vec(any::<Op>(), 0..100))] ops: Vec<Op>,
    ) {
        let mut queue = IndexedStreamsQueue::default();
        let mut reference = BTreeMap::<StreamId, PendingStream>::new();
        for op in ops {
            match op {
                Op::Push {
                    id,
                    priority,
                    recency,
                } => {
                    let pending = PendingStream {
                        id: stream_id(id),
                        priority,
                        recency,
                    };
                    reference.insert(pending.id, pending.clone());
                    queue.push(pending);
                }
                Op::Remove(id) => assert_eq!(
                    queue.remove(stream_id(id)),
                    reference.remove(&stream_id(id))
                ),
                Op::Pop => {
                    let expected = reference.values().max().cloned();
                    if let Some(pending) = &expected {
                        reference.remove(&pending.id);
                    }
                    assert_eq!(queue.pop(), expected);
                }
                Op::Clear => {
                    queue.clear();
                    reference.clear();
                }
            }
            check(&queue, &reference);
        }
        drain(&mut queue, &mut reference);
    }

    #[proptest]
    fn indexed_queue_remove_positions(
        #[strategy(proptest::collection::vec((priority(), recency()), 1..33))] entries: Vec<(
            i32,
            u64,
        )>,
        position: usize,
    ) {
        // Exercise the root, an internal node, the last slot, and an arbitrary slot.
        for index in [
            0,
            entries.len() / 4,
            entries.len() - 1,
            position % entries.len(),
        ] {
            let mut queue = IndexedStreamsQueue::default();
            let mut reference = BTreeMap::new();
            for (id, &(priority, recency)) in entries.iter().enumerate() {
                let pending = PendingStream {
                    id: stream_id(id as u64),
                    priority,
                    recency,
                };
                reference.insert(pending.id, pending.clone());
                queue.push(pending);
                check(&queue, &reference);
            }
            let id = queue.queue[index].id;
            assert_eq!(queue.remove(id), reference.remove(&id));
            assert!(queue.remove(id).is_none());
            check(&queue, &reference);
            drain(&mut queue, &mut reference);
        }
    }

    #[derive(Debug, Arbitrary)]
    enum Op {
        #[weight(20)]
        Push {
            #[strategy(0u64..16)]
            id: u64,
            #[strategy(priority())]
            priority: i32,
            #[strategy(recency())]
            recency: u64,
        },
        #[weight(8)]
        Remove(#[strategy(0u64..20)] u64),
        #[weight(8)]
        Pop,
        #[weight(1)]
        Clear,
    }

    fn priority() -> impl Strategy<Value = i32> {
        prop_oneof![3 => -2i32..=2, 1 => any::<i32>(), 1 => Just(i32::MIN), 1 => Just(i32::MAX)]
    }

    fn recency() -> impl Strategy<Value = u64> {
        prop_oneof![3 => 0u64..=4, 1 => any::<u64>(), 1 => Just(u64::MAX)]
    }

    fn stream_id(index: u64) -> StreamId {
        StreamId::new(Side::Client, Dir::Uni, index)
    }

    fn check(queue: &IndexedStreamsQueue, reference: &BTreeMap<StreamId, PendingStream>) {
        assert_eq!(queue.peek(), reference.values().max());
        assert_eq!(queue.is_empty(), reference.is_empty());
        assert_eq!(queue.len(), reference.len());
        assert_eq!(queue.locations.len(), reference.len());
        for (index, pending) in queue.queue.iter().enumerate() {
            assert_eq!(queue.locations.get(&pending.id), Some(&index));
            assert_eq!(reference.get(&pending.id), Some(pending));
            if index > 0 {
                assert!(queue.queue[(index - 1) / 2] >= *pending);
            }
        }
    }

    fn drain(queue: &mut IndexedStreamsQueue, reference: &mut BTreeMap<StreamId, PendingStream>) {
        while let Some(expected) = reference.values().max().cloned() {
            reference.remove(&expected.id);
            assert_eq!(queue.pop(), Some(expected));
            check(queue, reference);
        }
        assert!(queue.pop().is_none());
        check(queue, reference);
    }
}

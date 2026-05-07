#![cfg_attr(not(test), no_std)]

use core::ptr::NonNull;

use core::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

/// A lockless ringbuffer implementation intended for logging.
pub struct Ringbuffer<M: Metadata> {
    data_capacity: usize,
    data_buffer: NonNull<u8>,
    logical_start: AtomicUsize,
    logical_end: AtomicUsize,

    descriptor_count: usize,
    descriptor_buffer: NonNull<Descriptor>,
    metadata_buffer: NonNull<M::Atomic>,
    descriptor_id_start: AtomicDescriptorId,
    descriptor_id_end: AtomicDescriptorId,
}

impl<M: Metadata> Ringbuffer<M> {
    pub fn new(
        data_capacity: usize,
        data_buffer: NonNull<u8>,
        descriptor_count: usize,
        descriptor_buffer: NonNull<Descriptor>,
        metadata_buffer: NonNull<M::Atomic>,
    ) -> Self {
        todo!()
    }

    /// Reserves at least `size` bytes in this [`Ringbuffer`].
    ///
    /// The returned buffer may be greater than or equal to `size`.
    pub fn reserve(&self, size: usize) -> Option<ReservedMessage<'_, M>> {
        todo!()
    }

    /// Non-blocking read of the requested [`Message`] or (if the requested [`Message`] is gone, the
    /// next available [`Message`].
    pub fn read<'buffer>(
        &self,
        sequence: u64,
        buffer: &'buffer mut [u8],
    ) -> Option<Message<'buffer, M>> {
        todo!()
    }

    /// Returns the oldest sequence number still in this [`Ringbuffer`].
    ///
    /// This sequence number may correspond to log messages with only their metadata remaining.
    pub fn first_sequence(&self) -> u64 {
        todo!()
    }

    /// Returns the oldest sequence number that refers to a fully valid log message in this
    /// [`Ringbuffer`].
    pub fn first_valid_sequence(&self) -> u64 {
        todo!()
    }

    /// Returns the sequence number by which the next log message in this [`Ringbuffer`] will be
    /// referred.
    pub fn next_sequence(&self) -> u64 {
        todo!()
    }

    /// Returns the sequence number by which the next log message slot to be reserved in this
    /// [`Ringbuffer`] will be referred.
    pub fn next_reserve_sequence(&self) -> u64 {
        todo!()
    }
}

/// A reserved segment of the [`Ringbuffer`], along with
pub struct ReservedMessage<'buffer, M: Metadata> {
    ringbuffer: &'buffer Ringbuffer<M>,
    sequence: u64,
    buffer: &'buffer mut [u8],
}

impl<'buffer, M: Metadata> ReservedMessage<'buffer, M> {
    /// Returns the sequence of the [`Message`].
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns an immutable view of the [`Message`] buffer.
    pub fn buffer(&self) -> &[u8] {
        self.buffer
    }

    /// Returns a mutable view of the [`Message`] buffer.
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        self.buffer
    }

    /// Commits the [`Message`] and associated [`Metadata`] into the associated [`Ringbuffer`].
    pub fn commit(self, metadata: M) -> CommittedMessage<'buffer, M> {
        todo!()
    }

    /// Finalizes the [`Message`], which closes the editing window.
    pub fn finalize(self, metadata: M) {
        todo!()
    }
}

/// A committed [`Message`].
///
/// The associated [`Message`] might be able to be reopened for editing.
pub struct CommittedMessage<'buffer, M: Metadata> {
    ringbuffer: &'buffer Ringbuffer<M>,
    sequence: u64,
    metadata: M,
    buffer: &'buffer [AtomicU8],
}

impl<'buffer, M: Metadata> CommittedMessage<'buffer, M> {
    /// Attempts to reopen the [`CommittedMessage`] for additional manipulation.
    pub fn reopen(self, additional_size: usize) -> Result<(ReservedMessage<'buffer, M>, M), Self> {
        todo!()
    }

    /// Finalizes the [`Message`], which closes the editing window.
    pub fn finalize(self) {
        todo!()
    }
}

/// A log message, with its associated sequence number, metadata, and the extracted part of the
/// data buffer.
pub struct Message<'buffer, M: Metadata> {
    /// The sequence number of the [`Message`].
    pub sequence: u64,
    /// Associated [`Metadata`] of the [`Message`].
    pub metadata: M,
    /// Extracted portion of the data buffer associated with this [`Message`].
    ///
    /// If `None`, then the message was missing its associated data.
    pub buffer: Option<&'buffer mut [u8]>,
}

/// Internal data associated with a particular message.
pub struct Descriptor {
    state_id: AtomicDescriptorStateId,
    logical_start: AtomicUsize,
    logical_end: AtomicUsize,
    sequence: AtomicU64,
}

#[repr(transparent)]
struct AtomicDescriptorStateId(AtomicUsize);

#[repr(transparent)]
struct AtomicDescriptorId(AtomicUsize);

/// Data associated with a particular [`Message`].
///
/// This trait provides a methods to atomically read and write the [`Metadata`] for storage in a
/// [`Ringbuffer`].
pub trait Metadata: Copy {
    /// The atomic storage method.
    type Atomic;

    /// Loads the [`Metadata`] instance from the provided [`Metadata::Atomic`] instance with the
    /// provided [`Ordering`].
    fn load(atomic: &Self::Atomic, order: Ordering) -> Self;

    /// Stores the provided [`Metadata`] instance into the provided [`Metadata::Atomic`] instance
    /// with the provided [`Ordering`].
    fn store(atomic: &Self::Atomic, val: Self, order: Ordering);
}

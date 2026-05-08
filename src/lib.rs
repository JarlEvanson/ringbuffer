#![cfg_attr(not(test), no_std)]

use core::{hint::cold_path, ptr::NonNull};

use core::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(any(feature = "alloc", test))]
mod allocated;

#[cfg(any(feature = "alloc", test))]
pub use allocated::*;

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
    last_finalized_sequence: AtomicU64,

    empty: AtomicBool,
}

impl<M: Metadata> Ringbuffer<M> {
    pub fn new(
        data_capacity: usize,
        data_buffer: NonNull<u8>,
        descriptor_count: usize,
        descriptor_buffer: NonNull<Descriptor>,
        metadata_buffer: NonNull<M::Atomic>,
    ) -> Self {
        assert!(data_capacity != 0 && data_capacity.is_power_of_two());
        assert!(descriptor_count != 0 && descriptor_count.is_power_of_two());

        let starting_logical_position = usize::MAX.wrapping_sub(data_capacity).wrapping_add(1);
        let starting_descriptor_id =
            DescriptorId::new_truncating(usize::MAX.wrapping_sub(descriptor_count).wrapping_add(1));

        Self::initialize_descriptor_buffer(descriptor_buffer, descriptor_count);

        Self {
            data_capacity,
            data_buffer,
            logical_start: AtomicUsize::new(starting_logical_position),
            logical_end: AtomicUsize::new(starting_logical_position),

            descriptor_count,
            descriptor_buffer,
            metadata_buffer,
            descriptor_id_start: AtomicDescriptorId::new(starting_descriptor_id),
            descriptor_id_end: AtomicDescriptorId::new(starting_descriptor_id),
            last_finalized_sequence: AtomicU64::new(0),

            empty: AtomicBool::new(true),
        }
    }

    const fn initialize_descriptor_buffer(buffer: NonNull<Descriptor>, count: usize) {
        let mut index = 0;
        let mut id = DescriptorId::new_truncating(usize::MAX.wrapping_sub(count).wrapping_add(1));
        let mut sequence = u64::MAX.wrapping_sub(count as u64).wrapping_add(1);
        while index < count {
            let ptr = unsafe { buffer.add(index) };

            let descriptor = Descriptor {
                state_id: AtomicDescriptorStateId::new(DescriptorStateId::new(
                    id,
                    DescriptorState::MissingData,
                )),
                logical_start: AtomicUsize::new(0),
                logical_end: AtomicUsize::new(0),
                sequence: AtomicU64::new(sequence),
            };

            unsafe { ptr.write(descriptor) }

            sequence = sequence.wrapping_add(1);
            id = id.next();
            index += 1;
        }
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
        if self.empty.load(Ordering::Relaxed) {
            cold_path();
            return 0;
        }

        loop {
            let id = self.descriptor_id_end.load(Ordering::Acquire);

            let descriptor = self.descriptor(id);
            let start_state_id = descriptor.state_id.load(Ordering::Acquire);

            let sequence = descriptor.sequence.load(Ordering::Relaxed);

            let end_state_id = descriptor.state_id.load(Ordering::Acquire);
            if start_state_id != end_state_id {
                continue;
            }

            match end_state_id.state() {
                DescriptorState::MissingData | DescriptorState::Finalized => return sequence,
                DescriptorState::Reserved | DescriptorState::Committed => continue,
            }
        }
    }

    /// Returns the oldest sequence number that refers to a fully valid log message in this
    /// [`Ringbuffer`].
    pub fn first_valid_sequence(&self) -> u64 {
        if self.empty.load(Ordering::Relaxed) {
            cold_path();
            return 0;
        }

        todo!()
    }

    /// Returns the sequence number by which the next log message in this [`Ringbuffer`] will be
    /// referred.
    pub fn next_sequence(&self) -> u64 {
        if self.empty.load(Ordering::Relaxed) {
            cold_path();
            return 0;
        }

        let sequence = self.last_finalized_sequence.load(Ordering::Relaxed);
        sequence.wrapping_add(1)
    }

    /// Returns the sequence number by which the next log message slot to be reserved in this
    /// [`Ringbuffer`] will be referred.
    pub fn next_reserve_sequence(&self) -> u64 {
        loop {
            let mut last_finalized = self.last_finalized_sequence.load(Ordering::Acquire);
            let mut descriptor_id_start = self.descriptor_id_start.load(Ordering::Relaxed);

            let descriptor = self.descriptor(descriptor_id_start);
            let last_finalized_id = descriptor.state_id.load(Ordering::Relaxed).id();


        }

        todo!()
    }

    fn descriptor(&self, id: DescriptorId) -> &Descriptor {
        let index = id.to_raw() % self.descriptor_count;

        let ptr = unsafe { self.descriptor_buffer.add(index) };
        unsafe { ptr.as_ref() }
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

impl AtomicDescriptorStateId {
    const fn new(id: DescriptorStateId) -> Self {
        Self(AtomicUsize::new(id.to_raw()))
    }

    fn load(&self, order: Ordering) -> DescriptorStateId {
        DescriptorStateId::from_raw(self.0.load(order))
    }

    fn store(&self, id: DescriptorStateId, order: Ordering) {
        self.0.store(id.to_raw(), order);
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DescriptorStateId(usize);

impl DescriptorStateId {
    const ID_SHIFT: u32 = 0;
    const ID_BASE_MASK: usize = !Self::FLAGS_MASK;
    const ID_MASK: usize = Self::ID_BASE_MASK.strict_shl(Self::ID_SHIFT);

    const FLAGS_SHIFT: u32 = usize::BITS - 2;
    const FLAGS_BASE_MASK: usize = 0b11;
    const FLAGS_MASK: usize = Self::FLAGS_BASE_MASK.strict_shl(Self::FLAGS_SHIFT);

    const fn from_raw(val: usize) -> Self {
        Self(val)
    }

    const fn new(id: DescriptorId, state: DescriptorState) -> Self {
        let raw_state: usize = match state {
            DescriptorState::MissingData => 0b00,
            DescriptorState::Reserved => 0b01,
            DescriptorState::Committed => 0b10,
            DescriptorState::Finalized => 0b11,
        };

        Self(id.to_raw().strict_shl(Self::ID_SHIFT) | (raw_state.strict_shl(Self::FLAGS_SHIFT)))
    }

    const fn id(self) -> DescriptorId {
        DescriptorId::from_raw(self.0.strict_shr(Self::ID_SHIFT) & Self::ID_BASE_MASK)
    }

    const fn state(self) -> DescriptorState {
        match self.0.strict_shr(Self::FLAGS_SHIFT) & Self::FLAGS_BASE_MASK {
            0b00 => DescriptorState::MissingData,
            0b01 => DescriptorState::Reserved,
            0b10 => DescriptorState::Committed,
            0b11 => DescriptorState::Finalized,
            _ => unreachable!(),
        }
    }

    const fn to_raw(self) -> usize {
        self.0
    }
}

#[repr(transparent)]
struct AtomicDescriptorId(AtomicUsize);

impl AtomicDescriptorId {
    const fn new(id: DescriptorId) -> Self {
        Self(AtomicUsize::new(id.to_raw()))
    }

    fn load(&self, order: Ordering) -> DescriptorId {
        DescriptorId::from_raw(self.0.load(order))
    }

    fn store(&self, id: DescriptorId, order: Ordering) {
        self.0.store(id.to_raw(), order);
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DescriptorId(usize);

impl DescriptorId {
    const fn from_raw(val: usize) -> Self {
        Self(val)
    }

    const fn new_truncating(val: usize) -> Self {
        Self(val & DescriptorStateId::ID_MASK)
    }

    const fn next(self) -> Self {
        Self::new_truncating(self.to_raw().wrapping_add(1))
    }

    const fn prev_wrap(self, descriptor_count: usize) -> Self {
        Self::new_truncating(self.to_raw().wrapping_sub(descriptor_count))
    }

    const fn to_raw(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DescriptorState {
    MissingData,
    Reserved,
    Committed,
    Finalized,
}

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

mod test {
    use core::sync::atomic::{AtomicU64, Ordering};

    use crate::{AllocatedRingBuffer, Metadata, Ringbuffer};

    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    struct TestMetadata(u64);

    impl Metadata for TestMetadata {
        type Atomic = AtomicU64;

        fn load(atomic: &Self::Atomic, order: Ordering) -> Self {
            TestMetadata(atomic.load(order))
        }

        fn store(atomic: &Self::Atomic, val: Self, order: Ordering) {
            atomic.store(val.0, order)
        }
    }

    #[test]
    pub fn empty_buffer_sequences() {
        let buffer = AllocatedRingBuffer::<TestMetadata>::new(8, 1);

        assert_eq!(buffer.first_sequence(), 0);
        assert_eq!(buffer.first_valid_sequence(), 0);
        assert_eq!(buffer.next_sequence(), 0);
        assert_eq!(buffer.next_reserve_sequence(), 0);
    }
}

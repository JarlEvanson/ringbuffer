#![cfg_attr(not(test), no_std)]

use core::{hint::cold_path, mem, ptr::NonNull, slice};

use core::sync::atomic::{self, AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(any(feature = "alloc", test))]
mod allocated;

#[cfg(any(feature = "alloc", test))]
pub use allocated::*;

const BASE_SEQUENCE: u64 = 0;
const BASE_ID: usize = BASE_SEQUENCE as usize;

/// A lockless ringbuffer implementation intended for logging.
pub struct Ringbuffer<M: Metadata> {
    data_capacity: usize,
    data_buffer: NonNull<AtomicU8>,
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
        data_buffer: NonNull<AtomicU8>,
        descriptor_count: usize,
        descriptor_buffer: NonNull<Descriptor>,
        metadata_buffer: NonNull<M::Atomic>,
    ) -> Self {
        assert!(data_capacity != 0 && data_capacity.is_power_of_two());
        assert!(descriptor_count != 0 && descriptor_count.is_power_of_two());

        unsafe { core::ptr::write_bytes(data_buffer.as_ptr(), 0, data_capacity) }

        let starting_logical_position = 0usize.wrapping_sub(data_capacity);
        let starting_descriptor_id = DescriptorId::base_id().prev();

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
            last_finalized_sequence: AtomicU64::new(BASE_SEQUENCE),

            empty: AtomicBool::new(true),
        }
    }

    const fn initialize_descriptor_buffer(buffer: NonNull<Descriptor>, count: usize) {
        let mut index = 0;
        let mut id = DescriptorId::base_id().prev_wrap(count);
        let mut sequence = BASE_SEQUENCE.wrapping_sub(count as u64);
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
        let block_size = size
            .checked_add(mem::size_of::<usize>())?
            .checked_next_multiple_of(mem::align_of::<usize>())?;

        if size != 0 && block_size > self.data_capacity / 2 {
            return None;
        }

        let mut current_id = self.descriptor_id_start.load(Ordering::Acquire);
        let mut id;
        let mut previous_wrap_id;
        let (id, previous_wrap_id) = loop {
            id = current_id.next();
            previous_wrap_id = id.prev_wrap(self.descriptor_count);

            if previous_wrap_id == self.descriptor_id_end.load(Ordering::Acquire) {
                todo!()
            }

            let result = self.descriptor_id_start.compare_exchange(
                current_id,
                id,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            match result {
                Ok(_) => break (id, previous_wrap_id),
                Err(updated_current_id) => current_id = updated_current_id,
            }
        };

        let descriptor = self.descriptor(id);

        let current_state_id =
            DescriptorStateId::new(previous_wrap_id, DescriptorState::MissingData);
        let new_state_id = DescriptorStateId::new(id, DescriptorState::Reserved);
        if descriptor
            .state_id
            .compare_exchange(
                current_state_id,
                new_state_id,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_err()
        {
            // TODO: Handle ABA error
            return None;
        }

        let current_seq = descriptor.sequence.load(Ordering::Relaxed);
        descriptor.sequence.store(
            current_seq.wrapping_add(self.descriptor_count as u64),
            Ordering::Relaxed,
        );

        let prev_id = id.prev();
        let prev_descriptor = self.descriptor(prev_id);
        if prev_descriptor
            .state_id
            .compare_exchange(
                DescriptorStateId::new(prev_id, DescriptorState::Committed),
                DescriptorStateId::new(prev_id, DescriptorState::Finalized),
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.update_last_finalized();
        }

        let (logical_start, logical_end, block) = if size == 0 {
            let logical_start = self.logical_start.load(Ordering::Acquire);

            (logical_start, logical_start, None)
        } else {
            let mut logical_start = self.logical_start.load(Ordering::Acquire);

            let logical_end = loop {
                let logical_end = logical_start.wrapping_add(block_size);
                let logical_end = if logical_start >> self.data_capacity.trailing_zeros()
                    == logical_end >> self.data_capacity.trailing_zeros()
                {
                    logical_end
                } else {
                    (logical_end >> self.data_capacity.trailing_zeros()
                        << self.data_capacity.trailing_zeros())
                        + block_size
                };

                let result = self.logical_start.compare_exchange(
                    logical_start,
                    logical_end,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                match result {
                    Ok(_) => break logical_end,
                    Err(new_logical_start) => logical_start = new_logical_start,
                }
            };

            let mut block = self.data(logical_start);
            unsafe {
                block
                    .cast::<AtomicUsize>()
                    .as_ref()
                    .store(id.to_raw(), Ordering::Relaxed)
            };

            if logical_start >> self.data_capacity.trailing_zeros()
                != logical_end >> self.data_capacity.trailing_zeros()
            {
                block = self.data(0);
                unsafe {
                    block
                        .cast::<AtomicUsize>()
                        .as_ref()
                        .store(id.to_raw(), Ordering::Relaxed)
                };
            }

            let block = unsafe { block.add(mem::size_of::<AtomicUsize>()) };
            (logical_start, logical_end, Some(block))
        };

        descriptor
            .logical_start
            .store(logical_start, Ordering::Release);
        descriptor.logical_end.store(logical_end, Ordering::Release);

        let buffer = if let Some(block) = block {
            unsafe {
                slice::from_raw_parts_mut(
                    block.as_ptr(),
                    block_size.strict_sub(mem::size_of::<usize>()),
                )
            }
        } else {
            &mut []
        };

        let message = ReservedMessage {
            ringbuffer: self,
            sequence: descriptor.sequence.load(Ordering::Relaxed),
            buffer,
        };

        Some(message)
    }

    /// Non-blocking read of the requested [`Message`] or (if the requested [`Message`] is gone, the
    /// next available [`Message`].
    pub fn read<'buffer>(
        &self,
        sequence: u64,
        buffer: &'buffer mut [u8],
    ) -> Option<Message<'buffer, M>> {
        let mut sequence = sequence;
        loop {
            let descriptor_state = loop {
                let expected_id = DescriptorId::new_truncating(sequence as usize);

                let descriptor = self.descriptor(expected_id);
                let metadata = self.metadata(expected_id);

                let start_state_id = descriptor.state_id.load(Ordering::Acquire);
                if start_state_id != DescriptorStateId::new(expected_id, DescriptorState::Finalized)
                {
                    break start_state_id;
                }

                let loaded_sequence = descriptor.sequence.load(Ordering::Relaxed);
                let metadata = M::load(metadata, Ordering::Relaxed);

                atomic::fence(Ordering::Acquire);

                let end_state_id = descriptor.state_id.load(Ordering::Relaxed);
                if start_state_id != end_state_id {
                    continue;
                };

                if loaded_sequence != sequence {
                    break start_state_id;
                }

                let message = Message {
                    sequence,
                    metadata,
                    buffer: Some(buffer),
                };

                return Some(message);
            };

            let first_sequence = self.first_sequence();
            if sequence < first_sequence {
                sequence = first_sequence;
            } else if descriptor_state
                == DescriptorStateId::new(
                    DescriptorId::new_truncating(sequence as usize),
                    DescriptorState::MissingData,
                )
            {
                sequence = sequence.wrapping_add(1);
            } else {
                return None;
            }
        }
    }

    /// Returns the oldest sequence number still in this [`Ringbuffer`].
    //
    /// This sequence number may correspond to log messages with only their metadata remaining.
    pub fn first_sequence(&self) -> u64 {
        if self.empty.load(Ordering::Relaxed) {
            cold_path();
            return BASE_SEQUENCE;
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
            return BASE_SEQUENCE;
        }

        let mut sequence = BASE_SEQUENCE;
        loop {
            let expected_id = DescriptorId::new_truncating(sequence as usize);

            let descriptor = self.descriptor(expected_id);

            let start_state_id = descriptor.state_id.load(Ordering::Acquire);

            let loaded_sequence = descriptor.sequence.load(Ordering::Acquire);

            let end_state_id = descriptor.state_id.load(Ordering::Relaxed);
            if start_state_id != end_state_id {
                continue;
            };

            if end_state_id == DescriptorStateId::new(expected_id, DescriptorState::Finalized)
                && loaded_sequence == sequence
            {
                return sequence;
            }

            let first_sequence = self.first_sequence();
            if sequence < first_sequence {
                sequence = first_sequence;
            } else if end_state_id
                == DescriptorStateId::new(expected_id, DescriptorState::MissingData)
            {
                sequence = sequence.wrapping_add(1);
            } else {
                return 0;
            };
        }
    }

    /// Returns the sequence number by which the next log message in this [`Ringbuffer`] will be
    /// referred.
    pub fn next_sequence(&self) -> u64 {
        if self.empty.load(Ordering::Relaxed) {
            cold_path();
            return BASE_SEQUENCE;
        }

        let sequence = self.last_finalized_sequence.load(Ordering::Relaxed);
        sequence.wrapping_add(1)
    }

    /// Returns the sequence number by which the next log message slot to be reserved in this
    /// [`Ringbuffer`] will be referred.
    pub fn next_reserve_sequence(&self) -> u64 {
        loop {
            let last_finalized_sequence = self.last_finalized_sequence.load(Ordering::Acquire);
            let descriptor_id_start = self.descriptor_id_start.load(Ordering::Relaxed);

            let descriptor = self.descriptor(descriptor_id_start);

            let start_state_id = descriptor.state_id.load(Ordering::Acquire);

            let sequence = descriptor.sequence.load(Ordering::Acquire);

            let end_state_id = descriptor.state_id.load(Ordering::Acquire);
            if start_state_id != end_state_id || end_state_id.id() != descriptor_id_start {
                continue;
            }

            if end_state_id.state() != DescriptorState::Finalized
                || sequence != last_finalized_sequence
            {
                if self.empty.load(Ordering::Relaxed) {
                    cold_path();

                    let base_id = DescriptorId::base_id().prev();
                    if descriptor_id_start == base_id {
                        return BASE_SEQUENCE;
                    }

                    return last_finalized_sequence
                        .wrapping_add(descriptor_id_start.difference(base_id) as u64)
                        .wrapping_add(1);
                }
                continue;
            }

            let difference = descriptor_id_start.difference(end_state_id.id());
            return last_finalized_sequence
                .wrapping_add(difference as u64)
                .wrapping_add(1);
        }
    }

    fn update_last_finalized(&self) {
        let sequence = self.last_finalized_sequence.load(Ordering::Acquire);
        loop {
            let mut finalized_sequence = sequence;
            let mut attempt_sequence = finalized_sequence.wrapping_add(1);

            'outer: loop {
                let new_finalized_sequence = loop {
                    let expected_id = DescriptorId::new_truncating(attempt_sequence as usize);

                    let descriptor = self.descriptor(expected_id);

                    let start_state_id = descriptor.state_id.load(Ordering::Acquire);

                    let loaded_sequence = descriptor.sequence.load(Ordering::Acquire);

                    let end_state_id = descriptor.state_id.load(Ordering::Relaxed);
                    if start_state_id != end_state_id {
                        continue;
                    };

                    if end_state_id
                        == DescriptorStateId::new(expected_id, DescriptorState::Finalized)
                        && loaded_sequence == attempt_sequence
                    {
                        break loaded_sequence;
                    }

                    let first_sequence = self.first_sequence();
                    if attempt_sequence < first_sequence {
                        attempt_sequence = first_sequence;
                    } else if end_state_id
                        == DescriptorStateId::new(expected_id, DescriptorState::MissingData)
                    {
                        attempt_sequence = attempt_sequence.wrapping_add(1);
                    } else {
                        break 'outer;
                    };
                };

                finalized_sequence = new_finalized_sequence;
                attempt_sequence = new_finalized_sequence.wrapping_add(1);
            }

            if finalized_sequence == sequence {
                return;
            }

            if self
                .last_finalized_sequence
                .compare_exchange_weak(
                    sequence,
                    finalized_sequence,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    fn descriptor(&self, id: DescriptorId) -> &Descriptor {
        let index = id.to_raw() % self.descriptor_count;

        let ptr = unsafe { self.descriptor_buffer.add(index) };
        unsafe { ptr.as_ref() }
    }

    fn metadata(&self, id: DescriptorId) -> &M::Atomic {
        let index = id.to_raw() % self.descriptor_count;

        let ptr = unsafe { self.metadata_buffer.add(index) };
        unsafe { ptr.as_ref() }
    }

    fn data(&self, logical: usize) -> NonNull<AtomicU8> {
        let index = logical % self.data_capacity;

        unsafe { self.data_buffer.add(index) }
    }
}

/// A reserved segment of the [`Ringbuffer`].
pub struct ReservedMessage<'buffer, M: Metadata> {
    ringbuffer: &'buffer Ringbuffer<M>,
    sequence: u64,
    buffer: &'buffer [AtomicU8],
}

impl<'buffer, M: Metadata> ReservedMessage<'buffer, M> {
    /// Returns the sequence of the [`Message`].
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the [`Message`] buffer.
    pub fn buffer(&self) -> &[AtomicU8] {
        self.buffer
    }

    /// Commits the [`Message`] and associated [`Metadata`] into the associated [`Ringbuffer`].
    pub fn commit(self, metadata: M) -> CommittedMessage<'buffer, M> {
        let ringbuffer = self.ringbuffer;
        let sequence = self.sequence;
        let id = DescriptorId::new_truncating(sequence as usize);

        let descriptor = self.ringbuffer.descriptor(id);
        let metadata_ref = self.ringbuffer.metadata(id);

        drop(self);

        M::store(metadata_ref, metadata, Ordering::Release);

        let current_state_id = DescriptorStateId::new(id, DescriptorState::Reserved);
        let new_state_id = DescriptorStateId::new(id, DescriptorState::Committed);
        let result = descriptor.state_id.compare_exchange(
            current_state_id,
            new_state_id,
            Ordering::AcqRel,
            Ordering::Release,
        );
        if result.is_err() {
            unreachable!()
        }

        let descriptor_id_start = ringbuffer.descriptor_id_start.load(Ordering::Acquire);
        if descriptor_id_start != id {
            let current_state_id = DescriptorStateId::new(id, DescriptorState::Committed);
            let new_state_id = DescriptorStateId::new(id, DescriptorState::Finalized);

            if descriptor
                .state_id
                .compare_exchange(
                    current_state_id,
                    new_state_id,
                    Ordering::AcqRel,
                    Ordering::Release,
                )
                .is_ok()
            {
                ringbuffer.update_last_finalized();
            }
        }

        CommittedMessage {
            ringbuffer,
            sequence,
            metadata,
        }
    }

    /// Finalizes the [`Message`], which closes the editing window.
    pub fn finalize(self, metadata: M) {
        let ringbuffer = self.ringbuffer;
        let sequence = self.sequence;
        let id = DescriptorId::new_truncating(sequence as usize);

        let descriptor = self.ringbuffer.descriptor(id);
        let metadata_ref = self.ringbuffer.metadata(id);

        drop(self);

        M::store(metadata_ref, metadata, Ordering::Release);
        let current_state_id = DescriptorStateId::new(id, DescriptorState::Reserved);
        let new_state_id = DescriptorStateId::new(id, DescriptorState::Finalized);
        let result = descriptor.state_id.compare_exchange(
            current_state_id,
            new_state_id,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        if result.is_err() {
            unreachable!()
        }

        ringbuffer.update_last_finalized();
    }
}

/// A committed [`Message`].
///
/// The associated [`Message`] might be able to be reopened for editing.
pub struct CommittedMessage<'buffer, M: Metadata> {
    ringbuffer: &'buffer Ringbuffer<M>,
    sequence: u64,
    metadata: M,
}

impl<'buffer, M: Metadata> CommittedMessage<'buffer, M> {
    /// Attempts to reopen the [`CommittedMessage`] for additional manipulation.
    pub fn reopen(self, additional_size: usize) -> Result<(ReservedMessage<'buffer, M>, M), Self> {
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

    fn compare_exchange(
        &self,
        current: DescriptorStateId,
        new: DescriptorStateId,
        success: Ordering,
        failure: Ordering,
    ) -> Result<DescriptorStateId, DescriptorStateId> {
        self.0
            .compare_exchange(current.to_raw(), new.to_raw(), success, failure)
            .map(DescriptorStateId::from_raw)
            .map_err(DescriptorStateId::from_raw)
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

    fn compare_exchange(
        &self,
        current: DescriptorId,
        new: DescriptorId,
        success: Ordering,
        failure: Ordering,
    ) -> Result<DescriptorId, DescriptorId> {
        self.0
            .compare_exchange(current.to_raw(), new.to_raw(), success, failure)
            .map(DescriptorId::from_raw)
            .map_err(DescriptorId::from_raw)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DescriptorId(usize);

impl DescriptorId {
    /// Creates a new [`DescriptorId`] from the raw value.
    const fn from_raw(val: usize) -> Self {
        Self(val)
    }

    /// Creates a new [`DescriptorId`] from the provided value, truncating as necessary.
    const fn new_truncating(val: usize) -> Self {
        Self(val & DescriptorStateId::ID_MASK)
    }

    /// Returns the previous [`DescriptorId`] in the modular representation.
    const fn prev(self) -> Self {
        Self::new_truncating(self.to_raw().wrapping_sub(1))
    }

    /// Returns the next [`DescriptorId`] in the modular representation.
    const fn next(self) -> Self {
        Self::new_truncating(self.to_raw().wrapping_add(1))
    }

    /// Returns the [`DescriptorId`] associated with the same index, but one wrap earlier.
    const fn prev_wrap(self, descriptor_count: usize) -> Self {
        Self::new_truncating(self.to_raw().wrapping_sub(descriptor_count))
    }

    /// Returns how many [`DescriptorId`]s `self` is ahead of `lhs`.
    const fn difference(self, lhs: Self) -> usize {
        self.to_raw().wrapping_sub(lhs.to_raw()) & DescriptorStateId::ID_MASK
    }

    /// Returns the raw representation of this [`DescriptorId`].
    const fn to_raw(self) -> usize {
        self.0
    }

    const fn base_id() -> Self {
        Self::new_truncating(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DescriptorState {
    /// The slot is empty or the data was overwritten.
    MissingData,
    /// A producer has claimed this slot and is currently writing to it.
    Reserved,
    /// The data is written, and the message is visible, but it might still be "reopened" for more
    /// data.
    Committed,
    /// The message is immutable and ready to be consumed or eventually overwritten.
    Finalized,
}

struct Block {
    id: AtomicUsize,
    data: [u8],
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

#[cfg(test)]
mod test {
    use core::sync::atomic::{AtomicU64, Ordering};

    use crate::{AllocatedRingBuffer, BASE_SEQUENCE, Metadata};

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

        assert_eq!(buffer.first_sequence(), BASE_SEQUENCE);
        assert_eq!(buffer.first_valid_sequence(), BASE_SEQUENCE);
        assert_eq!(buffer.next_sequence(), BASE_SEQUENCE);
        assert_eq!(buffer.next_reserve_sequence(), BASE_SEQUENCE);
    }

    #[test]
    pub fn reserve() {
        let buffer = AllocatedRingBuffer::<TestMetadata>::new(2048, 4);

        let message_text = b"Hello World!";
        let metadata = TestMetadata(42);

        let reservation = buffer.reserve(message_text.len()).unwrap();

        assert_eq!(reservation.sequence(), 0);
        for (buffer_byte, byte) in reservation.buffer().iter().zip(message_text.iter()) {
            buffer_byte.store(*byte, Ordering::Relaxed);
        }

        reservation.finalize(metadata);

        let mut text_buffer = [0; 8];
        let message = buffer.read(0, &mut text_buffer).unwrap();

        assert_eq!(message.buffer.unwrap(), message_text);
        assert_eq!(message.metadata, metadata);
    }
}

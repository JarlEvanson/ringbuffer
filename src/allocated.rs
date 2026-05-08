use alloc::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use core::ptr::{self, NonNull};

use crate::{Descriptor, Metadata, Ringbuffer};

/// A [`Ringbuffer`] that automatically allocates and deallocates its buffers.
#[repr(transparent)]
pub struct AllocatedRingBuffer<M: Metadata>(Ringbuffer<M>);

impl<M: Metadata> AllocatedRingBuffer<M> {
    pub fn new(data_capacity: usize, descriptor_count: usize) -> Self {
        assert_ne!(data_capacity, 0);
        assert_ne!(descriptor_count, 0);

        let data_layout = Layout::array::<u8>(data_capacity)
            .expect("requested ringbuffer data size is too large");
        let descriptor_layout = Layout::array::<Descriptor>(descriptor_count)
            .expect("requested ringbuffer descriptor count is too large");
        let metadata_layout = Layout::array::<M::Atomic>(descriptor_count)
            .expect("requested ringbuffer descriptor count is too large");

        let data_buffer = unsafe { alloc(data_layout) };
        let descriptor_buffer = unsafe { alloc(descriptor_layout) };
        let metadata_buffer = if metadata_layout.size() != 0 {
            unsafe { alloc(metadata_layout) }
        } else {
            ptr::dangling_mut()
        };

        let buffers = [
            (data_buffer, data_layout),
            (descriptor_buffer, descriptor_layout),
            (metadata_buffer, metadata_layout),
        ];
        if let Some(&(_, layout)) = buffers.iter().find(|(buffer, _)| buffer.is_null()) {
            for &(buffer, layout) in buffers.iter().filter(|(buffer, _)| !buffer.is_null()) {
                if layout.size() == 0 {
                    continue;
                }
                unsafe { dealloc(buffer, layout) }
            }

            handle_alloc_error(layout);
        }

        let ringbuffer = Ringbuffer::new(
            data_capacity,
            NonNull::new(data_buffer).unwrap(),
            descriptor_count,
            NonNull::new(descriptor_buffer.cast::<Descriptor>()).unwrap(),
            NonNull::new(metadata_buffer.cast::<M::Atomic>()).unwrap(),
        );

        Self(ringbuffer)
    }
}

impl<M: Metadata> core::ops::Deref for AllocatedRingBuffer<M> {
    type Target = Ringbuffer<M>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<M: Metadata> core::ops::DerefMut for AllocatedRingBuffer<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<M: Metadata> core::ops::Drop for AllocatedRingBuffer<M> {
    fn drop(&mut self) {
        let data_layout = Layout::array::<u8>(self.data_capacity).unwrap();
        let descriptor_layout = Layout::array::<Descriptor>(self.descriptor_count).unwrap();
        let metadata_layout = Layout::array::<M::Atomic>(self.descriptor_count).unwrap();

        unsafe { dealloc(self.data_buffer.as_ptr(), data_layout) };
        unsafe {
            dealloc(
                self.descriptor_buffer.as_ptr().cast::<u8>(),
                descriptor_layout,
            )
        };
        if metadata_layout.size() != 0 {
            unsafe { dealloc(self.metadata_buffer.as_ptr().cast::<u8>(), metadata_layout) };
        }
    }
}

use core::sync::atomic::{AtomicU64, Ordering};

use ringbuffer::{AllocatedRingBuffer, Metadata};

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
#[ignore]
fn reserve_commit_read_roundtrip() {
    // Create a buffer with enough capacity for data and descriptors.
    let rb = AllocatedRingBuffer::<TestMetadata>::new(1024, 16);

    // Reserve a message, write contents, and commit with metadata.
    let mut reserved = rb.reserve(13).expect("reserve should succeed");
    assert!(reserved.buffer().len() >= 13);
    reserved.buffer_mut().copy_from_slice(b"hello, world!");
    let sequence = reserved.sequence();

    let committed = reserved.commit(TestMetadata(42));
    // Finalize to close editing window (which is required for immediate availability).
    committed.finalize();

    // Read back the message using the sequence we received.
    let mut buf = vec![0u8; 13];
    let msg_opt = rb.read(sequence, &mut buf);
    let msg = msg_opt.expect("expected message");

    assert_eq!(msg.sequence, sequence);
    assert_eq!(msg.metadata, TestMetadata(42));
    assert!(msg.buffer.is_some());
    assert_eq!(&buf, b"hello, world!");
}

#[test]
#[ignore]
fn wraparound_and_sequence_progression() {
    // Small buffer to force wraparound semantics quickly.
    let rb = AllocatedRingBuffer::<TestMetadata>::new(32, 8);

    // Fill several messages to exercise descriptor cycling.
    let mut sequences = Vec::new();
    for i in 0..10 {
        let size = 4;
        let mut reserved = rb.reserve(size).expect("reserve should succeed");
        reserved.buffer_mut().copy_from_slice(&[i as u8; 4]);
        let seq = reserved.sequence();
        reserved.commit(TestMetadata(i as u64));
        sequences.push((seq, i));
    }

    // Validate that reading by sequence yields expected metadata and payloads (or the next
    // available message).
    for (seq, expected_i) in sequences.into_iter() {
        let mut buf = vec![0u8; 4];
        if let Some(msg) = rb.read(seq, &mut buf) {
            // If the requested sequence is still present, it should match.
            assert_eq!(msg.metadata, TestMetadata(expected_i as u64));
            assert_eq!(&buf, &[expected_i as u8; 4]);
        }
    }
}

#[test]
#[ignore]
fn reopen_for_editing() {
    let rb = AllocatedRingBuffer::<TestMetadata>::new(128, 8);

    let mut reserved = rb.reserve(8).expect("reserve should succeed");
    reserved.buffer_mut().copy_from_slice(b"12345678");
    let seq = reserved.sequence();
    let committed = reserved.commit(TestMetadata(1));

    // Attempt to reopen with additional space.
    if let Ok((reopened, metadata)) = committed.reopen(4) {
        assert_eq!(metadata, TestMetadata(1));

        reopened.finalize(TestMetadata(metadata.0.wrapping_add(1)));
    }

    // Ensure the message is still readable.
    let mut buf = vec![0u8; 8];
    if let Some(msg) = rb.read(seq, &mut buf) {
        assert_eq!(msg.metadata, TestMetadata(2));
        assert_eq!(&buf, b"12345678");
    }
}

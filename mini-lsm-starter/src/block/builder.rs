use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.data.is_empty() {
            self.first_key = KeyVec::from_vec(key.raw_ref().to_vec());
        } else {
            const KEY_VAL_LEN: usize = 4;
            const NUM_OF_ELEMENTS_LEN: usize = 2;
            const OFFSET_LEN: usize = 2;
            let size_add = KEY_VAL_LEN + key.len() + value.len() + OFFSET_LEN; // entry + offset
            if self.offsets.len() * 2 + self.data.len() + size_add + NUM_OF_ELEMENTS_LEN
                > self.block_size
            {
                return false;
            }
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put_slice(key.raw_ref());

        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() || self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

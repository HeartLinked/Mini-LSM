#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
                println!("---------OVERFLOW!--------------");
                return false;
            }
        }
        // 获取 key 和 value 的字节切片
        let key_bytes = key.raw_ref();
        let value_bytes = value;
        // 获取 key 和 value 的长度
        let key_len = (key_bytes.len() as u16); // 包含 2 个元素为 u8 的数组， [u8, 2]
        let value_len = (value_bytes.len() as u16); // 包含 2 个元素为 u8 的数组， [u8, 2]
        self.data.extend_from_slice(&key_len.to_le_bytes());
        self.data.extend_from_slice(key_bytes);

        self.data.extend_from_slice(&value_len.to_le_bytes());
        self.data.extend_from_slice(value_bytes);
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

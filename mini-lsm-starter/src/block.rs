#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;
use nom::ExtendInto;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        bytes.extend_from_slice(&self.data);
        for &offset in &self.offsets {
            bytes.extend_from_slice(&offset.to_le_bytes());
        }
        bytes.put_u16(self.offsets.len() as u16);
        return bytes.freeze();
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // num_of_elements
        let num_of_elements_offset = data.len() - 2; // 偏移量是倒数第二个字节开始
        let num_of_elements = u16::from_le_bytes([
            data[num_of_elements_offset],
            data[num_of_elements_offset + 1],
        ]) as usize;

        let offsets_start = num_of_elements_offset - num_of_elements * 2;
        let mut offsets = Vec::with_capacity(num_of_elements);

        // offsets
        for i in 0..num_of_elements {
            let offset_pos = offsets_start + i * 2;
            let offset = u16::from_le_bytes([data[offset_pos], data[offset_pos + 1]]);
            offsets.push(offset);
        }

        // data
        let data_part = &data[..offsets_start]; // 数据部分到偏移开始为止

        Block {
            data: data_part.to_vec(),
            offsets,
        }
    }
}

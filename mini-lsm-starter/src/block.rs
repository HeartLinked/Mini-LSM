mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Default)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        bytes.put_slice(&self.data);
        for offset in &self.offsets {
            bytes.put_u16(*offset);
        }
        bytes.put_u16(self.offsets.len() as u16);

        bytes.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements_offset = data.len() - 2; // 偏移量是倒数第二个字节开始
        let num_of_elements = (&data[num_of_elements_offset..]).get_u16() as usize;

        let offsets_start = num_of_elements_offset - num_of_elements * 2;
        let mut offsets = Vec::new();

        // offsets
        for i in 0..num_of_elements {
            let offset_pos = offsets_start + i * 2;
            let offset = (&data[offset_pos..offset_pos + 2]).get_u16();
            offsets.push(offset);
        }
        // data
        let data_part = &data[..offsets_start]; // 数据部分到偏移开始为止

        Block {
            data: data_part.to_vec(),
            offsets,
        }
    }

    pub fn first_key(&self) -> Option<Bytes> {
        let offset = match self.offsets.first() {
            Some(offset) => *offset as usize,
            None => return None,
        };

        let mut data = &self.data[offset..];
        let len = data.get_u16() as usize;

        Some(Bytes::copy_from_slice(&data[..len]))
    }

    pub fn last_key(&self) -> Option<Bytes> {
        let offset = match self.offsets.last() {
            Some(offset) => *offset as usize,
            None => return None,
        };

        let mut data = &self.data[offset..];
        let len = data.get_u16() as usize;

        Some(Bytes::copy_from_slice(&data[..len]))
    }
}

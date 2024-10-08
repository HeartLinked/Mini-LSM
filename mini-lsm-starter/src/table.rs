#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use bytes::Bytes;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
/// you will need to maintain block metadata BlockMeta,
/// which includes the first/last keys in each block and the offsets of each block.
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for meta in block_meta {
            // 编码 offset，使用 8 字节（u64）来存储
            buf.extend_from_slice(&(meta.offset as u64).to_le_bytes());
            // 编码 first_key 的长度和内容
            let first_key_len = meta.first_key.raw_ref().len() as u64;
            buf.extend_from_slice(&first_key_len.to_le_bytes());
            buf.extend_from_slice(meta.first_key.raw_ref());
            // 编码 last_key 的长度和内容
            let last_key_len = meta.last_key.raw_ref().len() as u64;
            buf.extend_from_slice(&last_key_len.to_le_bytes());
            buf.extend_from_slice(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_metas = Vec::new();
        let mut buf = buf; // 不需要将 `buf` 声明为可变，因为 Buf trait 是内部可变的
        while buf.remaining() > 0 {
            // 解码 offset（8 字节）
            let offset = buf.get_u64_le() as usize;

            // 解码 first_key 的长度（8 字节），然后读取 first_key
            let first_key_len = buf.get_u64_le() as usize;
            let mut first_key = vec![0; first_key_len];
            buf.copy_to_slice(&mut first_key);

            // 解码 last_key 的长度（8 字节），然后读取 last_key
            let last_key_len = buf.get_u64_le() as usize;
            let mut last_key = vec![0; last_key_len];
            buf.copy_to_slice(&mut last_key);

            // 构建 BlockMeta 并推入列表
            block_metas.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes(Bytes::from(first_key)),
                last_key: KeyBytes::from_bytes(Bytes::from(last_key)),
            });
        }

        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let (mut file, file_size) = match file.0 {
            Some(f) => (f, file.1),
            None => return Err(anyhow!("file not exists")),
        };

        let mut bytes = vec![0; file_size as usize];
        file.read_exact(&mut bytes)?;

        const U32_SIZE: usize = size_of::<u32>();
        let block_meta_offset = (&bytes[bytes.len() - U32_SIZE..]).get_u32() as usize;

        let block_meta = &bytes[block_meta_offset..bytes.len() - U32_SIZE];
        let block_meta = BlockMeta::decode_block_meta(block_meta);
        let first_key = block_meta
            .first()
            .map(|meta| meta.first_key.clone())
            .unwrap_or_default();
        let last_key = block_meta
            .last()
            .map(|meta| meta.last_key.clone())
            .unwrap_or_default();

        file.rewind()?;
        Ok(Self {
            file: FileObject(Some(file), file_size),
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let file = match &self.file.0 {
            Some(f) => f,
            None => return Err(anyhow!("File not exists")),
        };

        let (start, length) = match (
            self.block_meta.get(block_idx),
            self.block_meta.get(block_idx + 1),
        ) {
            (Some(fir), Some(sec)) => (fir.offset, sec.offset - fir.offset),
            (Some(fir), None) => (fir.offset, self.block_meta_offset - fir.offset),
            _ => return Ok(Arc::default()),
        };

        let mut buffer = vec![0; length];
        file.read_exact_at(&mut buffer, start as u64)?;

        Ok(Arc::new(Block::decode(&buffer)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .binary_search_by_key(&key, |b| b.first_key.as_key_slice())
            .unwrap_or_else(|idx| idx)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}

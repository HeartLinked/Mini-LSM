#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{anyhow, Result};

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner, // MergeIterator<MemTableIterator>
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        Ok(Self { inner: iter })
    }

    pub fn skip_delete_key(&mut self) -> Result<()> {
        while self.is_valid() && !self.inner.key().is_empty() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn is_valid(&self) -> bool {
        return self.inner.is_valid();
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.skip_delete_key()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I, // 一个迭代器，可以自身错误（无法获得 key），也可以自身仍有效，但无法执行 next，这是两种不同的情况
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        // 这里二者的顺序不能调换，如果在迭代器包含错误的情况下调用is_valid，可能会直接 panic
        (!self.has_errored) && self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            // 有错误一定不能next
            return Err(anyhow!("The iterator is invalid!"));
        }
        if let Ok(res) = self.iter.next() {
            self.has_errored = false;
            Ok(res)
        } else {
            self.has_errored = true;
            Err(anyhow!("The iterator is invalid after next operation!"))
        }
    }
}

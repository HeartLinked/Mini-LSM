#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1 // 第二个字段 Box<I>
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0)) // 先比较当前 key，若比较结果相同再比较 usize 字段
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }
        let heap_peek = heap.pop();
        MergeIterator {
            iters: heap,
            current: heap_peek,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        match self.current.as_ref() {
            Some(wrapper) => wrapper.1.value(),
            None => &[],
        }
    }

    fn key(&self) -> KeySlice {
        match self.current.as_ref() {
            Some(wrapper) => wrapper.1.key(),
            None => KeySlice::from_slice(&[]),
        }
    }

    fn is_valid(&self) -> bool {
        // 当前 current 迭代器位置还有没有键值对
        match self.current.as_ref() {
            Some(wrapper) => wrapper.1.is_valid(),
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        let wrapper = match self.current.as_mut() {
            // wrapper: current 内的迭代器可变引用
            None => return Ok(()),
            Some(wrapper) => wrapper,
        };
        let key = wrapper.1.key(); // current 的 key

        // 首先去掉所有 MergeIterator 的 iters 字段 中的所有迭代器 中的所有与当前 key 相同的键值对
        while let Some(mut inner) = self.iters.peek_mut() {
            if inner.1.key() == key {
                match inner.1.next() {
                    Err(err) => {
                        PeekMut::pop(inner);
                        return Err(err); // 过程中出错，直接返回即可
                    }
                    Ok(_) => {}
                }
                if inner.1.is_valid() == false {
                    PeekMut::pop(inner); // 该迭代器所有元素耗尽，从堆中删除该迭代器
                }
            } else {
                break;
            }
        }

        wrapper.1.next()?;
        // 由于单个 memtable 是原地修改的，故在 current 内不会存在与当前 key 相同的键值对
        // 但是此时进行了 next，要讨论此时 current 是否仍有效
        if wrapper.1.is_valid() {
            if let Some(mut heap_wrapper) = self.iters.peek_mut() {
                if *wrapper < *heap_wrapper {
                    std::mem::swap(&mut *heap_wrapper, wrapper);
                }
            }
        } else {
            let heap_peek = self.iters.pop(); // 重新取堆顶成为新的 current
            self.current = heap_peek;
        }
        Ok(())
    }

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        let mut count = 0;
        for iter in &self.iters {
            count += iter.1.num_active_iterators();
        }

        if let Some(iter) = &self.current {
            count += iter.1.num_active_iterators()
        }

        count
    }
}

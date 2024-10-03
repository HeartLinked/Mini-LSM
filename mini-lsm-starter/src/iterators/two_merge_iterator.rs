use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(TwoMergeIterator { a, b })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn value(&self) -> &[u8] {
        match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) => {
                if self.a.key() <= self.b.key() {
                    self.a.value()
                } else {
                    self.b.value()
                }
            }
            (true, false) => self.a.value(),
            _ => self.b.value(),
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) => {
                if self.a.key() <= self.b.key() {
                    self.a.key()
                } else {
                    self.b.key()
                }
            }
            (true, false) => self.a.key(),
            _ => self.b.key(),
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) => {
                if self.a.key() < self.b.key() {
                    self.a.next()
                } else if self.a.key() == self.b.key() {
                    self.a.next()?;
                    self.b.next() // 如果 key 相等，则两个迭代器都要增加
                } else {
                    self.b.next()
                }
            }
            (true, false) => self.a.next(),
            _ => self.b.next(),
        }
    }
}

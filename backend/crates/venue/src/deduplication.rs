use std::collections::HashSet;
use std::hash::Hash;
use std::num::NonZeroUsize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeduplicationResult {
    New,
    Duplicate,
    CapacityExceeded,
}

#[derive(Debug)]
pub struct BoundedDeduplicator<K> {
    seen: HashSet<K>,
    capacity: NonZeroUsize,
    duplicate_count: u64,
}

impl<K> BoundedDeduplicator<K>
where
    K: Eq + Hash,
{
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            seen: HashSet::new(),
            capacity,
            duplicate_count: 0,
        }
    }

    pub fn observe(&mut self, key: K) -> DeduplicationResult {
        if self.seen.contains(&key) {
            self.duplicate_count = self.duplicate_count.saturating_add(1);
            return DeduplicationResult::Duplicate;
        }
        if self.seen.len() == self.capacity.get() {
            return DeduplicationResult::CapacityExceeded;
        }
        self.seen.insert(key);
        DeduplicationResult::New
    }

    pub fn unique_count(&self) -> usize {
        self.seen.len()
    }

    pub const fn duplicate_count(&self) -> u64 {
        self.duplicate_count
    }

    pub const fn capacity(&self) -> NonZeroUsize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_without_consuming_capacity() {
        let mut dedup = BoundedDeduplicator::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(dedup.observe(10), DeduplicationResult::New);
        assert_eq!(dedup.observe(10), DeduplicationResult::Duplicate);
        assert_eq!(dedup.observe(11), DeduplicationResult::New);
        assert_eq!(dedup.unique_count(), 2);
        assert_eq!(dedup.duplicate_count(), 1);
    }

    #[test]
    fn fails_closed_instead_of_evicting_an_identity() {
        let mut dedup = BoundedDeduplicator::new(NonZeroUsize::new(1).unwrap());
        assert_eq!(dedup.observe("first"), DeduplicationResult::New);
        assert_eq!(
            dedup.observe("second"),
            DeduplicationResult::CapacityExceeded
        );
        assert_eq!(dedup.observe("first"), DeduplicationResult::Duplicate);
    }
}

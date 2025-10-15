use std::{collections::HashMap, fmt::Debug, num::NonZeroUsize};

use anyhow::bail;
use parking_lot::RwLock;

type NodeId = usize;

struct Entry<K: Debug, V> {
    key: K,
    value: Option<V>,
    next: Option<NodeId>, // towards the LRU (tail)
    prev: Option<NodeId>, // towards the MRU (head)
}

impl<K: Debug, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value: Some(value),
            next: None,
            prev: None,
        }
    }
}

pub struct Inner<K: Debug, V: Clone, S = ahash::RandomState> {
    map: HashMap<K, NodeId, S>,
    nodes: Vec<Entry<K, V>>,
    free: Vec<NodeId>,
    head: Option<NodeId>, // MRU
    tail: Option<NodeId>, // LRU
    capacity: NonZeroUsize,
}

impl<K: Debug, V: Clone, S> Inner<K, V, S>
where
    K: Eq + std::hash::Hash + Clone,
    S: core::hash::BuildHasher + Default,
{
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            map: HashMap::with_hasher(S::default()),
            nodes: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
            capacity,
        }
    }

    /// Move an existing entry as MRU
    fn push_front(&mut self, id: NodeId) -> Option<V> {
        let old_head = self.head;

        {
            // Fix the prev and next of the new entry
            let n = &mut self.nodes[id];
            n.prev = None;
            n.next = old_head;
        }

        // Fix the old_head entry
        if let Some(h) = old_head {
            self.nodes[h].prev = Some(id)
        } else {
            // List was empty
            self.tail = Some(id)
        }

        // Fix the head
        self.head = Some(id);
        self.nodes[id].value.clone()
    }

    fn unlink(&mut self, id: NodeId) {
        let (prev, next) = {
            let n = &self.nodes[id];
            (n.prev, n.next)
        };

        // Fix prev neighbor
        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            // was head
            self.head = next;
        }
        // Fix next neighbor
        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            // was tail
            self.tail = prev;
        }

        {
            // Fix the prev and next of the unlinked entry
            let n = &mut self.nodes[id];
            n.next = None;
            n.prev = None;
        }
    }

    fn evict(&mut self) -> Option<()> {
        let tail = self.tail?;

        let prev = self.nodes[tail].prev;

        if let Some(p) = prev {
            let prev_entry = &mut self.nodes[p];
            prev_entry.next = None;
            self.tail = Some(p)
        } else {
            // was empty
            self.head = None;
            self.tail = None;
        }

        let old_key = &self.nodes[tail].key;
        self.map.remove_entry(old_key)?;
        // Mark the entry as free
        self.free.push(tail);
        // Remove the value from the entry
        std::mem::take(&mut self.nodes[tail].value);
        Some(())
    }

    fn insert_front(&mut self, key: K, value: V) -> NodeId {
        let old_head = self.head;

        // Create new entry and fix the next
        let mut entry = Entry::new(key.clone(), value);
        entry.next = old_head;

        // Get the index of the new entry.
        // Insert into nodes
        let index = match self.free.pop() {
            Some(i) => {
                let _ = std::mem::replace(&mut self.nodes[i], entry);
                self.map.insert(key, i);
                i
            }
            None => {
                let index = self.nodes.len() as NodeId;
                self.nodes.push(entry);
                self.map.insert(key, index);
                index
            }
        };

        // Handle old head, or if list is empty
        if let Some(h) = old_head {
            let e = &mut self.nodes[h];
            e.prev = Some(index);
        } else {
            self.tail = Some(index);
        }

        // Set the new head
        self.head = Some(index);
        index
    }

    fn peek(&self, id: NodeId) -> Option<V> {
        self.nodes[id].value.clone()
    }
}

pub struct LruCache<K: Debug, V: Clone, S = ahash::RandomState> {
    list: RwLock<Inner<K, V, S>>,
}

impl<K: Debug, V: Clone, S> LruCache<K, V, S>
where
    K: Eq + std::hash::Hash + Clone,
    S: core::hash::BuildHasher + Default,
{
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).expect("Non-zero capacity");
        Self {
            list: RwLock::new(Inner::new(capacity)),
        }
    }

    pub fn get(&self, key: K) -> Option<V> {
        let mut guard = self.list.write();
        let id = *guard.map.get(&key)?;
        if guard.nodes[id].value.is_none() {
            return None
        };
        guard.unlink(id);
        guard.push_front(id)
    }

    pub fn with_get_mut<R>(&self, key: K, f: impl FnOnce(&mut V) -> R) -> Option<R> {
        let mut guard = self.list.write();
        if !guard.map.contains_key(&key) {
            return None;
        }
        let id = *guard.map.get(&key)?;
        if guard.nodes[id].value.is_none() {
            return None
        };
        guard.unlink(id);
        guard.push_front(id);
        let entry = &mut guard.nodes[id];
        Some(f(entry.value.as_mut().unwrap()))
    }

    // Returns the old value if it already exists
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let mut guard = self.list.write();
        {
            while guard.map.len() >= guard.capacity.into() {
                guard.evict();
            }
        }
        if let Some(&id) = guard.map.get(&key) {
            // Entry already exists
            let old = guard.nodes[id].value.replace(value);
            guard.unlink(id);
            guard.push_front(id);
            old
        } else {
            guard.insert_front(key, value);
            None
        }
    }

    pub fn insert_many<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut guard = self.list.write();

        for (key, value) in entries {
            while guard.map.len() >= guard.capacity.into() {
                guard.evict();
            }

            if let Some(&id) = guard.map.get(&key) {
                guard.nodes[id].value.replace(value.clone());
                guard.unlink(id);
                guard.push_front(id);
            } else {
                guard.insert_front(key, value);
            }
        }

        drop(guard);
    }

    pub fn remove(&self, key: K) -> Option<V> {
        let mut guard = self.list.write();
        if let Some(&p) = guard.map.get(&key) {
            // Fix the neighbors
            guard.unlink(p);
            // Remove from the map
            guard.map.remove(&key);
            // Mark the entry as free
            guard.free.push(p);
            // Remove the value from the entry
            return std::mem::take(&mut guard.nodes[p].value);
        }
        None
    }

    pub fn peek(&self, key: K) -> Option<V> {
        let guard = self.list.read();
        if let Some(&id) = guard.map.get(&key) {
            let entry = guard.nodes.get(id)?;
            entry.value.clone()
        } else {
            None
        }
    }

    /// Takes the value out of the cache, without removing the entry
    ///
    /// Promotes the entry
    pub fn take_and_promote(&self, key: K) -> Option<V> {
        let mut guard = self.list.write();
        if let Some(&id) = guard.map.get(&key) {
            guard.push_front(id);
            return std::mem::take(&mut guard.nodes[id].value);
        }
        None
    }

    /// Puts the value back
    ///
    /// Only called when value was take out with take_and_promote
    pub fn put_back(&self, key: K, value: V) -> anyhow::Result<()> {
        let mut guard = self.list.write();
        if let Some(&id) = guard.map.get(&key)
            && guard.nodes[id].value.is_none()
        {
            guard.nodes[id].value.replace(value);
            return Ok(());
        }
        bail!("Could not find entry in cache")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::fs::fileattr::{FileAttr, dir_attr};

    #[test]
    fn test_lru_cache_insert() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        assert!(lru.peek(1).is_some());

        lru.insert(2, attr);
        assert!(lru.peek(1).is_some());
        assert!(lru.peek(2).is_some());

        lru.insert(3, attr);
        assert!(lru.peek(1).is_some());
        assert!(lru.peek(2).is_some());
        assert!(lru.peek(3).is_some());

        lru.insert(4, attr);
        assert!(lru.peek(1).is_none());

        lru.insert(5, attr);
        assert!(lru.peek(2).is_none());

        lru.insert(6, attr);
        assert!(lru.peek(3).is_none());

        lru.get(4);

        lru.insert(7, attr);
        assert!(lru.peek(4).is_some());
        assert!(lru.peek(5).is_none());
    }

    #[test]
    fn test_lru_promote_on_get_prevents_eviction() {
        // cap = 3
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.get(1).is_some());

        lru.insert(4, attr);

        assert!(lru.peek(1).is_some());
        assert!(lru.peek(3).is_some());
        assert!(lru.peek(4).is_some());
        assert!(lru.peek(2).is_none());
    }

    #[test]
    fn test_lru_peek_does_not_promote() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.peek(1).is_some());

        lru.insert(4, attr);

        assert!(lru.peek(1).is_none());
        assert!(lru.peek(2).is_some());
        assert!(lru.peek(3).is_some());
        assert!(lru.peek(4).is_some());
    }

    #[test]
    fn test_lru_eviction_order_basic() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(2);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(10, attr);
        lru.insert(20, attr);

        lru.insert(30, attr);

        assert!(lru.peek(10).is_none());
        assert!(lru.peek(20).is_some());
        assert!(lru.peek(30).is_some());
    }

    #[test]
    fn test_lru_remove_unlinks_and_deletes() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.remove(2).is_some());
        assert!(lru.peek(2).is_none());

        lru.insert(4, attr);
        lru.insert(5, attr);
        let survivors = (lru.peek(1).is_some() as u8) + (lru.peek(3).is_some() as u8);
        assert_eq!(survivors, 1);
        assert!(lru.peek(4).is_some());
    }

    #[test]
    fn test_lru_get_miss_returns_none() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(2);
        assert!(lru.get(9999).is_none());
        assert!(lru.peek(9999).is_none());
        assert!(lru.remove(9999).is_none());
    }

    #[test]
    fn test_lru_single_element_behaviour() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(1);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        assert!(lru.peek(1).is_some());

        assert!(lru.get(1).is_some());

        lru.insert(2, attr);
        assert!(lru.peek(1).is_none());
        assert!(lru.peek(2).is_some());
    }

    #[test]
    fn test_lru_promote_then_evict_correct_tail() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.get(2).is_some());

        lru.insert(4, attr);
        assert!(lru.peek(1).is_none());
        assert!(lru.peek(2).is_some());
        assert!(lru.peek(3).is_some());
        assert!(lru.peek(4).is_some());
    }

    #[test]
    fn test_lru_repeated_get_is_idempotent_for_head() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.get(3).is_some());
        assert!(lru.get(3).is_some());
        assert!(lru.get(3).is_some());

        lru.insert(4, attr);
        assert!(lru.peek(1).is_none());
        assert!(lru.peek(2).is_some());
        assert!(lru.peek(3).is_some());
        assert!(lru.peek(4).is_some());
    }

    #[test]
    fn test_lru_with_get_mut_updates_value_and_promotes() {
        let lru: LruCache<i32, i32> = LruCache::new(2);

        lru.insert(1, 10);
        lru.insert(2, 20);

        let out = lru.with_get_mut(1, |v| {
            *v += 5;
            *v
        });
        assert_eq!(out, Some(15));
        assert_eq!(lru.peek(1), Some(15));

        lru.insert(3, 30);
        assert_eq!(lru.peek(2), None);
        assert_eq!(lru.peek(1), Some(15));
        assert_eq!(lru.peek(3), Some(30));
    }

    #[test]
    fn test_lru_with_get_mut_miss_returns_none_and_does_not_insert() {
        let lru: LruCache<i32, i32> = LruCache::new(2);

        lru.insert(1, 1);
        lru.insert(2, 2);

        let called = lru.with_get_mut(999, |v| {
            *v += 1;
            *v
        });
        assert_eq!(called, None);
        assert_eq!(lru.peek(999), None);

        assert_eq!(lru.peek(1), Some(1));
        assert_eq!(lru.peek(2), Some(2));
    }

    #[test]
    fn test_lru_with_get_mut_is_idempotent_for_head() {
        let lru: LruCache<i32, i32> = LruCache::new(2);

        lru.insert(1, 100);
        lru.insert(2, 200);

        let r1 = lru.with_get_mut(2, |v| {
            *v += 1;
            *v
        });
        let r2 = lru.with_get_mut(2, |v| {
            *v += 1;
            *v
        });
        assert_eq!(r1, Some(201));
        assert_eq!(r2, Some(202));
        assert_eq!(lru.peek(2), Some(202));

        lru.insert(3, 300);
        assert_eq!(lru.peek(1), None);
        assert_eq!(lru.peek(2), Some(202));
        assert_eq!(lru.peek(3), Some(300));
    }

    #[test]
    fn test_lru_with_get_mut_return_value_is_bubbled_up() {
        let lru: LruCache<i32, i32> = LruCache::new(3);
        lru.insert(42, 7);

        let ret = lru.with_get_mut(42, |v| {
            *v *= 3;
            *v % 10
        });

        assert_eq!(ret, Some(1)); // (7*3)=21; 21 % 10 = 1
        assert_eq!(lru.peek(42), Some(21));
    }

    #[test]
    fn with_get_mut_on_tail_promotes_to_head_then_eviction_spares_it() {
        let lru: LruCache<i32, i32> = LruCache::new(3);
        lru.insert(1, 11);
        lru.insert(2, 22);
        lru.insert(3, 33);

        assert_eq!(
            lru.with_get_mut(1, |v| {
                *v += 1;
                *v
            }),
            Some(12)
        );
        assert_eq!(lru.peek(1), Some(12));

        lru.insert(4, 44);
        assert_eq!(lru.peek(2), None);
        assert_eq!(lru.peek(1), Some(12));
        assert_eq!(lru.peek(3), Some(33));
        assert_eq!(lru.peek(4), Some(44));

        lru.insert(5, 55);
        assert_eq!(lru.peek(3), None);
        assert_eq!(lru.peek(1), Some(12));
        assert_eq!(lru.peek(4), Some(44));
        assert_eq!(lru.peek(5), Some(55));
    }

    #[test]
    fn test_lru_change_size_in_attr() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let mut attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();
        attr.size = 10;

        lru.insert(1, attr);
        lru.with_get_mut(1, |a| a.size = 12);
        let attr = lru.get(1).unwrap();
        assert_eq!(attr.size, 12);
    }
}

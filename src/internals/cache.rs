use std::{collections::HashMap, fmt::Debug, num::NonZeroUsize};

use parking_lot::RwLock;

use crate::fs::meta_db::DbReturn;

type NodeId = usize;

#[derive(Clone)]
struct Entry<K: Debug, V> {
    pub key: K,
    pub value: V,
    pub next: Option<NodeId>, // towards the LRU (tail)
    pub prev: Option<NodeId>, // towards the MRU (head)
}

impl<K: Debug, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            next: None,
            prev: None,
        }
    }
}

pub struct Inner<K: Debug, V: Clone, S = std::hash::DefaultHasher> {
    map: HashMap<K, NodeId, S>,
    nodes: Vec<Option<Entry<K, V>>>,
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
    fn push_front(&mut self, id: NodeId) {
        let old_head = self.head;

        {
            // Fix the prev and next of the new entry
            let Some(n) = &mut self.nodes[id] else {
                // Value was already removed from cache
                return;
            };
            n.prev = None;
            n.next = old_head;
        }

        // Fix the old_head entry
        if let Some(h) = old_head {
            if let Some(value) = self.nodes[h].as_mut() {
                value.prev = Some(id);
            }
        } else {
            // List was empty
            self.tail = Some(id);
        }

        // Fix the head
        self.head = Some(id);
    }

    fn unlink(&mut self, id: NodeId) -> bool {
        // Find the entry and get its prev and next
        // Early return if not found
        let (prev, next) = match self
            .nodes
            .get(id)
            .and_then(|e| e.as_ref().map(|e| (e.prev, e.next)))
        {
            Some(pn) => pn,
            None => return false,
        };

        // Fix prev and next neighbors
        // If not found, move along and assume entry was head / tail
        if let Some(p) = prev {
            if let Some(pe) = self.nodes.get_mut(p).and_then(Option::as_mut) {
                pe.next = next;
            };
        } else {
            // was head
            self.head = next;
        }
        if let Some(n) = next {
            if let Some(ne) = self.nodes.get_mut(n).and_then(Option::as_mut) {
                ne.prev = prev
            };
        } else {
            // was tail
            self.tail = prev;
        }

        // Fix the prev and next of the unlinked entry. This shouldn't fail anymore
        if let Some(n) = self.nodes.get_mut(id).and_then(Option::as_mut) {
            n.next = None;
            n.prev = None;
            true
        } else {
            false
        }
    }

    fn evict(&mut self) -> Option<()> {
        // Get tail entry. If it's None, there is nothing to evict
        let tail = self.tail?;
        let tail_e = self.nodes.get_mut(tail)?.take()?;

        // Get entry prev to it
        // If None, tail as also head. List is now empty
        if let Some(prev_e_id) = tail_e.prev {
            if let Some(prev_e) = self.nodes.get_mut(prev_e_id).and_then(Option::as_mut) {
                prev_e.next = None;
                self.tail = Some(prev_e_id)
            } else {
                self.head = None;
                self.tail = None;
            }
            self.tail = Some(prev_e_id)
        } else {
            self.head = None;
            self.tail = None;
        }

        let old_tail = tail_e.key;
        let _ = self.map.remove_entry(&old_tail);
        self.free.push(tail);
        Some(())
    }

    fn insert_front(&mut self, key: K, value: V) -> NodeId {
        let old_head = self.head;

        // Create new entry and fix the next
        let mut entry = Entry::new(key.clone(), value);
        entry.next = old_head;

        // Get the index of the new entry.
        // Insert into nodes
        let index = if let Some(i) = self.free.pop() {
            let _ = self.nodes[i].replace(entry);
            i
        } else {
            let index = self.nodes.len() as NodeId;
            self.nodes.push(Some(entry));
            index
        };
        self.map.insert(key.clone(), index);

        // Handle old head, or if list is empty
        if let Some(h) = old_head {
            if let Some(e) = &mut self.nodes.get_mut(h).and_then(Option::as_mut) {
                e.prev = Some(index);
            }
        } else {
            self.tail = Some(index);
        }
        self.head = Some(index);

        // Set the new head
        index
    }

    #[allow(dead_code)]
    fn peek(&self, id: NodeId) -> Option<V> {
        if let Some(entry) = self.nodes.get(id).and_then(Option::as_ref) {
            return Some(entry.value.clone());
        }
        None
    }
}

pub struct LruCache<K: Debug, V: Clone, S = std::hash::RandomState> {
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

    pub fn get(&self, key: &K) -> DbReturn<V> {
        let mut guard = self.list.write();
        let Some(&id) = guard.map.get(key) else {
            return DbReturn::Missing;
        };
        if guard.free.contains(&id) {
            return DbReturn::Missing;
        }
        guard.unlink(id);
        guard.push_front(id);
        guard.peek(id).into()
    }

    pub fn with_many_mut(&self, keys: &[K], f: impl Fn(&mut V)) {
        let mut guard = self.list.write();

        for key in keys {
            let Some(&id) = guard.map.get(key) else {
                continue;
            };

            if let Some(entry) = &mut guard.nodes[id] {
                f(&mut entry.value);
                guard.unlink(id);
                guard.push_front(id);
            }
        }
    }

    pub fn with_get_mut<R>(&self, key: &K, f: impl FnOnce(&mut V) -> R) -> Option<R> {
        let mut guard = self.list.write();
        let &id = guard.map.get(key)?;
        if let Some(entry) = &mut guard.nodes[id] {
            let res = f(&mut entry.value);
            guard.unlink(id);
            guard.push_front(id);
            return Some(res);
        }
        None
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
            if let Some(old_e) = &mut guard.nodes[id] {
                let old = std::mem::replace(&mut old_e.value, value);
                guard.unlink(id);
                guard.push_front(id);
                return Some(old);
            } else {
                // Dangling entry. Should not happen in theory
                let _ = guard.map.remove(&key);
            }
        };
        guard.insert_front(key, value);
        None
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
                // Entry already exists
                if let Some(old_e) = &mut guard.nodes[id] {
                    let _ = std::mem::replace(&mut old_e.value, value);
                    guard.unlink(id);
                    guard.push_front(id);
                } else {
                    // Dangling entry. Should not happen in theory
                    let _ = guard.map.remove(&key);
                    guard.insert_front(key, value);
                }
            } else {
                guard.insert_front(key, value);
            }
        }
    }

    pub fn remove_many(&self, entries: &[K]) {
        let mut guard = self.list.write();

        for target in entries {
            if let Some(&p) = guard.map.get(target) {
                // Fix the neighbors
                guard.unlink(p);
                // Remove from the map
                guard.map.remove(target);
                // Mark the entry as free
                guard.free.push(p);
                // Remove the value from the entry
                let _ = std::mem::take(&mut guard.nodes[p]);
            } else {
                continue;
            }
        }
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let mut guard = self.list.write();
        if let Some(&p) = guard.map.get(key) {
            // Fix the neighbors
            guard.unlink(p);
            // Remove from the map
            guard.map.remove(key);
            // Mark the entry as free
            guard.free.push(p);
            // Remove the value from the entry
            if let Some(old_entry) = std::mem::take(&mut guard.nodes[p]) {
                return Some(old_entry.value);
            }
        }
        None
    }

    pub fn peek(&self, key: &K) -> Option<V> {
        let guard = self.list.read();
        if let Some(&id) = guard.map.get(key) {
            if let Some(entry) = guard.nodes.get(id)? {
                return Some(entry.value.clone());
            } else {
                return None;
            }
        }
        None
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
        assert!(lru.peek(&1).is_some());

        lru.insert(2, attr);
        assert!(lru.peek(&1).is_some());
        assert!(lru.peek(&2).is_some());

        lru.insert(3, attr);
        assert!(lru.peek(&1).is_some());
        assert!(lru.peek(&2).is_some());
        assert!(lru.peek(&3).is_some());

        lru.insert(4, attr);
        assert!(lru.peek(&1).is_none());

        lru.insert(5, attr);
        assert!(lru.peek(&2).is_none());

        lru.insert(6, attr);
        assert!(lru.peek(&3).is_none());

        lru.get(&4);

        lru.insert(7, attr);
        assert!(lru.peek(&4).is_some());
        assert!(lru.peek(&5).is_none());
    }

    #[test]
    fn test_lru_promote_on_get_prevents_eviction() {
        // cap = 3
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.get(&1).is_found());

        lru.insert(4, attr);

        assert!(lru.peek(&1).is_some());
        assert!(lru.peek(&3).is_some());
        assert!(lru.peek(&4).is_some());
        assert!(lru.peek(&2).is_none());
    }

    #[test]
    fn test_lru_peek_does_not_promote() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.peek(&1).is_some());

        lru.insert(4, attr);

        assert!(lru.peek(&1).is_none());
        assert!(lru.peek(&2).is_some());
        assert!(lru.peek(&3).is_some());
        assert!(lru.peek(&4).is_some());
    }

    #[test]
    fn test_lru_eviction_order_basic() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(2);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(10, attr);
        lru.insert(20, attr);

        lru.insert(30, attr);

        assert!(lru.peek(&10).is_none());
        assert!(lru.peek(&20).is_some());
        assert!(lru.peek(&30).is_some());
    }

    #[test]
    fn test_lru_remove_unlinks_and_deletes() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.remove(&2).is_some());
        assert!(lru.peek(&2).is_none());

        lru.insert(4, attr);
        lru.insert(5, attr);
        let survivors = (lru.peek(&1).is_some() as u8) + (lru.peek(&3).is_some() as u8);
        assert_eq!(survivors, 1);
        assert!(lru.peek(&4).is_some());
    }

    #[test]
    fn test_lru_get_miss_returns_none() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(2);
        assert!(!lru.get(&9999).is_found());
        assert!(lru.peek(&9999).is_none());
        assert!(lru.remove(&9999).is_none());
    }

    #[test]
    fn test_lru_single_element_behaviour() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(1);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        assert!(lru.peek(&1).is_some());

        assert!(lru.get(&1).is_found());

        lru.insert(2, attr);
        assert!(lru.peek(&1).is_none());
        assert!(lru.peek(&2).is_some());
    }

    #[test]
    fn test_lru_promote_then_evict_correct_tail() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.get(&2).is_found());

        lru.insert(4, attr);
        assert!(lru.peek(&1).is_none());
        assert!(lru.peek(&2).is_some());
        assert!(lru.peek(&3).is_some());
        assert!(lru.peek(&4).is_some());
    }

    #[test]
    fn test_lru_repeated_get_is_idempotent_for_head() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();

        lru.insert(1, attr);
        lru.insert(2, attr);
        lru.insert(3, attr);

        assert!(lru.get(&3).is_found());
        assert!(lru.get(&3).is_found());
        assert!(lru.get(&3).is_found());

        lru.insert(4, attr);
        assert!(lru.peek(&1).is_none());
        assert!(lru.peek(&2).is_some());
        assert!(lru.peek(&3).is_some());
        assert!(lru.peek(&4).is_some());
    }

    #[test]
    fn test_lru_with_get_mut_updates_value_and_promotes() {
        let lru: LruCache<i32, i32> = LruCache::new(2);

        lru.insert(1, 10);
        // 1(10) ->
        lru.insert(2, 20);
        // 2(20) -> 1(10)

        let out = lru.with_get_mut(&1, |v| {
            *v += 5;
            *v
        });
        // 1(15) -> 2(20)
        assert_eq!(out, Some(15));
        assert_eq!(lru.peek(&1), Some(15));

        lru.insert(3, 30);
        // 3(30) -> 1(15)
        assert_eq!(lru.peek(&2), None);
        assert_eq!(lru.peek(&1), Some(15));
        assert_eq!(lru.peek(&3), Some(30));
    }

    #[test]
    fn test_lru_with_get_mut_miss_returns_none_and_does_not_insert() {
        let lru: LruCache<i32, i32> = LruCache::new(2);

        lru.insert(1, 1);
        lru.insert(2, 2);

        let called = lru.with_get_mut(&999, |v| {
            *v += 1;
            *v
        });
        assert_eq!(called, None);
        assert_eq!(lru.peek(&999), None);

        assert_eq!(lru.peek(&1), Some(1));
        assert_eq!(lru.peek(&2), Some(2));
    }

    #[test]
    fn test_lru_with_get_mut_is_idempotent_for_head() {
        let lru: LruCache<i32, i32> = LruCache::new(2);

        lru.insert(1, 100);
        lru.insert(2, 200);

        let r1 = lru.with_get_mut(&2, |v| {
            *v += 1;
            *v
        });
        let r2 = lru.with_get_mut(&2, |v| {
            *v += 1;
            *v
        });
        assert_eq!(r1, Some(201));
        assert_eq!(r2, Some(202));
        assert_eq!(lru.peek(&2), Some(202));

        lru.insert(3, 300);
        assert_eq!(lru.peek(&1), None);
        assert_eq!(lru.peek(&2), Some(202));
        assert_eq!(lru.peek(&3), Some(300));
    }

    #[test]
    fn test_lru_with_get_mut_return_value_is_bubbled_up() {
        let lru: LruCache<i32, i32> = LruCache::new(3);
        lru.insert(42, 7);

        let ret = lru.with_get_mut(&42, |v| {
            *v *= 3;
            *v % 10
        });

        assert_eq!(ret, Some(1));
        assert_eq!(lru.peek(&42), Some(21));
    }

    #[test]
    fn with_get_mut_on_tail_promotes_to_head_then_eviction_spares_it() {
        let lru: LruCache<i32, i32> = LruCache::new(3);
        lru.insert(1, 11);
        lru.insert(2, 22);
        lru.insert(3, 33);

        assert_eq!(
            lru.with_get_mut(&1, |v| {
                *v += 1;
                *v
            }),
            Some(12)
        );
        assert_eq!(lru.peek(&1), Some(12));

        lru.insert(4, 44);
        assert_eq!(lru.peek(&2), None);
        assert_eq!(lru.peek(&1), Some(12));
        assert_eq!(lru.peek(&3), Some(33));
        assert_eq!(lru.peek(&4), Some(44));

        lru.insert(5, 55);
        assert_eq!(lru.peek(&3), None);
        assert_eq!(lru.peek(&1), Some(12));
        assert_eq!(lru.peek(&4), Some(44));
        assert_eq!(lru.peek(&5), Some(55));
    }

    #[test]
    fn test_lru_change_size_in_attr() {
        let lru: LruCache<u64, FileAttr> = LruCache::new(3);
        let mut attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();
        attr.size = 10;

        lru.insert(1, attr);
        lru.with_get_mut(&1, |a| a.size = 12);
        let attr = lru.get(&1).unwrap();
        assert_eq!(attr.size, 12);
    }
}

use std::{
    collections::{HashMap, hash_map},
    num::NonZeroUsize,
    sync::Mutex,
};

struct Entry<V: Clone> {
    pub value: V,
    pub next: Option<u64>, // towards the LRU (tail)
    pub prev: Option<u64>, // towards the MRU (head)
}

impl<V: Clone> Entry<V> {
    pub fn new(value: V) -> Self {
        Self {
            value,
            next: None,
            prev: None,
        }
    }
}

struct Inner<V: Clone> {
    map: HashMap<u64, Entry<V>>,
    head: Option<u64>, // MRU
    tail: Option<u64>, // LRU
    capacity: NonZeroUsize,
}

#[allow(private_interfaces)]
pub struct LruCache<V: Clone> {
    pub list: Mutex<Inner<V>>,
}

impl<V: Clone> Inner<V> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            map: HashMap::new(),
            head: None,
            tail: None,
            capacity,
        }
    }

    /// Unlink entry from the list and re-wire former neighbors
    ///
    /// Does not remove the entry from the map
    fn unlink(&mut self, ino: u64) {
        let (prev, next, was_head, was_tail) = {
            if let Some(node) = self.map.get_mut(&ino) {
                (
                    node.prev,
                    node.next,
                    self.head == Some(ino),
                    self.tail == Some(ino),
                )
            } else {
                tracing::error!("lru unlink: {ino} not found");
                return;
            }
        };

        if let Some(p) = prev
            && let Some(e) = self.map.get_mut(&p)
        {
            e.next = next
        }

        if let Some(n) = next
            && let Some(e) = self.map.get_mut(&n)
        {
            e.prev = prev
        }

        if let Some(node) = self.map.get_mut(&ino) {
            node.prev = None;
            node.next = None;
        }

        if was_head {
            self.head = next;
        }
        if was_tail {
            self.tail = prev;
        }
    }

    /// Move an existing entry as MRU
    fn push_front_unckecked(&mut self, ino: u64) {
        let old_head = self.head;

        {
            // Move the promoted note to the head of the map
            let Some(node) = self.map.get_mut(&ino) else {
                tracing::error!("lru promote: {ino} not found");
                return;
            };
            node.prev = None;
            node.next = old_head;
            self.head = Some(ino)
        }

        // Move the node previously at the head (old_head)
        if let Some(h) = old_head {
            if let Some(head_e) = self.map.get_mut(&h) {
                head_e.prev = Some(ino)
                // head_e.next does not change
            }
        } else {
            // The list was empty. ino is the only entry
            self.tail = Some(ino)
        }
    }

    /// Insert a new entry as MRU
    fn insert_front(&mut self, ino: u64, entry: V) {
        let old_head = self.head;

        // Create the new entry
        let mut entry = Entry::new(entry);
        entry.next = old_head;
        self.map.insert(ino, entry);

        // Modify the old head
        if let Some(h) = old_head {
            if let Some(e) = self.map.get_mut(&h) {
                e.prev = Some(ino);
            }
        } else {
            // List was empty, adjust the tail
            self.tail = Some(ino)
        }

        // Modify the head
        self.head = Some(ino)
    }

    /// Evicts the LRU entry and returns it
    fn evict(&mut self) -> Option<(u64, V)> {
        let tail_key = self.tail?;

        // Get the entry prev to the tail
        let prev_key = {
            let tail_node = self.map.get(&tail_key)?;
            tail_node.prev
        };

        match prev_key {
            Some(p) => {
                // Set the prev entry as tail
                if let Some(prev) = self.map.get_mut(&p) {
                    prev.next = None;
                };
                self.tail = Some(p);
            }
            None => {
                // List is not empty
                self.head = None;
                self.tail = None;
            }
        }

        let evicted = self.map.remove(&tail_key)?;
        Some((tail_key, evicted.value))
    }

    fn peek(&self, ino: u64) -> Option<V> {
        let entry = self.map.get(&ino)?;
        Some(entry.value.clone())
    }
}

impl<V: Clone> LruCache<V> {
    pub fn new(capacity: usize) -> Self {
        if capacity == 0 {
            tracing::error!("Cache capacity must be greater than 0!")
        }
        let capacity = NonZeroUsize::new(capacity).unwrap();
        Self {
            list: Mutex::new(Inner::new(capacity)),
        }
    }

    /// Lookup an entry and return a copy of the value
    ///
    /// Promotes the value to MRU
    pub fn get(&self, ino: u64) -> Option<V> {
        let mut guard = self.list.lock().unwrap();
        if !guard.map.contains_key(&ino) {
            return None;
        };
        guard.unlink(ino);
        guard.push_front_unckecked(ino);
        guard.map.get(&ino).map(|e| e.value.clone())
    }

    pub fn with_get_mut<R>(&self, ino: u64, f: impl FnOnce(&mut V) -> R) -> Option<R> {
        let mut guard = self.list.lock().unwrap();
        if !guard.map.contains_key(&ino) {
            return None;
        }
        guard.unlink(ino);
        guard.push_front_unckecked(ino);
        let node = guard.map.get_mut(&ino).expect("present after promote");
        Some(f(&mut node.value))
    }

    /// Insert a new entry
    pub fn insert(&self, ino: u64, entry: V) -> Option<V> {
        let mut guard = self.list.lock().unwrap();
        {
            while guard.map.len() >= guard.capacity.into() {
                guard.evict()?;
            }
        }
        guard.insert_front(ino, entry);
        guard.peek(ino)
    }

    pub fn insert_many(&self, items: Vec<(u64, V)>) -> Option<()> {
        let mut guard = self.list.lock().unwrap();
        for item in items {
            let ino = item.0;
            let e = item.1;
            guard.insert_front(ino, e);
        }
        {
            while guard.map.len() >= guard.capacity.into() {
                guard.evict()?;
            }
        }
        Some(())
    }

    /// Lookup an entry without promotion
    pub fn peek(&self, ino: u64) -> Option<V> {
        let guard = self.list.lock().unwrap();
        guard.peek(ino)
    }

    /// Remove an entry if it exists
    pub fn remove(&self, ino: u64) -> Option<V> {
        let mut guard = self.list.lock().unwrap();
        match guard.map.entry(ino) {
            hash_map::Entry::Occupied(_) => {
                guard.unlink(ino);
                guard.map.remove(&ino).map(|e| e.value)
            }
            hash_map::Entry::Vacant(_) => None,
        }
    }
}

mod test {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use crate::fs::fileattr::{FileAttr, dir_attr};

    #[test]
    fn test_lru_cache_insert() {
        let lru: LruCache<FileAttr> = LruCache::new(3);
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
        let lru: LruCache<FileAttr> = LruCache::new(3);
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
        let lru: LruCache<FileAttr> = LruCache::new(3);
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
        let lru: LruCache<FileAttr> = LruCache::new(2);
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
        let lru: LruCache<FileAttr> = LruCache::new(3);
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
        let lru: LruCache<FileAttr> = LruCache::new(2);
        assert!(lru.get(9999).is_none());
        assert!(lru.peek(9999).is_none());
        assert!(lru.remove(9999).is_none());
    }

    #[test]
    fn test_lru_single_element_behaviour() {
        let lru: LruCache<FileAttr> = LruCache::new(1);
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
        let lru: LruCache<FileAttr> = LruCache::new(3);
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
        let lru: LruCache<FileAttr> = LruCache::new(3);
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
        let lru = LruCache::new(2);

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
        let lru = LruCache::new(2);

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
        let lru = LruCache::new(2);

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
        let lru = LruCache::new(3);
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
        let lru = LruCache::new(3);
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
        let lru: LruCache<FileAttr> = LruCache::new(3);
        let mut attr: FileAttr = dir_attr(crate::fs::fileattr::InoFlag::LiveRoot).into();
        attr.size = 10;

        lru.insert(1, attr);
        lru.with_get_mut(1, |a| a.size = 12);
        let attr = lru.get(1).unwrap();
        assert_eq!(attr.size, 12);
    }
}

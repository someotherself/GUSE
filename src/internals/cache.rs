use std::{
    collections::{HashMap, hash_map},
    num::NonZeroUsize,
    sync::Mutex,
};

struct Entry<V: Copy> {
    pub value: V,
    pub next: Option<u64>, // towards the LRU (tail)
    pub prev: Option<u64>, // towards the MRU (head)
}

impl<V: Copy> Entry<V> {
    pub fn new(value: V) -> Self {
        Self {
            value,
            next: None,
            prev: None,
        }
    }
}

struct Inner<V: Copy> {
    map: HashMap<u64, Entry<V>>,
    head: Option<u64>, // MRU
    tail: Option<u64>, // LRU
    capacity: NonZeroUsize,
}

#[allow(private_interfaces)]
pub struct LruCache<V: Copy> {
    pub list: Mutex<Inner<V>>,
}

impl<V: Copy> Inner<V> {
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
        Some(entry.value)
    }
}

impl<V: Copy> LruCache<V> {
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
        guard.map.get(&ino).map(|e| e.value)
    }

    pub fn with_get_mut<R>(&mut self, ino: u64, f: impl FnOnce(&mut V) -> R) -> Option<R> {
        let mut guard = self.list.lock().unwrap();
        let existed = guard.map.contains_key(&ino);
        if !existed {
            return None;
        }
        guard.unlink(ino);
        guard.push_front_unckecked(ino);
        let node = guard.map.get_mut(&ino).expect("present after promote");
        Some(f(&mut node.value))
    }

    /// Lookup an entry and promote it to MRU
    pub fn get_with_mut(&self, _ino: u64) -> Option<V> {
        todo!()
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
}

use std::collections::HashMap;

pub struct Entry<V> {
    pub value: V,
    pub next: Option<u64>,
    pub prev: Option<u64>,
}

impl<V> Entry<V> {
    pub fn new(value: V) -> Self {
        Self {
            value,
            next: None,
            prev: None,
        }
    }
}

pub struct Inner<V> {
    map: HashMap<u64, Entry<V>>,
    head: Option<u64>,
    tail: Option<u64>,
    capacity: usize,
}

struct LruCache<V> {
    list: Inner<V>,
}

impl<V> Inner<V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            head: None,
            tail: None,
            capacity,
        }
    }

    /// Lookup an entry and promote it to LRU
    pub fn get(&self, _ino: u64) -> Option<&V> {
        todo!()
    }

    /// Insert a new entry
    pub fn insert(&self, _attr: V) -> Option<&V>{
        todo!()
    }

    /// Lookup an entry without promotion
    pub fn peek(&self, _ino: u64) -> Option<&V> {
        todo!()
    }

    /// Remove an entry if it exists
    pub fn remove(&self, _ino: u64) -> Option<&V> {
        todo!()
    }

    fn unlink(&self, _ino: u64) {}
}

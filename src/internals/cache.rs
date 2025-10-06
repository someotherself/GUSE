use std::collections::HashMap;

use crate::fs::fileattr::FileAttr;

pub struct Entry {
    pub value: FileAttr,
    pub next: Option<u64>,
    pub prev: Option<u64>,
}

impl Entry {
    pub fn new(value: FileAttr) -> Self {
        Self {
            value,
            next: None,
            prev: None,
        }
    }

    pub fn get(&self, _ino: u64) -> FileAttr {
        todo!()
    }

    pub fn insert(&self, _attr: FileAttr) {
        todo!()
    }

    pub fn peek(&self, _ino: u64) -> FileAttr {
        todo!()
    }

    pub fn remove(&self, _ino: u64) -> FileAttr {
        todo!()
    }
}

pub struct Inner {
    map: HashMap<u64, Entry>,
    head: Option<u64>,
    tail: Option<u64>,
    capacity: usize,
}

struct LruCache {
    list: Inner,
}

impl Inner {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            head: None,
            tail: None,
            capacity,
        }
    }
}

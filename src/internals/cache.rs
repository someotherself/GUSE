use std::{collections::HashMap, sync::Mutex};

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
    capacity: usize,
}

#[allow(private_interfaces)]
pub struct LruCache<V: Copy> {
    pub list: Mutex<Inner<V>>,
}

impl<V: Copy> Inner<V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            head: None,
            tail: None,
            capacity,
        }
    }

    fn is_head(&self, ino: u64) -> bool {
        self.head == Some(ino)
    }

    // Remove an entry from the list and re-wire former neighbors
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

        if was_head {
            self.head = next;
        }
        if was_tail {
            self.tail = prev;
        }
    }

    /// Move an existing entry at the head
    fn promote(&mut self, ino: u64) {
        let old_head = self.head;

        {
            // Move the promoted note to the head of the map
            let Some(node) = self.map.get_mut(&ino) else {
                tracing::error!("lru promote: {ino} not found");
                return;
            };
            node.prev = None;
            node.next = old_head;
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
                self.tail = None;
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
}

impl<V: Copy> LruCache<V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            list: Mutex::new(Inner::new(capacity)),
        }
    }

    /// Lookup an entry and return a copy of the value
    ///
    /// Promotes the value to MRU
    pub fn get(&self, ino: u64) -> Option<V> {
        let mut guard = self.list.lock().unwrap();
        if guard.map.is_empty() && !guard.map.contains_key(&ino) {
            return None;
        };
        guard.unlink(ino);
        guard.promote(ino);
        guard.map.get(&ino).map(|e| e.value)
    }

    /// Lookup an entry and promote it to MRU
    pub fn get_with_mut(&self, _ino: u64) -> Option<V> {
        todo!()
    }

    /// Insert a new entry
    pub fn insert(&self, _attr: V) -> Option<V> {
        todo!()
    }

    /// Lookup an entry without promotion
    pub fn peek(&self, _ino: u64) -> Option<V> {
        todo!()
    }

    /// Remove an entry if it exists
    pub fn remove(&self, _ino: u64) -> Option<V> {
        todo!()
    }
}

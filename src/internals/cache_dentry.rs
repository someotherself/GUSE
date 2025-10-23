use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    num::NonZeroUsize,
};

use parking_lot::RwLock;

use crate::fs::fileattr::Dentry;

type NodeId = usize;

struct Entry {
    /// (`target_inode`, `target_name`)
    pub value: Dentry,
    pub next: Option<NodeId>, // towards the LRU (tail)
    pub prev: Option<NodeId>, // towards the MRU (head)
}

impl Entry {
    pub fn new(value: Dentry) -> Self {
        Self {
            value,
            next: None,
            prev: None,
        }
    }
}

pub struct DentryInner<S = ahash::RandomState> {
    target_ino_map: HashMap<u64, Vec<NodeId>, S>,
    parent_ino_name_map: HashMap<(u64, OsString), NodeId, S>,
    target_ino_name_map: HashMap<(u64, OsString), NodeId, S>,
    /// <(`target_inode`, `target_name`), Dentry>
    nodes: Vec<Entry>,
    free: Vec<NodeId>,
    head: Option<NodeId>, // MRU
    tail: Option<NodeId>, // LRU
    capacity: NonZeroUsize,
}

impl<S> DentryInner<S>
where
    S: core::hash::BuildHasher + Default,
{
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            target_ino_map: HashMap::with_hasher(S::default()),
            parent_ino_name_map: HashMap::with_hasher(S::default()),
            target_ino_name_map: HashMap::with_hasher(S::default()),
            nodes: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
            capacity,
        }
    }

    fn push_front(&mut self, id: NodeId) -> Dentry {
        let old_head = self.head;

        {
            // Fix the prev and next of the new entry
            let n = &mut self.nodes[id];
            n.prev = None;
            n.next = old_head;
        }

        // Fix the old_head entry
        if let Some(h) = old_head {
            self.nodes[h].prev = Some(id);
        } else {
            // List was empty
            self.tail = Some(id);
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

    fn unlink_all(&mut self, ids: &[NodeId]) {
        use std::collections::HashSet;

        let mut seen = HashSet::with_capacity(ids.len());
        for id in ids.iter().filter(|&id| seen.insert(id)) {
            let in_list = {
                let n = &self.nodes[*id];
                n.prev.is_some()
                    || n.next.is_some()
                    || self.head == Some(*id)
                    || self.tail == Some(*id)
            };

            if in_list {
                // Short, fresh &mut borrow per call — no overlapping borrows.
                self.unlink(*id);
            }
        }
    }

    fn evict(&mut self) -> Option<()> {
        let tail = self.tail?;

        let prev = self.nodes[tail].prev;

        if let Some(p) = prev {
            let prev_entry = &mut self.nodes[p];
            prev_entry.next = None;
            self.tail = Some(p);
        } else {
            // was empty
            self.head = None;
            self.tail = None;
        }

        let dentry = &self.nodes[tail].value;
        self.target_ino_map.remove(&dentry.target_ino);
        self.parent_ino_name_map
            .remove(&(dentry.parent_ino, dentry.target_name.clone()));
        self.target_ino_name_map
            .remove(&(dentry.target_ino, dentry.target_name.clone()));

        // Mark the entry as free
        self.free.push(tail);
        Some(())
    }

    fn insert_front(&mut self, value: Dentry) -> NodeId {
        let old_head = self.head;

        // Create new entry and fix the next tail
        let mut entry = Entry::new(value.clone());
        entry.next = old_head;

        // Get the index of the new entry.
        // Insert into nodes
        let index = if let Some(i) = self.free.pop() {
            let _ = std::mem::replace(&mut self.nodes[i], entry);
            i
        } else {
            let index = self.nodes.len() as NodeId;
            self.nodes.push(entry);
            index
        };
        if let Some(vec) = self.target_ino_map.get_mut(&value.target_ino) {
            vec.push(index);
        } else {
            self.target_ino_map.insert(value.target_ino, vec![index]);
        }
        self.parent_ino_name_map
            .insert((value.parent_ino, value.target_name.clone()), index);
        self.target_ino_name_map
            .insert((value.target_ino, value.target_name), index);

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

    // TODO: Value could be missing (maybe?)
    #[allow(dead_code)]
    fn peek(&self, id: NodeId) -> Dentry {
        self.nodes[id].value.clone()
    }
}

pub struct DentryLru<S = ahash::RandomState> {
    list: RwLock<DentryInner<S>>,
}

impl<S> DentryLru<S>
where
    S: core::hash::BuildHasher + Default,
{
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).expect("Non-zero capacity");
        Self {
            list: RwLock::new(DentryInner::new(capacity)),
        }
    }

    pub fn get_by_target(&self, key: u64) -> Option<Vec<Dentry>> {
        let mut guard = self.list.write();
        let ids = guard.target_ino_map.get(&key)?.clone();

        guard.unlink_all(&ids);
        let mut values = vec![];
        for id in ids {
            values.push(guard.push_front(id));
        }
        Some(values)
    }

    pub fn get_single_dentry(&self, target_ino: u64) -> Option<Dentry> {
        let mut guard = self.list.write();
        let mut ids = guard.target_ino_map.get(&target_ino)?.clone();

        guard.unlink_all(&ids);
        ids.pop().map(|id| guard.push_front(id))
    }

    pub fn get_by_target_and_name(&self, target_ino: u64, target_name: &OsStr) -> Option<Dentry> {
        let mut guard = self.list.write();
        let id = *guard
            .target_ino_name_map
            .get(&(target_ino, target_name.to_os_string()))?;

        guard.unlink(id);
        Some(guard.push_front(id))
    }

    pub fn get_by_parent_and_name(&self, parent_ino: u64, target_name: &OsStr) -> Option<Dentry> {
        let mut guard = self.list.write();
        let id = *guard
            .parent_ino_name_map
            .get(&(parent_ino, target_name.to_os_string()))?;

        guard.unlink(id);
        Some(guard.push_front(id))
    }

    pub fn insert(&self, value: Dentry) -> Option<Dentry> {
        let mut guard = self.list.write();
        {
            while guard.target_ino_map.len() >= guard.capacity.into() {
                guard.evict();
            }
        }

        let key = (value.target_ino, value.target_name.clone());
        if let Some(&id) = guard.target_ino_name_map.get(&key) {
            // Entry already exists
            let old = std::mem::replace(&mut guard.nodes[id].value, value);
            guard.unlink(id);
            guard.push_front(id);
            Some(old)
        } else {
            guard.insert_front(value);
            None
        }
    }

    pub fn insert_many<I>(&self, entries: I)
    where
        I: IntoIterator<Item = Dentry>,
    {
        let mut guard = self.list.write();

        for entry in entries {
            while guard.target_ino_map.len() >= guard.capacity.into() {
                guard.evict();
            }
            let key = (entry.target_ino, entry.target_name.clone());

            if let Some(&id) = guard.target_ino_name_map.get(&key) {
                let _ = std::mem::replace(&mut guard.nodes[id].value, entry);
                guard.unlink(id);
                guard.push_front(id);
            } else {
                guard.insert_front(entry);
            }
        }
    }

    /// key: (`parent_ino`, `target_name`)
    pub fn remove_by_parent(&self, parent_ino: u64, target_name: &OsStr) -> Option<Dentry> {
        let mut guard = self.list.write();

        if let Some(&p) = guard
            .parent_ino_name_map
            .get(&(parent_ino, target_name.to_os_string()))
        {
            let value = guard.nodes[p].value.clone();
            // Fix the neighbors
            guard.unlink(p);
            // Remove from the map
            guard.target_ino_map.remove(&value.target_ino);
            guard
                .target_ino_name_map
                .remove(&(value.target_ino, value.target_name.clone()));
            guard
                .parent_ino_name_map
                .remove(&(parent_ino, target_name.to_os_string()));
            // Mark the entry as free
            guard.free.push(p);
            return Some(value);
        }
        None
    }

    /// key: (`target_ino`, `target_name`)
    pub fn remove_by_target(&self, target_ino: u64, target_name: &OsStr) -> Option<Dentry> {
        let mut guard = self.list.write();

        if let Some(&p) = guard
            .parent_ino_name_map
            .get(&(target_ino, target_name.to_os_string()))
        {
            let value = guard.nodes[p].value.clone();
            // Fix the neighbors
            guard.unlink(p);
            // Remove from the map
            guard.target_ino_map.remove(&value.target_ino);
            guard
                .target_ino_name_map
                .remove(&(target_ino, target_name.to_os_string()));
            guard
                .parent_ino_name_map
                .remove(&(value.parent_ino, value.target_name.clone()));
            // Mark the entry as free
            guard.free.push(p);
            return Some(value);
        }
        None
    }

    /// key: (`target_ino`, `target_name`)
    pub fn peek(&self, target_ino: u64, target_name: &OsStr) -> Option<Dentry> {
        let guard = self.list.read();
        if let Some(&id) = guard
            .target_ino_name_map
            .get(&(target_ino, target_name.to_os_string()))
        {
            let entry = guard.nodes.get(id)?;
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn get_all_parents(&self, target_ino: u64) -> Option<Vec<u64>> {
        let guard = self.list.read();

        let ids = guard.target_ino_map.get(&target_ino)?;
        Some(
            ids.iter()
                .map(|&e| guard.nodes[e].value.parent_ino)
                .collect::<Vec<u64>>(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dentry_lru_cache_1_insert_and_remove() {
        let lru: DentryLru = DentryLru::new(3);
        let dentry1 = Dentry {
            target_ino: 1,
            parent_ino: 2,
            target_name: "aaa".into(),
            is_active: true,
        };
        // Entry with the same parent
        let dentry2 = Dentry {
            target_ino: 3,
            parent_ino: 2,
            target_name: "bbb".into(),
            is_active: true,
        };
        // Hard link with the same inode
        let dentry3 = Dentry {
            target_ino: 1,
            parent_ino: 6,
            target_name: "ccc".into(),
            is_active: true,
        };

        lru.insert(dentry1.clone());
        assert!(
            lru.peek(dentry1.clone().target_ino, &dentry1.target_name)
                .is_some()
        );

        lru.insert(dentry2.clone());
        assert!(
            lru.peek(dentry2.clone().target_ino, &dentry2.target_name)
                .is_some()
        );

        lru.insert(dentry3.clone());
        assert!(
            lru.peek(dentry3.clone().target_ino, &dentry3.target_name)
                .is_some()
        );

        let dentry_res_1_par_name = lru
            .get_by_parent_and_name(dentry1.parent_ino, &dentry1.target_name)
            .unwrap();
        assert_eq!(dentry_res_1_par_name.target_ino, dentry1.target_ino);

        let dentry_res_1_tar_name =
            lru.get_by_target_and_name(dentry1.target_ino, &dentry1.target_name);
        assert!(dentry_res_1_tar_name.is_some());

        let dentry_res_1_tar = lru.get_by_target(dentry1.target_ino).unwrap();

        assert_eq!(dentry_res_1_tar.len(), 2);
    }

    #[test]
    fn test_dentry_lru_cache_2_insert() {
        let lru: DentryLru = DentryLru::new(3);
        let dentry1 = Dentry {
            target_ino: 1,
            parent_ino: 11,
            target_name: "aaa".into(),
            is_active: true,
        };
        let dentry2 = Dentry {
            target_ino: 2,
            parent_ino: 11,
            target_name: "aaa".into(),
            is_active: true,
        };
        let dentry3 = Dentry {
            target_ino: 3,
            parent_ino: 11,
            target_name: "aaa".into(),
            is_active: true,
        };
        let dentry4 = Dentry {
            target_ino: 4,
            parent_ino: 11,
            target_name: "aaa".into(),
            is_active: true,
        };
        let dentry5 = Dentry {
            target_ino: 5,
            parent_ino: 111,
            target_name: "aaa".into(),
            is_active: true,
        };
        let dentry6 = Dentry {
            target_ino: 6,
            parent_ino: 11,
            target_name: "aaa".into(),
            is_active: true,
        };
        let dentry7 = Dentry {
            target_ino: 7,
            parent_ino: 11,
            target_name: "aaa".into(),
            is_active: true,
        };

        lru.insert(dentry1.clone());
        assert!(
            lru.peek(dentry1.clone().target_ino, &dentry1.target_name)
                .is_some()
        );
        // 1 ->

        lru.insert(dentry2.clone());
        assert!(
            lru.peek(dentry1.clone().target_ino, &dentry1.target_name)
                .is_some()
        );
        assert!(
            lru.peek(dentry2.clone().target_ino, &dentry2.target_name)
                .is_some()
        );
        // 2 -> 1

        lru.insert(dentry3.clone());
        assert!(lru.peek(dentry1.target_ino, &dentry1.target_name).is_some());
        assert!(lru.peek(dentry2.target_ino, &dentry2.target_name).is_some());
        assert!(lru.peek(dentry3.target_ino, &dentry3.target_name).is_some());
        // 3 -> 2 -> 1

        lru.insert(dentry4.clone());
        assert!(lru.peek(dentry1.target_ino, &dentry1.target_name).is_none());
        // 4 -> 3 -> 2

        lru.insert(dentry5.clone());
        assert!(lru.peek(dentry2.target_ino, &dentry2.target_name).is_none());
        // 5 -> 4 -> 3

        lru.insert(dentry6);
        assert!(lru.peek(dentry3.target_ino, &dentry3.target_name).is_none());
        // 6 -> 5 -> 4

        lru.get_by_target_and_name(dentry5.target_ino, &dentry5.target_name);
        // 5 -> 6 -> 4

        lru.insert(dentry7);
        // 7 -> 6 -> 5
        assert!(lru.peek(dentry4.target_ino, &dentry4.target_name).is_none());
        assert!(lru.peek(dentry3.target_ino, &dentry3.target_name).is_none());
    }
}

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    hash::Hash,
    io::{Read, Write},
};

use git2::Oid;

use crate::fs::repo::{RefKind, RefState};

const HEADER: [u8; 4] = [b'R', b'F', b'S', b'T'];
const VERSION: [u8; 4] = 1_u32.to_le_bytes();

pub trait BinEncode {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()>;
}

pub trait BinDecode: Sized {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self>;
}

impl BinDecode for u32 {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl BinEncode for u32 {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.to_le_bytes())
    }
}

impl BinDecode for i64 {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }
}

impl BinEncode for i64 {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.to_le_bytes())
    }
}

impl BinDecode for String {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let length = u32::bin_load(reader)?;

        let mut buf = vec![0u8; length as usize];
        reader.read_exact(&mut buf)?;

        String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

impl BinEncode for String {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = self.as_bytes();

        let len = bytes.len() as u32;
        len.bin_store(writer)?;

        writer.write_all(bytes)
    }
}

impl BinDecode for Oid {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buf = [0u8; 20];
        reader.read_exact(&mut buf)?;
        Oid::from_bytes(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

impl BinEncode for Oid {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = self.as_bytes();
        writer.write_all(bytes)
    }
}

impl<A: BinDecode, B: BinDecode> BinDecode for (A, B) {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        Ok((A::bin_load(reader)?, B::bin_load(reader)?))
    }
}

impl<A: BinEncode, B: BinEncode> BinEncode for (A, B) {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.0.bin_store(writer)?;
        self.1.bin_store(writer)?;
        Ok(())
    }
}

impl<T: BinDecode> BinDecode for Vec<T> {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let length = u32::bin_load(reader)?;
        let mut vec = Vec::with_capacity(length as usize);
        for _ in 0..length {
            vec.push(T::bin_load(reader)?);
        }
        Ok(vec)
    }
}

impl<T: BinEncode> BinEncode for Vec<T> {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let length = self.len() as u32;
        length.bin_store(writer)?;
        for val in self {
            val.bin_store(writer)?;
        }
        Ok(())
    }
}

impl<V: BinDecode + Ord> BinDecode for BTreeSet<V> {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let length = u32::bin_load(reader)?;
        let mut set = BTreeSet::new();
        for _ in 0..length {
            set.insert(V::bin_load(reader)?);
        }
        Ok(set)
    }
}

impl<V: BinEncode> BinEncode for BTreeSet<V> {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let length = self.len() as u32;
        length.bin_store(writer)?;
        for val in self {
            val.bin_store(writer)?;
        }
        Ok(())
    }
}

impl<K: BinDecode + Eq + Hash, V: BinDecode> BinDecode for HashMap<K, V> {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let length = u32::bin_load(reader)?;
        let mut map = HashMap::with_capacity(length as usize);
        for _ in 0..length {
            let key = K::bin_load(reader)?;
            let set = V::bin_load(reader)?;
            map.insert(key, set);
        }
        Ok(map)
    }
}

impl<K: BinEncode + Eq + Hash, V: BinEncode> BinEncode for HashMap<K, V> {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let length = self.len() as u32;
        length.bin_store(writer)?;

        for (key, val) in self {
            key.bin_store(writer)?;
            val.bin_store(writer)?;
        }

        Ok(())
    }
}

impl<K: BinEncode + Eq + Hash> BinEncode for HashSet<K> {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let length = self.len() as u32;
        length.bin_store(writer)?;

        for val in self {
            val.bin_store(writer)?;
        }

        Ok(())
    }
}

impl<K: BinDecode + Eq + Hash> BinDecode for HashSet<K> {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let length = u32::bin_load(reader)?;
        let mut set = HashSet::with_capacity(length as usize);
        for _ in 0..length {
            let val = K::bin_load(reader)?;
            set.insert(val);
        }
        Ok(set)
    }
}

impl RefKind {
    pub fn into_var(&self) -> [u8; 2] {
        match *self {
            Self::Branch(_) => [b'B', b'R'],
            Self::Head(_) => [b'H', b'D'],
            Self::Pr(_) => [b'P', b'R'],
            Self::PrMerge(_) => [b'P', b'M'],
            Self::Tag(_) => [b'T', b'G'],
            Self::Main(_) => [b'M', b'N'],
        }
    }

    pub fn from_var(kind: [u8; 2], string: String) -> Option<Self> {
        match kind {
            [b'B', b'R'] => Some(RefKind::Branch(string)),
            [b'H', b'D'] => Some(RefKind::Head(string)),
            [b'P', b'R'] => Some(RefKind::Pr(string)),
            [b'P', b'M'] => Some(RefKind::PrMerge(string)),
            [b'T', b'G'] => Some(RefKind::Tag(string)),
            [b'M', b'N'] => Some(RefKind::Main(string)),
            _ => None,
        }
    }
}

impl BinDecode for RefKind {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut variant = [0u8; 2];
        reader.read_exact(&mut variant)?;
        let string = String::bin_load(reader)?;
        if let Some(rfkind) = RefKind::from_var(variant, string) {
            Ok(rfkind)
        } else {
            Err(std::io::Error::other("Invalid RefKind format"))
        }
    }
}

impl BinEncode for RefKind {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let variant = self.into_var();
        writer.write_all(&variant)?;
        let string = self.as_str().to_string();
        string.bin_store(writer)?;
        Ok(())
    }
}

impl BinDecode for RefState {
    fn bin_load<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut header = [0u8; 4];
        reader.read_exact(&mut header)?;
        if header != HEADER {
            return Err(std::io::Error::other("Invalid header"));
        }

        let mut version = [0u8; 4];
        reader.read_exact(&mut version)?;
        if version != VERSION {
            return Err(std::io::Error::other("Wrong version"));
        }

        let mut fingerprint = [0u8; 32];
        reader.read_exact(&mut fingerprint)?;
        let snaps_to_ref: HashMap<Oid, BTreeSet<RefKind>> = HashMap::bin_load(reader)?;
        let refs_to_snaps: HashMap<RefKind, Vec<(i64, Oid)>> = HashMap::bin_load(reader)?;
        let unique_namespaces: HashSet<String> = HashSet::bin_load(reader)?;

        Ok(RefState {
            fingerprint,
            snaps_to_ref,
            refs_to_snaps,
            unique_namespaces,
        })
    }
}

impl BinEncode for RefState {
    fn bin_store<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let header: [u8; 4] = HEADER;
        writer.write_all(&header)?;
        let version: [u8; 4] = VERSION;
        writer.write_all(&version)?;
        writer.write_all(&self.fingerprint)?;
        self.snaps_to_ref.bin_store(writer)?;
        self.refs_to_snaps.bin_store(writer)?;
        self.unique_namespaces.bin_store(writer)?;
        Ok(())
    }
}

use std::{fmt::Display, ops::Deref};

use crate::fs::VDIR_BIT;

#[derive(Debug, Clone, Copy, Hash, PartialEq)]
pub enum Inodes {
    NormalIno(u64),
    VirtualIno(u64),
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct NormalIno(pub u64);

impl NormalIno {
    pub fn to_virt(&self) -> VirtualIno {
        VirtualIno(self | VDIR_BIT)
    }

    pub fn to_virt_u64(&self) -> u64 {
        self | VDIR_BIT
    }

    pub fn to_norm_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct VirtualIno(pub u64);

impl VirtualIno {
    pub fn to_norm(self) -> NormalIno {
        NormalIno(self.0 & !VDIR_BIT)
    }

    pub fn to_norm_u64(&self) -> u64 {
        self.0 & !VDIR_BIT
    }

    pub fn to_virt_u64(&self) -> u64 {
        self.0
    }
}

impl Inodes {
    pub fn to_norm(self) -> NormalIno {
        match self {
            Inodes::NormalIno(ino) => NormalIno(ino),
            Inodes::VirtualIno(ino) => {
                let ino = ino & !VDIR_BIT;
                NormalIno(ino)
            }
        }
    }

    pub fn to_virt(self) -> VirtualIno {
        match self {
            Inodes::NormalIno(ino) => {
                let ino = ino | VDIR_BIT;
                VirtualIno(ino)
            }
            Inodes::VirtualIno(ino) => VirtualIno(ino),
        }
    }

    pub fn to_u64_n(self) -> u64 {
        match self {
            Inodes::NormalIno(ino) | Inodes::VirtualIno(ino) => ino & !VDIR_BIT,
        }
    }

    pub fn to_u64_v(self) -> u64 {
        match self {
            Inodes::NormalIno(ino) | Inodes::VirtualIno(ino) => ino | VDIR_BIT,
        }
    }
}

impl From<u64> for Inodes {
    fn from(value: u64) -> Self {
        if (value & VDIR_BIT) != 0 {
            Inodes::VirtualIno(value)
        } else {
            Inodes::NormalIno(value)
        }
    }
}

impl From<NormalIno> for u64 {
    fn from(n: NormalIno) -> Self {
        n.0
    }
}

impl From<VirtualIno> for u64 {
    fn from(v: VirtualIno) -> Self {
        v.0
    }
}

impl From<Inodes> for u64 {
    fn from(i: Inodes) -> Self {
        match i {
            Inodes::NormalIno(ino) | Inodes::VirtualIno(ino) => ino,
        }
    }
}

impl From<&Inodes> for u64 {
    fn from(i: &Inodes) -> Self {
        match *i {
            Inodes::NormalIno(ino) | Inodes::VirtualIno(ino) => ino,
        }
    }
}

impl AsRef<u64> for Inodes {
    fn as_ref(&self) -> &u64 {
        match self {
            Inodes::NormalIno(ino) | Inodes::VirtualIno(ino) => ino,
        }
    }
}

impl std::ops::BitAnd<u64> for &Inodes {
    type Output = u64;
    fn bitand(self, rhs: u64) -> Self::Output {
        u64::from(self) & rhs
    }
}

impl std::ops::BitOr<u64> for &NormalIno {
    type Output = u64;
    fn bitor(self, rhs: u64) -> Self::Output {
        self.0 | rhs
    }
}

impl Deref for Inodes {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        match self {
            Inodes::NormalIno(ino) => ino,
            Inodes::VirtualIno(ino) => ino,
        }
    }
}

impl Display for Inodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Inodes::NormalIno(ino) | Inodes::VirtualIno(ino) => write!(f, "{ino}"),
        }
    }
}

impl Display for NormalIno {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for VirtualIno {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<u64> for Inodes {
    fn eq(&self, other: &u64) -> bool {
        match self {
            Inodes::NormalIno(ino) | Inodes::VirtualIno(ino) => ino == other,
        }
    }
}

impl PartialOrd for VirtualIno {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VirtualIno {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

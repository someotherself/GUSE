use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::{OsStrExt, OsStringExt},
};

/// Parses the file name when name is supplied as name@ or name@10
///
/// Only works when name matches a `FileType::RegularFile` and the file has a non-zero Oid
///
/// @ signals that we want a virtial directory
///
/// cd name@ will go through the commit history of the file and create
/// a virtial directory with all the blobs found.
///
/// cd name@10 will run git blame -L 10 name
pub struct NameSpec<'a> {
    pub name: &'a OsStr,
    /// None if @ is not present
    ///
    /// Ok(None) if no line number present
    ///
    /// Ok(usize) if line or range present
    pub line: Option<Option<usize>>,
}

impl<'a> NameSpec<'a> {
    #[must_use]
    pub fn parse(name: &'a OsStr) -> Self {
        let bytes = name.as_bytes();

        let Some(at) = memchr::memchr(b'@', bytes) else {
            return Self { name, line: None };
        };

        if at + 1 == bytes.len() {
            let base = OsStr::from_bytes(&bytes[..at]);
            return Self {
                name: base,
                line: Some(None),
            };
        }

        let tail = &bytes[at + 1..];
        if tail.iter().all(u8::is_ascii_digit)
            && let Ok(s) = std::str::from_utf8(tail)
            && let Ok(n) = s.parse::<usize>()
        {
            let base = OsStr::from_bytes(&bytes[..at]);
            return Self {
                name: base,
                line: Some(Some(n)),
            };
        }

        Self { name, line: None }
    }

    #[must_use]
    pub fn is_virtual(&self) -> bool {
        self.line.is_some()
    }

    #[must_use]
    pub fn line(&self) -> Option<usize> {
        self.line.flatten()
    }
}

#[must_use]
pub fn split_once_os(name: &OsStr, needle: u8) -> Option<(OsString, OsString)> {
    let bytes = name.as_bytes();
    let pos = memchr::memchr(needle, name.as_bytes())?;
    let (left, right) = bytes.split_at(pos);
    let right = &right[1..];
    Some((
        OsStr::from_bytes(left).to_os_string(),
        OsStr::from_bytes(right).to_os_string(),
    ))
}

/// Used to convert `OsString` to i32
///
/// Used by readir on the year for MONTH folders
#[must_use]
pub fn parse_i32_os(s: &OsStr) -> Option<i32> {
    let b = s.as_bytes();
    let (neg, digits) = if b.first() == Some(&b'-') {
        (true, &b[1..])
    } else {
        (false, b)
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let s = std::str::from_utf8(digits).ok()?; // no alloc
    let n: i32 = s.parse().ok()?; // parse from &str
    Some(if neg { -n } else { n })
}

/// Used to convert `OsString` to i32
///
/// Used by readir on the month for MONTH folders
#[must_use]
pub fn parse_u32_os(s: &OsStr) -> Option<u32> {
    let b = s.as_bytes();
    if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?; // no alloc
    s.parse().ok()
}

/// Used to remove characters from a filename (`OsString`)
#[must_use]
pub fn clean_name(input: &OsStr) -> OsString {
    let mut out = Vec::with_capacity(input.as_bytes().len());
    for &b in input.as_bytes() {
        match b {
            b'\n' | b'\t' => out.push(b' '),
            _ => out.push(b),
        }
    }
    OsString::from_vec(out)
}

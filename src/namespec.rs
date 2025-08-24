/// Parses the file name when name is supplied as name@ or name@10
///
/// Only works when name matches a FileType::RegularFile and the file has a non-zero Oid
///
/// @ signals that we want a virtial directory
///
/// cd name@ will go through the commit history of the file and create
/// a virtial directory with all the blobs found.
///
/// cd name@10 will run git blame -L 10 name
pub struct NameSpec<'a> {
    pub name: &'a str,
    /// None if @ is not present
    ///
    /// Ok(None) if no line number present
    ///
    /// Ok(usize) if line or range present
    pub line: Option<Option<usize>>,
}

impl<'a> NameSpec<'a> {
    pub fn parse(name: &'a str) -> Self {
        if !name.contains('@') {
            return Self { name, line: None };
        };

        if name.ends_with('@') {
            let base = name.trim_end_matches('@');
            return Self {
                name: base,
                line: Some(None),
            };
        }

        if let Some((base, tail)) = name.split_once('@')
            && let Ok(line) = tail.parse::<usize>()
        {
            return Self {
                name: base,
                line: Some(Some(line)),
            };
        }
        Self { name, line: None }
    }

    pub fn is_virtual(&self) -> bool {
        self.line.is_some()
    }

    pub fn line(&self) -> Option<usize> {
        self.line.flatten()
    }
}

use std::path::PathBuf;

use git2::Oid;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::fs::{FileAttr, FsError, FsResult, ROOT_INO};

pub struct MetaDb {
    pub conn: Connection,
}

impl MetaDb {
    // DB layout
    //   inode        INTEGER   PRIMARY KEY,    -> the u64 inode
    //   parent_inode INTEGER   NOT NULL,       -> the parent directory’s inode
    //   name         TEXT      NOT NULL,       -> the filename or directory name
    //   oid          TEXT      NOT NULL,       -> the Git OID
    //   filemode     INTEGER   NOT NULL        -> the raw Git filemode
    // nodes: Vec<(parent inode, parent name, FileAttr)>
    pub fn write_inodes_to_db(&mut self, nodes: Vec<(u64, String, FileAttr)>) -> FsResult<()> {
        let tx: Transaction<'_> = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO inode_map
            (inode, parent_inode, name, oid, filemode)
            VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;

            for (parent_inode, name, fileattr) in nodes {
                stmt.execute(params![
                    fileattr.inode as i64,
                    parent_inode as i64,
                    name,
                    fileattr.oid.to_string(),
                    fileattr.mode as i64,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_parent_ino(&self, ino: u64) -> FsResult<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_inode
                   FROM inode_map
                  WHERE inode = ?1",
        )?;

        // Execute it; fail if the row is missing
        let parent_i64: i64 = stmt.query_row(params![ino as i64], |row| row.get(0))?;

        Ok(parent_i64 as u64)
    }

    pub fn get_ino_from_db(&self, parent: u64, name: &str) -> FsResult<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT inode
               FROM inode_map
              WHERE parent_inode = ?1 AND name = ?2",
        )?;

        let ino_opt: Option<i64> = stmt
            .query_row(rusqlite::params![parent as i64, name], |row| row.get(0))
            .optional()?;
        if let Some(ino) = ino_opt {
            Ok(ino as u64)
        } else {
            Err(FsError::NotFound {
                thing: format!("inode {parent}"),
            })
        }
    }

    pub fn get_mode_from_db(&self, ino: u64) -> FsResult<i64> {
        let mut stmt = self.conn.prepare(
            "SELECT filemode
           FROM inode_map
          WHERE inode = ?1",
        )?;

        let filemode_opt: Option<i64> = stmt
            .query_row(rusqlite::params![ino as i64], |row| row.get(0))
            .optional()?;

        if let Some(filemode) = filemode_opt {
            Ok(filemode)
        } else {
            Err(FsError::NotFound {
                thing: format!("inode {ino}"),
            })
        }
    }

    pub fn get_oid_from_db(&self, ino: u64) -> FsResult<Oid> {
        let mut stmt = self.conn.prepare(
            "SELECT oid
           FROM inode_map
          WHERE inode = ?1",
        )?;

        let oid_str: Option<String> = stmt
            .query_row(rusqlite::params![ino as i64], |row| row.get(0))
            .optional()?;

        let oid_str = oid_str.ok_or_else(|| FsError::NotFound {
            thing: format!("inode {ino}"),
        })?;
        Ok(git2::Oid::from_str(&oid_str)?)
    }
    pub fn get_name_from_db(&self, ino: u64) -> FsResult<String> {
        let mut stmt = self.conn.prepare(
            "SELECT name
           FROM inode_map
          WHERE inode = ?1",
        )?;

        let name_str: Option<String> = stmt
            .query_row(rusqlite::params![ino as i64], |row| row.get(0))
            .optional()?;

        let name_str = name_str.ok_or_else(|| FsError::NotFound {
            thing: format!("inode {ino}"),
        })?;
        Ok(name_str.to_string())
    }

    pub fn get_path_from_db(&self, inode: u64) -> FsResult<PathBuf> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_inode, name
               FROM inode_map
              WHERE inode = ?1",
        )?;
        let mut components = Vec::new();
        let mut curr = inode as i64;

        loop {
            let row: Option<(i64, String)> = stmt
                .query_row(params![curr], |r| {
                    rusqlite::Result::Ok((r.get(0)?, r.get(1)?))
                })
                .optional()?;

            match row {
                Some((parent, name)) => {
                    if name == "live" {
                        curr = parent;
                        continue;
                    }
                    components.push(name);
                    curr = parent;
                }
                None => break,
            }
        }
        if components.is_empty() && inode != ROOT_INO {
            return Err(FsError::NotFound {
                thing: format!("inode {inode}"),
            });
        }

        components.reverse();

        Ok(components.iter().collect::<PathBuf>())
    }

    pub fn exists_by_name(&self, parent: u64, name: &str) -> FsResult<bool> {
        let exists: i64 = self
            .conn
            .query_row(
                "SELECT EXISTS(
             SELECT 1 FROM inode_map
              WHERE parent_inode = ?1 AND name = ?2
         )",
                params![parent as i64, name],
                |row| row.get(0),
            )
            .map_err(|_| FsError::NotFound {
                thing: format!("inode {parent}"),
            })?;
        Ok(exists != 0)
    }
}

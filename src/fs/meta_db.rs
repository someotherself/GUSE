use std::{collections::HashSet, path::PathBuf};

use anyhow::{anyhow, bail};
use git2::Oid;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::fs::{FileAttr, GitFs, ROOT_INO};

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
    pub fn write_inodes_to_db(
        &mut self,
        nodes: Vec<(u64, String, FileAttr)>,
    ) -> anyhow::Result<()> {
        let tx: Transaction<'_> = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO inode_map
            (inode, parent_inode, name, oid, filemode)
            VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;

            for (parent_inode, name, fileattr) in nodes {
                stmt.execute(params![
                    fileattr.ino as i64,
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

    pub fn populate_res_inodes(&self) -> anyhow::Result<HashSet<u64>> {
        let conn = &self.conn;
        let mut set = HashSet::new();
        let mut stmt = conn.prepare("SELECT inode FROM inode_map")?;
        let rows = stmt.query_map(params![], |row| row.get::<_, i64>(0))?;
        for r in rows {
            set.insert(r? as u64);
        }
        Ok(set)
    }

    pub fn get_parent_ino(&self, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_inode
                   FROM inode_map
                  WHERE inode = ?1",
        )?;

        // Execute it; fail if the row is missing
        let parent_i64: i64 = stmt.query_row(params![ino as i64], |row| row.get(0))?;

        Ok(parent_i64 as u64)
    }

    pub fn get_ino_from_db(&self, parent: u64, name: &str) -> anyhow::Result<u64> {
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
            bail!(format!("inode {parent} not found"))
        }
    }

    pub fn get_mode_from_db(&self, ino: u64) -> anyhow::Result<i64> {
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
            bail!(format!("inode {ino} not found"))
        }
    }

    pub fn get_oid_from_db(&self, ino: u64) -> anyhow::Result<Oid> {
        let mut stmt = self.conn.prepare(
            "SELECT oid
           FROM inode_map
          WHERE inode = ?1",
        )?;

        let oid_str: Option<String> = stmt
            .query_row(rusqlite::params![ino as i64], |row| row.get(0))
            .optional()?;

        let oid_str = oid_str.ok_or_else(|| anyhow!(format!("inode {ino} not found")))?;
        Ok(git2::Oid::from_str(&oid_str)?)
    }
    pub fn get_name_from_db(&self, ino: u64) -> anyhow::Result<String> {
        let mut stmt = self.conn.prepare(
            "SELECT name
           FROM inode_map
          WHERE inode = ?1",
        )?;

        let name_str: Option<String> = stmt
            .query_row(rusqlite::params![ino as i64], |row| row.get(0))
            .optional()?;

        let name_str = name_str.ok_or_else(|| anyhow!(format!("inode {ino} not found")))?;
        Ok(name_str.to_string())
    }

    pub fn get_repo_id(&self) -> anyhow::Result<u16> {
        let low48_mask: i64 = 0x0000_FFFF_FFFF_FFFFu64 as i64;

        // Find a "live" entry whose least-significant 48 bits == 1
        let inode: u64 = self
            .conn
            .query_row(
                r#"
            SELECT inode
            FROM inode_map
            WHERE name = ?1
              AND (inode & ?2) = 1
            LIMIT 1
            "#,
                params!["live", low48_mask],
                |row| row.get(0),
            )
            .optional()? // -> Option<u64>
            .ok_or_else(|| anyhow!("No matching live entry found"))?;

        // repo_id is the top 16 bits of the inode
        Ok((inode >> 48) as u16)
    }

    pub fn change_repo_id(&mut self, repo_id: u16) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;

        let repo_ino = GitFs::repo_id_to_ino(repo_id);
        let low48_mask: i64 = 0x0000_FFFF_FFFF_FFFFu64 as i64;

        tx.execute(
            r#"
            UPDATE inode_map
            SET inode = (?1 | (inode & ?2))
            "#,
            params![repo_ino, low48_mask],
        )?;

        tx.execute(
            r#"
            UPDATE inode_map
            SET parent_inode = (?1 | (parent_inode & ?2))
            WHERE parent_inode != 0
            "#,
            params![repo_ino, low48_mask],
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn remove_db_record(&self, ino: u64) -> anyhow::Result<()> {
        self.conn.execute(
            "DELETE FROM inode_map WHERE inode = ?1",
            params![ino as i64],
        )?;
        Ok(())
    }

    pub fn get_path_from_db(&self, ino: u64) -> anyhow::Result<PathBuf> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_inode, name
               FROM inode_map
              WHERE inode = ?1",
        )?;
        let mut components = Vec::new();
        let mut curr = ino as i64;

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
        if components.is_empty() && ino != ROOT_INO {
            bail!(format!("inode {ino} not found"))
        }

        components.reverse();

        Ok(components.iter().collect::<PathBuf>())
    }

    pub fn exists_by_name(&self, parent: u64, name: &str) -> anyhow::Result<Option<u64>> {
        let parent_i64 = i64::try_from(parent)?;
        let ino: Option<u64> = self
            .conn
            .query_row(
                r#"SELECT inode
                FROM inode_map
                WHERE parent_inode = ?1 AND name = ?2
                LIMIT 1"#,
                params![parent_i64, name],
                |row| row.get(0),
            )
            .optional()?; // None if no row
        Ok(ino)
    }
}

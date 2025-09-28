use std::{collections::HashSet, path::PathBuf};

use anyhow::{Context, anyhow, bail};
use git2::Oid;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::{
    fs::{
        GitFs, ROOT_INO,
        fileattr::{
            FileAttr, FileType, InoFlag, SetStoredAttr, StorageNode, StoredAttr,
            pair_to_system_time, try_into_filetype,
        },
        ops::readdir::DirectoryEntry,
    },
    inodes::NormalIno,
};

pub struct MetaDb {
    pub conn: Connection,
}

impl MetaDb {
    pub fn write_inodes_to_db(&mut self, nodes: Vec<StorageNode>) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;

        {
            let mut upsert_inode = tx.prepare(
                r#"
            INSERT INTO inode_map
                (inode, oid, git_mode, size, inode_flag,
                uid, gid, nlink,
                atime_secs, atime_nsecs,
                mtime_secs, mtime_nsecs,
                ctime_secs, ctime_nsecs,
                rdev, flags)
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(inode) DO UPDATE SET
                oid         = excluded.oid,
                git_mode    = excluded.git_mode,
                size        = excluded.size,
                uid         = excluded.uid,
                gid         = excluded.gid,
                atime_secs  = excluded.atime_secs,
                atime_nsecs = excluded.atime_nsecs,
                mtime_secs  = excluded.mtime_secs,
                mtime_nsecs = excluded.mtime_nsecs,
                ctime_secs  = excluded.ctime_secs,
                ctime_nsecs = excluded.ctime_nsecs,
                rdev        = excluded.rdev,
                flags       = excluded.flags
            ;
            "#,
            )?;

            let mut insert_dentry = tx.prepare(
                r#"
            INSERT INTO dentries (parent_inode, name, target_inode)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(parent_inode, name) DO UPDATE
            SET target_inode = excluded.target_inode;
            "#,
            )?;

            for node in nodes {
                let a = &node.attr;

                upsert_inode.execute(params![
                    a.ino as i64,
                    a.oid.to_string(),
                    a.git_mode as i64,
                    a.size as i64,
                    a.ino_flag as i64,
                    a.uid as i64,
                    a.gid as i64,
                    a.atime_secs,
                    a.atime_nsecs as i64,
                    a.mtime_secs,
                    a.mtime_nsecs as i64,
                    a.ctime_secs,
                    a.ctime_nsecs as i64,
                    a.rdev as i64,
                    a.flags as i64,
                ])?;

                insert_dentry.execute(params![node.parent_ino as i64, node.name, a.ino as i64,])?;
            }
        }

        tx.execute_batch(
            r#"
            UPDATE inode_map
            SET nlink = COALESCE(
                (SELECT COUNT(*) FROM dentries d WHERE d.target_inode = inode_map.inode),
                0
            );
            "#,
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn ensure_root(&mut self) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;

        tx.execute(
            r#"
        INSERT INTO inode_map
            (inode, oid, git_mode, size, inode_flag, uid, gid, atime_secs, atime_nsecs, mtime_secs, mtime_nsecs, ctime_secs, ctime_nsecs, nlink, rdev, flags)
        VALUES
            (?1, '', 0, ?2, ?3, 0, ?4, 0, 0, 0, 0, 0, 0, 1, 0, 0)
        ON CONFLICT(inode) DO NOTHING;
        "#,
            rusqlite::params![
                ROOT_INO as i64,
                InoFlag::Root as i64,
                unsafe { libc::getuid() } as i64,
                unsafe { libc::getgid() } as i64,
            ],
        )?;
        tx.commit()?;

        Ok(())
    }

    pub fn update_inodes_table(&mut self, attr: SetStoredAttr) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;

        let git_mode: u32 = tx
            .prepare("SELECT git_mode FROM inode_map WHERE inode=?1")?
            .query_row(params![attr.ino as i64], |r| r.get(0))
            .optional()?
            .ok_or_else(|| anyhow!("inode {} not found in inode_map", attr.ino))?;

        if attr.size.is_some() {
            let typ = git_mode & 0o170000;
            anyhow::ensure!(
                typ == 0o100000,
                "truncate only allowed on regular files (ino {})",
                attr.ino
            );
        }

        tx.execute(
            r#"
            UPDATE inode_map SET
                size  = COALESCE(?2, size),
                uid   = COALESCE(?3, uid),
                gid   = COALESCE(?4, gid),
                flags = COALESCE(?5, flags)
            WHERE inode = ?1
            "#,
            params![
                attr.ino as i64,
                attr.size.map(|v| v as i64),
                attr.uid.map(|v| v as i64),
                attr.gid.map(|v| v as i64),
                attr.flags.map(|v| v as i64),
            ],
        )?;
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

    pub fn clear(&self) -> anyhow::Result<()> {
        self.conn.execute("DELETE FROM inode_map", params![])?;
        self.conn.execute("DELETE FROM dentries", params![])?;
        self.conn.execute_batch("VACUUM")?;
        Ok(())
    }

    pub fn get_parent_ino(&self, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_inode
                   FROM dentries
                  WHERE target_inode = ?1",
        )?;

        // Execute it; fail if the row is missing
        let parent_i64: i64 = stmt.query_row(params![ino as i64], |row| row.get(0))?;

        Ok(parent_i64 as u64)
    }

    pub fn get_parent_name_from_ino(&self, parent_ino: u64) -> anyhow::Result<String> {
        let mut stmt = self.conn.prepare(
            "
            SELECT name
            FROM dentries
            WHERE target_inode = ?1
            LIMIT 2
        ",
        )?;

        let name_opt: Option<String> = stmt.query_row(params![parent_ino], |row| row.get(0))?;

        match name_opt {
            Some(n) => Ok(n),
            None => bail!("Parent ino {parent_ino} not found"),
        }
    }

    pub fn get_parent_name_from_child(
        &self,
        child_ino: u64,
        child_name: &str,
    ) -> anyhow::Result<(u64, String)> {
        let mut stmt = self.conn.prepare(
            "
            SELECT parent_inode
            FROM dentries
            WHERE target_inode = ?1 AND name = ?2
            LIMIT 2
        ",
        )?;

        let mut rows = stmt.query((child_ino as i64, child_name))?;
        let Some(row) = rows.next()? else {
            anyhow::bail!("No parent found for inode {child_ino} with name {child_name}");
        };

        let parent_ino: i64 = row.get(0)?;

        let mut stmt2 = self.conn.prepare(
            "
            SELECT name
            FROM dentries
            WHERE target_inode = ?1
            LIMIT 1
            ",
        )?;

        let parent_name: String = stmt2.query_row([parent_ino], |row| row.get(0))?;

        Ok((parent_ino as u64, parent_name))
    }

    pub fn get_dir_parent(&self, ino: NormalIno) -> anyhow::Result<u64> {
        let ino = ino.to_norm_u64();
        let parent: Option<i64> = self
            .conn
            .query_row(
                r#"
                SELECT parent_inode
                FROM dentries
                WHERE target_inode = ?1
                ORDER BY parent_inode
                LIMIT 1
                "#,
                [ino as i64],
                |r| r.get(0),
            )
            .optional()?;

        match parent {
            Some(p) => Ok(p as u64),
            None => Err(anyhow!("get_dir_parent: no parent found for dir ino={ino}")),
        }
    }

    pub fn get_all_parents(&self, ino: u64) -> anyhow::Result<Vec<u64>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT parent_inode
            FROM dentries
            WHERE target_inode = ?1
            ORDER BY parent_inode
            "#,
        )?;

        let rows = stmt.query_map(params![ino as i64], |r| r.get::<_, i64>(0))?;

        let mut out: Vec<u64> = Vec::new();
        for row in rows {
            let p: i64 = row?;
            out.push(u64::try_from(p)?);
        }

        out.dedup();

        Ok(out)
    }

    pub fn count_children(&self, ino: u64) -> anyhow::Result<usize> {
        let mut stmt = self.conn.prepare(
            "
            SELECT COUNT(*) 
            FROM dentries 
            WHERE parent_inode = ?1
            ",
        )?;

        let count: usize = stmt.query_row([ino], |row| row.get(0))?;
        Ok(count)
    }

    pub fn list_dentries_for_inode(&self, ino: u64) -> anyhow::Result<Vec<(u64, String)>> {
        let ino_i64 = i64::try_from(ino).context("inode u64→i64 overflow")?;

        let mut stmt = self.conn.prepare(
            r#"
            SELECT parent_inode, name
            FROM dentries
            WHERE target_inode = ?1
            ORDER BY parent_inode, name
            "#,
        )?;

        let rows = stmt
            .query_map(params![ino_i64], |row| {
                let parent_i64: i64 = row.get(0)?;
                let name: String = row.get(1)?;
                let parent_u64 = u64::try_from(parent_i64)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(parent_i64 as usize, parent_i64))?;
                Ok((parent_u64, name))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("collect dentries for inode")?;

        Ok(rows)
    }

    pub fn read_children(&self, parent_ino: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
        let sql = r#"
            SELECT d.name, d.target_inode, im.oid, im.git_mode
            FROM dentries AS d
            JOIN inode_map AS im ON im.inode = d.target_inode
            WHERE d.parent_inode = ?1
            ORDER BY d.name
        "#;

        let mut stmt = self
            .conn
            .prepare_cached(sql)
            .context("prepare read_dir_entries")?;

        let mut rows = stmt
            .query(params![parent_ino as i64])
            .context("query read_dir_entries")?;

        let mut out = Vec::new();

        while let Some(row) = rows.next().context("iterate read_dir_entries rows")? {
            let name: String = row.get(0)?;
            let child_i64: i64 = row.get(1)?;
            let oid_str: String = row.get(2)?;
            let git_mode_i64: i64 = row.get(3)?;

            let ino = u64::try_from(child_i64)
                .map_err(|_| anyhow!("child_ino out of range: {}", child_i64))?;
            let git_mode = u32::try_from(git_mode_i64)
                .map_err(|_| anyhow!("git_mode out of range: {}", git_mode_i64))?;
            let kind = match git_mode & 0o170000 {
                0o040000 => FileType::Directory,
                0o120000 => FileType::Symlink,
                _ => FileType::RegularFile,
            };

            let oid = Oid::from_str(&oid_str)
                .with_context(|| format!("invalid OID '{}' for inode {}", oid_str, ino))?;

            out.push(DirectoryEntry {
                ino,
                oid,
                name: name.into(),
                kind,
                git_mode,
            });
        }

        Ok(out)
    }

    pub fn get_single_parent(&self, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT parent_inode
            FROM dentries
            WHERE target_inode = ?1
            ORDER BY parent_inode
            LIMIT 2
            "#,
        )?;
        let mut rows = stmt.query(params![ino as i64])?;

        let first = match rows.next()? {
            Some(row) => Some(row.get::<_, i64>(0)?),
            None => None,
        };

        match first {
            None => bail!("no parent found for ino={ino}"),
            Some(p1) => Ok(u64::try_from(p1)?),
        }
    }

    pub fn get_ino_from_db(&self, parent: u64, name: &str) -> anyhow::Result<u64> {
        let sql = r#"
            SELECT target_inode
            FROM dentries
            WHERE parent_inode = ?1 AND name = ?2
            LIMIT 2
        "#;

        let mut stmt = self.conn.prepare_cached(sql)?;

        let mut rows = stmt.query((parent as i64, name))?;

        let first = rows.next()?;
        let Some(row) = first else {
            bail!("Not found: {name} under parent ino {parent}");
        };

        let child_i64: i64 = row.get(0)?;
        let child = u64::try_from(child_i64)
            .map_err(|_| anyhow!("child_ino out of range: {}", child_i64))?;

        if rows.next()?.is_some() {
            bail!("DB invariant violation: multiple dentries for ({parent}, {name})");
        }

        Ok(child)
    }

    pub fn update_size_in_db(&self, ino: u64, new_size: u64) -> anyhow::Result<()> {
        let changed = self.conn.execute(
            "UPDATE inode_map SET size = ?1 WHERE inode = ?2",
            rusqlite::params![new_size as i64, ino as i64],
        )?;

        if changed != 1 {
            bail!(
                "update_size_in_db: expected to update 1 row for ino {}, updated {}",
                ino,
                changed
            );
        }
        Ok(())
    }

    pub fn get_mode_from_db(&self, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT git_mode
           FROM inode_map
          WHERE inode = ?1",
        )?;

        let git_mode_opt: Option<i64> = stmt
            .query_row(rusqlite::params![ino as i64], |row| row.get(0))
            .optional()?;

        if let Some(git_mode) = git_mode_opt {
            Ok(git_mode as u64)
        } else {
            bail!(format!("Could not find mode for {ino}"))
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

        let oid_str = oid_str.ok_or_else(|| anyhow!(format!("Could not find Oid for {ino}")))?;
        Ok(git2::Oid::from_str(&oid_str)?)
    }

    pub fn get_ino_flag_from_db(&self, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT inode_flag
           FROM inode_map
          WHERE inode = ?1",
        )?;

        let ino_flag_opt: Option<i64> = stmt
            .query_row(rusqlite::params![ino as i64], |row| row.get(0))
            .optional()?;

        if let Some(ino_flag) = ino_flag_opt {
            Ok(ino_flag as u64)
        } else {
            bail!(format!("Could not find mode for {ino}"))
        }
    }

    pub fn get_name_from_db(&self, ino: u64) -> anyhow::Result<String> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT name
            FROM dentries
            WHERE target_inode = ?1
            "#,
        )?;

        let mut rows = stmt.query(params![ino as i64])?;

        let first = match rows.next()? {
            Some(row) => Some(row.get::<_, String>(0)?),
            None => None,
        };

        match first {
            None => bail!("No name found for ino={ino}"),
            Some(p1) => Ok(p1),
        }
    }

    pub fn get_name_in_parent(&self, parent_ino: u64, ino: u64) -> anyhow::Result<String> {
        let mut stmt = self.conn.prepare(
            r#"
        SELECT name
        FROM dentries
        WHERE parent_inode = ?1 AND target_inode = ?2
        "#,
        )?;
        let name: Option<String> = stmt
            .query_row(rusqlite::params![parent_ino as i64, ino as i64], |row| {
                row.get(0)
            })
            .optional()?;
        name.ok_or_else(|| anyhow::anyhow!("name not found for ino={ino} in parent={parent_ino}"))
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

    pub fn write_dentry(
        &mut self,
        parent_ino: u64,
        source_ino: u64,
        target_name: &str,
    ) -> anyhow::Result<()> {
        let parent_i64 = i64::try_from(parent_ino)?;
        let source_i64 = i64::try_from(source_ino)?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("begin write_dentry tx")?;

        let parent_exists: Option<i64> = tx
            .prepare("SELECT 1 FROM inode_map WHERE inode = ?1")?
            .query_row(params![parent_i64], |r| r.get(0))
            .optional()?;
        if parent_exists.is_none() {
            bail!("write_dentry: parent inode {} does not exist", parent_ino);
        }

        let target_exists: Option<i64> = tx
            .prepare("SELECT 1 FROM inode_map WHERE inode = ?1")?
            .query_row(params![source_i64], |r| r.get(0))
            .optional()?;
        if target_exists.is_none() {
            bail!("write_dentry: Source inode {} does not exist", source_ino);
        }

        let inserted = tx.execute(
            r#"
            INSERT INTO dentries (parent_inode, target_inode, name)
            VALUES (?1, ?2, ?3)
            "#,
            params![parent_i64, source_i64, target_name],
        )?;
        if inserted != 1 {
            bail!(
                "write_dentry: expected to insert 1 dentry, inserted {}",
                inserted
            );
        }

        let updated = tx.execute(
            r#"
            UPDATE inode_map
            SET nlink = (SELECT COUNT(*) FROM dentries WHERE target_inode = ?1)
            WHERE inode = ?1
            "#,
            params![source_i64],
        )?;
        if updated != 1 {
            bail!(
                "write_dentry: failed to update nlink for inode {}",
                source_ino
            );
        }

        tx.commit()?;
        Ok(())
    }

    pub fn update_db_record(
        &mut self,
        old_parent: u64,
        old_name: &str,
        node: StorageNode,
    ) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;

        {
            let a = &node.attr;
            tx.execute(
                r#"
                INSERT INTO inode_map
                    (inode, oid, git_mode, size, inode_flag, uid, gid, atime_secs, atime_nsecs, nlink, mtime_secs, mtime_nsecs, ctime_secs, ctime_nsecs, rdev, flags)
                VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                ON CONFLICT(inode) DO UPDATE SET
                    oid      = excluded.oid,
                    git_mode = excluded.git_mode,
                    size     = excluded.size,
                    uid      = excluded.uid,
                    gid      = excluded.gid,
                    atime_secs  = excluded.atime_secs,
                    atime_nsecs = excluded.atime_nsecs,
                    mtime_secs  = excluded.mtime_secs,
                    mtime_nsecs = excluded.mtime_nsecs,
                    ctime_secs  = excluded.ctime_secs,
                    ctime_nsecs = excluded.ctime_nsecs,
                    rdev     = excluded.rdev,
                    flags    = excluded.flags
                ;
                "#,
                params![
                    a.ino as i64,
                    a.oid.to_string(),
                    a.git_mode as i64,
                    a.size as i64,
                    a.ino_flag as i64,
                    a.uid as i64,
                    a.gid as i64,
                    a.atime_secs,
                    a.atime_nsecs as i64,
                    a.mtime_secs,
                    a.mtime_nsecs as i64,
                    a.ctime_secs,
                    a.ctime_nsecs as i64,
                    a.rdev as i64,
                    a.flags as i64,
                ],
            )?;
        }

        let _ = tx.execute(
            r#"
        DELETE FROM dentries
        WHERE parent_inode = ?1 AND name = ?2 AND target_inode = ?3
        "#,
            rusqlite::params![old_parent as i64, old_name, node.attr.ino as i64],
        )?;

        tx.execute(
            r#"
        INSERT INTO dentries (parent_inode, name, target_inode)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(parent_inode, name) DO UPDATE
        SET target_inode = excluded.target_inode
        "#,
            rusqlite::params![node.parent_ino as i64, node.name, node.attr.ino as i64],
        )?;

        tx.execute(
            r#"
        UPDATE inode_map
        SET nlink = (SELECT COUNT(*) FROM dentries WHERE target_inode = ?1)
        WHERE inode = ?1
        "#,
            rusqlite::params![node.attr.ino as i64],
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn remove_db_record(&mut self, parent_ino: u64, target_name: &str) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;

        let target_inode: u64 = tx
            .prepare(
                r#"
            SELECT target_inode
            FROM dentries
            WHERE parent_inode = ?1 AND name = ?2
            "#,
            )?
            .query_row(params![parent_ino as i64, target_name], |row| {
                row.get::<_, i64>(0)
            })
            .optional()?
            .map(|v| v as u64)
            .ok_or_else(|| {
                anyhow!(
                    "No such dentry: parent_ino={} name={}",
                    parent_ino,
                    target_name
                )
            })?;

        let affected = tx.execute(
            r#"
        DELETE FROM dentries
        WHERE parent_inode = ?1 AND name = ?2
        "#,
            params![parent_ino as i64, target_name],
        )?;
        if affected != 1 {
            bail!("Expected to delete exactly 1 dentry, deleted {}", affected);
        }

        let remaining_links: i64 = tx.query_row(
            r#"
        SELECT COUNT(*) FROM dentries WHERE target_inode = ?1
        "#,
            params![target_inode as i64],
            |row| row.get(0),
        )?;

        if remaining_links == 0 {
            tx.execute(
                r#"
            DELETE FROM inode_map WHERE inode = ?1
            "#,
                params![target_inode as i64],
            )?;
        } else {
            tx.execute(
                r#"
            UPDATE inode_map SET nlink = ?2 WHERE inode = ?1
            "#,
                params![target_inode as i64, remaining_links],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    // TODO: Move to fs.rs TODO
    // TODO: Move to fs.rs TODO
    // TODO: Move to fs.rs TODO
    pub fn get_path_from_db(&self, ino: u64) -> anyhow::Result<PathBuf> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_inode, name
               FROM dentries
              WHERE target_inode = ?1",
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
            bail!(format!("Could not build path for {ino}"))
        }

        components.reverse();

        Ok(components.iter().collect::<PathBuf>())
    }

    pub fn exists_by_name(&self, parent: NormalIno, name: &str) -> anyhow::Result<Option<u64>> {
        let parent = parent.to_norm_u64();
        let parent_i64 = i64::try_from(parent)?;
        let mut stmt = self.conn.prepare(
            "
            SELECT target_inode
            FROM dentries
            WHERE parent_inode = ?1 AND name = ?2",
        )?;

        let ino_i64: Option<i64> = stmt
            .query_row(params![parent_i64, name], |row| row.get(0))
            .optional()?;
        ino_i64
            .map(u64::try_from)
            .transpose()
            .context("Could not convert to u64")
    }

    pub fn get_metadata_by_name(
        &self,
        parent_ino: u64,
        child_name: &str,
    ) -> anyhow::Result<FileAttr> {
        let target_ino = self.get_ino_from_db(parent_ino, child_name)?;
        self.get_metadata(target_ino)
    }

    pub fn get_metadata(&self, target_ino: u64) -> anyhow::Result<FileAttr> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT
                inode,
                oid,
                git_mode,
                size,
                inode_flag,
                uid,
                gid,
                atime_secs,
                atime_nsecs,
                mtime_secs,
                mtime_nsecs,
                ctime_secs,
                ctime_nsecs,
                nlink,
                rdev,
                flags
            FROM inode_map
            WHERE inode = ?1
            LIMIT 1
            "#,
        )?;

        #[allow(clippy::type_complexity)]
        let (
            ino,
            oid,
            git_mode,
            size,
            inode_flag,
            uid,
            gid,
            atime_secs,
            atime_nsecs,
            mtime_secs,
            mtime_nsecs,
            ctime_secs,
            ctime_nsecs,
            nlink,
            rdev,
            flags,
        ): (
            i64,
            String,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
        ) = stmt.query_row(params![i64::try_from(target_ino)?], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
                row.get(11)?,
                row.get(12)?,
                row.get(13)?,
                row.get(14)?,
                row.get(15)?,
            ))
        })?;

        let oid = Oid::from_str(&oid)?;
        let ino_flag = u64::try_from(inode_flag)?;
        let ino_flag = InoFlag::try_from(ino_flag)?;
        let kind: FileType =
            try_into_filetype(git_mode as u64).ok_or_else(|| anyhow!("Invalid filetype"))?;
        let size = size as u64;
        let blocks = size.div_ceil(512);
        let atime = pair_to_system_time(atime_secs, atime_nsecs as i32);
        let mtime = pair_to_system_time(mtime_secs, mtime_nsecs as i32);
        let ctime = pair_to_system_time(ctime_secs, ctime_nsecs as i32);

        let perm = 0o775;

        let attr: FileAttr = FileAttr {
            ino: ino as u64,
            ino_flag,
            oid,
            size,
            blocks,
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind,
            perm,
            git_mode: git_mode as u32,
            nlink: nlink as u32,
            uid: uid as u32,
            gid: gid as u32,
            rdev: rdev as u32,
            blksize: 4096,
            flags: flags as u32,
        };

        Ok(attr)
    }

    pub fn get_storage_node_from_db(&self, ino: u64) -> anyhow::Result<StoredAttr> {
        let mut stmt = self.conn.prepare(
            r#"
        SELECT
            inode,
            oid,
            git_mode,
            size,
            inode_flag,
            uid,
            gid,
            atime,
            mtime,
            ctime,
            nlink,
            rdev,
            flags
        FROM inode_map
        WHERE inode = ?1
        LIMIT 1
        "#,
        )?;

        let row = stmt
            .query_row(params![i64::try_from(ino)?], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, i64>(12)?,
                    row.get::<_, i64>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, i64>(15)?,
                ))
            })
            .optional()?
            .ok_or_else(|| anyhow!("inode {} not found in inode_map", ino))?;

        let (
            inode_i,
            oid_str,
            git_mode_i,
            size_i,
            inode_flag_i,
            uid_i,
            gid_i,
            atime_secs,
            atime_nsecs,
            mtime_secs,
            mtime_nsecs,
            ctime_secs,
            ctime_nsecs,
            _nlink_i,
            rdev_i,
            flags_i,
        ) = row;

        let oid = Oid::from_str(&oid_str)?;

        let ino_flag_u64 = u64::try_from(inode_flag_i)?;
        let ino_flag = InoFlag::try_from(ino_flag_u64)
            .map_err(|_| anyhow!("invalid inode_flag {:#x} for inode {}", ino_flag_u64, ino))?;

        Ok(StoredAttr {
            ino: u64::try_from(inode_i)?,
            ino_flag,
            oid,
            size: u64::try_from(size_i)?,
            git_mode: u32::try_from(git_mode_i)?,
            uid: u32::try_from(uid_i)?,
            gid: u32::try_from(gid_i)?,
            atime_secs,
            atime_nsecs: i32::try_from(atime_nsecs)?,
            mtime_secs,
            mtime_nsecs: i32::try_from(mtime_nsecs)?,
            ctime_secs,
            ctime_nsecs: i32::try_from(ctime_nsecs)?,
            rdev: u32::try_from(rdev_i)?,
            flags: u32::try_from(flags_i)?,
        })
    }
}

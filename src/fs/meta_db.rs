use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow, bail};
use crossbeam_channel::{Receiver, Sender};
use git2::Oid;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OpenFlags, OptionalExtension, TransactionBehavior, params};

use crate::{
    fs::{
        ROOT_INO,
        fileattr::{
            FileAttr, FileType, InoFlag, SetStoredAttr, StorageNode, pair_to_system_time,
            try_into_filetype,
        },
        ops::readdir::DirectoryEntry,
    },
    inodes::NormalIno,
};

pub struct MetaDb {
    pub ro_pool: Pool<SqliteConnectionManager>,
    pub writer_tx: Sender<DbWriteMsg>,
}

pub fn set_wal_once(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let mode: String = conn.query_row("PRAGMA journal_mode=WAL;", [], |r| r.get(0))?;
    if mode.to_lowercase() != "wal" {
        return Err(rusqlite::Error::ExecuteReturnedResults);
    }
    Ok(())
}

// https://github.com/the-lean-crate/criner/issues/1#issue-577429787
pub fn set_conn_pragmas(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;
        PRAGMA temp_store=MEMORY;
        PRAGMA journal_size_limit = 67108864; -- 64 megabytes
        PRAGMA mmap_size = 134217728; -- 128 megabytes
        PRAGMA cache_size=-20000;
        PRAGMA wal_autocheckpoint=1000;
        PRAGMA read_uncommitted=OFF;
        PRAGMA busy_timeout = 5000
    "#,
    )?;

    Ok(())
}

pub fn new_repo_db<P: AsRef<Path>>(db_path: P) -> anyhow::Result<std::sync::Arc<MetaDb>> {
    let ro_mgr = SqliteConnectionManager::file(&db_path)
        .with_flags(OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI)
        .with_init(|c| set_conn_pragmas(c));

    let ro_pool = Pool::builder()
        .max_size(12_u32)
        .min_idle(Some(2))
        .build(ro_mgr)?;

    let writer = rusqlite::Connection::open(&db_path)?;
    set_conn_pragmas(&writer)?;

    let (writer_tx, _) = spawn_repo_writer(db_path.as_ref().to_path_buf())?;

    Ok(std::sync::Arc::new(MetaDb { ro_pool, writer_tx }))
}

pub type Resp<T> = crossbeam_channel::Sender<anyhow::Result<T>>;

/// Creates a one-shot channel for sending a single `anyhow::Result<T>` response.
///
/// Returns a `(Sender, Receiver)` pair backed by a bounded crossbeam channel of size 1.
pub fn oneshot<T>() -> (Resp<T>, crossbeam_channel::Receiver<anyhow::Result<T>>) {
    crossbeam_channel::bounded(1)
}

pub enum DbWriteMsg {
    EnsureRoot {
        resp: Resp<()>,
    },
    WriteDentry {
        parent_ino: NormalIno,
        target_ino: NormalIno,
        target_name: OsString,
        resp: Resp<()>,
    },
    /// Send and forget
    WriteInodes {
        nodes: Vec<StorageNode>,
        resp: Option<Resp<()>>,
    },
    UpdateMetadata {
        attr: SetStoredAttr,
        resp: Resp<()>,
    },
    /// Send and forget
    UpdateSize {
        ino: NormalIno,
        size: u64,
        resp: Resp<()>,
    },
    /// Send and forget
    UpdateRecord {
        old_parent: NormalIno,
        old_name: OsString,
        node: StorageNode,
        resp: Option<Resp<()>>,
    },
    /// Send and forget
    RemoveDentry {
        parent_ino: NormalIno,
        target_name: OsString,
        resp: Option<Resp<()>>,
    },
    /// Send and forget
    CleanupEntry {
        target_ino: NormalIno,
        resp: Option<Resp<()>>,
    },
}

fn spawn_repo_writer(
    db_path: PathBuf,
) -> anyhow::Result<(Sender<DbWriteMsg>, std::thread::JoinHandle<()>)> {
    let (tx, rx): (Sender<DbWriteMsg>, Receiver<DbWriteMsg>) = crossbeam_channel::unbounded();

    let handle = std::thread::Builder::new()
        .name(format!("db-writer-{}", db_path.display()))
        .spawn(move || {
            let mut conn = match rusqlite::Connection::open(&db_path) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Writer open failed: {e}");
                    return;
                }
            };
            if let Err(e) = set_conn_pragmas(&conn) {
                tracing::error!("Writer PRAGMA failed: {e}");
                return;
            }

            while let Ok(first) = rx.recv() {
                if let Err(e) = (|| -> anyhow::Result<()> {
                    let tx_sql = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

                    let mut acks: Vec<crossbeam_channel::Sender<anyhow::Result<()>>> = Vec::new();

                    apply_msg(&tx_sql, first, &mut acks)?;

                    for _ in 0..16 {
                        match rx.try_recv() {
                            Ok(m) => apply_msg(&tx_sql, m, &mut acks)?,
                            Err(crossbeam_channel::TryRecvError::Empty) => break,
                            Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                        }
                    }

                    tx_sql.commit()?;

                    for r in acks {
                        let _ = r.send(Ok(()));
                    }
                    Ok(())
                })() {
                    tracing::error!("Writer failed: {e}");
                }
            }
        })?;

    Ok((tx, handle))
}

fn apply_msg<C>(
    conn: &C,
    msg: DbWriteMsg,
    results: &mut Vec<crossbeam_channel::Sender<anyhow::Result<()>>>,
) -> anyhow::Result<()>
where
    C: std::ops::Deref<Target = rusqlite::Connection>,
{
    match msg {
        DbWriteMsg::EnsureRoot { resp } => {
            MetaDb::ensure_root(conn).map(|_| ())?;
            results.push(resp);
            Ok(())
        }
        DbWriteMsg::WriteDentry {
            parent_ino,
            target_ino,
            target_name,
            resp,
        } => {
            MetaDb::write_dentry(conn, parent_ino.into(), target_ino.into(), &target_name)?;
            results.push(resp);
            Ok(())
        }
        DbWriteMsg::WriteInodes { nodes, resp } => {
            let res = MetaDb::write_inodes_to_db(conn, nodes);
            match resp {
                Some(tx) => {
                    results.push(tx);
                    Ok(())
                }
                None => {
                    if let Err(e) = &res {
                        tracing::warn!("db write_inodes_to_db failed: {e}");
                    }
                    Ok(())
                }
            }
        }
        DbWriteMsg::UpdateMetadata { attr, resp } => {
            MetaDb::update_inodes_table(conn, attr)?;
            results.push(resp);
            Ok(())
        }
        DbWriteMsg::UpdateSize { ino, size, resp } => {
            MetaDb::update_size_in_db(conn, ino.into(), size)?;
            results.push(resp);
            Ok(())
        }
        DbWriteMsg::UpdateRecord {
            old_parent,
            old_name,
            node,
            resp,
        } => {
            let res = MetaDb::update_db_record(conn, old_parent.into(), &old_name, node);
            match resp {
                Some(tx) => {
                    results.push(tx);
                    Ok(())
                }
                None => {
                    if let Err(e) = &res {
                        tracing::warn!("db update_db_record failed: {e}");
                    }
                    Ok(())
                }
            }
        }
        DbWriteMsg::RemoveDentry {
            parent_ino,
            target_name,
            resp,
        } => {
            let res = MetaDb::remove_db_dentry(conn, parent_ino.into(), &target_name);
            match resp {
                Some(tx) => {
                    results.push(tx);
                    Ok(())
                }
                None => {
                    if let Err(e) = &res {
                        tracing::warn!("db remove_db_dentry failed: {e}");
                    }
                    Ok(())
                }
            }
        }
        DbWriteMsg::CleanupEntry { target_ino, resp } => {
            let res = MetaDb::cleanup_dentry(conn, target_ino.into());
            match resp {
                Some(tx) => {
                    results.push(tx);
                    Ok(())
                }
                None => {
                    if let Err(e) = &res {
                        tracing::warn!("db cleanup_dentry failed: {e}");
                    }
                    Ok(())
                }
            }
        }
    }
}

impl MetaDb {
    pub fn write_inodes_to_db<C>(tx: &C, nodes: Vec<StorageNode>) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
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
            (?1, ?2, ?3, ?4, ?5,
             ?6, ?7, 0,
             ?8, ?9,
             ?10, ?11,
             ?12, ?13,
             ?14, ?15)
        ON CONFLICT(inode) DO UPDATE SET
            oid         = excluded.oid,
            git_mode    = excluded.git_mode,
            size        = excluded.size,
            inode_flag  = excluded.inode_flag,
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

        let mut affected: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();

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

            insert_dentry.execute(params![
                node.parent_ino as i64,
                node.name.as_bytes(),
                a.ino as i64,
            ])?;

            affected.insert(a.ino as i64);
        }

        let mut upd = tx.prepare(
            "UPDATE inode_map
         SET nlink = (SELECT COUNT(*) FROM dentries d WHERE d.target_inode = inode_map.inode)
         WHERE inode = ?1",
        )?;
        for ino in affected {
            upd.execute(params![ino])?;
        }

        Ok(())
    }

    pub fn ensure_root<C>(tx: &C) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
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

        Ok(())
    }

    pub fn update_inodes_table<C>(tx: &C, attr: SetStoredAttr) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        tx.execute(
            r#"
            UPDATE inode_map SET
                size        = COALESCE(:size, size),
                uid         = COALESCE(:uid, uid),
                gid         = COALESCE(:gid, gid),
                flags       = COALESCE(:flags, flags),
                atime_secs  = COALESCE(:atime_s, atime_secs),
                atime_nsecs = COALESCE(:atime_ns, atime_nsecs),
                mtime_secs  = COALESCE(:mtime_s, mtime_secs),
                mtime_nsecs = COALESCE(:mtime_ns, mtime_nsecs)
            WHERE inode = :ino
            "#,
            rusqlite::named_params! {
                ":ino":       attr.ino as i64,
                ":size":      attr.size.map(|v| v as i64),
                ":uid":       attr.uid.map(|v| v as i64),
                ":gid":       attr.gid.map(|v| v as i64),
                ":flags":     attr.flags.map(|v| v as i64),
                ":atime_s":   attr.atime_secs,
                ":atime_ns":  attr.atime_nsecs.map(|v| v as i64),
                ":mtime_s":   attr.mtime_secs,
                ":mtime_ns":  attr.mtime_nsecs.map(|v| v as i64),
            },
        )?;
        Ok(())
    }

    pub fn populate_res_inodes(conn: &rusqlite::Connection) -> anyhow::Result<HashSet<u64>> {
        let mut set = HashSet::new();
        let mut stmt = conn.prepare("SELECT inode FROM inode_map")?;
        let rows = stmt.query_map(params![], |row| row.get::<_, i64>(0))?;
        for r in rows {
            set.insert(r? as u64);
        }
        Ok(set)
    }

    pub fn clear(conn: &rusqlite::Connection) -> anyhow::Result<()> {
        conn.execute("DELETE FROM inode_map", params![])?;
        conn.execute("DELETE FROM dentries", params![])?;
        conn.execute_batch("VACUUM")?;
        Ok(())
    }

    pub fn get_parent_ino(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = conn.prepare(
            "SELECT parent_inode
                   FROM dentries
                  WHERE target_inode = ?1",
        )?;

        // Execute it; fail if the row is missing
        let parent_i64: i64 = stmt.query_row(params![ino as i64], |row| row.get(0))?;

        Ok(parent_i64 as u64)
    }

    pub fn get_parent_name_from_ino(
        conn: &rusqlite::Connection,
        parent_ino: u64,
    ) -> anyhow::Result<OsString> {
        let mut stmt = conn.prepare(
            "
            SELECT name
            FROM dentries
            WHERE target_inode = ?1
            LIMIT 2
        ",
        )?;

        let name_opt: Option<Vec<u8>> = stmt.query_row(params![parent_ino], |row| row.get(0))?;

        match name_opt {
            Some(n) => Ok(OsString::from_vec(n)),
            None => bail!("Parent ino {parent_ino} not found"),
        }
    }

    pub fn get_parent_name_from_child(
        conn: &rusqlite::Connection,
        child_ino: u64,
        child_name: &OsStr,
    ) -> anyhow::Result<(u64, OsString)> {
        let mut stmt = conn.prepare(
            "
            SELECT parent_inode
            FROM dentries
            WHERE target_inode = ?1 AND name = ?2
            LIMIT 2
        ",
        )?;

        let mut rows = stmt.query((child_ino as i64, child_name.as_bytes()))?;
        let Some(row) = rows.next()? else {
            anyhow::bail!(
                "No parent found for inode {child_ino} with name {}",
                child_name.display()
            );
        };

        let parent_ino: i64 = row.get(0)?;

        let mut stmt2 = conn.prepare(
            "
            SELECT name
            FROM dentries
            WHERE target_inode = ?1
            LIMIT 1
            ",
        )?;

        let parent_name: Vec<u8> = stmt2.query_row([parent_ino], |row| row.get(0))?;

        Ok((parent_ino as u64, OsString::from_vec(parent_name)))
    }

    pub fn get_dir_parent(conn: &rusqlite::Connection, ino: NormalIno) -> anyhow::Result<u64> {
        let ino = ino.to_norm_u64();
        let parent: Option<i64> = conn
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

    pub fn get_all_parents(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<Vec<u64>> {
        let mut stmt = conn.prepare(
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

    pub fn count_children(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<usize> {
        let mut stmt = conn.prepare(
            "
            SELECT COUNT(*) 
            FROM dentries 
            WHERE parent_inode = ?1
            ",
        )?;

        let count: usize = stmt.query_row([ino], |row| row.get(0))?;
        Ok(count)
    }

    pub fn list_dentries_for_inode(
        conn: &rusqlite::Connection,
        ino: u64,
    ) -> anyhow::Result<Vec<(u64, OsString)>> {
        let ino_i64 = i64::try_from(ino).context("inode u64→i64 overflow")?;

        let mut stmt = conn.prepare(
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
                let name = OsString::from_vec(row.get(1)?);
                let parent_u64 = u64::try_from(parent_i64).map_err(|_| {
                    rusqlite::Error::IntegralValueOutOfRange(parent_i64 as usize, parent_i64)
                })?;
                Ok((parent_u64, name))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("collect dentries for inode")?;

        Ok(rows)
    }

    pub fn read_children(
        conn: &rusqlite::Connection,
        parent_ino: u64,
    ) -> anyhow::Result<Vec<DirectoryEntry>> {
        let sql = r#"
            SELECT d.name, d.target_inode, im.oid, im.git_mode
            FROM dentries AS d
            JOIN inode_map AS im ON im.inode = d.target_inode
            WHERE d.parent_inode = ?1
            ORDER BY d.name
        "#;

        let mut stmt = conn
            .prepare_cached(sql)
            .context("prepare read_dir_entries")?;

        let mut rows = stmt
            .query(params![parent_ino as i64])
            .context("query read_dir_entries")?;

        let mut out = Vec::new();

        while let Some(row) = rows.next().context("iterate read_dir_entries rows")? {
            let name = OsString::from_vec(row.get(0)?);
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
                name,
                kind,
                git_mode,
            });
        }

        Ok(out)
    }

    pub fn get_single_parent(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = conn.prepare(
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

    pub fn get_ino_from_db(
        conn: &rusqlite::Connection,
        parent: u64,
        name: &OsStr,
    ) -> anyhow::Result<u64> {
        let sql = r#"
            SELECT target_inode
            FROM dentries
            WHERE parent_inode = ?1 AND name = ?2
            LIMIT 2
        "#;

        let mut stmt = conn.prepare_cached(sql)?;

        let mut rows = stmt.query((parent as i64, name.as_bytes()))?;

        let first = rows.next()?;
        let Some(row) = first else {
            bail!("Not found: {} under parent ino {parent}", name.display());
        };

        let child_i64: i64 = row.get(0)?;
        let child = u64::try_from(child_i64)
            .map_err(|_| anyhow!("child_ino out of range: {}", child_i64))?;

        if rows.next()?.is_some() {
            bail!(
                "DB invariant violation: multiple dentries for ({parent}, {})",
                name.display()
            );
        }

        Ok(child)
    }

    pub fn update_size_in_db<C>(tx: &C, ino: u64, new_size: u64) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        let changed = tx.execute(
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

    pub fn get_mode_from_db(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = conn.prepare(
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

    pub fn get_oid_from_db(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<Oid> {
        let mut stmt = conn.prepare(
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

    pub fn inode_exists(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<bool> {
        let exists: i64 = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM inode_map WHERE inode = ?1)",
            [ino as i64],
            |row| row.get(0),
        )?;
        Ok(exists != 0)
    }

    pub fn get_ino_flag_from_db(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<u64> {
        let mut stmt = conn.prepare(
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
            bail!(format!("Could not find {ino} - ino_flag"))
        }
    }

    pub fn get_name_from_db(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<OsString> {
        let mut stmt = conn.prepare(
            r#"
            SELECT name
            FROM dentries
            WHERE target_inode = ?1
            "#,
        )?;

        let mut rows = stmt.query(params![ino as i64])?;

        let first = match rows.next()? {
            Some(row) => Some(row.get::<_, Vec<u8>>(0)?),
            None => None,
        };

        match first {
            None => bail!("No name found for ino={ino}"),
            Some(p1) => Ok(OsString::from_vec(p1)),
        }
    }

    pub fn get_name_in_parent(
        conn: &rusqlite::Connection,
        parent_ino: u64,
        ino: u64,
    ) -> anyhow::Result<OsString> {
        let mut stmt = conn.prepare(
            r#"
        SELECT name
        FROM dentries
        WHERE parent_inode = ?1 AND target_inode = ?2
        "#,
        )?;
        let name_opt: Option<Vec<u8>> = stmt
            .query_row(rusqlite::params![parent_ino as i64, ino as i64], |row| {
                row.get(0)
            })
            .optional()?;

        match name_opt {
            Some(n) => Ok(OsString::from_vec(n)),
            None => bail!("name not found for ino={ino} in parent={parent_ino}"),
        }
    }

    pub fn write_dentry<C>(
        tx: &C,
        parent_ino: u64,
        source_ino: u64,
        target_name: &OsStr,
    ) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        let parent_i64 = i64::try_from(parent_ino)?;
        let source_i64 = i64::try_from(source_ino)?;

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
            params![parent_i64, source_i64, target_name.as_bytes()],
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

        Ok(())
    }

    pub fn update_db_record<C>(
        tx: &C,
        old_parent: u64,
        old_name: &OsStr,
        node: StorageNode,
    ) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        {
            let a = &node.attr;
            tx.execute(
                r#"
                INSERT INTO inode_map
                    (inode, oid, git_mode, size, inode_flag, uid, gid, nlink, atime_secs, atime_nsecs, mtime_secs, mtime_nsecs, ctime_secs, ctime_nsecs, rdev, flags)
                VALUES
                    (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                ON CONFLICT(inode) DO UPDATE SET
                    oid      = excluded.oid,
                    git_mode = excluded.git_mode,
                    size     = excluded.size,
                    inode_flag = excluded.inode_flag,
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
            rusqlite::params![old_parent as i64, old_name.as_bytes(), node.attr.ino as i64],
        )?;

        tx.execute(
            r#"
        INSERT INTO dentries (parent_inode, name, target_inode)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(parent_inode, name) DO UPDATE
        SET target_inode = excluded.target_inode
        "#,
            rusqlite::params![
                node.parent_ino as i64,
                node.name.as_bytes(),
                node.attr.ino as i64
            ],
        )?;

        tx.execute(
            r#"
        UPDATE inode_map
        SET nlink = (SELECT COUNT(*) FROM dentries WHERE target_inode = ?1)
        WHERE inode = ?1
        "#,
            rusqlite::params![node.attr.ino as i64],
        )?;

        Ok(())
    }

    /// Will only remove the dentry and decrement the nlink in inode_map
    ///
    /// Record is removed from inode_map when there are no more open file handles
    /// (see [`crate::fs::GitFs::release`])
    pub fn remove_db_dentry<C>(tx: &C, parent_ino: u64, target_name: &OsStr) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        let target_inode: u64 = tx
            .prepare(
                r#"
            SELECT target_inode
            FROM dentries
            WHERE parent_inode = ?1 AND name = ?2
            "#,
            )?
            .query_row(params![parent_ino as i64, target_name.as_bytes()], |row| {
                row.get::<_, i64>(0)
            })
            .optional()?
            .map(|v| v as u64)
            .ok_or_else(|| {
                anyhow!(
                    "No such dentry: parent_ino={} name={}",
                    parent_ino,
                    target_name.display()
                )
            })?;

        tx.execute(
            r#"
        DELETE FROM dentries
        WHERE parent_inode = ?1 AND name = ?2
        "#,
            params![parent_ino as i64, target_name.as_bytes()],
        )?;

        tx.execute(
            r#"
            UPDATE inode_map
            SET nlink = (
                SELECT COUNT(*)
                FROM dentries
                WHERE target_inode = ?1
            )
            WHERE inode = ?1
            "#,
            params![target_inode as i64],
        )?;

        Ok(())
    }

    pub fn cleanup_dentry<C>(tx: &C, target_ino: u64) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        let remaining_links: i64 = tx.query_row(
            r#"
        SELECT COUNT(*)
        FROM dentries
        WHERE target_inode = ?1
        "#,
            params![target_ino as i64],
            |row| row.get(0),
        )?;

        if remaining_links == 0 {
            tx.execute(
                r#"
            DELETE FROM inode_map
            WHERE inode = ?1
            "#,
                params![target_ino as i64],
            )
            .context("Failed to delete from inode_map")?;
        }

        Ok(())
    }

    // TODO: Move to fs.rs TODO
    // TODO: Move to fs.rs TODO
    // TODO: Move to fs.rs TODO
    pub fn get_path_from_db(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<PathBuf> {
        let mut stmt = conn.prepare(
            "SELECT parent_inode, name
               FROM dentries
              WHERE target_inode = ?1",
        )?;
        let mut components = Vec::new();
        let mut curr = ino as i64;

        loop {
            let row: Option<(i64, OsString)> = stmt
                .query_row(params![curr], |r| {
                    rusqlite::Result::Ok((r.get(0)?, OsString::from_vec(r.get(1)?)))
                })
                .optional()?;

            match row {
                Some((parent, name)) => {
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

    pub fn exists_by_name(
        conn: &rusqlite::Connection,
        parent: NormalIno,
        name: &OsStr,
    ) -> anyhow::Result<Option<u64>> {
        let parent = parent.to_norm_u64();
        let parent_i64 = i64::try_from(parent)?;
        let name_blob = name.as_bytes();
        let mut stmt = conn.prepare(
            "
            SELECT target_inode
            FROM dentries
            WHERE parent_inode = ?1 AND name = ?2",
        )?;

        let ino_i64: Option<i64> = stmt
            .query_row(params![parent_i64, name_blob], |row| row.get(0))
            .optional()?;
        ino_i64
            .map(u64::try_from)
            .transpose()
            .context("Could not convert to u64")
    }

    pub fn get_metadata_by_name(
        conn: &rusqlite::Connection,
        parent_ino: u64,
        child_name: &OsStr,
    ) -> anyhow::Result<FileAttr> {
        let target_ino = MetaDb::get_ino_from_db(conn, parent_ino, child_name)?;
        MetaDb::get_metadata(conn, target_ino)
    }

    pub fn get_metadata(conn: &rusqlite::Connection, target_ino: u64) -> anyhow::Result<FileAttr> {
        let mut stmt = conn.prepare(
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
}

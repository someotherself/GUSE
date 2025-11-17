use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail};
use crossbeam_channel::{Receiver, Sender};
use git2::Oid;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{OpenFlags, OptionalExtension, TransactionBehavior, params};

use crate::{
    fs::{
        ROOT_INO,
        fileattr::{
            FileAttr, FileType, InoFlag, SetFileAttr, pair_to_system_time, system_time_to_pair,
            try_into_filetype_u32,
        },
        ops::readdir::{BuildCtxMetadata, DirectoryEntry},
        repo,
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
        .max_size(num_cpus::get() as u32 * 2)
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
    WriteInodes {
        nodes: Vec<FileAttr>,
        resp: Option<Resp<()>>,
    },
    UpdateMetadata {
        attr: SetFileAttr,
        resp: Option<Resp<()>>,
    },
    UpdateSize {
        ino: NormalIno,
        size: u64,
        resp: Option<Resp<()>>,
    },
    UpdateRecord {
        old_parent: NormalIno,
        old_name: OsString,
        node: FileAttr,
        resp: Option<Resp<()>>,
    },
    RemoveEntry {
        parent_ino: NormalIno,
        target_name: OsString,
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

                    for _ in 0..24 {
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
            let res = MetaDb::update_inodes_table(conn, attr);
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
        DbWriteMsg::UpdateSize { ino, size, resp } => {
            let res = MetaDb::update_size_in_db(conn, ino.into(), size);
            match resp {
                Some(tx) => {
                    results.push(tx);
                    Ok(())
                }
                None => {
                    if let Err(e) = &res {
                        tracing::warn!("db update_size_in_db failed: {e}");
                    }
                    Ok(())
                }
            }
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
        DbWriteMsg::RemoveEntry {
            parent_ino,
            target_name,
            resp,
        } => {
            let res = MetaDb::remove_db_entry(conn, parent_ino.into(), &target_name);
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
    }
}

impl MetaDb {
    pub fn write_inodes_to_db<C>(tx: &C, nodes: Vec<FileAttr>) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        let mut upsert_inode = tx.prepare(
            r#"
        INSERT INTO inode_map
            (inode, parent_inode, name, oid, git_mode,
             size, inode_flag, uid, gid,
             atime_secs, atime_nsecs,
             mtime_secs, mtime_nsecs,
             ctime_secs, ctime_nsecs,
             rdev, flags, uuid)
        VALUES
            (?1, ?2, ?3, ?4, ?5,
             ?6, ?7, ?8, ?9,
             ?10, ?11,
             ?12, ?13,
             ?14, ?15,
             ?16, ?17, ?18)
        ON CONFLICT(inode) DO UPDATE SET
            parent_inode = excluded.parent_inode,
            name         = excluded.name,
            oid          = excluded.oid,
            git_mode     = excluded.git_mode,
            size         = excluded.size,
            inode_flag   = excluded.inode_flag,
            uid          = excluded.uid,
            gid          = excluded.gid,
            atime_secs   = excluded.atime_secs,
            atime_nsecs  = excluded.atime_nsecs,
            mtime_secs   = excluded.mtime_secs,
            mtime_nsecs  = excluded.mtime_nsecs,
            ctime_secs   = excluded.ctime_secs,
            ctime_nsecs  = excluded.ctime_nsecs,
            rdev         = excluded.rdev,
            flags        = excluded.flags,
            uuid         = excluded.uuid
        ;
        "#,
        )?;

        for a in nodes {
            let (atime_secs, atime_nsecs) = system_time_to_pair(a.atime);
            let (mtime_secs, mtime_nsecs) = system_time_to_pair(a.mtime);
            let (ctime_secs, ctime_nsecs) = system_time_to_pair(a.ctime);

            upsert_inode.execute(rusqlite::params![
                a.ino as i64,
                a.parent_ino as i64,
                a.name.as_bytes(),
                a.oid.to_string(),
                a.git_mode as i64,
                a.size as i64,
                a.ino_flag as i64,
                a.uid as i64,
                a.gid as i64,
                atime_secs,
                atime_nsecs,
                mtime_secs,
                mtime_nsecs,
                ctime_secs,
                ctime_nsecs,
                a.rdev as i64,
                a.flags as i64,
                a.uuid,
            ])?;
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
            (inode, parent_inode, name, oid, git_mode,
             size, inode_flag, uid, gid,
             atime_secs, atime_nsecs,
             mtime_secs, mtime_nsecs,
             ctime_secs, ctime_nsecs,
             rdev, flags, uuid)
        VALUES
            (?1, ?2, ?3, '', 0,
             0, ?4, ?5, ?6,
             0, 0,
             0, 0,
             0, 0,
             0, 0, '')
        ON CONFLICT(inode) DO NOTHING;
        "#,
            rusqlite::params![
                ROOT_INO as i64,
                ROOT_INO as i64,
                b"ROOT" as &[u8],
                InoFlag::Root as i64,
                unsafe { libc::getuid() } as i64,
                unsafe { libc::getgid() } as i64,
            ],
        )?;

        Ok(())
    }

    pub fn update_inodes_table<C>(tx: &C, attr: SetFileAttr) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        let (atime_secs, atime_nsecs) = match attr.atime {
            Some(atime) => {
                let (s, n) = system_time_to_pair(atime);
                (Some(s), Some(n))
            }
            None => (None, None),
        };
        let (mtime_secs, mtime_nsecs) = match attr.mtime {
            Some(mtime) => {
                let (s, n) = system_time_to_pair(mtime);
                (Some(s), Some(n))
            }
            None => (None, None),
        };
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
                ":atime_s":   atime_secs,
                ":atime_ns":  atime_nsecs.map(|v| v as i64),
                ":mtime_s":   mtime_secs,
                ":mtime_ns":  mtime_nsecs.map(|v| v as i64),
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

    pub fn get_dir_parent(conn: &rusqlite::Connection, ino: NormalIno) -> anyhow::Result<u64> {
        let ino = ino.to_norm_u64();
        let parent: Option<i64> = conn
            .query_row(
                r#"
                SELECT parent_inode
                FROM inode_map
                WHERE inode = ?1
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

    pub fn count_children(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<usize> {
        let mut stmt = conn.prepare(
            "
            SELECT COUNT(*) 
            FROM inode_map
            WHERE parent_inode = ?1
            ",
        )?;

        let count: usize = stmt.query_row([ino], |row| row.get(0))?;
        Ok(count)
    }

    pub fn read_children(
        conn: &rusqlite::Connection,
        parent_ino: u64,
    ) -> anyhow::Result<Vec<DirectoryEntry>> {
        let sql = r#"
        SELECT
            name,
            inode,
            oid,
            git_mode
        FROM inode_map
        WHERE parent_inode = ?1
        ORDER BY name
    "#;

        let mut stmt = conn.prepare_cached(sql)?;

        let mut rows = stmt.query(params![parent_ino as i64])?;

        let mut out = Vec::new();

        while let Some(row) = rows.next()? {
            let name_bytes: Vec<u8> = row.get(0)?;
            let name = OsString::from_vec(name_bytes);

            let child_i64: i64 = row.get(1)?;
            let ino = u64::try_from(child_i64)?;

            let oid_str: String = row.get(2)?;
            let oid = Oid::from_str(&oid_str)?;

            let git_mode_i64: i64 = row.get(3)?;
            let git_mode = u32::try_from(git_mode_i64)?;

            let kind = match git_mode & 0o170000 {
                0o040000 => FileType::Directory,
                0o120000 => FileType::Symlink,
                _ => FileType::RegularFile,
            };

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
            FROM inode_map
            WHERE inode = ?1
            ORDER BY parent_inode
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
        let mut stmt = conn.prepare_cached(
            r#"
            SELECT ino
            FROM inode_map
            WHERE parent_inode = ?1 AND name = ?2
            LIMIT 1
            "#,
        )?;
        let result: Option<i64> = stmt
            .query_row((parent as i64, name.as_bytes()), |r| r.get(0))
            .optional()?;

        match result {
            None => bail!("No entry found for {} {}", parent, name.display()),
            Some(child_i64) => {
                let child = u64::try_from(child_i64)
                    .map_err(|_| anyhow::anyhow!("child_ino out of range: {child_i64}"))?;
                Ok(child)
            }
        }
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

    pub fn get_size_from_db<C>(tx: &C, ino: u64) -> anyhow::Result<u64>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        let mut stmt = tx.prepare(
            "SELECT size
            FROM inode_map
            WHERE inode = ?1",
        )?;

        let size_opt: i64 = stmt.query_row(params![ino as i64], |row| row.get(0))?;

        let size = u64::try_from(size_opt)?;
        Ok(size)
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
            FROM inode_map
            WHERE inode = ?1
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
        FROM inode_map
        WHERE parent_inode = ?1 AND inode = ?2
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

    // Used by rename (mv)
    pub fn update_db_record<C>(
        tx: &C,
        old_parent: u64,
        old_name: &OsStr,
        node: FileAttr,
    ) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        MetaDb::write_inodes_to_db(tx, vec![node])?;
        MetaDb::remove_db_entry(tx, old_parent, old_name)?;
        Ok(())
    }

    /// Will only remove the dentry and decrement the nlink in inode_map
    ///
    /// Record is removed from inode_map when there are no more open file handles
    /// (see [`crate::fs::GitFs::release`])
    pub fn remove_db_entry<C>(tx: &C, parent_ino: u64, target_name: &OsStr) -> anyhow::Result<()>
    where
        C: std::ops::Deref<Target = rusqlite::Connection>,
    {
        tx.execute(
            r#"
        DELETE FROM inode_map
        WHERE parent_inode = ?1 AND name = ?2
        "#,
            params![parent_ino as i64, target_name.as_bytes()],
        )?;

        Ok(())
    }

    pub fn get_path_from_db(conn: &rusqlite::Connection, ino: u64) -> anyhow::Result<PathBuf> {
        let mut stmt = conn.prepare(
            "SELECT parent_inode, name
               FROM inode_map
              WHERE inode = ?1",
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
    ) -> anyhow::Result<u64> {
        MetaDb::get_ino_from_db(conn, parent.into(), name)
    }

    pub fn get_metadata_by_name(
        conn: &rusqlite::Connection,
        parent_ino: u64,
        child_name: &OsStr,
    ) -> anyhow::Result<FileAttr> {
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
            rdev,
            flags,
            uuid
        FROM inode_map
        WHERE parent_inode = ?1 AND name = ?2
        "#,
        )?;

        let res = stmt.query_row(
            params![i64::try_from(parent_ino)?, child_name.as_bytes()],
            |row| {
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
                    row.get::<_, Option<String>>(15)?,
                ))
            },
        );

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
            rdev,
            flags,
            uuid,
        ) = match res {
            Ok(row) => row,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                bail!("Entry does not exist {}", child_name.display());
            }
            Err(e) => return Err(e.into()),
        };

        let oid = Oid::from_str(&oid)?;
        let ino_flag = u64::try_from(inode_flag)?;
        let ino_flag = InoFlag::try_from(ino_flag)?;
        let kind: FileType =
            try_into_filetype_u32(git_mode as u32).ok_or_else(|| anyhow!("Invalid filetype"))?;
        let size = size as u64;
        let blocks = size.div_ceil(512);
        let atime = pair_to_system_time(atime_secs, atime_nsecs as i32);
        let mtime = pair_to_system_time(mtime_secs, mtime_nsecs as i32);
        let ctime = pair_to_system_time(ctime_secs, ctime_nsecs as i32);

        let perm = 0o775;

        let attr: FileAttr = FileAttr {
            ino: ino as u64,
            parent_ino,
            name: child_name.to_os_string(),
            oid,
            ino_flag,
            size,
            blocks,
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind,
            perm,
            git_mode: git_mode as u32,
            uid: uid as u32,
            gid: gid as u32,
            rdev: rdev as u32,
            blksize: 4096,
            flags: flags as u32,
            uuid,
        };

        Ok(attr)
    }

    pub fn get_metadata(conn: &rusqlite::Connection, target_ino: u64) -> anyhow::Result<FileAttr> {
        let mut stmt = conn.prepare(
            r#"
        SELECT
            parent_inode,
            name,
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
            rdev,
            flags,
            uuid
        FROM inode_map
        WHERE inode = ?1
        "#,
        )?;

        let res = stmt.query_row(params![i64::try_from(target_ino)?], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
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
                row.get::<_, Option<String>>(16)?,
            ))
        });

        let (
            parent_inode,
            name,
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
            rdev,
            flags,
            uuid,
        ) = match res {
            Ok(row) => row,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                bail!("Entry does not exist {target_ino}");
            }
            Err(e) => return Err(e.into()),
        };

        let parent_ino = u64::try_from(parent_inode)?;
        let name = OsString::from_vec(name);
        let oid = Oid::from_str(&oid)?;
        let ino_flag = u64::try_from(inode_flag)?;
        let ino_flag = InoFlag::try_from(ino_flag)?;
        let kind: FileType =
            try_into_filetype_u32(git_mode as u32).ok_or_else(|| anyhow!("Invalid filetype"))?;
        let size = size as u64;
        let blocks = size.div_ceil(512);
        let atime = pair_to_system_time(atime_secs, atime_nsecs as i32);
        let mtime = pair_to_system_time(mtime_secs, mtime_nsecs as i32);
        let ctime = pair_to_system_time(ctime_secs, ctime_nsecs as i32);

        let perm = 0o775;

        let attr: FileAttr = FileAttr {
            ino: target_ino,
            parent_ino,
            name,
            oid,
            ino_flag,
            size,
            blocks,
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind,
            perm,
            git_mode: git_mode as u32,
            uid: uid as u32,
            gid: gid as u32,
            rdev: rdev as u32,
            blksize: 4096,
            flags: flags as u32,
            uuid,
        };

        Ok(attr)
    }

    pub fn get_builctx_metadata(
        conn: &rusqlite::Connection,
        ino: u64,
    ) -> anyhow::Result<BuildCtxMetadata> {
        let sql = r#"
        SELECT git_mode, oid, ino_flag, name
        FROM inode_map
        WHERE inode = ?1
        LIMIT 1
    "#;

        let mut stmt = conn.prepare_cached(sql)?;
        let row = stmt
            .query_row(params![ino as i64], |row| {
                let mode_i: i64 = row.get(0)?;
                let oid_txt: String = row.get(1)?;
                let flag_i: i64 = row.get(2)?;
                let name_raw: Vec<u8> = row.get(3)?;

                let mode = repo::try_into_filemode(mode_i as u64)
                    .ok_or_else(|| rusqlite::Error::InvalidQuery)?;
                let oid: Oid = oid_txt.parse().map_err(|_| rusqlite::Error::InvalidQuery)?;
                let ino_flag = (flag_i as u64)
                    .try_into()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?;

                Ok(BuildCtxMetadata {
                    mode,
                    oid,
                    ino_flag,
                    name: OsString::from_vec(name_raw),
                })
            })
            .optional()?
            .ok_or_else(|| anyhow!("inode {} not found in inode_map", ino))?;

        Ok(row)
    }
}

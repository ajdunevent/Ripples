// src/journal.rs
//
// The sync journal: one row per file/directory we know about, per
// connection. Tracks three states for each item:
//   - current local (mtime, size)
//   - current remote (ETag, from a direct WebDAV request -- see
//     webdav.rs, no GVfs involved)
//   - last-synced local AND last-synced remote (the "common ancestor" --
//     what both sides looked like the last time we successfully
//     reconciled them)
//
// decide_action() is the actual sync decision table, implemented as a
// pure function with no I/O -- deliberately, so it can be unit-tested in
// isolation from any real filesystem/network access.
//
// Known, stated limitation for v1: rename detection is same-directory,
// same-connection only (inode/fileid based -- see sync.rs's own top-of-
// file comment and rename-pairing blocks). A move across connections, or
// one that also changes content, still shows up as a delete + a create.

use rusqlite::{params, Connection, OptionalExtension, Result as SqlResult};
use std::path::Path;
use crate::config::SyncDirection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    Pending,
    Synced,
    Conflict,
    /// Transfer would exceed the configured max file size. Left as-is on
    /// both sides -- not an error, just parked until the file shrinks or
    /// the limit is raised. Revisited every sync round like anything else
    /// pending, so it self-resolves the moment either happens.
    SkippedTooLarge,
    /// Both sides exist, never synced before, and size+mtime already
    /// agree -- likely already identical, but not yet *confirmed*
    /// identical via a real content comparison. See the design
    /// discussion this came from: size+mtime matching is a strong, cheap
    /// signal, but not proof -- two files can share both by genuine
    /// coincidence (especially across tools that deliberately preserve
    /// mtime, which is the common, expected case for exactly this
    /// "adopting an already-synced share" scenario), and neither field
    /// would ever catch silent, in-place corruption (bit rot) that
    /// doesn't change a file's length. So this is a deliberately
    /// separate, internal-only status, invisible in the Sync Issues
    /// carousel (it's not a conflict the user needs to act on) --
    /// distinct from Synced specifically so nothing ever treats "looks
    /// the same" as equivalent to "confirmed the same." A background
    /// verification pass (see main.rs) is the only thing that ever
    /// promotes an item out of this status, into either Synced (hash
    /// matched) or Conflict (it didn't, despite matching metadata).
    PendingVerification,
}

impl SyncStatus {
    fn as_str(self) -> &'static str {
        match self {
            SyncStatus::Pending => "pending",
            SyncStatus::Synced => "synced",
            SyncStatus::Conflict => "conflict",
            SyncStatus::SkippedTooLarge => "skipped_too_large",
            SyncStatus::PendingVerification => "pending_verification",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "synced" => SyncStatus::Synced,
            "conflict" => SyncStatus::Conflict,
            "skipped_too_large" => SyncStatus::SkippedTooLarge,
            "pending_verification" => SyncStatus::PendingVerification,
            _ => SyncStatus::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncItem {
    // Every SyncItem is retrieved via a query that already scopes by
    // connection_id (get_item, find_by_inode, items_in_status all take
    // it as a parameter) -- callers already have this value from
    // context and never need to read it back off the returned struct.
    // Redundant by design, not a bug: kept because a SyncItem
    // representing "this row, as stored" without its own connection_id
    // would be a strange thing to hand around, even if nothing
    // currently re-reads it.
    #[allow(dead_code)]
    pub connection_id: String,
    pub relative_path: String,
    pub is_directory: bool,
    pub local_mtime: Option<i64>,
    pub local_size: Option<i64>,
    pub remote_etag: Option<String>,
    pub last_synced_mtime: Option<i64>,
    pub last_synced_size: Option<i64>,
    pub last_synced_etag: Option<String>,
    /// See idx_sync_items_inode's own comment -- the inode this path had
    /// as of its last successful sync, used to recognize "this vanished
    /// local path and this brand-new one are actually the same file,
    /// just moved" without needing to hash anything. Also read directly
    /// off a returned SyncItem now (not just used as a find_by_inode
    /// query filter) -- see is_fully_cleanable's own doc comment in
    /// sync.rs for why: corroborating "is this file the journal knows
    /// about actually the same one sitting at this path right now"
    /// needs exactly this, since mtime+size alone can't tell two
    /// extractions of the same archive apart.
    pub last_synced_inode: Option<i64>,
    /// See idx_sync_items_fileid's own comment -- the server-assigned
    /// oc:fileid this path had as of its last successful sync, when the
    /// server reported one at all. The remote-side mirror of
    /// last_synced_inode just above: a stable identity that survives a
    /// server-side rename/move, the one thing a plain ETag comparison
    /// can't tell apart from "deleted, then a different file uploaded
    /// to a reused path." None on any server that doesn't support this
    /// property (confirmed genuinely optional, not something this app
    /// requires -- see RemoteEntry::fileid's own doc comment in
    /// webdav.rs) -- every caller already treats that the same as
    /// "can't corroborate this way," never as a reason to skip the
    /// fallback checks that already existed before this was added.
    /// Only ever consulted as a SQL query filter (find_by_fileid's own
    /// WHERE clause) -- never read back out of a returned SyncItem,
    /// since the caller is matching on the value it already knows (the
    /// new path's current fileid), not inspecting what the old row
    /// itself stored. Same exact situation last_synced_inode was
    /// originally in, before is_fully_cleanable's own corroboration
    /// check gave it a second use -- this may well see the same thing
    /// happen later, but doesn't yet.
    #[allow(dead_code)]
    pub last_synced_fileid: Option<String>,
    /// Set when the most recent attempt on this item failed with what
    /// looks like a permission error (see sync::is_permission_error) --
    /// either side, local or remote. While set, the normal walk skips
    /// this item entirely rather than re-attempting (and re-failing,
    /// and re-logging) something with no reason to believe has changed.
    /// Cleared at session start, on manual unpause, or by a targeted,
    /// user-initiated retry -- never by a background timer, since
    /// nothing about time passing on its own is a reason to believe a
    /// permission actually changed.
    pub permission_error: bool,
    /// The original error's own Display text, captured when permission_
    /// error was actually set -- see this column's own doc comment in
    /// open()'s CREATE TABLE. None whenever permission_error is false,
    /// or for a row from before this field existed.
    pub permission_error_message: Option<String>,
    pub status: SyncStatus,
    /// When this row last changed -- in particular, when a conflict was
    /// flagged, for display in the Unresolved Conflicts list. 0 for a
    /// fresh, not-yet-persisted item from get_or_new.
    pub updated_at: i64,
}

impl SyncItem {
    fn new(connection_id: &str, relative_path: &str, is_directory: bool) -> Self {
        Self {
            connection_id: connection_id.to_string(),
            relative_path: relative_path.to_string(),
            is_directory,
            local_mtime: None,
            local_size: None,
            remote_etag: None,
            last_synced_mtime: None,
            last_synced_size: None,
            last_synced_etag: None,
            last_synced_inode: None,
            last_synced_fileid: None,
            permission_error: false,
            permission_error_message: None,
            status: SyncStatus::Pending,
            updated_at: 0,
        }
    }
}

/// What the copy loop should actually do about a given item, decided
/// purely from journal state -- no filesystem/network access here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAction {
    /// Neither side changed since last sync. Nothing to do.
    Noop,
    /// Local changed, remote didn't -- push local to remote.
    Upload,
    /// Remote changed, local didn't -- pull remote to local.
    Download,
    /// Both sides changed since last sync. v1 policy: don't silently pick
    /// a winner -- the copy loop should rename the local file aside
    /// (Nextcloud's own "(conflicted copy TIMESTAMP)" convention, so this
    /// is familiar to anyone who's used the real client) and then
    /// download the remote version.
    ConflictBothChanged,
    /// Present locally, gone remotely, local unchanged since last sync --
    /// the remote deletion should propagate.
    DeleteLocal,
    /// Present remotely, gone locally, remote unchanged since last sync --
    /// the local deletion should propagate.
    DeleteRemote,
    /// Deleted on one side, but the *other* side also changed since last
    /// sync -- ambiguous. v1 policy: prefer not to lose data, so treat
    /// as a conflict and re-download rather than honoring the delete.
    ConflictDeleteVsChange,
    /// Gone on both sides. Just drop the journal row, nothing to sync.
    ForgetRow,
}

impl SyncAction {
    /// Human-readable label for this action, specifically for the
    /// throttled WebDAV-failure log line (see sync.rs's own
    /// tag_webdav_action and its callers) -- not used anywhere decisions
    /// are actually made, just user-facing text. ConflictBothChanged and
    /// ConflictDeleteVsChange both ultimately mean "download the remote
    /// version" per their own doc comments just above, so they share
    /// Download's own label rather than each inventing a separate,
    /// more-technical-than-useful one. Noop and ForgetRow never reach a
    /// network call at all, so they're never actually displayed, but
    /// still need a real label to keep this a total function.
    pub fn label(&self) -> &'static str {
        match self {
            SyncAction::Noop => "Noop",
            SyncAction::Upload => "Upload",
            SyncAction::Download | SyncAction::ConflictBothChanged | SyncAction::ConflictDeleteVsChange => "Download",
            SyncAction::DeleteLocal => "Delete",
            SyncAction::DeleteRemote => "Delete",
            SyncAction::ForgetRow => "ForgetRow",
        }
    }
}

/// current_local: None if the path no longer exists locally.
/// current_local_inode: the local file's current inode, when it exists --
/// see local_changed's own use below for why this matters, not just
/// mtime+size.
/// current_remote_etag: None if the path no longer exists remotely.
///
/// direction folded directly into this match, rather than computed once
/// for Bidirectional and remapped afterward -- a cleverer, smaller-
/// looking remap would need to re-derive which of several genuinely
/// different original cases produced a given bidirectional action before
/// it could even decide how to adjust it (Upload alone, for instance,
/// comes from either a brand-new, never-synced local file with nothing
/// remote at all, or an ordinary "local changed, remote didn't" case --
/// UploadOnly treats the first as nothing to do at all and the second as
/// a genuine upload). Explicit, verifiable line-by-line against this
/// enum's own three-direction table is worth the extra length here, on
/// the one function an error in silently loses or overwrites real data.
pub fn decide_action(
    item: &SyncItem,
    current_local: Option<(i64, i64)>, // (mtime, size)
    current_local_inode: Option<i64>,
    current_remote_etag: Option<&str>,
    direction: SyncDirection,
) -> SyncAction {
    let never_synced = item.last_synced_mtime.is_none() && item.last_synced_etag.is_none();

    if never_synced {
        // No common ancestor at all -- "gone" here means "never existed
        // on that side," not "was deleted." A brand-new local file with
        // no remote copy yet must upload, not be treated as a delete
        // conflict against a remote that was never there to begin with.
        //
        // One-way directions mirror: the source side is authoritative,
        // so an item present only on the other side is removed. The
        // connection wizard warns about this when a one-way direction
        // is chosen.
        return match (current_local, current_remote_etag, direction) {
            (Some(_), None, SyncDirection::Bidirectional | SyncDirection::UploadOnly) => SyncAction::Upload,
            (Some(_), None, SyncDirection::DownloadOnly) => SyncAction::DeleteLocal,
            (None, Some(_), SyncDirection::Bidirectional | SyncDirection::DownloadOnly) => SyncAction::Download,
            (None, Some(_), SyncDirection::UploadOnly) => SyncAction::DeleteRemote,
            (None, None, _) => SyncAction::ForgetRow,
            // Present on both sides with no prior reconciliation
            // history -- could be coincidentally identical, but we
            // can't safely assume that, so Bidirectional doesn't
            // silently pick a winner. UploadOnly/DownloadOnly have no
            // such ambiguity at all, by definition: only one side's
            // content is ever allowed to matter, so there's nothing
            // here for either of them to actually treat as a conflict.
            (Some(_), Some(_), SyncDirection::Bidirectional) => SyncAction::ConflictBothChanged,
            (Some(_), Some(_), SyncDirection::UploadOnly) => SyncAction::Upload,
            (Some(_), Some(_), SyncDirection::DownloadOnly) => SyncAction::Download,
        };
    }

    let local_changed = match current_local {
        Some((mtime, size)) => {
            Some(mtime) != item.last_synced_mtime
                || Some(size) != item.last_synced_size
                // Real, reported bug without this: mtime+size alone
                // can't tell a genuinely continuous file apart from a
                // brand-new one that happens to land on identical
                // metadata -- confirmed against a real repro (quit the
                // app, delete a path on both sides by hand so the
                // journal's row for it is never cleared, then recreate
                // it with content whose mtime+size coincidentally
                // matches the stale record, e.g. re-extracting the same
                // archive with timestamp-preserving extraction). Without
                // this, that reads as "local unchanged since last sync,"
                // and if the remote side is also gone (deleted the same
                // way), DeleteLocal would silently wipe out the brand-new
                // file believing it's just propagating an ordinary
                // remote deletion of the SAME file. Only fires when both
                // sides of the comparison are actually available --
                // absent on either (an old journal row predating this
                // field, or a caller that hasn't looked up an inode)
                // means "can't corroborate," not "assume changed," so
                // this never produces a false positive against an
                // older, still-perfectly-valid journal row.
                || match (current_local_inode, item.last_synced_inode) {
                    (Some(current), Some(recorded)) => current != recorded,
                    _ => false,
                }
        }
        None => item.last_synced_mtime.is_some(), // existed before, gone now
    };
    let remote_changed = match current_remote_etag {
        Some(etag) => item.last_synced_etag.as_deref() != Some(etag),
        None => item.last_synced_etag.is_some(), // existed before, gone now
    };

    match (current_local, current_remote_etag, local_changed, remote_changed, direction) {
        // Gone on both sides -- nothing left to reconcile, regardless of
        // direction.
        (None, None, _, _, _) => SyncAction::ForgetRow,

        // Gone locally only.
        (None, Some(_), _, false, SyncDirection::Bidirectional | SyncDirection::UploadOnly) => SyncAction::DeleteRemote,
        // DownloadOnly: local's own deletion never propagates -- remote
        // still exists (unchanged), so the correct read is "the local
        // copy is just missing," restored by re-pulling it, the mirror
        // of UploadOnly's own "remote deletions cause a re-upload" a few
        // arms below.
        (None, Some(_), _, false, SyncDirection::DownloadOnly) => SyncAction::Download,
        (None, Some(_), _, true, SyncDirection::Bidirectional) => SyncAction::ConflictDeleteVsChange,
        // UploadOnly: local's own deletion is the only side that can
        // ever win here -- push it, discarding whatever changed
        // remotely, the same "local always wins outright" reasoning
        // that applies to every other case below.
        (None, Some(_), _, true, SyncDirection::UploadOnly) => SyncAction::DeleteRemote,
        // DownloadOnly: the remote-side change is the only one that
        // exists as far as this direction is concerned -- pull it, by
        // request, rather than let a change this direction was never
        // going to notice as "local" silently win by default.
        (None, Some(_), _, true, SyncDirection::DownloadOnly) => SyncAction::Download,

        // Gone remotely only.
        (Some(_), None, false, _, SyncDirection::Bidirectional | SyncDirection::DownloadOnly) => SyncAction::DeleteLocal,
        // UploadOnly: by explicit request -- a remote deletion causes a
        // re-upload, not a local delete. Local is unchanged, but still
        // the only side that's allowed to matter, so re-pushing it
        // (restoring the remote copy) is the correct read here, the
        // mirror of DownloadOnly's own local-deletion case just above.
        (Some(_), None, false, _, SyncDirection::UploadOnly) => SyncAction::Upload,
        (Some(_), None, true, _, SyncDirection::Bidirectional) => SyncAction::ConflictDeleteVsChange,
        // UploadOnly: local changed, remote's own deletion doesn't get a
        // vote -- re-push local's own current content.
        (Some(_), None, true, _, SyncDirection::UploadOnly) => SyncAction::Upload,
        // DownloadOnly: remote's own deletion is the only side that can
        // ever win here -- propagate it, discarding the local change.
        (Some(_), None, true, _, SyncDirection::DownloadOnly) => SyncAction::DeleteLocal,

        // Present on both sides.
        (Some(_), Some(_), false, false, _) => SyncAction::Noop,
        (Some(_), Some(_), true, false, SyncDirection::Bidirectional | SyncDirection::UploadOnly) => SyncAction::Upload,
        // DownloadOnly: local's own change never gets a vote -- the
        // remote copy (unchanged, but still the only side that's
        // allowed to matter) overwrites it.
        (Some(_), Some(_), true, false, SyncDirection::DownloadOnly) => SyncAction::Download,
        (Some(_), Some(_), false, true, SyncDirection::Bidirectional | SyncDirection::DownloadOnly) => SyncAction::Download,
        // UploadOnly: by request -- re-assert local's own (unchanged)
        // content over whatever changed remotely, rather than silently
        // accepting a remote-side change this direction otherwise never
        // acts on at all.
        (Some(_), Some(_), false, true, SyncDirection::UploadOnly) => SyncAction::Upload,
        (Some(_), Some(_), true, true, SyncDirection::Bidirectional) => SyncAction::ConflictBothChanged,
        (Some(_), Some(_), true, true, SyncDirection::UploadOnly) => SyncAction::Upload,
        (Some(_), Some(_), true, true, SyncDirection::DownloadOnly) => SyncAction::Download,
    }
}

pub fn open(db_path: &Path) -> SqlResult<Connection> {
    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    // NORMAL, not the default FULL -- safe specifically because it's
    // paired with WAL above: WAL+NORMAL doesn't risk corruption the way
    // a rollback-journal+NORMAL combination would, and it's meaningfully
    // faster than FULL for a write-heavy workload like this journal.
    // Confirmed as the real Nextcloud desktop client's own reasoning
    // (checked its actual syncjournaldb.cpp), not designed from scratch.
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    // EXCLUSIVE, not the default -- this process is the only one that
    // ever opens this specific database file, so there's no other
    // connection to coordinate with; this avoids per-statement lock/
    // unlock overhead entirely rather than paying for coordination that
    // will never actually be needed. Same reasoning confirmed in the
    // real Nextcloud client's own connection setup.
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
    init_schema(&conn)?;
    Ok(conn)
}

/// Creates every table and index the journal needs (idempotent). Split
/// out of open() so tests can build the real schema on an in-memory
/// connection instead of keeping hand-written copies that drift.
fn init_schema(conn: &Connection) -> SqlResult<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sync_items (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            connection_id     TEXT NOT NULL,
            relative_path     TEXT NOT NULL,
            is_directory      INTEGER NOT NULL,
            local_mtime       INTEGER,
            local_size        INTEGER,
            remote_etag       TEXT,
            last_synced_mtime INTEGER,
            last_synced_size  INTEGER,
            last_synced_etag  TEXT,
            last_synced_inode INTEGER,
            last_synced_fileid TEXT,
            permission_error  INTEGER NOT NULL DEFAULT 0,
            -- By request -- the resolver's own PermissionError page
            -- previously had no way to show the user why a given path
            -- was flagged, only the bare fact that it was; this is that
            -- original error's own Display text, captured at the moment
            -- mark_permission_error is actually called. NULL for every
            -- row from before this column existed, and for any row
            -- that's never been flagged at all -- see mark_permission_
            -- error/clear_permission_error's own doc comments for when
            -- it's written and cleared. Added straight into this CREATE
            -- TABLE, matching every prior column added to this table
            -- (permission_error, last_synced_inode, last_synced_fileid)
            -- -- this app has no ALTER TABLE migration path at all yet,
            -- so an already-existing database still won't gain this
            -- column without a database wipe or a migration step.
            permission_error_message TEXT,
            status            TEXT NOT NULL DEFAULT 'pending',
            updated_at        INTEGER NOT NULL,
            UNIQUE(connection_id, relative_path)
        );
        CREATE INDEX IF NOT EXISTS idx_sync_items_status
            ON sync_items(connection_id, status);
        -- Partial, not a plain index on permission_error -- a standard
        -- B-tree index doesn't meaningfully speed up an inequality
        -- lookup on a column where the overwhelming majority of rows are
        -- 0, since it can't skip straight to the rare, actually-matching
        -- ones. This index only ever contains the flagged rows, so
        -- items_with_permission_error's own query costs are proportional
        -- to how many items are actually flagged, not how large the
        -- whole table has grown -- confirmed as a real, severe bug at
        -- scale: a large journal meant this query (fired on every
        -- Conflict/PermissionError event) was effectively scanning the
        -- entire table every time, regardless of how few rows ever
        -- matched.
        CREATE INDEX IF NOT EXISTS idx_sync_items_permission_error
            ON sync_items(connection_id) WHERE permission_error = 1;
        -- Local rename/move detection (see sync.rs's pairing logic):
        -- given a currently-existing local file/directory's own inode,
        -- find whichever previously-synced row (if any) recorded that
        -- same inode -- the one thing a rename never changes on POSIX,
        -- since it's purely a directory-entry operation, not something
        -- that touches the underlying inode at all. Same real mechanism
        -- the actual Nextcloud desktop client uses, confirmed against
        -- its own source rather than designed from scratch.
        CREATE INDEX IF NOT EXISTS idx_sync_items_inode
            ON sync_items(connection_id, last_synced_inode);
        -- Remote-side rename/move detection -- the mirror-image of
        -- idx_sync_items_inode just above, for the direction that index
        -- can't cover: a file renamed *on the server* (web UI, another
        -- synced device) rather than locally. oc:fileid (see webdav.rs's
        -- own PROPFIND request body) is a WebDAV extension property, not
        -- part of core WebDAV/RFC 4918 -- confirmed genuinely shared
        -- between Nextcloud and ownCloud (the \"oc:\" namespace literally
        -- is ownCloud's own, predating Nextcloud's fork; both projects'
        -- current developer docs still show it), not something specific
        -- to just one of the two. A server that doesn't support it (a
        -- generic WebDAV server, or an older/unusual Nextcloud/ownCloud
        -- deployment) simply never populates this column at all -- see
        -- RemoteEntry::fileid's own doc comment in webdav.rs for why
        -- that's a purely additive, optional bonus throughout this app,
        -- never a requirement anything else depends on.
        CREATE INDEX IF NOT EXISTS idx_sync_items_fileid
            ON sync_items(connection_id, last_synced_fileid) WHERE last_synced_fileid IS NOT NULL;

        -- One row per connection: the remote root directory's own ETag,
        -- as of the last round that actually confirmed it (whether via
        -- a real full crawl or a prior ETag-only check finding no
        -- change). See sync.rs's ETag-tier check: before committing to
        -- an expensive full crawl, a single, cheap depth-0 PROPFIND on
        -- the root is compared against whatever's stored here -- a
        -- match means nothing changed anywhere in the whole subtree
        -- (confirmed as real Nextcloud/ownCloud server behavior, not a
        -- generic WebDAV guarantee -- see that check's own gating logic
        -- for why this is only ever trusted for servers confirmed to be
        -- Nextcloud/ownCloud specifically), and the round can skip the
        -- full crawl entirely for this connection.
        --
        -- Deliberately its own tiny table, not a column bolted onto
        -- sync_items -- this is one value per *connection*, not per
        -- item, and doesn't belong to any single row there.
        CREATE TABLE IF NOT EXISTS connection_root_etag (
            connection_id  TEXT PRIMARY KEY,
            etag           TEXT NOT NULL,
            updated_at     INTEGER NOT NULL
        );",
    )?;
    Ok(())
}

/// The remote root's own ETag as of the last round that confirmed it --
/// None if this connection has never completed a round at all (its
/// first-ever round always needs a real crawl regardless, so there's
/// nothing to compare against yet anyway).
pub fn get_root_etag(conn: &Connection, connection_id: &str) -> SqlResult<Option<String>> {
    conn.query_row(
        "SELECT etag FROM connection_root_etag WHERE connection_id = ?1",
        params![connection_id],
        |row| row.get(0),
    ).optional()
}

/// Records the remote root's current ETag for this connection -- called
/// both after a real full crawl completes (establishing/refreshing the
/// baseline) and after an ETag-only check confirms no change (keeping
/// the stored value equal to reality, though in that specific case it's
/// already equal by definition and this is mostly a updated_at refresh).
pub fn set_root_etag(conn: &Connection, connection_id: &str, etag: &str, now: i64) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO connection_root_etag (connection_id, etag, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(connection_id) DO UPDATE SET etag = excluded.etag, updated_at = excluded.updated_at",
        params![connection_id, etag, now],
    )?;
    Ok(())
}

pub fn get_item(conn: &Connection, connection_id: &str, relative_path: &str) -> SqlResult<Option<SyncItem>> {
    conn.query_row(
        "SELECT connection_id, relative_path, is_directory, local_mtime, local_size,
                remote_etag, last_synced_mtime, last_synced_size, last_synced_etag,
                last_synced_inode, last_synced_fileid, permission_error,
                permission_error_message, status, updated_at
         FROM sync_items WHERE connection_id = ?1 AND relative_path = ?2",
        params![connection_id, relative_path],
        |row| {
            Ok(SyncItem {
                connection_id: row.get(0)?,
                relative_path: row.get(1)?,
                is_directory: row.get::<_, i64>(2)? != 0,
                local_mtime: row.get(3)?,
                local_size: row.get(4)?,
                remote_etag: row.get(5)?,
                last_synced_mtime: row.get(6)?,
                last_synced_size: row.get(7)?,
                last_synced_etag: row.get(8)?,
                last_synced_inode: row.get(9)?,
                last_synced_fileid: row.get(10)?,
                permission_error: row.get::<_, i64>(11)? != 0,
                permission_error_message: row.get(12)?,
                status: SyncStatus::from_str(&row.get::<_, String>(13)?),
                updated_at: row.get(14)?,
            })
        },
    ).optional()
}

/// Fetches the row if present, otherwise returns a fresh in-memory
/// SyncItem (not yet persisted) -- callers decide an action, act on it,
/// then call mark_synced/mark_conflict/etc to persist the outcome.
pub fn get_or_new(conn: &Connection, connection_id: &str, relative_path: &str, is_directory: bool) -> SqlResult<SyncItem> {
    Ok(get_item(conn, connection_id, relative_path)?
        .unwrap_or_else(|| SyncItem::new(connection_id, relative_path, is_directory)))
}

/// Records a successful sync: both "last_synced_*" fields move up to the
/// current local/remote state, since they're now reconciled. `inode` is
/// `None` for a directory-less remote-only concept (there isn't one --
/// this is always Some for anything actually on the local filesystem,
/// only None if the item doesn't exist locally at all, e.g. a pure
/// remote-side entry that was never downloaded). `fileid` is None
/// whenever the server didn't report one (see RemoteEntry::fileid's own
/// doc comment in webdav.rs for why that's an ordinary, expected case,
/// not a problem) -- every caller already has whatever webdav.rs handed
/// back for this item, so there's nothing extra to fetch here.
pub fn mark_synced(conn: &Connection, connection_id: &str, relative_path: &str, is_directory: bool,
    local_mtime: i64, local_size: i64, remote_etag: &str, inode: Option<i64>, fileid: Option<&str>, now: i64) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO sync_items
            (connection_id, relative_path, is_directory, local_mtime, local_size, remote_etag,
             last_synced_mtime, last_synced_size, last_synced_etag, last_synced_inode, last_synced_fileid, permission_error, status, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?4, ?5, ?6, ?7, ?8, 0, 'synced', ?9)
         ON CONFLICT(connection_id, relative_path) DO UPDATE SET
            local_mtime = excluded.local_mtime,
            local_size = excluded.local_size,
            remote_etag = excluded.remote_etag,
            last_synced_mtime = excluded.local_mtime,
            last_synced_size = excluded.local_size,
            last_synced_etag = excluded.remote_etag,
            last_synced_inode = excluded.last_synced_inode,
            last_synced_fileid = excluded.last_synced_fileid,
            permission_error = 0,
            status = 'synced',
            updated_at = excluded.updated_at",
        params![connection_id, relative_path, is_directory as i64, local_mtime, local_size, remote_etag, inode, fileid, now],
    )?;
    Ok(())
}

/// Flags `relative_path` as having failed with what looks like a
/// permission error (see sync::is_permission_error) -- distinct from an
/// ordinary ItemFailed: the normal walk will keep skipping this item
/// entirely (see decide_action's own caller) until the flag is cleared,
/// rather than re-attempting, re-failing, and re-logging the same doomed
/// operation every single round. Upserts rather than requiring an
/// existing row -- the very first attempt at a brand-new path can fail
/// this way just as easily as a previously-synced one.
///
/// message: the original error's own Display text -- by request, so the
/// resolver's own PermissionError page can show *why* a given path was
/// flagged, not just the bare fact that it was. Stored as given, no
/// truncation/sanitizing here; the resolver's own display is responsible
/// for how much of it actually gets shown.
pub fn mark_permission_error(conn: &Connection, connection_id: &str, relative_path: &str, is_directory: bool, message: &str, now: i64) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO sync_items (connection_id, relative_path, is_directory, permission_error, permission_error_message, status, updated_at)
         VALUES (?1, ?2, ?3, 1, ?4, 'pending', ?5)
         ON CONFLICT(connection_id, relative_path) DO UPDATE SET
            permission_error = 1,
            permission_error_message = excluded.permission_error_message,
            updated_at = excluded.updated_at",
        params![connection_id, relative_path, is_directory as i64, message, now],
    )?;
    Ok(())
}

/// Clears permission_error (and its own stored message, so a later,
/// genuinely new failure never inherits stale text from whatever the
/// previous one said) for one specific item -- the targeted, user-
/// initiated "Retry"/"Retry Now" path from the carousel. Only clears the
/// flag/message; whether the subsequent real attempt succeeds is a
/// separate question the caller still has to act on. A no-op, not an
/// error, if the row doesn't exist at all (nothing to clear).
pub fn clear_permission_error(conn: &Connection, connection_id: &str, relative_path: &str) -> SqlResult<()> {
    conn.execute(
        "UPDATE sync_items SET permission_error = 0, permission_error_message = NULL WHERE connection_id = ?1 AND relative_path = ?2",
        params![connection_id, relative_path],
    )?;
    Ok(())
}

/// Clears permission_error across every connection at once -- the two
/// global triggers (session start, manual unpause), neither of which is
/// about any one specific item. Deliberately not scoped to a single
/// connection_id, unlike clear_permission_error above.
pub fn clear_all_permission_errors(conn: &Connection) -> SqlResult<()> {
    conn.execute("UPDATE sync_items SET permission_error = 0 WHERE permission_error = 1", [])?;
    Ok(())
}

/// Records that `relative_path` has a conflict -- both sides changed
/// since the last reconciliation, or one side was deleted while the
/// other changed -- WITHOUT touching either file. This is the entire
/// mechanism behind the "detect, don't act" policy: no rename, no
/// download, no upload happens here or anywhere in the caller. local_mtime/
/// local_size/remote_etag are overwritten with the snapshot observed at
/// detection time (None on whichever side didn't have the item, e.g. a
/// delete-vs-change conflict) so a later resolution UI has something to
/// show the user without needing to re-touch the filesystem itself.
///
/// last_synced_mtime/last_synced_size/last_synced_etag are deliberately
/// NOT touched -- they still hold the old common-ancestor values (or
/// remain NULL if this item was never synced before), which is exactly
/// what a fresh decide_action() call needs once the conflict is actually
/// resolved and mark_synced() runs again to move both sides forward
/// together.
///
/// Callers MUST stop touching this item's row (and the files it
/// describes) entirely once this is called, until an explicit resolution
/// clears it -- see sync_child_file's status == Conflict short-circuit in
/// sync.rs. Re-running detection every round on an already-flagged
/// conflict would just re-freeze the same snapshot (harmless) but also
/// re-emit the same SyncEvent::Conflict every round, which is exactly
/// the kind of per-round spam the .no-sync exclusion reporting already
/// had to be fixed for -- so the freeze needs to happen one level up,
/// before decide_action is even called again.

/// Looks up whichever previously-synced row (if any) recorded the given
/// inode -- the actual detection mechanism behind local rename/move
/// pairing. Scoped to `is_directory` too: a file's inode should never be
/// considered a candidate match for a directory or vice versa, even
/// though inode numbers are technically comparable across types.
pub fn find_by_inode(conn: &Connection, connection_id: &str, inode: i64, is_directory: bool) -> SqlResult<Option<SyncItem>> {
    conn.query_row(
        "SELECT connection_id, relative_path, is_directory, local_mtime, local_size,
                remote_etag, last_synced_mtime, last_synced_size, last_synced_etag,
                last_synced_inode, last_synced_fileid, permission_error,
                permission_error_message, status, updated_at
         FROM sync_items WHERE connection_id = ?1 AND last_synced_inode = ?2 AND is_directory = ?3",
        params![connection_id, inode, is_directory as i64],
        |row| {
            Ok(SyncItem {
                connection_id: row.get(0)?,
                relative_path: row.get(1)?,
                is_directory: row.get::<_, i64>(2)? != 0,
                local_mtime: row.get(3)?,
                local_size: row.get(4)?,
                remote_etag: row.get(5)?,
                last_synced_mtime: row.get(6)?,
                last_synced_size: row.get(7)?,
                last_synced_etag: row.get(8)?,
                last_synced_inode: row.get(9)?,
                last_synced_fileid: row.get(10)?,
                permission_error: row.get::<_, i64>(11)? != 0,
                permission_error_message: row.get(12)?,
                status: SyncStatus::from_str(&row.get::<_, String>(13)?),
                updated_at: row.get(14)?,
            })
        },
    ).optional()
}

/// Remote-side rename/move detection -- the mirror of find_by_inode
/// just above, for a server-assigned oc:fileid instead of a local
/// inode. See idx_sync_items_fileid's own doc comment for the full
/// reasoning; this is the query that index exists for.
pub fn find_by_fileid(conn: &Connection, connection_id: &str, fileid: &str, is_directory: bool) -> SqlResult<Option<SyncItem>> {
    conn.query_row(
        "SELECT connection_id, relative_path, is_directory, local_mtime, local_size,
                remote_etag, last_synced_mtime, last_synced_size, last_synced_etag,
                last_synced_inode, last_synced_fileid, permission_error,
                permission_error_message, status, updated_at
         FROM sync_items WHERE connection_id = ?1 AND last_synced_fileid = ?2 AND is_directory = ?3",
        params![connection_id, fileid, is_directory as i64],
        |row| {
            Ok(SyncItem {
                connection_id: row.get(0)?,
                relative_path: row.get(1)?,
                is_directory: row.get::<_, i64>(2)? != 0,
                local_mtime: row.get(3)?,
                local_size: row.get(4)?,
                remote_etag: row.get(5)?,
                last_synced_mtime: row.get(6)?,
                last_synced_size: row.get(7)?,
                last_synced_etag: row.get(8)?,
                last_synced_inode: row.get(9)?,
                last_synced_fileid: row.get(10)?,
                permission_error: row.get::<_, i64>(11)? != 0,
                permission_error_message: row.get(12)?,
                status: SyncStatus::from_str(&row.get::<_, String>(13)?),
                updated_at: row.get(14)?,
            })
        },
    ).optional()
}

/// Executes a confirmed FILE rename/move in the journal: the existing
/// row (found via find_by_inode) moves to its new path and gets marked
/// freshly synced there, rather than the old row getting deleted and a
/// new one created from scratch -- preserves the row's own history
/// (including its inode) across the move, which a delete+insert
/// wouldn't. Renaming a directory needs rename_synced_prefix below
/// instead, since it has to move every row underneath it too, not just
/// one row.
/// Escapes `%`, `_`, and the escape character itself (`\`) so a plain,
/// literal path can be safely embedded in a SQL LIKE pattern without its
/// own characters being reinterpreted as wildcards -- see this
/// function's own callers (rename_synced_prefix, forget_prefix,
/// has_synced_descendant, direct_children) for the real, confirmed bug
/// this closes: a folder name containing a literal `%` or `_` (`_`
/// especially common in real names -- "Q1_2024", "my_folder") would
/// otherwise have that character silently reinterpreted as "match
/// anything" or "match any single character," letting a rename/forget/
/// existence-check pattern accidentally also match a completely
/// unrelated path that happens to fit the resulting, unintended
/// wildcard -- `_` matches itself as one of its own "any character"
/// possibilities, so the intended match was never the problem, only the
/// extra, unintended ones the unescaped pattern let through alongside
/// it. Requires `ESCAPE '\'` on the LIKE clause itself at every call
/// site -- escaping the input alone does nothing without that.
fn escape_like_pattern(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\\' || c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

pub fn rename_synced_path(
    conn: &Connection, connection_id: &str, old_relative_path: &str, new_relative_path: &str,
    new_remote_etag: &str, local_mtime: i64, local_size: i64, inode: i64, now: i64,
) -> SqlResult<()> {
    conn.execute(
        "UPDATE sync_items SET
            relative_path = ?3,
            local_mtime = ?4, local_size = ?5, remote_etag = ?6,
            last_synced_mtime = ?4, last_synced_size = ?5, last_synced_etag = ?6, last_synced_inode = ?7,
            status = 'synced', updated_at = ?8
         WHERE connection_id = ?1 AND relative_path = ?2",
        params![connection_id, old_relative_path, new_relative_path, local_mtime, local_size, new_remote_etag, inode, now],
    )?;
    Ok(())
}

/// Executes a confirmed DIRECTORY rename/move in the journal: every row
/// under `old_prefix` (the directory's own row, plus every file and
/// subdirectory inside it, at any depth) gets its path rewritten from
/// `old_prefix` to `new_prefix`, in one statement -- not a per-child
/// loop. `old_prefix`/`new_prefix` are the directories' own relative
/// paths, no trailing slash. Deliberately does NOT touch
/// last_synced_etag on the children (only the directory's own row's
/// etag is meaningful post-move; a WebDAV MOVE with Depth: infinity
/// relocates children without generating new individual ETags the way a
/// fresh PUT would) -- children are already correctly synced content-
/// wise, only their recorded path was stale.
pub fn rename_synced_prefix(conn: &Connection, connection_id: &str, old_prefix: &str, new_prefix: &str, now: i64) -> SqlResult<usize> {
    let old_len = old_prefix.len() as i64;
    let escaped_prefix = escape_like_pattern(old_prefix);
    conn.execute(
        "UPDATE sync_items SET
            relative_path = ?3 || substr(relative_path, ?4 + 1),
            updated_at = ?5
         WHERE connection_id = ?1
           AND (relative_path = ?2 OR relative_path LIKE ?6 || '/%' ESCAPE '\\')",
        params![connection_id, old_prefix, new_prefix, old_len, now, escaped_prefix],
    )
}

pub fn mark_conflict_pending(
    conn: &Connection,
    connection_id: &str,
    relative_path: &str,
    is_directory: bool,
    current_local: Option<(i64, i64)>,
    current_remote_etag: Option<&str>,
    now: i64,
) -> SqlResult<()> {
    let (local_mtime, local_size) = match current_local {
        Some((mtime, size)) => (Some(mtime), Some(size)),
        None => (None, None),
    };
    conn.execute(
        "INSERT INTO sync_items
            (connection_id, relative_path, is_directory, local_mtime, local_size, remote_etag,
             status, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'conflict', ?7)
         ON CONFLICT(connection_id, relative_path) DO UPDATE SET
            local_mtime = excluded.local_mtime,
            local_size = excluded.local_size,
            remote_etag = excluded.remote_etag,
            status = 'conflict',
            updated_at = excluded.updated_at",
        params![connection_id, relative_path, is_directory as i64, local_mtime, local_size, current_remote_etag, now],
    )?;
    Ok(())
}

pub fn mark_skipped_too_large(conn: &Connection, connection_id: &str, relative_path: &str, is_directory: bool, now: i64) -> SqlResult<()> {
    conn.execute(
        "INSERT INTO sync_items (connection_id, relative_path, is_directory, status, updated_at)
         VALUES (?1, ?2, ?3, 'skipped_too_large', ?4)
         ON CONFLICT(connection_id, relative_path) DO UPDATE SET
            status = 'skipped_too_large', updated_at = excluded.updated_at",
        params![connection_id, relative_path, is_directory as i64, now],
    )?;
    Ok(())
}

/// Records the deferred-verification state -- see SyncStatus::PendingVerification's
/// own doc comment for the full reasoning. Stores the same metadata
/// mark_conflict_pending does, since the background verification pass
/// (main.rs) needs current_local/current_remote_etag to actually perform
/// its own, real content comparison later, not just a status flag with
/// nothing to act on.
pub fn mark_pending_verification(
    conn: &Connection,
    connection_id: &str,
    relative_path: &str,
    is_directory: bool,
    current_local: Option<(i64, i64)>,
    current_remote_etag: Option<&str>,
    now: i64,
) -> SqlResult<()> {
    let (local_mtime, local_size) = match current_local {
        Some((mtime, size)) => (Some(mtime), Some(size)),
        None => (None, None),
    };
    conn.execute(
        "INSERT INTO sync_items
            (connection_id, relative_path, is_directory, local_mtime, local_size, remote_etag,
             status, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending_verification', ?7)
         ON CONFLICT(connection_id, relative_path) DO UPDATE SET
            local_mtime = excluded.local_mtime,
            local_size = excluded.local_size,
            remote_etag = excluded.remote_etag,
            status = 'pending_verification',
            updated_at = excluded.updated_at",
        params![connection_id, relative_path, is_directory as i64, local_mtime, local_size, current_remote_etag, now],
    )?;
    Ok(())
}

pub fn forget_item(conn: &Connection, connection_id: &str, relative_path: &str) -> SqlResult<()> {
    conn.execute(
        "DELETE FROM sync_items WHERE connection_id = ?1 AND relative_path = ?2",
        params![connection_id, relative_path],
    )?;
    Ok(())
}

/// Forgets an entire directory subtree in one statement -- the
/// directory's own row plus every file and subdirectory underneath it,
/// at any depth. Used when a previously-synced directory's only
/// remaining content turns out to be auto_removable-flagged excluded
/// patterns and the whole folder is being cleaned up (see sync.rs's
/// folder-cleanup logic) -- mirrors rename_synced_prefix's own WHERE
/// clause exactly, just DELETE instead of UPDATE.
pub fn forget_prefix(conn: &Connection, connection_id: &str, prefix: &str) -> SqlResult<usize> {
    let escaped_prefix = escape_like_pattern(prefix);
    conn.execute(
        "DELETE FROM sync_items
         WHERE connection_id = ?1
           AND (relative_path = ?2 OR relative_path LIKE ?3 || '/%' ESCAPE '\\')",
        params![connection_id, prefix, escaped_prefix],
    )
}

/// Whether the journal has *any* row under `prefix` (itself or anywhere
/// in its subtree) that's actually been synced before -- see
/// sync_child_directory's own use for the real, reported bug this exists
/// to fix. A directory's own top-level row can end up without
/// last_synced_mtime set for reasons that don't actually mean "this was
/// never synced" (a database wiped mid-testing while
/// the real files/directories on both sides survived untouched, an
/// unresolved edge case from an earlier session, etc.) -- but if
/// anything genuinely underneath it still carries a real, previously-
/// synced record, that's direct, independent evidence the directory
/// itself was known before, regardless of what its own bare row does or
/// doesn't say. Same prefix-matching shape as forget_prefix, deliberately
/// -- these are conceptually the same "is prefix, or under prefix"
/// question, just read instead of deleted.
pub fn has_synced_descendant(conn: &Connection, connection_id: &str, prefix: &str) -> SqlResult<bool> {
    let escaped_prefix = escape_like_pattern(prefix);
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sync_items
            WHERE connection_id = ?1
              AND (relative_path = ?2 OR relative_path LIKE ?3 || '/%' ESCAPE '\\')
              AND last_synced_mtime IS NOT NULL
            LIMIT 1
         )",
        params![connection_id, prefix, escaped_prefix],
        |row| row.get::<_, bool>(0),
    )
}

/// Purges every trace of one connection from the journal -- both its
/// sync_items rows and its connection_root_etag row. Called when the
/// user explicitly removes a connection from the Connections tab: that's
/// a "stop managing this folder" decision, so what we know about it
/// stops being ours to keep, the same reasoning that already makes a
/// removed connection's conflicts disappear from the Unresolved
/// Conflicts list (see refresh_conflicts_ui in main.rs). Deliberately
/// NOT called from anywhere driven by GOA account state -- only an
/// explicit, in-app, user-clicked removal should ever trigger this. See
/// that call site's own comment for why.
pub fn forget_connection(conn: &Connection, connection_id: &str) -> SqlResult<()> {
    conn.execute("DELETE FROM sync_items WHERE connection_id = ?1", params![connection_id])?;
    conn.execute("DELETE FROM connection_root_etag WHERE connection_id = ?1", params![connection_id])?;
    Ok(())
}

pub fn items_in_status(conn: &Connection, connection_id: &str, status: SyncStatus) -> SqlResult<Vec<SyncItem>> {
    let mut stmt = conn.prepare(
        "SELECT connection_id, relative_path, is_directory, local_mtime, local_size,
                remote_etag, last_synced_mtime, last_synced_size, last_synced_etag,
                last_synced_inode, last_synced_fileid, permission_error,
                permission_error_message, status, updated_at
         FROM sync_items WHERE connection_id = ?1 AND status = ?2",
    )?;
    let rows = stmt.query_map(params![connection_id, status.as_str()], |row| {
        Ok(SyncItem {
            connection_id: row.get(0)?,
            relative_path: row.get(1)?,
            is_directory: row.get::<_, i64>(2)? != 0,
            local_mtime: row.get(3)?,
            local_size: row.get(4)?,
            remote_etag: row.get(5)?,
            last_synced_mtime: row.get(6)?,
            last_synced_size: row.get(7)?,
            last_synced_etag: row.get(8)?,
            last_synced_inode: row.get(9)?,
            last_synced_fileid: row.get(10)?,
            permission_error: row.get::<_, i64>(11)? != 0,
            permission_error_message: row.get(12)?,
            status: SyncStatus::from_str(&row.get::<_, String>(13)?),
            updated_at: row.get(14)?,
        })
    })?;
    rows.collect()
}

/// Same filter as items_in_status above, but a plain COUNT(*) rather
/// than fetching full rows -- for display purposes (the "Scanning..."
/// subtitle's own live PendingVerification count) where only the number
/// itself is needed, not each item's full data. Already covered by the
/// existing (connection_id, status) index, so this stays cheap even
/// called frequently.
pub fn count_in_status(conn: &Connection, connection_id: &str, status: SyncStatus) -> SqlResult<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM sync_items WHERE connection_id = ?1 AND status = ?2",
        params![connection_id, status.as_str()],
        |row| row.get(0),
    )
}

/// Same shape as items_in_status above, but filtering on the
/// permission_error flag instead of status -- a flagged item can be in
/// any status (most commonly still 'pending', since it never got far
/// enough to become anything else), so this is a genuinely separate
/// query, not a status value to add to that one.
pub fn items_with_permission_error(conn: &Connection, connection_id: &str) -> SqlResult<Vec<SyncItem>> {
    let mut stmt = conn.prepare(
        "SELECT connection_id, relative_path, is_directory, local_mtime, local_size,
                remote_etag, last_synced_mtime, last_synced_size, last_synced_etag,
                last_synced_inode, last_synced_fileid, permission_error,
                permission_error_message, status, updated_at
         FROM sync_items WHERE connection_id = ?1 AND permission_error = 1",
    )?;
    let rows = stmt.query_map(params![connection_id], |row| {
        Ok(SyncItem {
            connection_id: row.get(0)?,
            relative_path: row.get(1)?,
            is_directory: row.get::<_, i64>(2)? != 0,
            local_mtime: row.get(3)?,
            local_size: row.get(4)?,
            remote_etag: row.get(5)?,
            last_synced_mtime: row.get(6)?,
            last_synced_size: row.get(7)?,
            last_synced_etag: row.get(8)?,
            last_synced_inode: row.get(9)?,
            last_synced_fileid: row.get(10)?,
            permission_error: row.get::<_, i64>(11)? != 0,
            permission_error_message: row.get(12)?,
            status: SyncStatus::from_str(&row.get::<_, String>(13)?),
            updated_at: row.get(14)?,
        })
    })?;
    rows.collect()
}
/// journal already knew about, regardless of whether they're still
/// visible in a live local or remote listing. Needed so a directory walk
/// can notice "this used to exist and is now gone from both sides" --
/// something that's invisible if you only ever look at current listings,
/// since a fully-vanished item doesn't appear in either one.
/// `parent_relative` = "" for the sync root's direct children.
pub fn direct_children(conn: &Connection, connection_id: &str, parent_relative: &str) -> SqlResult<Vec<String>> {
    let prefix = if parent_relative.is_empty() {
        String::new()
    } else {
        format!("{parent_relative}/")
    };
    // Escaped only for the LIKE pattern itself -- `prefix` above stays
    // raw/unescaped for the literal strip_prefix check below, which
    // needs to match the actual, real characters stored in relative_path,
    // not an escaped representation of them. This query was already
    // incidentally safe from the underlying bug (see escape_like_
    // pattern's own doc comment) purely because that check discards any
    // row an unescaped wildcard let through that doesn't actually,
    // literally start with prefix -- escaped here too anyway, for
    // consistency with every other prefix-matching query in this file,
    // and so a future caller of this same query shape can't reintroduce
    // the same bug by copying it without that same, easy-to-miss
    // protection.
    let escaped_prefix = escape_like_pattern(&prefix);
    let mut stmt = conn.prepare(
        "SELECT relative_path FROM sync_items WHERE connection_id = ?1 AND relative_path LIKE ?2 || '%' ESCAPE '\\'",
    )?;
    let rows = stmt.query_map(params![connection_id, escaped_prefix], |row| row.get::<_, String>(0))?;

    let mut names = std::collections::HashSet::new();
    for path in rows {
        let path = path?;
        let Some(rest) = path.strip_prefix(&prefix) else { continue };
        if rest.is_empty() {
            continue;
        }
        // Only the immediate next segment -- deeper descendants get
        // picked up when that subdirectory itself is walked.
        let direct = rest.split('/').next().unwrap_or(rest);
        names.insert(direct.to_string());
    }
    Ok(names.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory connection with the real schema from init_schema().
    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn
    }

    fn fresh_item() -> SyncItem {
        SyncItem::new("acct1", "Documents/todo.txt", false)
    }

    fn synced_item(mtime: i64, size: i64, etag: &str) -> SyncItem {
        let mut item = fresh_item();
        item.last_synced_mtime = Some(mtime);
        item.last_synced_size = Some(size);
        item.last_synced_etag = Some(etag.to_string());
        item
    }

    fn synced_item_with_inode(mtime: i64, size: i64, etag: &str, inode: i64) -> SyncItem {
        let mut item = synced_item(mtime, size, etag);
        item.last_synced_inode = Some(inode);
        item
    }

    #[test]
    fn brand_new_local_file_uploads() {
        let item = fresh_item(); // never synced before
        let action = decide_action(&item, Some((100, 50)), None, None, SyncDirection::Bidirectional);
        // local exists, remote doesn't, and it "changed" relative to a
        // never-synced state -- should upload, not delete-remote.
        assert_eq!(action, SyncAction::Upload);
    }

    #[test]
    fn brand_new_remote_file_downloads() {
        let item = fresh_item(); // never synced before
        let action = decide_action(&item, None, None, Some("etag1"), SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::Download);
    }

    #[test]
    fn never_synced_local_only_by_direction() {
        let item = fresh_item();
        let local = Some((100, 50));
        assert_eq!(decide_action(&item, local, None, None, SyncDirection::Bidirectional), SyncAction::Upload);
        assert_eq!(decide_action(&item, local, None, None, SyncDirection::UploadOnly), SyncAction::Upload);
        // DownloadOnly: the remote is authoritative, so a local-only
        // item is removed rather than uploaded.
        assert_eq!(decide_action(&item, local, None, None, SyncDirection::DownloadOnly), SyncAction::DeleteLocal);
    }

    #[test]
    fn never_synced_remote_only_by_direction() {
        let item = fresh_item();
        let remote = Some("etag1");
        assert_eq!(decide_action(&item, None, None, remote, SyncDirection::Bidirectional), SyncAction::Download);
        assert_eq!(decide_action(&item, None, None, remote, SyncDirection::DownloadOnly), SyncAction::Download);
        // UploadOnly: local is authoritative, so a remote-only item is
        // removed rather than downloaded.
        assert_eq!(decide_action(&item, None, None, remote, SyncDirection::UploadOnly), SyncAction::DeleteRemote);
    }

    #[test]
    fn never_synced_absent_on_both_sides_forgets_row_in_every_direction() {
        let item = fresh_item();
        for d in [SyncDirection::Bidirectional, SyncDirection::UploadOnly, SyncDirection::DownloadOnly] {
            assert_eq!(decide_action(&item, None, None, None, d), SyncAction::ForgetRow);
        }
    }

    #[test]
    fn same_path_appears_both_sides_with_no_history_is_conflict() {
        let item = fresh_item(); // never synced before
        let action = decide_action(&item, Some((100, 50)), None, Some("etag1"), SyncDirection::Bidirectional);
        // Can't safely assume these are the same content just because
        // neither side has been reconciled yet.
        assert_eq!(action, SyncAction::ConflictBothChanged);
    }

    #[test]
    fn unchanged_both_sides_is_noop() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, Some((100, 50)), None, Some("etag1"), SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::Noop);
    }

    #[test]
    fn local_edit_only_uploads() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, Some((200, 60)), None, Some("etag1"), SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::Upload);
    }

    #[test]
    fn remote_edit_only_downloads() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, Some((100, 50)), None, Some("etag2"), SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::Download);
    }

    #[test]
    fn both_sides_edited_is_conflict() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, Some((200, 60)), None, Some("etag2"), SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::ConflictBothChanged);
    }

    #[test]
    fn remote_deleted_local_unchanged_deletes_local() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, Some((100, 50)), None, None, SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::DeleteLocal);
    }

    #[test]
    fn local_deleted_remote_unchanged_deletes_remote() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, None, None, Some("etag1"), SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::DeleteRemote);
    }

    #[test]
    fn local_deleted_but_remote_also_changed_is_conflict_not_silent_delete() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, None, None, Some("etag2"), SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::ConflictDeleteVsChange);
    }

    #[test]
    fn remote_deleted_but_local_also_changed_is_conflict_not_silent_delete() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, Some((200, 60)), None, None, SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::ConflictDeleteVsChange);
    }

    #[test]
    fn gone_on_both_sides_forgets_the_row() {
        let item = synced_item(100, 50, "etag1");
        let action = decide_action(&item, None, None, None, SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::ForgetRow);
    }

    #[test]
    fn remote_deleted_mtime_size_coincide_but_inode_differs_is_conflict_not_silent_delete() {
        // The real, reported bug this fix closes: quit the app, delete
        // a path on both sides by hand (journal row never gets
        // cleared), then recreate it with content whose mtime+size
        // happen to coincide with the stale record -- e.g. re-
        // extracting the same archive with an extractor that preserves
        // timestamps. mtime+size alone would read this as "local
        // unchanged since last sync," and with remote genuinely gone,
        // would silently DeleteLocal the brand-new file. A mismatched
        // inode is proof this isn't the same file continuously
        // present, so it must surface as a conflict instead of a
        // silent delete.
        let item = synced_item_with_inode(100, 50, "etag1", 111);
        let action = decide_action(&item, Some((100, 50)), Some(222), None, SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::ConflictDeleteVsChange);
    }

    #[test]
    fn remote_deleted_inode_matches_still_deletes_local() {
        // The ordinary, safe case this fix must not break: a
        // continuously-present file (same inode throughout) whose
        // remote copy was genuinely deleted still propagates as a
        // plain, silent DeleteLocal -- exactly the pre-fix behavior,
        // confirmed unchanged when inode corroborates rather than
        // contradicts.
        let item = synced_item_with_inode(100, 50, "etag1", 111);
        let action = decide_action(&item, Some((100, 50)), Some(111), None, SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::DeleteLocal);
    }

    #[test]
    fn remote_deleted_no_inode_data_still_deletes_local() {
        // Backward compatibility: an older journal row predating this
        // field (last_synced_inode is None) or a caller that couldn't
        // look up a current inode must fall back to the original
        // mtime+size-only behavior exactly, not treat missing data as
        // a mismatch.
        let item = synced_item(100, 50, "etag1"); // no inode recorded
        let action = decide_action(&item, Some((100, 50)), Some(222), None, SyncDirection::Bidirectional);
        assert_eq!(action, SyncAction::DeleteLocal);
    }

    #[test]
    fn journal_roundtrip() {
        let conn = test_conn();

        mark_synced(&conn, "acct1", "a.txt", false, 100, 50, "etag1", None, None, 1000).unwrap();
        let item = get_item(&conn, "acct1", "a.txt").unwrap().unwrap();
        assert_eq!(item.status, SyncStatus::Synced);
        assert_eq!(item.last_synced_etag.as_deref(), Some("etag1"));

        mark_conflict_pending(&conn, "acct1", "a.txt", false, Some((150, 55)), Some("etag2"), 2000).unwrap();
        let item = get_item(&conn, "acct1", "a.txt").unwrap().unwrap();
        assert_eq!(item.status, SyncStatus::Conflict);
        // Frozen detection-time snapshot, not touched further until resolved.
        assert_eq!(item.local_mtime, Some(150));
        assert_eq!(item.local_size, Some(55));
        assert_eq!(item.remote_etag.as_deref(), Some("etag2"));
        // Old common ancestor left alone -- still there for reference.
        assert_eq!(item.last_synced_etag.as_deref(), Some("etag1"));

        let conflicted = items_in_status(&conn, "acct1", SyncStatus::Conflict).unwrap();
        assert_eq!(conflicted.len(), 1);
        assert_eq!(conflicted[0].relative_path, "a.txt");

        forget_item(&conn, "acct1", "a.txt").unwrap();
        assert!(get_item(&conn, "acct1", "a.txt").unwrap().is_none());
    }

    #[test]
    fn mark_conflict_pending_freezes_missing_side_as_none() {
        let conn = test_conn();

        // Delete-vs-change flavor: deleted locally, changed remotely --
        // current_local is None, current_remote_etag is Some.
        mark_conflict_pending(&conn, "acct1", "b.txt", false, None, Some("etag9"), 1000).unwrap();
        let item = get_item(&conn, "acct1", "b.txt").unwrap().unwrap();
        assert_eq!(item.status, SyncStatus::Conflict);
        assert_eq!(item.local_mtime, None);
        assert_eq!(item.local_size, None);
        assert_eq!(item.remote_etag.as_deref(), Some("etag9"));
    }
}

// src/sync.rs
//
// Walks a local directory and its corresponding remote directory (via a
// direct WebDAV client -- webdav.rs, no GVfs/gvfsd-dav involved anywhere;
// see that module's own top-level doc comment for the migration this
// replaced) in parallel, decides what to do about each item using
// journal::decide_action, and executes it.
//
// STATED LIMITATIONS, confirmed accurate as of this comment's own last
// rewrite -- this block drifted badly out of date once before (it used
// to describe the pre-migration, GVfs-based architecture verbatim, long
// after that migration actually happened), so treat it with appropriate
// skepticism rather than assuming it's still accurate indefinitely:
//   - No rename detection across directory boundaries, or between
//     connections -- a same-directory rename/move IS detected and
//     propagated as a real WebDAV/filesystem MOVE, not a delete+create
//     (see the rename-pairing blocks in sync_child_file/
//     sync_child_directory, keyed on local inode / remote fileid). A
//     move to a different top-level connection, or one that also
//     changes content, still shows up as delete+create.
//   - Symlinks are detected but deliberately never synced at all --
//     matching the Qt desktop client's own behavior, not an oversight
//     (see list_local's own is_symlink field and its callers).
//   - Permission/executable-bit preservation is local-only and best-
//     effort: WebDAV has no protocol-level concept of unix permission
//     bits at all, so a downloaded file's prior local mode is reapplied
//     afterward (see download_with_mode), but nothing is ever
//     transmitted to or read from the remote side -- a brand-new local
//     file just gets whatever the OS/umask gives it.
//   - Content comparison is mtime+size vs. ETag for the ordinary case,
//     with a real, streamed XXH3 hash comparison (see contents_match)
//     as a fallback specifically for conflict detection and for
//     avoiding an unnecessary transfer when metadata alone looked
//     changed but the actual bytes didn't -- not run on every single
//     item every round, since that would be needless work for the
//     overwhelming majority of files that are genuinely unchanged.
//   - Large uploads (see CHUNKED_UPLOAD_THRESHOLD_BYTES) use Nextcloud's
//     own chunking API when the account's been confirmed to support it,
//     falling back to a single PUT otherwise -- downloads have no
//     equivalent resumable path yet; an interrupted large download
//     restarts from scratch.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use std::time::{SystemTime, UNIX_EPOCH};
// Needed specifically for .for_each_concurrent() -- a trait method
// (StreamExt), unlike future::select/Either used elsewhere in this
// codebase via fully-qualified paths with no import at all. Rust's
// method-call syntax requires the trait itself in scope to resolve a
// method call, even when the type it's implemented for is referenced by
// a fully-qualified path.
use futures_util::StreamExt;

use gio::prelude::*;
use rusqlite::Connection;

use crate::exclude;
use crate::journal::{self, SyncAction};
use crate::status::{SyncEvent, TransferDirection};
use crate::webdav;

// No #[derive(Debug)] -- confirmed unused anywhere in the codebase, and
// SyncItem/Zeroizing below aren't worth deriving it for either.
pub struct SyncTarget {
    pub connection_id: String,
    /// User-facing name (config::SyncConnection::name), used only in log
    /// lines / events -- connection_id above is a UUID and stays that way
    /// since it's a journal key, not something to show anyone.
    pub connection_name: String,
    pub local_root: PathBuf,
    /// This connection's own configured direction -- threaded straight
    /// through to journal::decide_action, the one place it actually
    /// changes anything. Bidirectional for every connection created
    /// before this field existed (see config::SyncDirection's own
    /// #[default]), so nothing here changes behavior for a connection
    /// that never asked for anything but the original, symmetric sync.
    pub direction: crate::config::SyncDirection,
    /// Populated once, eagerly, by the caller (run_sync_round/build_
    /// sync_target_for_connection in main.rs, via AccountSource::
    /// credentials()/server_url() -- covering either a GOA account or a
    /// direct one with the same call, sync.rs itself has no account-
    /// type-specific lookup logic of its own anywhere) before sync_
    /// target is ever called at all -- not by sync_target's own body,
    /// which now assumes this is already
    /// Some. Everything deeper in the crawl (remote_url_for, the
    /// rename-pairing logic, etc.) reads this rather than re-fetching
    /// it, which would both be wasteful and risk hitting the account's
    /// own credential endpoint (GOA's D-Bus call, or the direct-account
    /// equivalent) far more than necessary. `RefCell<Option<>>`, not a
    /// plain field: SyncTarget's own construction and this value's own
    /// resolution are two separate steps for the caller, close together
    /// but not atomic, so this genuinely starts empty for a moment.
    ///
    /// Already the plain https:// root this connection's own
    /// remote_subpath should be joined onto -- not the bare account
    /// root the way the old mount-based remote_root was; there's no
    /// separate "resolve_relative_path" step happening anywhere else
    /// the way there used to be against a gio::File.
    pub remote_base_url: RefCell<Option<String>>,
    /// The account's own .../remote.php/dav/uploads/{userid} root --
    /// Nextcloud's chunking API lives under a completely separate DAV
    /// collection from ordinary file operations (confirmed directly
    /// against their developer docs), not something reachable by
    /// joining a relative path onto remote_base_url the way everything
    /// else in this app works. None whenever account.server_url wasn't
    /// available at setup time -- every caller already treats that the
    /// same as "chunked upload isn't available for this round," falling
    /// back to the ordinary single-PUT path, the same tolerance already
    /// established for oc:fileid and every other optional bonus in this
    /// app.
    pub chunking_base_url: RefCell<Option<String>>,
    pub username: RefCell<Option<String>>,
    /// Zeroizing, not a plain String -- see AccountSource::credentials'
    /// own doc comment (whichever of GoaWatcher::get_password_for_http_auth
    /// or nextcloud_login::get_app_password it actually called, depending
    /// on the account type, carries this same guarantee) on why this app
    /// is deliberate about how long a raw credential sits in memory.
    /// Held for the round's own duration here (unlike either of those
    /// functions' own single-request callers), since every remote
    /// request this round makes needs it -- still zeroized the moment
    /// this SyncTarget itself is dropped, same guarantee, just a longer
    /// legitimate lifetime.
    pub password: RefCell<Option<zeroize::Zeroizing<String>>>,
    /// Not plain fields anymore -- max_file_size_bytes, move_deleted_to_trash,
    /// the global exclude patterns, and this connection's own
    /// connection_patterns are bundled together into one CachedSettings,
    /// refreshed together as a single unit whenever settings_version
    /// changes. See CachedSettings and current_settings' own doc
    /// comments for the full reasoning -- in short, this is the same
    /// "cheap to check, rare to actually refresh" mechanism this session
    /// already built once for exclude patterns alone, generalized to
    /// cover every setting a round actually consults, since the same
    /// mid-round staleness problem applies equally to any of them, not
    /// just exclusion patterns.
    pub settings_version: Rc<Cell<u64>>,
    pub cached_settings: RefCell<(u64, CachedSettings)>,
    /// Paths already handled by a rename/move pairing earlier THIS round
    /// (see the pairing logic in sync_child_file/sync_child_directory).
    /// Two jobs: (1) since sync_directory snapshots a directory's
    /// local/remote listing once, up front, a path elsewhere in the tree
    /// whose journal row got renamed out from under it mid-round would
    /// otherwise be revisited against a now-stale snapshot -- checking
    /// this set first makes that a clean no-op instead of a confusing
    /// doomed re-delete/re-download attempt; (2) stops a second
    /// candidate from also claiming the same already-claimed source path
    /// (mirroring the real Nextcloud client's own `isRenamed` check).
    /// `Rc<RefCell<>>`, not a `&'a` reference -- SyncTarget is
    /// constructed fresh per round already, so a plain owned field needs
    /// no lifetime parameter threaded through every function that
    /// already takes `&SyncTarget`.
    pub renamed_away: Rc<RefCell<HashSet<String>>>,
    /// Round-wide progress counters -- see main.rs's own construction of
    /// SyncTarget for why these are the *same* Rc<Cell<>> shared across
    /// every connection processed within one round, not a fresh pair per
    /// connection: "Scanning..." is a single, app-wide status, not a
    /// per-connection one, so the progress display reads one aggregate
    /// total across everything currently being walked, not several
    /// separate per-connection numbers.
    ///
    /// Deliberately plain Cell<u64> increments, not a SyncEvent emitted
    /// per item -- with a tree in the hundreds of thousands of files,
    /// one event per discovery/completion would reintroduce exactly the
    /// kind of unbounded, high-frequency event volume this project
    /// already found and fixed twice this session (the log-rendering
    /// bug, the refresh_conflicts_ui bug). A Cell increment costs
    /// essentially nothing (no channel send, no allocation); the
    /// existing update_status_ui, which already runs on every real
    /// SyncEvent regardless, just reads the current totals directly when
    /// it does -- no new, additional high-frequency mechanism needed at
    /// all for the display to stay live.
    pub discovered_count: Rc<Cell<u64>>,
    /// Incremented once per item, at the point its own outcome is fully
    /// decided either way -- covers the early-return paths (excluded,
    /// symlink) as well as sync_child_file/sync_child_directory actually
    /// completing, so this always ends up equal to discovered_count once
    /// a round's real walk (not the background verification pass, which
    /// doesn't touch either counter) finishes.
    pub processed_count: Rc<Cell<u64>>,
    /// True only for the first round of a given process's own session --
    /// see AppState::first_round_this_session in main.rs, which this is
    /// set from at construction. Folded into the normal walk (see
    /// sync_directory's own temp-file check) rather than a dedicated,
    /// separate pre-pass that used to block everything else in
    /// run_background (the event listener, the periodic sync loop, the
    /// local-file-watch loop) until an entire extra walk of the whole
    /// remote tree finished -- confirmed as a real, severe bug on a large
    /// remote tree: the UI sat at "Idle" with zero feedback, sometimes
    /// for minutes, before anything else in the app could even start.
    /// Once per session, not once per round, for the same reason the
    /// original dedicated sweep was one-time: there's no ongoing need to
    /// keep re-checking for leftovers once a session's first full walk
    /// has already found and cleared whatever was there.
    pub cleanup_temp_files: bool,
    /// Shared across the entire recursive tree walk for this connection's
    /// round -- see Semaphore's own doc comment for the bug this exists
    /// to fix. An Rc-clone of the single, app-wide Semaphore AppState
    /// itself owns (see build_sync_target_for_connection in main.rs) --
    /// what makes this "one, single, real ceiling for the whole walk,"
    /// not a fresh allowance handed out again at every recursive level.
    pub content_semaphore: Rc<Semaphore>,
    /// Pool 1 (discovery) in this session's own terms -- an Rc-clone of
    /// the single, app-wide HybridPool AppState itself owns, same
    /// pattern and same reasoning as content_semaphore just above.
    /// Guarantees list_remote_with_recovery specifically (the calls that
    /// actually drive the recursive walk) can keep making progress even
    /// while content_semaphore is fully saturated with transfers/
    /// verifications -- "keeping up with reality" (what still needs
    /// attention) matters more than "acting on it promptly" (uploading/
    /// downloading it right now), since a journal that's fallen behind
    /// on what it even knows needs doing can't be caught up on later
    /// without guessing, where a journal that's current but has a
    /// backlog of un-acted-on work can always be worked through
    /// afterward with no loss of information.
    pub discovery_pool: Rc<HybridPool>,
    /// Pool 2 (confirm/detect changes) in this session's own terms --
    /// same pattern, same reasoning as discovery_pool just above, for a
    /// narrower set of calls: standalone stat/etag checks that confirm a
    /// specific, already-suspected change rather than enumerate a whole
    /// directory (Pool 1) or transfer/verify content (Pool 3). Serves
    /// Pool 1's own accuracy more than being an independent priority in
    /// its own right -- a rename confirmed by the walk but never
    /// stat-checked for its new ETag is exactly the kind of gap Pool 1's
    /// own reserve exists to prevent one level up.
    pub confirm_pool: Rc<HybridPool>,
    /// Set (never cleared here -- main.rs resets it fresh at the start
    /// of every round) whenever MIN_FILE_AGE_SECS defers an Upload this
    /// round -- see that check's own doc comment for why the defer
    /// itself is correct and deliberate, and see this field's own use in
    /// main.rs's trigger_sync_round for the real, reported gap it closes:
    /// a deferred file was previously only ever picked back up by
    /// whatever round happens to run next, which could be a very long
    /// wait (the full remote_poll_interval, possibly hours) if nothing
    /// else locally active happens to trigger a sooner one in the
    /// meantime -- confirmed as a real report: a freshly-extracted
    /// archive's directory got created immediately but its files never
    /// uploaded, with nothing further happening afterward to ever retry
    /// them. `Rc<Cell<bool>>`, shared across every connection processed
    /// within one round -- same shape as discovered_count/processed_count
    /// above, for the same reason: this is a single, round-wide signal
    /// ("did *anything* get deferred this round"), not a per-connection
    /// one.
    pub had_freshness_deferral: Rc<Cell<bool>>,
    /// The *same* Rc AppState::dirty_dirs owns, not a fresh or copied
    /// map -- so a freshness deferral (see had_freshness_deferral's own
    /// doc comment) can record exactly which directory needs a
    /// follow-up look by inserting into it directly, the same way a
    /// watch.rs FileMonitor callback already does. This is what lets
    /// main.rs's schedule_freshness_retry fire a *scoped* follow-up
    /// round -- restricted to just the directories that actually had
    /// something deferred -- instead of a full crawl: by the time that
    /// follow-up round runs, this map already has the right entries
    /// waiting in it, via the exact same drain-and-scope machinery a
    /// real local-watch event uses.
    pub dirty_dirs: Rc<RefCell<HashMap<String, HashSet<PathBuf>>>>,
    /// Restricts this round's own walk to a specific set of directories
    /// (plus their ancestors, plus anything genuinely new -- see
    /// sync_child_directory's own recursion gate for the exact rule),
    /// rather than a full recursive crawl of the entire tree. `None`
    /// (the ordinary case: the periodic timer, a manual "Sync Now," a
    /// settings change, notify_push's force_full_check) means "no
    /// restriction, walk everything," exactly the behavior this app
    /// always had. `Some(_)` only ever comes from main.rs's own
    /// watch-debounce path (see run_sync_round's own `scoped` parameter)
    /// -- a local FileMonitor callback fired on specific directories, so
    /// there's no need to re-walk the ones nothing was reported dirty
    /// on. Already expanded to include every ancestor path component
    /// (see expand_dirty_scope) before it ever reaches here, so the
    /// recursion check itself is a plain set-membership test, not a
    /// prefix scan repeated at every level.
    pub dirty_scope: Option<HashSet<String>>,
    /// Per-connection, but persistent *across* rounds within one session
    /// (unlike renamed_away, which is fresh every round) -- an Rc clone
    /// of the same map AppState owns, not a fresh one constructed here.
    /// See sync_child_directory's own use, and CLEANUP_RETRY_COOLDOWN_SECS'
    /// own doc comment, for the real bug this exists to fix: a folder-
    /// cleanup deletion that just failed shouldn't be retried on literally
    /// every subsequent round. Keyed by relative_path within this
    /// connection (connection_id is already implicit -- each connection
    /// gets its own SyncTarget, and therefore its own clone of this map,
    /// at construction time in main.rs).
    pub cleanup_retry_cooldown: Rc<RefCell<std::collections::HashMap<String, std::time::Instant>>>,
    /// Same shape and same reasoning as cleanup_retry_cooldown just
    /// above (an Rc clone of a map AppState owns, persistent across
    /// rounds, keyed by relative_path) -- for a different purpose: how
    /// many CONSECUTIVE rounds in a row a file's own (mtime, size) pair
    /// has been observed completely unchanged, alongside the last pair
    /// actually observed. See the upload-readiness gate's own doc
    /// comment (just below MIN_FILE_AGE_SECS' own usage) for why this
    /// exists as a second, independent requirement on top of the
    /// existing age-based cooldown, not a replacement for it -- a large
    /// file that's still slowly growing can easily stay "recently
    /// modified" for far longer than MIN_FILE_AGE_SECS alone would ever
    /// catch.
    pub stability_streak: Rc<RefCell<std::collections::HashMap<String, ((i64, i64), u32)>>>,
    /// The Download-side counterpart to stability_streak just above --
    /// same purpose (don't act on a file that might still be mid-write
    /// elsewhere), same two-requirement shape (an age cooldown plus a
    /// consecutive-observation streak), but for the remote side, which
    /// had no equivalent at all before this: the age/stability gate just
    /// above only ever ran for SyncAction::Upload, confirmed as a real,
    /// reported asymmetry -- by explicit request, this closes it rather
    /// than only fixing the specific delete-settle gap deletion_pending
    /// below already covered symmetrically. (etag, size), not
    /// (mtime, size) -- a remote file's own mtime is optional and
    /// server-reported, less reliable than its own etag (which changes
    /// deterministically whenever content or metadata actually changes,
    /// by WebDAV's own contract), so etag is the primary signal here,
    /// with mtime used only for the age-cooldown half when the server
    /// happens to report one.
    pub remote_stability_streak: Rc<RefCell<std::collections::HashMap<String, ((String, i64), u32)>>>,
    /// Same shape and pattern as stability_streak just above, for the
    /// same underlying reason but the opposite direction: a settle-time
    /// guard for DeleteLocal/DeleteRemote, which had no equivalent to
    /// the upload-readiness gate at all before this -- a single
    /// observation of "this path is missing" was acted on immediately,
    /// no confirmation required. Confirmed as a real, reported bug: an
    /// atomic "write a new temp file, then rename it over the original"
    /// save (the standard pattern for editors and this app's own
    /// create_file/str_replace tools, specifically to avoid a reader
    /// ever observing a half-written file) briefly unlinks the old inode
    /// before the new one lands at the same path. A scoped,
    /// inotify-triggered round landing in that exact, narrow window sees
    /// a genuinely-if-transiently missing file and fires DeleteRemote
    /// immediately -- with the delete already having propagated by the
    /// time a later round notices the file is back (now under a new
    /// inode), producing exactly the "deleted, then a brand new file
    /// created" behavior reported, and occasionally a conflict against a
    /// now-empty remote side if the timing lines up.
    ///
    /// This gate itself always covered DeleteLocal/DeleteRemote, but
    /// ConflictDeleteVsChange -- one side missing, the *other* side
    /// changed since last sync -- went ungated for a real, confirmed
    /// stretch of this app's own history, despite this same doc comment
    /// already naming that exact conflict case above as something this
    /// was meant to prevent. The narrow-window replace race doesn't care
    /// which action a transient absence happens to route to; whichever
    /// of the two syncs first this round, the other side's own most
    /// recent change (real or itself mid-write) is exactly what tips a
    /// same-side-missing case from a plain delete into this one instead,
    /// so leaving it out meant the single most disruptive outcome of
    /// this exact race -- landing in the resolver, not just a quiet
    /// republish -- was the one case this gate wasn't actually stopping.
    ///
    /// No (mtime, size) snapshot to compare here the way stability_streak
    /// has -- a missing file has neither. Just a plain count of
    /// consecutive observations of absence, alongside when the absence
    /// was first observed (the cooldown's own basis, in place of a
    /// mtime that doesn't exist for a file that isn't there).
    ///
    /// Gated by DELETION_MIN_AGE_SECS/DELETION_REQUIRED_STREAK, not
    /// MIN_FILE_AGE_SECS/REQUIRED_STABILITY_STREAK -- confirmed as a
    /// real, reported bug when this shared the latter: a remote, server-
    /// side overwrite can genuinely take longer than a local atomic-
    /// rename's own, much faster race to settle, so this direction needs
    /// real, deliberately more generous numbers of its own, not the same
    /// ones tuned for the local side's own, faster race.
    pub deletion_pending: Rc<RefCell<std::collections::HashMap<String, (std::time::Instant, u32)>>>,
    /// Consecutive WebDAV/HTTP/transport failure count, per relative
    /// path -- specifically for throttled failure logging, not a retry
    /// gate the way stability_streak/deletion_pending above are (nothing
    /// here ever blocks an attempt from proceeding; every round still
    /// tries again regardless of this count). By explicit request:
    /// ItemFailed previously logged every single failure, every round,
    /// in full -- for a persistently flaky connection this meant the
    /// same "couldn't reach the server" failure repeating in the log on
    /// every scoped or periodic round indefinitely. Only SyncError::
    /// WebDav failures increment this (see tag_webdav_action's own doc
    /// comment on why only that error kind carries the action label the
    /// throttled line needs); Local/Remote/Journal failures are rarer
    /// and more meaningful, so they still log every time, unthrottled,
    /// through the ordinary ItemFailed path. Session-only, not
    /// persisted, matching every other per-path tracker on this struct
    /// -- a restart clearing the count is fine, since this exists to
    /// catch ongoing flakiness within a session, not accumulate a
    /// lifetime total. Cleared the moment a path actually succeeds (see
    /// its own clear site in retry_single_item/the main walk loop), so a
    /// later, unrelated failure starts counting fresh.
    pub webdav_failure_streak: Rc<RefCell<std::collections::HashMap<String, u32>>>,
    /// One Semaphore(capacity 1) per destination parent directory URL a
    /// MOVE has targeted, created lazily on first use -- serializes
    /// concurrent MOVEs that would otherwise land in the same directory
    /// at once. See webdav_move_serialized's own doc comment for the
    /// full, confirmed root cause this fixes (a Nextcloud MOVE is
    /// create+delete internally, not an atomic rename, and briefly locks
    /// the destination's own parent directory as part of that).
    /// Session-wide, cloned from AppState into every SyncTarget at
    /// construction time -- same lifecycle as webdav_failure_streak just
    /// above (confirmed by checking, not assumed: every field on this
    /// struct sharing this same "state.field.clone()" pattern really is
    /// session-wide, not rebuilt fresh each round the way this doc
    /// comment originally, incorrectly claimed before that got checked).
    /// A lingering, unused directory's own semaphore entry outliving the
    /// round that created it is harmless -- the same accepted, bounded-
    /// per-unique-key growth this struct's own other trackers already
    /// carry for a session's lifetime.
    pub move_locks: Rc<RefCell<std::collections::HashMap<String, Rc<Semaphore>>>>,
}

/// Expands a set of dirty relative directory paths to also include every
/// ancestor of each one (plus the root, ""), so sync_directory's own
/// recursion check can be a simple, direct set-membership test at every
/// level of the walk instead of a prefix comparison against the full,
/// unexpanded set repeated at each step. E.g. `{"a/b/c"}` expands to
/// `{"", "a", "a/b", "a/b/c"}` -- the walk needs to pass through "a" and
/// "a/b" to ever reach "a/b/c" at all, so both need to read as
/// "in scope" even though neither was itself reported dirty.
pub fn expand_dirty_scope(dirs: HashSet<String>) -> HashSet<String> {
    let mut expanded = HashSet::new();
    expanded.insert(String::new()); // the root itself, relative == ""
    for dir in dirs {
        let mut prefix = String::new();
        expanded.insert(prefix.clone());
        for component in dir.split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(component);
            expanded.insert(prefix.clone());
        }
    }
    expanded
}

/// Every setting a sync round actually consults, bundled together so
/// they're all refreshed as one unit rather than each having their own,
/// separate cache-and-generation dance. Deliberately bundled rather than
/// kept as separate SyncTarget fields (the previous design, for
/// max_file_size_bytes/move_deleted_to_trash/connection_patterns) --
/// confirmed as a real, if smaller-scale, version of the same staleness
/// bug caching exclude patterns alone already had: any of these changing
/// mid-round (raising the file-size limit, excluding a specific
/// connection's own subpath) previously couldn't take effect until the
/// next round, for exactly the same reason a global exclude pattern
/// couldn't. One combined AppState::settings_version, not a separate
/// counter per setting or per connection -- see the design discussion
/// this came from: settings changes are inherently rare (a person
/// editing Settings, not a per-file event), so a small amount of
/// "unrelated" over-invalidation costs nothing in practice, and it's a
/// much simpler mental model than tracking which of several counters a
/// given setting belongs to.
#[derive(Clone, Default)]
pub struct CachedSettings {
    pub global_patterns: Vec<crate::config::ExcludePattern>,
    pub connection_patterns: Vec<String>,
    pub max_file_size_bytes: u64,
    pub move_deleted_to_trash: bool,
}

impl SyncTarget {
    /// Builds this connection's own full remote URL for a relative
    /// path -- the direct replacement for the `.child(name)`-chaining
    /// gio::File navigation used to do. Panics if called before
    /// sync_target's own setup has populated remote_base_url -- every
    /// call site downstream of that setup can rely on it already being
    /// populated, the same assumption the old remote_root field's own
    /// doc comment already made about mounting happening first.
    fn remote_url_for(&self, relative: &str) -> String {
        let base = self.remote_base_url.borrow();
        let base = base.as_deref().expect("remote_url_for called before sync_target's own setup populated remote_base_url");
        webdav::build_url(base, relative)
    }

    /// This connection's own username/password, cloned out for a
    /// `run_blocking` closure to move onto its own thread -- cloned
    /// rather than borrowed since the closure needs to own what it
    /// carries across that thread boundary. Panics under the same
    /// "setup must have already run" assumption as remote_url_for.
    fn credentials(&self) -> (String, zeroize::Zeroizing<String>) {
        let username = self.username.borrow().clone().expect("credentials() called before sync_target's own setup populated it");
        let password = self.password.borrow().as_ref().expect("credentials() called before sync_target's own setup populated it").clone();
        (username, password)
    }

    async fn webdav_list(&self, url: &str) -> Result<HashMap<String, webdav::RemoteEntry>, webdav::WebDavError> {
        let (username, password) = self.credentials();
        let url = url.to_string();
        run_blocking(move || webdav::list(&url, &username, &password)).await
    }

    async fn webdav_delete(&self, url: &str) -> Result<(), webdav::WebDavError> {
        let (username, password) = self.credentials();
        let url = url.to_string();
        run_blocking(move || webdav::delete(&url, &username, &password)).await
    }

    async fn webdav_mkcol(&self, url: &str) -> Result<(), webdav::WebDavError> {
        let (username, password) = self.credentials();
        let url = url.to_string();
        run_blocking(move || webdav::mkcol(&url, &username, &password)).await
    }

    async fn webdav_move(&self, from_url: &str, to_url: &str, overwrite: bool, mtime: Option<i64>) -> Result<(), webdav::WebDavError> {
        let (username, password) = self.credentials();
        let (from_url, to_url) = (from_url.to_string(), to_url.to_string());
        run_blocking(move || webdav::move_item(&from_url, &to_url, overwrite, mtime, &username, &password)).await
    }

    /// One item's own current metadata -- the direct replacement for
    /// query_info_with_recovery's role (that function no longer exists;
    /// see its own removal note in this file's git history), most
    /// commonly used right after a MOVE to learn the new ETag a MOVE's
    /// own response doesn't carry the way a PUT's does.
    async fn webdav_stat(&self, url: &str) -> Result<webdav::RemoteEntry, webdav::WebDavError> {
        let (username, password) = self.credentials();
        let url = url.to_string();
        run_blocking(move || webdav::stat(&url, &username, &password)).await
    }

    /// Whole-body-in-memory GET -- only ever used for genuinely small
    /// files (.no-sync markers, at present), never the actual content
    /// transfer path, which is what webdav::download's own streamed,
    /// temp-file-then-rename version exists for instead.
    async fn webdav_download_to_memory(&self, url: &str) -> Result<Vec<u8>, webdav::WebDavError> {
        let (username, password) = self.credentials();
        let url = url.to_string();
        run_blocking(move || webdav::get_to_memory(&url, &username, &password)).await
    }

    /// XXH3-128 of url's current content, computed incrementally as it
    /// streams in -- see webdav::hash_content's own doc comment for why
    /// this exists as its own thing rather than reusing
    /// webdav_download_to_memory for content verification (that one
    /// buffers the whole body and caps the request at 30 seconds total,
    /// neither of which is safe for a file that could legitimately be
    /// gigabytes in size), and for why XXH3 rather than SHA256 (what
    /// this used to be).
    async fn webdav_hash_content(&self, url: &str) -> Result<u128, webdav::WebDavError> {
        let (username, password) = self.credentials();
        let url = url.to_string();
        run_blocking(move || webdav::hash_content(&url, &username, &password)).await
    }

    /// Returns the current settings bundle (global exclude patterns,
    /// this connection's own connection_patterns, max_file_size_bytes,
    /// move_deleted_to_trash), re-fetching from disk only when
    /// settings_version has actually changed since the last fetch --
    /// otherwise this is just two cheap integer reads and a comparison,
    /// no I/O at all, which is what makes it affordable to call once per
    /// name checked (see sync_directory's own per-item exclusion check)
    /// rather than once per directory or once per round.
    ///
    /// Confirmed as a real, meaningful gap without this: with settings
    /// fetched once, up front, and cached for an entire round (the
    /// original design, built for exclude patterns specifically before
    /// being generalized here), a change made by the user mid-round
    /// couldn't take effect until the *next* round started -- for a
    /// directory already being actively walked (and for a large, deep
    /// tree, potentially still being walked for a long time), that meant
    /// continuing to upload exactly what the user had just tried to
    /// stop. Checking the generation before every single name now means
    /// a newly-added exclusion (or any other setting change) is honored
    /// starting with the very next name checked, not the next round --
    /// the only thing that can't be interrupted is whatever single
    /// transfer is already physically in flight at that exact instant,
    /// which no mechanism checking between items rather than
    /// mid-transfer ever could stop anyway.
    ///
    /// Two separate disk reads on a refresh -- config::AppConfig::load()
    /// for max_file_size_bytes/move_deleted_to_trash/this connection's
    /// own connection_patterns (all three live in the one main config
    /// file), and config::effective_exclude_patterns() for global_patterns
    /// (its own separate, shareable file, see that function's own doc
    /// comment for why). Both only ever happen together, on the same
    /// rare refresh, not as two independent cache checks.
    pub fn current_settings(&self) -> CachedSettings {
        let current_gen = self.settings_version.get();
        if self.cached_settings.borrow().0 != current_gen {
            let config = crate::config::AppConfig::load();
            let connection_patterns = config.connections.iter()
                .find(|c| c.id == self.connection_id)
                .map(|c| c.excluded_subpaths.clone())
                .unwrap_or_default();
            let fresh = CachedSettings {
                global_patterns: crate::config::effective_exclude_patterns(),
                connection_patterns,
                max_file_size_bytes: config.max_file_size_bytes,
                move_deleted_to_trash: config.move_deleted_to_trash,
            };
            *self.cached_settings.borrow_mut() = (current_gen, fresh);
        }
        // Cloned out and returned by value, not the borrowed Ref this
        // used to return -- confirmed as a real, reported panic
        // ("RefCell already borrowed") without this: with a live Ref
        // held by one caller, any other concurrent task sharing this
        // same SyncTarget (see for_each_concurrent's own per-item
        // closures, all borrowing the same `target` reference) that
        // called this at the wrong moment would conflict with it.
        // Returning an owned value instead means the borrow here is
        // only ever held for the instant it takes to clone out, never
        // for as long as the caller happens to hold onto the result --
        // eliminating the whole panic class rather than chasing down
        // which specific call site's own timing happened to trigger it
        // this time. CachedSettings itself is small (two short
        // Vec<String>s and a couple of scalars), so the clone cost here is
        // negligible next to the actual I/O this whole cache exists to
        // avoid.
        self.cached_settings.borrow().1.clone()
    }
}

/// Wraps target.webdav_move with two complementary fixes for the same,
/// confirmed root cause -- by request, implementing both rather than
/// picking one: a genuine, real Nextcloud MOVE isn't an atomic rename at
/// the storage layer, it's implemented internally as create+delete
/// (confirmed directly against a Nextcloud maintainer's own comment on
/// a real, public GitHub issue reproducing a single, non-concurrent,
/// bare MOVE hitting exactly this: "DAV move actually does a
/// create+delete, which might be the cause. (parents still locked, or
/// something like that)"), which needs to briefly lock the
/// destination's own parent directory as part of updating its listing.
/// This app's own deliberate per-file concurrency within a sync round
/// means several files finishing their own uploads into the very same
/// directory at nearly the same moment is completely ordinary, expected
/// behavior, not a rare edge case -- and each one's own finalizing MOVE
/// needs that exact same, shared parent-directory lock, so they can
/// genuinely, legitimately collide with each other. Entirely self-
/// inflicted, confirmed with real evidence: the PUT to each file's own,
/// distinct temp object never had any of this trouble (a brand new,
/// isolated object has no directory listing to update, hence no lock to
/// contend over), only the finalizing MOVE onto each one's own,
/// already-tracked destination.
///
/// First fix: real, proactive serialization. Two of this app's own
/// MOVEs that would land in the same destination parent directory now
/// queue behind each other (see SyncTarget::move_locks' own doc
/// comment) rather than racing -- this alone should eliminate the self-
/// collision case entirely, the common case this whole investigation
/// started from.
///
/// Second fix: a bounded retry specifically for whatever serialization
/// alone can't cover -- a genuinely different process (another client
/// entirely, a web UI action, anything else touching this same account)
/// still holding that same lock for a moment. Deliberately its own,
/// separate, narrow retry -- not folded into retry_once/retry_transfer_
/// on_permission_error, since a 423 needs no probe to decide whether
/// retrying is worth it the way a 401/403 does; the status code alone
/// already, definitively means "transient, someone else briefly holds
/// this," not "possibly a real, lasting problem." Reuses the exact same
/// already-uploaded temp object on every retry -- never a fresh PUT --
/// since only the MOVE itself, not the underlying content, was ever in
/// question.
async fn webdav_move_serialized(target: &SyncTarget, from_url: &str, to_url: &str, overwrite: bool, mtime: Option<i64>) -> Result<(), webdav::WebDavError> {
    // Falls back to the full to_url itself if parent extraction somehow
    // fails -- harmless even then, since this is purely the lock's own
    // key, not the actual MOVE target (which still uses the real to_url
    // regardless): worst case, this one, unusual URL just gets its own,
    // unshared lock instead of correctly queuing behind siblings, not
    // anything actually incorrect.
    let parent_url = webdav::parent_url(to_url).unwrap_or_else(|| to_url.to_string());
    let move_lock = {
        let mut locks = target.move_locks.borrow_mut();
        locks.entry(parent_url).or_insert_with(|| Rc::new(Semaphore::with_capacity(1))).clone()
    };
    let _permit = move_lock.acquire().await;

    const MAX_ATTEMPTS: u32 = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        match target.webdav_move(from_url, to_url, overwrite, mtime).await {
            Ok(()) => return Ok(()),
            Err(webdav::WebDavError::UnexpectedStatus { status: 423, .. }) if attempt < MAX_ATTEMPTS => {
                // Short and slightly backing off, not a long or
                // aggressive wait -- these are, by their own confirmed
                // nature, brief, self-clearing contentions (another
                // MOVE's own create+delete finishing up), not a lasting
                // problem that would benefit from patience instead.
                glib::timeout_future(std::time::Duration::from_millis(200 * attempt as u64)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("the loop's only continue branch is gated on attempt < MAX_ATTEMPTS, so the final iteration always returns instead")
}

/// Minimum time since a temp file's own mtime before it's treated as an
/// actual leftover rather than a currently-in-progress transfer --
/// distinguishes a crash-orphaned .ripples-partial-* file from one this
/// very session is still actively writing to. Generous on purpose: this
/// check only ever runs once per session, so there's no cost to being
/// patient, but a real one to being wrong in the other direction.
pub const MIN_TEMP_FILE_AGE_SECS: u64 = 3600; // 1 hour

/// Minimum time since a file's local mtime before an automatic Upload
/// will act on it -- see the check in sync_child_file for the full
/// reasoning. 5 seconds roughly matches the watch loop's own 2s poll +
/// 3s settle delay (main.rs), so a watch-triggered round and a
/// periodic-timer round get comparable protection rather than the
/// periodic one having none at all.
const MIN_FILE_AGE_SECS: i64 = 5;

/// How many CONSECUTIVE rounds in a row a file's own (mtime, size) pair
/// must be observed completely unchanged before an automatic Upload
/// will act on it -- a second, independent requirement alongside
/// MIN_FILE_AGE_SECS just above, not a replacement for it. Confirmed
/// need: MIN_FILE_AGE_SECS alone only proves time has passed since the
/// mtime last moved, not that the file has actually stopped changing --
/// a large file still being slowly written (a multi-gigabyte extraction
/// or copy, say) can easily keep growing well past that window, with
/// its own mtime updating throughout. Three, not some other number, per
/// explicit request -- see sync_child_file's own doc comment on the
/// full check for how this and the cooldown combine.
const REQUIRED_STABILITY_STREAK: u32 = 3;

/// deletion_pending's own settling requirements -- deliberately separate
/// constants from MIN_FILE_AGE_SECS/REQUIRED_STABILITY_STREAK above, not
/// shared with them anymore. Confirmed as a real, reported bug when they
/// were shared: those two were tuned for a fast, sub-second local
/// atomic-rename race (write a temp file, rename it over the original),
/// but a remote, server-side overwrite -- especially several files at
/// once, e.g. a build process regenerating a handful of icons -- can
/// genuinely take longer than 3 rounds/5 seconds to settle across every
/// single poll. The old, shared thresholds meant a real, reported case
/// where a remote overwrite got treated as a genuine deletion (DeleteLocal
/// actually executing, forget_item wiping the journal row, the file then
/// reappearing under what decide_action could only see as a brand-new,
/// never-synced path moments later) purely because the settling window
/// was too short for how long the remote side actually took, not because
/// anything was genuinely wrong. By explicit request -- reconsidered for
/// local too, not just remote, once raising this for remote alone made
/// clear how much slower a "fast" race can turn out to be in practice: a
/// delayed, correctly-detected change is always preferable to an
/// unnecessary one, so both directions get the same, more generous
/// numbers rather than local keeping the old, tighter ones.
const DELETION_MIN_AGE_SECS: i64 = 30;
/// Same reasoning as REQUIRED_STABILITY_STREAK's own doc comment above,
/// just a larger number for the same reason DELETION_MIN_AGE_SECS is
/// larger than MIN_FILE_AGE_SECS -- at the existing 8-second freshness-
/// retry interval, 5 consecutive rounds already naturally spans roughly
/// 40 seconds on its own, with DELETION_MIN_AGE_SECS mainly acting as a
/// backstop for rounds triggered more frequently than that (notify_push,
/// Sync Now).
const DELETION_REQUIRED_STREAK: u32 = 5;

/// How many times download_with_mode/upload_temp_first will retry with a
/// fresh temp file if the current one vanishes between being written and
/// the final rename/MOVE onto the real destination -- see either
/// function's own doc comment for the full scenario this guards against.
/// Small and bounded on purpose: this exists to survive a one-off
/// interference (another client's cleanup sweep, a confused user), not
/// to paper over something persistently deleting temp files, which
/// should fail cleanly and fall through to the normal per-item-failure
/// handling rather than retry forever.
const TEMP_FILE_RETRY_LIMIT: u32 = 2;

/// How long a failed folder-cleanup deletion (see sync_child_directory's
/// own Cleanability::FullyCleanable handling) waits before being
/// re-attempted -- see that call site's own doc comment for the real,
/// reported bug this exists to fix: without a cooldown, a directory
/// whose DELETE fails (most commonly gvfsd-dav's own directory-listing
/// cache not yet reflecting a delete flurry that only just finished)
/// gets retried on every single round, and with notify_push connected
/// that can mean every few seconds, indefinitely. 60 seconds is short
/// enough that a genuinely resolved case (the cache catches up, or a
/// person fixes something) recovers within a person's normal patience,
/// long enough that it's a clearly different order of magnitude from
/// notify_push's own near-continuous retriggering during an active
/// bulk-delete.
const CLEANUP_RETRY_COOLDOWN_SECS: u64 = 60;

/// Runs the actual recursive walk for one connection. `target`'s own
/// remote_base_url/chunking_base_url/username/password are expected to
/// already be populated -- the caller resolves these once, up front,
/// via AccountSource::credentials()/server_url() (covering either
/// account type with the same call), the same established pattern
/// build_sync_target_for_connection in main.rs already uses for its own
/// callers (single-item retry, conflict resolution). This function used
/// to do that resolution itself, but only ever via a GOA-specific D-Bus
/// lookup (watcher.account_info) -- a real, confirmed bug fixed by
/// moving this out entirely, not by teaching this function to also
/// handle a direct account: every other caller of SyncTarget's own
/// per-item logic (retry_single_item, verify_pending_item, conflict
/// resolution) already assumed pre-populated credentials, so unifying
/// this one, remaining exception onto that same, already-correct
/// pattern removes the duplication and the bug in the same change.
pub async fn sync_target(
    journal_conn: &Connection,
    target: &SyncTarget,
    events_tx: &async_channel::Sender<SyncEvent>,
) {
    if let Err(e) = sync_directory(journal_conn, target, &target.local_root, "", events_tx).await {
        eprintln!("sync[{}]: error at root: {e}", target.connection_id);
        let _ = events_tx.try_send(SyncEvent::ConnectionFailed {
            connection: target.connection_name.clone(),
            error: e.to_string(),
        });
    }
}

/// Targeted retry for exactly one item, by relative path -- the
/// "Retry"/"Retry Now" action from the Sync Issues carousel. Reuses the
/// exact same sync_child_file/sync_child_directory the ordinary walk
/// calls, just for one path instead of recursing an entire tree -- this
/// is genuinely the same decision-and-execution logic, not a separate,
/// parallel implementation that could quietly drift from it over time.
///
/// `target` is expected to already have its credentials populated (see
/// build_sync_target_for_connection in main.rs) -- the caller fetches
/// them once, up front, same as any other single-item action already
/// does (conflict resolution, a live remote-info lookup).
///
/// Clears the permission_error flag first, before attempting anything --
/// if this attempt fails again with the same kind of error, the normal
/// per-item error handling below re-flags it immediately, same as the
/// ordinary walk would. This isn't "assume it's fixed," it's "give it
/// exactly one real attempt now that something (the user, directly)
/// gave a reason to believe it might have changed."
/// Resolves one item already sitting in the PendingVerification queue --
/// see SyncStatus::PendingVerification's own doc comment in journal.rs
/// for the full design. Deliberately does NOT call decide_action/
/// sync_child_file or retry_single_item -- re-running the ordinary sync
/// logic would just re-discover the same size+mtime match and defer to
/// PendingVerification again, forever, never actually reaching a real
/// answer. This function exists specifically to skip that shortcut and
/// go straight to the real, unconditional content comparison -- by the
/// time an item is here, "does the metadata match" has already been
/// answered; the only open question left is "does the content actually
/// match," which is exactly what this checks.
pub async fn verify_pending_item(
    journal_conn: &Connection,
    target: &SyncTarget,
    item: &journal::SyncItem,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    let local_path = target.local_root.join(&item.relative_path);
    let remote_url = target.remote_url_for(&item.relative_path);

    let now = now_unix();
    let size = item.local_size.unwrap_or(0);
    let comparison = contents_match(target, &item.relative_path, &local_path, &remote_url, size, target.current_settings().max_file_size_bytes).await;

    match comparison {
        ContentComparison::Match => {
            let mtime = item.local_mtime.expect("PendingVerification items always have local_mtime recorded (see mark_pending_verification)");
            let etag = item.remote_etag.clone().unwrap_or_default();
            journal::mark_synced(journal_conn, &target.connection_id, &item.relative_path, item.is_directory, mtime, size, &etag, local_inode(&local_path), None, now)?;
            eprintln!("sync[{}]: {} background verification confirmed identical -- no copy needed", target.connection_id, item.relative_path);
            let _ = events_tx.try_send(SyncEvent::ConflictResolvedIdentical {
                connection: target.connection_name.clone(),
                path: item.relative_path.clone(),
            });
        }
        ContentComparison::Different => {
            journal::mark_conflict_pending(
                journal_conn, &target.connection_id, &item.relative_path, item.is_directory,
                item.local_mtime.map(|m| (m, size)), item.remote_etag.as_deref(), now,
            )?;
            eprintln!("sync[{}]: {} background verification found a genuine content difference despite matching size+mtime -- now a real conflict", target.connection_id, item.relative_path);
            let _ = events_tx.try_send(SyncEvent::Conflict {
                connection: target.connection_name.clone(),
                path: item.relative_path.clone(),
            });
        }
        // Deliberately left untouched -- the item is already in
        // PendingVerification (that's why it was picked up here at all),
        // so simply not resolving it means it naturally stays queued for
        // the next verification pass to try again, rather than either
        // side of the real outcome being guessed at from a check that
        // never actually completed.
        ContentComparison::Indeterminate => {}
    }
    Ok(())
}

/// Genuinely single-item -- if relative_path is a directory, only that
/// directory itself (create/confirm it exists, mark it synced) is
/// attempted, never its own children; see sync_child_directory's own
/// recurse parameter and doc comment for the confirmed, reported bug
/// this exists to avoid: a directory holding several, separately
/// PermissionError-flagged children being silently, surprisingly
/// "resolved" wholesale by retrying just one of them.
///
/// Also genuinely attempts an actual transfer or deletion for a file, not
/// just silently defers one -- see sync_child_file's own skip_settling_checks
/// parameter and doc comment for a second and third confirmed bug in the
/// same family, found in the same audit: every settling gate that requires
/// several consecutive observations before acting (the stability-streak
/// checks guarding against uploading/downloading a file still mid-write,
/// AND deletion_pending guarding against a transient replace race briefly
/// looking like a real deletion) is keyed on a given SyncTarget's own
/// in-memory map, which this function always builds fresh, so none of them
/// could ever once accumulate their own required streak within a single,
/// one-shot call. Every Retry/Retry Now on an item actually needing an
/// Upload/Download/deletion was silently, instantly deferred with nothing
/// attempted at all, reported to the resolver as an outright success
/// regardless.
pub async fn retry_single_item(
    journal_conn: &Connection,
    target: &SyncTarget,
    relative_path: &str,
    is_directory: bool,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    journal::clear_permission_error(journal_conn, &target.connection_id, relative_path)?;

    // Split into parent directory + this item's own name -- sync_child_file/
    // sync_child_directory both operate relative to a parent, the same
    // shape the ordinary directory walk already calls them with.
    let (parent_relative, name) = match relative_path.rsplit_once('/') {
        Some((parent, name)) => (parent, name),
        None => ("", relative_path),
    };
    let local_dir = if parent_relative.is_empty() { target.local_root.clone() } else { target.local_root.join(parent_relative) };
    let remote_dir_url = target.remote_url_for(parent_relative);

    // Enumerating the whole parent directory just to read one entry back
    // out of it is more than strictly necessary, but reuses list_local/
    // list_remote exactly as they already exist rather than writing a
    // third, single-entry variant of the same query -- acceptable here
    // specifically because this is a rare, user-initiated action, not a
    // hot path the way the ordinary recursive walk is.
    let local_dir_owned = local_dir.clone();
    let local_children = run_blocking(move || list_local(&local_dir_owned)).await?;
    let remote_children = list_remote_with_recovery(target, &remote_dir_url).await?;
    let local_entry = local_children.get(name);
    let remote_entry = remote_children.get(name);

    let result = if is_directory {
        sync_child_directory(journal_conn, target, &local_dir, &remote_dir_url, relative_path, name,
            local_entry.is_some(), remote_entry.is_some(), remote_entry.and_then(|e| e.fileid.as_deref()), false, events_tx).await
    } else {
        sync_child_file(journal_conn, target, &local_dir, &remote_dir_url, relative_path, name,
            local_entry, remote_entry, true, events_tx).await
    };

    // Same per-item error handling the ordinary walk's own loop does --
    // a repeat permission failure re-flags immediately rather than
    // silently leaving the just-cleared flag in a stale, incorrect
    // "fixed" state; anything else still surfaces as an ordinary
    // ItemFailed, exactly as it would have from within the normal walk.
    if let Err(e) = &result {
        if is_permission_error(e) {
            journal::mark_permission_error(journal_conn, &target.connection_id, relative_path, is_directory, &e.to_string(), now_unix())?;
            let _ = events_tx.try_send(SyncEvent::PermissionError {
                connection: target.connection_name.clone(),
                path: relative_path.to_string(),
            });
        } else {
            report_item_failed(target, events_tx, relative_path.to_string(), e);
        }
    } else {
        // Clears the streak on genuine success -- see webdav_failure_
        // streak's own doc comment on SyncTarget for why: a later,
        // unrelated failure at this same path should start counting
        // fresh, not inherit an already-elevated count from a prior,
        // now-resolved bout of flakiness.
        target.webdav_failure_streak.borrow_mut().remove(relative_path);
    }
    result
}

#[derive(Debug)]
pub enum SyncError {
    Local(std::io::Error),
    /// gio's own local trash API (gio::File::trash_future) -- the *only*
    /// remaining source of a glib::Error anywhere in this file, confirmed
    /// directly: no other call site produces one at all. Named Trash, not
    /// Remote -- this variant used to genuinely be about GVfs-mounted
    /// remote operations, back when this app spoke WebDAV through gvfsd-
    /// dav rather than directly (see webdav.rs's own top-level doc
    /// comment for that migration); once the migration finished, this
    /// became the local-trash-call's own error type by default, since
    /// gio::File::trash_future still returns a glib::Error regardless of
    /// GVfs being gone from the actual sync path entirely, but the name
    /// itself was never updated to match. Confirmed as a real, reported
    /// point of confusion this caused: "remote (GVfs) error" in a
    /// Display string read as exactly what it sounds like -- an actual
    /// GVfs-mounted remote operation -- when what it was actually
    /// reporting was a local trash-move failure with nothing remote
    /// about it at all.
    Trash(glib::Error),
    Journal(rusqlite::Error),
    /// The new, non-GVfs remote path (see webdav.rs's own top-level doc
    /// comment for the full migration this is part of). Kept as a
    /// separate variant from Trash(glib::Error) rather than folded
    /// into it -- the two are genuinely different error shapes (an
    /// HTTP status/transport failure vs. gio's own local trash API), and
    /// callers further up (is_permission_error, the disk-log Display
    /// text) benefit from being able to tell which one actually happened
    /// rather than a single, conflated bucket.
    ///
    /// action: which SyncAction was being attempted when this failed --
    /// None by default (the blanket From<webdav::WebDavError> impl just
    /// below sets it that way, since most `?`-propagation sites have no
    /// way to know this), Some only where a caller that genuinely does
    /// know the action explicitly tags it via tag_webdav_action. Exists
    /// specifically for the throttled failure-logging in retry_
    /// single_item/the main walk loop's own ItemFailed handling -- see
    /// that logic's own doc comment for the full "{action} failed
    /// ({tries} tries) @ {path}" format this feeds.
    WebDav(webdav::WebDavError, Option<&'static str>),
}
impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Local(e) => write!(f, "local I/O error: {e}"),
            SyncError::Trash(e) => write!(f, "local trash error: {e}"),
            SyncError::Journal(e) => write!(f, "journal error: {e}"),
            SyncError::WebDav(e, _) => write!(f, "remote (WebDAV) error: {e}"),
        }
    }
}
impl From<std::io::Error> for SyncError { fn from(e: std::io::Error) -> Self { SyncError::Local(e) } }
impl From<glib::Error> for SyncError { fn from(e: glib::Error) -> Self { SyncError::Trash(e) } }
impl From<rusqlite::Error> for SyncError { fn from(e: rusqlite::Error) -> Self { SyncError::Journal(e) } }
impl From<webdav::WebDavError> for SyncError { fn from(e: webdav::WebDavError) -> Self { SyncError::WebDav(e, None) } }

/// Tags a SyncError::WebDav with which SyncAction was actually being
/// attempted, if it doesn't already carry one -- see that variant's own
/// doc comment for why this exists. A plain .map_err(...) rather than a
/// method on SyncError itself, since it only ever needs calling from a
/// small handful of sites that already know the action (execute_action's
/// own call site in sync_child_file; the directory-level webdav_move/
/// webdav_mkcol/webdav_delete calls in sync_child_directory), not
/// something every SyncError-returning function needs to remember.
/// Non-WebDav errors, and a WebDav error that's already tagged (a
/// nested call already tagged it more precisely), pass through
/// unchanged -- this never overwrites an existing, more specific tag.
fn tag_webdav_action(err: SyncError, action: &'static str) -> SyncError {
    match err {
        SyncError::WebDav(e, None) => SyncError::WebDav(e, Some(action)),
        other => other,
    }
}

/// How many consecutive WebDAV/HTTP/transport failures at the same path
/// it takes before this reports one -- see webdav_failure_streak's own
/// doc comment on SyncTarget for the full reasoning.
const WEBDAV_FAILURE_LOG_INTERVAL: u32 = 5;

/// Reports one item failure -- the single place all three call sites in
/// this file (retry_single_item, and the two per-item loops in
/// sync_directory) that used to build their own SyncEvent::ItemFailed
/// now go through, so the throttling logic below only needs writing (and
/// getting right) once. A tagged SyncError::WebDav (see tag_webdav_
/// action's own doc comment) is throttled -- only a genuine multiple of
/// WEBDAV_FAILURE_LOG_INTERVAL consecutive failures at this exact path
/// actually sends anything, as SyncEvent::WebDavFailureThrottled rather
/// than the ordinary ItemFailed. Every other kind of failure (Local,
/// Remote, Journal, or a WebDav error that somehow never got tagged)
/// reports as an ordinary, unthrottled ItemFailed instead, exactly as it
/// always did -- these are rarer and more meaningful, by explicit
/// request, so they keep the full, immediate, every-time treatment.
fn report_item_failed(target: &SyncTarget, events_tx: &async_channel::Sender<SyncEvent>, path: String, error: &SyncError) {
    if let SyncError::WebDav(_, Some(action)) = error {
        let action = *action;
        let tries = {
            let mut streaks = target.webdav_failure_streak.borrow_mut();
            let count = streaks.get(&path).copied().unwrap_or(0) + 1;
            streaks.insert(path.clone(), count);
            count
        };
        if tries % WEBDAV_FAILURE_LOG_INTERVAL != 0 {
            return; // not a reporting multiple yet -- stay quiet
        }
        let _ = events_tx.try_send(SyncEvent::WebDavFailureThrottled {
            connection: target.connection_name.clone(),
            action,
            tries,
            path,
        });
    } else {
        let _ = events_tx.try_send(SyncEvent::ItemFailed {
            connection: target.connection_name.clone(),
            path,
            error: error.to_string(),
        });
    }
}

/// kind of failure -- deliberately checks both Local and Trash, not
/// just the remote WebDAV side most of the design discussion around this
/// focused on: a local file/directory can be just as genuinely
/// read-only as a remote one (a chmod'd file, a read-only local mount),
/// and std::io::Error's own ErrorKind::PermissionDenied is at least as
/// reliable a signal as gio::Error's equivalent, arguably more so, since
/// it reflects real, local POSIX permission bits directly rather than
/// depending on how gio's own local trash API happens to report things.
/// Journal errors are never treated as permission errors -- a SQLite
/// failure says nothing about filesystem permissions on either side.
pub fn is_permission_error(err: &SyncError) -> bool {
    match err {
        SyncError::Local(e) => e.kind() == std::io::ErrorKind::PermissionDenied,
        // Local trash-move failure specifically -- see SyncError::Trash's
        // own doc comment for why this isn't a remote/GVfs check at all,
        // despite what an earlier name/Display text on this variant used
        // to suggest.
        SyncError::Trash(e) => e.matches(gio::IOErrorEnum::PermissionDenied),
        SyncError::Journal(_) => false,
        SyncError::WebDav(webdav::WebDavError::UnexpectedStatus { status, .. }, _) => *status == 401 || *status == 403,
        SyncError::WebDav(_, _) => false,
    }
}


struct LocalEntry { is_dir: bool, mtime: i64, size: i64, is_symlink: bool, inode: i64 }
/// Alias, not a redefinition -- webdav::RemoteEntry (see that module's
/// own doc comment) was deliberately built field-for-field identical to
/// what this used to define locally, specifically so consolidating the
/// two is this one line rather than hunting down and rewriting every
/// call site already using the bare `RemoteEntry` name throughout this
/// file.
type RemoteEntry = webdav::RemoteEntry;

fn list_local(dir: &Path) -> std::io::Result<std::collections::HashMap<String, LocalEntry>> {
    use std::os::unix::fs::MetadataExt;
    let mut out = std::collections::HashMap::new();
    let read = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out), // not created yet -- fine
        Err(e) => return Err(e),
    };
    for entry in read {
        let entry = entry?;
        let meta = entry.metadata()?; // DirEntry::metadata() does NOT follow symlinks -- this is symlink-vs-target info, not target info
        let mtime = meta.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        let name = entry.file_name().to_string_lossy().into_owned();
        out.insert(name, LocalEntry { is_dir: meta.is_dir(), mtime, size: meta.len() as i64, is_symlink: meta.is_symlink(), inode: meta.ino() as i64 });
    }
    Ok(out)
}

/// Thin wrapper, not a recovery mechanism any more -- see this
/// function's own git history (or webdav.rs's top-level doc comment)
/// for what used to live here: a 60-second timeout plus a forced
/// remount retry, purpose-built for gvfsd-dav's own specific failure
/// mode of hanging indefinitely with no error at all. That failure mode
/// doesn't exist here -- webdav::agent's own timeout_connect/
/// timeout_send_request/timeout_recv_response already bound every
/// phase of the request and return a proper, ordinary error instead of
/// hanging, so there's nothing left for this wrapper to add on top.
/// Kept as its own named function anyway, rather than inlining
/// target.webdav_list(url) at every call site directly -- this is still
/// the one place a future, genuinely-needed retry policy (network
/// blips, as opposed to a wedged daemon) would go, without touching
/// every caller again.
async fn list_remote_with_recovery(target: &SyncTarget, url: &str) -> Result<HashMap<String, RemoteEntry>, SyncError> {
    // Acquired here, inside this thin wrapper, rather than at each of
    // its own call sites -- safe specifically because this function is
    // only ever called from the normal recursive walk, never from a
    // conflict-resolution action, so there's no risk of accidentally
    // gating the "resolutions bypass the queue entirely" behavior. Pool
    // 1 (discovery) specifically, not the plain shared content_semaphore
    // -- this is exactly the "keep the map of what's actually there
    // current" work Pool 1's own reserve exists to protect, see
    // SyncTarget::discovery_pool's own doc comment for the full
    // reasoning.
    let _permit = target.discovery_pool.acquire(&target.content_semaphore, "Pool 1 (discovery)").await;
    target.webdav_list(url).await.map_err(Into::into)
}

fn join_relative(parent: &str, name: &str) -> String {
    if parent.is_empty() { name.to_string() } else { format!("{parent}/{name}") }
}

/// Runs a blocking closure on a fresh, one-off OS thread and returns its
/// result asynchronously -- for contents_match's own local file read and
/// hash computation, which are otherwise entirely synchronous, CPU/IO-
/// bound work with no yield points at all. Confirmed as a real, serious
/// bug (not just a theoretical one): std::fs::read on a large file, plus
/// two full SHA256 computations, all directly on the single glib
/// MainContext thread, would block the entire UI -- no redraws, no
/// input processing -- for as long as that one file takes, exactly
/// matching a real, reported "stuck on a large/slow file, then a sudden
/// burst of activity" symptom. Same pattern as push::run_blocking,
/// deliberately duplicated here rather than made a cross-module
/// dependency for something this small and self-contained.
async fn run_blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = async_channel::bounded(1);
    std::thread::spawn(move || {
        let result = f();
        let _ = tx.send_blocking(result);
    });
    rx.recv().await.expect("the spawned thread always sends exactly one result before finishing")
}

/// Wraps a single, expensive transfer attempt (an actual upload or
/// download, not a metadata call) with one, cheap, cost-bounded retry
/// specifically for a permission error that looks like it might have
/// been transient. By request, resolving what extending retry_once to
/// WebDavError::UnexpectedStatus (see that function's own doc comment)
/// left open: that fix only ever covered the cheap metadata calls
/// (list/stat/mkcol/delete/move_item) -- retry_once itself was
/// deliberately never used for upload/download, since blindly retrying
/// a large, partway-failed transfer wholesale is a genuinely more
/// expensive tradeoff than retrying a cheap metadata call, and that
/// exclusion is still correct as far as it goes.
///
/// This closes the actual, remaining gap without ever paying transfer's
/// own real cost unless there's good reason to believe it'll actually
/// help: on a 401/403 specifically, one plain, bodiless stat() call
/// against `probe_url` decides first -- if credentials and reachability
/// are confirmed still fine right now, the original failure was almost
/// certainly the exact kind of one-off blip (a proxy hiccup, a session
/// expiring at the wrong moment) this whole investigation started from,
/// and only THEN is a single retry of the real transfer worth paying
/// for. If the probe itself also fails, that's real confirmation, not a
/// fluke -- the ORIGINAL transfer error is what's returned in that
/// case, not the probe's own, separate one, since the original already
/// says exactly what happened to the operation the caller actually
/// asked for; the probe exists purely to decide whether retrying is
/// worth it, not to replace the transfer's own error reporting.
///
/// `probe_url` deliberately isn't always the same URL the transfer
/// itself targeted -- see this function's own callers for why: a
/// download's probe target is the file being downloaded, which should
/// already exist, but an upload's probe target is its parent directory,
/// not the temp object being written to, since that temp object may
/// legitimately not exist yet if the PUT never got far enough to create
/// it, and a 404 for a not-yet-created object would otherwise look
/// exactly like a real, confirmed permission problem.
fn retry_transfer_on_permission_error<T>(
    probe_url: &str,
    username: &str,
    password: &str,
    mut attempt: impl FnMut() -> Result<T, webdav::WebDavError>,
) -> Result<T, webdav::WebDavError> {
    let first_result = attempt();
    let Err(webdav::WebDavError::UnexpectedStatus { status, .. }) = &first_result else {
        return first_result;
    };
    if *status != 401 && *status != 403 {
        return first_result;
    }
    if webdav::stat(probe_url, username, password).is_err() {
        return first_result;
    }
    attempt()
}

/// Distinguishes a genuine content difference from simply not being able
/// to complete the check at all -- confirmed as a real, reported
/// problem when these were conflated: a connection blip during content
/// verification (see list_remote_with_recovery's own doc comment for
/// how routine that can be against a struggling GVfs mount) could flag
/// many items as real conflicts at once, none of which were actually
/// examined, flooding the resolver with items that probably aren't
/// conflicts at all. Callers treat Indeterminate as "try again later"
/// (left in, or returned to, PendingVerification), not as evidence of
/// anything -- only Different is ever actually shown to the user as a
/// real conflict.
enum ContentComparison {
    Match,
    Different,
    Indeterminate,
}

/// Quick content-equality check before creating a conflicted copy --
/// only attempted when both sides are the same size and within the
/// same configured max_file_size_bytes limit that gates whether a file
/// syncs at all (SyncTarget::max_file_size_bytes) -- a file already
/// small enough to sync in the first place is small enough to read
/// twice for this comparison too, so there's no reason for a second,
/// smaller, separate cap here. Any failure here (network hiccup,
/// permission issue, size mismatch on actual read) returns false rather
/// than risking a false "they match" -- the safe default is to preserve
/// both copies via the existing conflicted-copy path, not to skip it on
/// a shaky read.
/// Chunk size for the large-file comparison path below -- 128 MiB, per
/// this session's own conversation. Deliberately not derived from
/// anything (no server limit, no memory/CPU calculation) -- just large
/// enough that per-chunk overhead (a fresh HTTP request, a fresh local
/// seek) stays negligible relative to the work each chunk actually does.
const CHUNKED_HASH_CHUNK_BYTES: u64 = 128 * 1024 * 1024;

/// Minimum file size before the chunked comparison path is even
/// attempted -- 4 chunks' worth, not 1. A file only slightly over one
/// chunk wouldn't gain much from the chunking machinery's own overhead
/// (spawning multiple concurrent tasks, tracking early-exit state)
/// relative to just hashing it directly the plain way; this only kicks
/// in once there's a genuinely large file to actually get concurrency
/// and early-exit benefit out of splitting.
const CHUNKED_HASH_THRESHOLD_BYTES: u64 = 4 * CHUNKED_HASH_CHUNK_BYTES;

/// How many of one file's own chunks can be in flight at once -- a
/// deliberate compromise, not either extreme. Letting one huge file
/// claim all of Pool 3 would finish that one file fastest, but blocks
/// every other, unrelated piece of Pool 3 work (uploads, downloads,
/// other files' own hashing) for the file's entire duration -- exactly
/// the "processed_count stalls, then bursts" symptom this whole
/// investigation started from. Capping at 1 would remove that risk
/// entirely but give up all the concurrency benefit chunking exists to
/// provide. 4 leaves 2 lanes free for other work the common, mid-round
/// case (many other things ready to run) while still giving a huge file
/// real, multi-way parallelism.
const CHUNKED_HASH_MAX_CONCURRENCY_PER_FILE: usize = 4;

async fn contents_match(target: &SyncTarget, relative_path: &str, local_path: &Path, remote_url: &str, size: i64, max_file_size_bytes: u64) -> ContentComparison {
    let connection_id = &target.connection_id;
    // Every early return here used to be completely silent and
    // indistinguishable from a genuine hash mismatch -- confirmed as a
    // real diagnostic gap: "frozen as a conflict" looked identical
    // whether the files were actually different, or a transient local/
    // remote read failure meant they were never actually compared at
    // all. Each path now says specifically which happened.
    if size < 0 || size as u64 > max_file_size_bytes {
        eprintln!("sync[{connection_id}]: {relative_path} -- skipping content check, size ({size}) exceeds the configured max-file-size limit ({max_file_size_bytes})");
        return ContentComparison::Indeterminate;
    }

    // Large files go through the chunked path -- see its own doc
    // comment for the full reasoning. Falls back to the plain path below
    // (not returned directly) if the server turns out not to honor
    // Range requests for this particular file -- a real, if rare,
    // possibility this app can't assume away just because its own
    // primary target (Nextcloud) is already confirmed to support it via
    // download's own resumable-download usage.
    if size as u64 >= CHUNKED_HASH_THRESHOLD_BYTES {
        match contents_match_chunked(target, relative_path, local_path, remote_url, size as u64).await {
            ChunkedComparisonResult::Comparison(result) => return result,
            ChunkedComparisonResult::RangeNotSupported => {
                eprintln!("sync[{connection_id}]: {relative_path} -- server didn't honor a Range request for this file, falling back to a single, whole-file hash instead of the chunked comparison");
            }
        }
    }

    // Acquired before the real, expensive work below (a remote read plus
    // two hash computations) begins -- same global, tree-wide ceiling as
    // Upload/Download share (see Semaphore's own doc comment), since a
    // content check is equally real, network-heavy work that could
    // otherwise compound the same way across a deep, wide tree.
    let _permit = target.content_semaphore.acquire().await;
    // Streamed and hashed incrementally, never buffered whole -- see
    // webdav::hash_content's own doc comment for the real, confirmed bug
    // this fixes: max_file_size_bytes only bounds this to *some* size,
    // not to "small," and a real multi-gigabyte file in this app's own
    // test tree was hitting this check. The old webdav_download_to_memory
    // call here used metadata_client's own 30-second overall timeout
    // (meant for small metadata-only calls) and buffered the entire
    // remote body into a Vec<u8> -- the request couldn't even complete
    // for a file that size, so this was failing every single attempt,
    // forever, the file staying permanently stuck mid-verification
    // rather than ever actually resolving either way.
    let remote_hash = match target.webdav_hash_content(remote_url).await {
        Ok(h) => h,
        Err(_) => {
            eprintln!("sync[{connection_id}]: {relative_path} -- couldn't read remote file for content check, queued for another attempt rather than treated as a conflict");
            return ContentComparison::Indeterminate;
        }
    };

    // Local side hashed the same streamed way, moved to a background
    // thread since it's blocking I/O plus CPU-bound hashing, neither of
    // which belongs on the glib MainContext. A straight XXH3 mismatch
    // already implies "genuinely different" regardless of whether the
    // difference is content or length -- no separate length check
    // needed on top of it the way comparing two fully-buffered byte
    // vectors used to want one for.
    let local_path = local_path.to_path_buf();
    let connection_id_owned = connection_id.to_string();
    let relative_path_owned = relative_path.to_string();
    run_blocking(move || {
        use std::io::Read;
        let local_hash: Option<u128> = (|| {
            let file = std::fs::File::open(&local_path).ok()?;
            let mut reader = std::io::BufReader::new(file);
            let mut hasher = xxhash_rust::xxh3::Xxh3::new();
            let mut buf = [0u8; 65536];
            loop {
                let n = reader.read(&mut buf).ok()?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            Some(hasher.digest128())
        })();
        let Some(local_hash) = local_hash else {
            eprintln!("sync[{connection_id_owned}]: {relative_path_owned} -- couldn't read local file for content check, queued for another attempt rather than treated as a conflict");
            return ContentComparison::Indeterminate;
        };
        if local_hash == remote_hash {
            ContentComparison::Match
        } else {
            eprintln!("sync[{connection_id_owned}]: {relative_path_owned} -- same size ({size} bytes) but different content (XXH3 mismatch) -- genuinely different files, not a false alarm");
            ContentComparison::Different
        }
    }).await
}

enum ChunkedComparisonResult {
    Comparison(ContentComparison),
    RangeNotSupported,
}

/// Splits a large file's content comparison into CHUNKED_HASH_CHUNK_BYTES
/// pieces, hashing each side's own corresponding chunk independently and
/// comparing as results come back, rather than reading the entire file
/// on both sides before any comparison can happen at all -- see this
/// session's own conversation for the full reasoning (a single,
/// genuinely massive file was confirmed, from real production logs, to
/// hold one Pool 3 lane for over 7 minutes with 5 others sitting
/// completely idle, since nothing else was left in the tree to fill
/// them by that point in the round).
///
/// Stops submitting new chunks the moment any chunk reveals a mismatch
/// (or a read failure, or the server not honoring Range) -- but chunks
/// already in flight at that instant are allowed to finish naturally and
/// their results discarded, rather than forcibly cancelled. A streaming
/// hash mid-read on its own OS thread (via run_blocking) can't be safely
/// aborted at all once started; CHUNKED_HASH_MAX_CONCURRENCY_PER_FILE's
/// own cap is what bounds how much of that wasted work an early mismatch
/// can ever cause (at most that many minus one chunks already running),
/// not an attempt to eliminate it entirely.
async fn contents_match_chunked(target: &SyncTarget, relative_path: &str, local_path: &Path, remote_url: &str, size: u64) -> ChunkedComparisonResult {
    let connection_id = &target.connection_id;
    let mut ranges = Vec::new();
    let mut offset = 0u64;
    while offset < size {
        let end = (offset + CHUNKED_HASH_CHUNK_BYTES - 1).min(size - 1);
        ranges.push((offset, end));
        offset += CHUNKED_HASH_CHUNK_BYTES;
    }
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] {relative_path}: chunked comparison starting, {} bytes across {} chunks", now_hms(), size, ranges.len());
    }

    // Checked by each chunk before starting its own real work -- set the
    // moment any chunk (already running) comes back mismatched, so no
    // *new* chunk starts once the file's already known to differ. Not
    // shared with mismatch below via a single combined enum -- kept as
    // two separate flags since they need genuinely different handling
    // upstream (Different is a confirmed, real answer; the other two
    // outcomes below mean this comparison couldn't actually be
    // completed at all).
    let mismatch_found = Rc::new(Cell::new(false));
    let read_failed = Rc::new(Cell::new(false));
    let range_not_supported = Rc::new(Cell::new(false));

    futures_util::stream::iter(ranges).for_each_concurrent(Some(CHUNKED_HASH_MAX_CONCURRENCY_PER_FILE), |(start, end)| {
        let mismatch_found = mismatch_found.clone();
        let read_failed = read_failed.clone();
        let range_not_supported = range_not_supported.clone();
        async move {
            // Checked both before and after acquiring a permit -- a
            // mismatch (or failure) found by another chunk could easily
            // land while this one was still waiting its turn, and
            // there's no reason to actually start the real work below
            // once the outcome is already decided.
            if mismatch_found.get() || read_failed.get() || range_not_supported.get() {
                if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] {relative_path}: chunk {start}-{end} skipped, outcome already decided", now_hms());
                }
                return;
            }
            let _permit = target.content_semaphore.acquire().await;
            if mismatch_found.get() || read_failed.get() || range_not_supported.get() {
                if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] {relative_path}: chunk {start}-{end} skipped after acquiring, outcome already decided", now_hms());
                }
                return;
            }

            let (username, password) = target.credentials();
            let remote_url_owned = remote_url.to_string();
            let remote_result = run_blocking(move || webdav::hash_content_range(&remote_url_owned, &username, &password, start, end)).await;
            let remote_hash = match remote_result {
                Ok(webdav::RangeHashOutcome::Hash(h)) => h,
                Ok(webdav::RangeHashOutcome::RangeNotSupported) => {
                    if crate::is_debug_enabled() {
                        eprintln!("[{}] [debug] {relative_path}: chunk {start}-{end} -- server didn't honor Range, abandoning chunked comparison", now_hms());
                    }
                    range_not_supported.set(true);
                    return;
                }
                Err(_) => {
                    if crate::is_debug_enabled() {
                        eprintln!("[{}] [debug] {relative_path}: chunk {start}-{end} -- remote read failed", now_hms());
                    }
                    read_failed.set(true);
                    return;
                }
            };

            let local_path_owned = local_path.to_path_buf();
            let local_hash = run_blocking(move || {
                use std::io::{Read, Seek, SeekFrom};
                let mut file = std::fs::File::open(&local_path_owned).ok()?;
                file.seek(SeekFrom::Start(start)).ok()?;
                let mut hasher = xxhash_rust::xxh3::Xxh3::new();
                let mut remaining = end - start + 1;
                let mut buf = [0u8; 65536];
                while remaining > 0 {
                    let want = (buf.len() as u64).min(remaining) as usize;
                    let n = file.read(&mut buf[..want]).ok()?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    remaining -= n as u64;
                }
                Some(hasher.digest128())
            }).await;

            let Some(local_hash) = local_hash else {
                if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] {relative_path}: chunk {start}-{end} -- local read failed", now_hms());
                }
                read_failed.set(true);
                return;
            };

            if local_hash != remote_hash {
                if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] {relative_path}: chunk {start}-{end} -- MISMATCH, stopping new chunks", now_hms());
                }
                mismatch_found.set(true);
            } else if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] {relative_path}: chunk {start}-{end} -- match", now_hms());
            }
        }
    }).await;

    if range_not_supported.get() {
        ChunkedComparisonResult::RangeNotSupported
    } else if read_failed.get() {
        eprintln!("sync[{connection_id}]: {relative_path} -- couldn't read one or more chunks during content check, queued for another attempt rather than treated as a conflict");
        ChunkedComparisonResult::Comparison(ContentComparison::Indeterminate)
    } else if mismatch_found.get() {
        eprintln!("sync[{connection_id}]: {relative_path} -- same size ({size} bytes) but different content (chunked XXH3 mismatch) -- genuinely different files, not a false alarm");
        ChunkedComparisonResult::Comparison(ContentComparison::Different)
    } else {
        ChunkedComparisonResult::Comparison(ContentComparison::Match)
    }
}

fn conflicted_copy_path(local_path: &Path) -> PathBuf {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let stem = local_path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    let ext = local_path.extension().map(|s| s.to_string_lossy().into_owned());
    let new_name = match ext {
        Some(ext) => format!("{stem} (conflicted copy {now}).{ext}"),
        None => format!("{stem} (conflicted copy {now})"),
    };
    local_path.with_file_name(new_name)
}

/// A single, shared concurrency ceiling for every connection this app
/// makes to the server -- transfers (upload/download), content hashing
/// (contents_match), and metadata operations (PROPFIND/MKCOL/DELETE/
/// MOVE) all draw from this same pool now, rather than each having its
/// own separately-tuned limit.
///
/// This used to be memory-derived: available RAM divided by
/// max_file_size_bytes, on the reasoning that every concurrent operation
/// might be buffering up to a full max-size file's worth of data at
/// once. That stopped being true once upload/download/hash were all
/// confirmed to stream -- fixed-size chunks through a small buffer,
/// never the whole file in memory regardless of its size -- so the
/// formula was gating concurrency against a memory cost that no longer
/// existed. MAX_PERMITS is now just a fixed constant (see its own doc
/// comment for why 6, and where that number actually comes from).
///
/// A simple async semaphore built on async_channel (already a
/// dependency here, so no new crate needed) rather than a raw counter --
/// acquiring a permit means receiving a token from a channel pre-filled
/// with `permits` of them, which naturally, cheaply suspends the caller
/// if none are currently available, with no manual polling/waking logic
/// to get wrong. Releasing means sending a token back; the returned
/// guard does this automatically on Drop, so a permit can never be
/// accidentally leaked even if the holding task returns early via `?`
/// or panics.
///
/// Exists specifically to fix a real, confirmed bug: a per-directory-
/// level for_each_concurrent bound, on its own, only ever bounded
/// concurrency *per directory level*, not globally across a whole tree
/// walk -- since sync_directory recurses, each subdirectory's own
/// for_each_concurrent call got its own, completely independent budget,
/// on top of every other one already in flight elsewhere in the tree.
/// That compounds multiplicatively with depth (8 directories concurrent,
/// each with 8 more subdirectories concurrent, each with 8 files
/// transferring concurrently, is already up to 512 at only three levels
/// deep) -- confirmed in practice as Active Transfers rising into the
/// hundreds, not the single digits a local bound alone was actually
/// meant to guarantee. One Semaphore, constructed once per SyncTarget
/// (see its own content_semaphore field) and shared across the entire
/// recursive walk for that connection's round, is what actually enforces
/// a single, real, tree-wide ceiling.
pub struct Semaphore {
    tx: async_channel::Sender<()>,
    rx: async_channel::Receiver<()>,
    // Tracks how many permits are genuinely held right now (not the
    // fixed ceiling MAX_PERMITS already tells us, but live, current
    // usage), purely so acquire()/SemaphoreGuard's own Drop can log it
    // when --debug is passed (see main.rs's own is_debug_enabled). Kept
    // as a permanent debug facility, not scaffolding meant to come back
    // out -- confirmed genuinely useful for real diagnosis more than
    // once already, not just this session's own original "only one
    // thing seems to actually be processing at a time" question.
    in_use: Rc<Cell<u32>>,
}

impl Semaphore {
    /// The one, single source of truth for global content/metadata
    /// concurrency across the whole app -- everything (transfers,
    /// hashing, and metadata calls like PROPFIND/MKCOL/DELETE/MOVE)
    /// reads this same value, rather than each having its own
    /// separately-tuned number. 6, matching the real Nextcloud desktop
    /// client's own default (OWNCLOUD_MAX_PARALLEL, confirmed directly
    /// against Nextcloud's own documentation) -- not a value this app
    /// invented, but the one battle-tested across millions of real
    /// installs and a wide range of server hardware. This used to be
    /// memory-derived, recalculated fresh every round via a formula that
    /// assumed every concurrent operation could buffer up to
    /// max_file_size_bytes worth of data in memory -- that stopped being
    /// true once upload/download/hash were all confirmed to stream
    /// (fixed-size chunks through a small buffer, never the whole file
    /// in memory regardless of its size), so a fixed constant is both
    /// simpler and no less correct than the formula it replaced.
    pub const MAX_PERMITS: usize = 6;

    pub fn new() -> Self {
        Self::with_capacity(Self::MAX_PERMITS)
    }

    /// Generalized out of new() itself specifically for the resolver
    /// dialog's own privileged, outside-the-pool lane -- a second,
    /// genuinely separate Semaphore instance (capacity 1), never shared
    /// with the app-wide one, exists purely so a queued resolution can
    /// have somewhere else to run when the shared pool is genuinely
    /// saturated, without waiting behind it. Every other caller in this
    /// app still goes through plain new(), unaffected.
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, rx) = async_channel::bounded(capacity);
        for _ in 0..capacity {
            tx.try_send(()).expect("just-created bounded channel sized to exactly capacity");
        }
        Self { tx, rx, in_use: Rc::new(Cell::new(0)) }
    }

    pub async fn acquire(&self) -> SemaphoreGuard {
        // Only fails if every sender were dropped, which can't happen --
        // this struct holds its own tx alongside rx for their shared
        // lifetime, and every guard's own clone is dropped well before
        // that.
        self.rx.recv().await.expect("this Semaphore's own tx is never dropped while rx is still in use");
        self.on_acquired()
    }

    /// Non-blocking -- returns None immediately if every permit is
    /// currently held, rather than waiting for one to free up. Exists
    /// specifically for HybridPool's own "try the shared pool first,
    /// only fall back to a dedicated reserve if it's genuinely full
    /// right now" pattern -- see HybridPool's own doc comment for the
    /// full reasoning (Pool 1/discovery and Pool 2/confirm-detect, in
    /// this session's own terms).
    pub fn try_acquire(&self) -> Option<SemaphoreGuard> {
        self.rx.try_recv().ok().map(|()| self.on_acquired())
    }

    fn on_acquired(&self) -> SemaphoreGuard {
        self.in_use.set(self.in_use.get() + 1);
        // [debug] -- see in_use's own doc comment above.
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] semaphore: permit acquired, {} now in use (of {})", now_hms(), self.in_use.get(), Self::MAX_PERMITS);
        }
        SemaphoreGuard { tx: self.tx.clone(), in_use: self.in_use.clone() }
    }
}

pub struct SemaphoreGuard {
    tx: async_channel::Sender<()>,
    // See Semaphore::in_use's own doc comment -- a permanent debug
    // facility, not scaffolding meant to come back out.
    in_use: Rc<Cell<u32>>,
}

impl Drop for SemaphoreGuard {
    fn drop(&mut self) {
        // try_send, not send/await, since Drop itself can't be async --
        // fine regardless, since the channel's own fixed MAX_PERMITS
        // capacity always covers however many permits could genuinely be
        // in circulation at once, so this can never actually fail from
        // being full.
        let _ = self.tx.try_send(());
        self.in_use.set(self.in_use.get().saturating_sub(1));
        // [debug] -- see Semaphore::in_use's own doc comment.
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] semaphore: permit released, {} now in use", now_hms(), self.in_use.get());
        }
    }
}

/// A "try the shared pool first, fall back to a small dedicated reserve"
/// permit source -- Pool 1 (discovery) and Pool 2 (confirm/detect) both
/// use this same shape, in this session's own terms. Two genuinely
/// separate underlying channels, not one -- a single channel with
/// "labeled" slots can't actually enforce "these specific slots are
/// reserved," since any waiting receiver just gets whatever token comes
/// up next regardless of who's supposed to own it. Two independent
/// pools with a fallback order is what it actually takes to guarantee a
/// Pool 3 request can never consume a Pool 1/2 reserve slot.
///
/// Pool 3 (the shared Semaphore) never looks at what these reserves are
/// doing, and these reserves are only ever touched when Pool 3's own 6
/// are genuinely all in use right now -- not a global running total
/// across every pool combined. A confirmed, deliberate choice over the
/// alternative (Pool 3 itself checking a combined total before handing
/// out its own permits): the two behave identically when nothing's
/// contended, and differ only under genuine, all-pools-saturated
/// conditions, where Pool 1/2 work is expected to be brief regardless of
/// which model is used -- not worth the real added complexity (Pool 3
/// would need to wake on releases from pools it doesn't otherwise know
/// about) for a difference that's this narrow in practice.
pub struct HybridPool {
    reserve_tx: async_channel::Sender<()>,
    reserve_rx: async_channel::Receiver<()>,
}

impl HybridPool {
    pub fn new(reserve_size: usize) -> Self {
        let (reserve_tx, reserve_rx) = async_channel::bounded(reserve_size);
        for _ in 0..reserve_size {
            reserve_tx.try_send(()).expect("just-created bounded channel sized to exactly reserve_size");
        }
        Self { reserve_tx, reserve_rx }
    }

    /// Tries `shared` first, non-blocking; only falls back to this
    /// pool's own dedicated reserve (blocking) if `shared` is genuinely
    /// full right now. A per-request decision, not a sticky one -- an
    /// in-flight request never migrates between the shared pool and the
    /// reserve once it's acquired either kind of permit, it just holds
    /// whichever one it got until it finishes. The next request checks
    /// fresh, which is what lets Pool 1/2 ride on Pool 3's own capacity
    /// again the moment any of it frees up, without needing anything to
    /// actively reshuffle already-held permits. `name` is purely for the
    /// [debug] fallback log below -- lets a real log distinguish "Pool 1
    /// (discovery)" from "Pool 2 (confirm/detect)" falling back, since
    /// this same method serves both.
    pub async fn acquire(&self, shared: &Semaphore, name: &str) -> HybridGuard {
        if let Some(guard) = shared.try_acquire() {
            return HybridGuard::Shared(guard);
        }
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] {name}: Pool 3 was full, falling back to its own reserve", now_hms());
        }
        self.reserve_rx.recv().await.expect("this HybridPool's own reserve_tx is never dropped while reserve_rx is still in use");
        HybridGuard::Reserve(self.reserve_tx.clone())
    }
}

pub enum HybridGuard {
    // Field is never explicitly read, only ever held -- that's
    // deliberate, not an oversight the compiler's own suggested fix
    // (changing the field to `()`) should be followed for. Removing the
    // real SemaphoreGuard from this variant would let it drop
    // immediately at construction instead of being held for the actual
    // operation's own duration, releasing the Pool 3 permit right away
    // rather than when the work it's guarding finishes -- exactly the
    // silent correctness bug this whole guard type exists to prevent.
    #[allow(dead_code)]
    Shared(SemaphoreGuard),
    Reserve(async_channel::Sender<()>),
}

impl Drop for HybridGuard {
    fn drop(&mut self) {
        // Shared(SemaphoreGuard) releases itself via SemaphoreGuard's own
        // Drop impl automatically once this variant's own field is
        // dropped -- nothing extra needed for that case here. Only the
        // Reserve case needs an explicit release, the same try_send
        // reasoning as SemaphoreGuard's own Drop.
        if let HybridGuard::Reserve(tx) = self {
            let _ = tx.try_send(());
        }
    }
}

fn sync_directory<'a>(
    journal_conn: &'a Connection,
    target: &'a SyncTarget,
    local_dir: &'a Path,
    relative: &'a str,
    events_tx: &'a async_channel::Sender<SyncEvent>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SyncError>> + 'a>> {
    Box::pin(async move {
        // remote_dir_url built fresh here from target's own base URL +
        // this directory's relative path, rather than threaded down as
        // a parameter the way remote_dir: &gio::File used to be -- a
        // plain, cheap string join (see webdav::build_url), not a
        // network round-trip or a mount navigation, so there's no real
        // cost to deriving it locally at every level instead of passing
        // it through.
        let remote_dir_url = target.remote_url_for(relative);
        // Wrapped in run_blocking -- list_local is fully synchronous
        // (std::fs::read_dir plus one metadata() stat per entry), unlike
        // list_remote just below, which already properly yields via
        // gio's own async API. Confirmed as a real, serious bug (not
        // just theoretical): called once per directory across a walk of
        // tens or hundreds of thousands of files, on the single glib
        // MainContext thread, with zero yield points anywhere in it --
        // this is the same class of problem contents_match's own
        // std::fs::read already turned out to be, just far more
        // frequent, and matches a reported "responsiveness gets much
        // better once the directory count stops climbing" symptom
        // directly.
        let local_dir_owned = local_dir.to_path_buf();
        // Concurrent, not sequential -- a real, confirmed inefficiency
        // without this: these two calls have no dependency on each
        // other at all (list_remote_with_recovery only ever touches
        // target/remote_dir_url, never local_children), but were
        // previously awaited one after the other, meaning every single
        // directory in the whole recursive walk paid the *sum* of local-
        // disk-read latency plus remote round-trip latency instead of
        // just the *max* of the two -- the remote call dominates either
        // way (a real network round trip against a local disk read), so
        // running them concurrently effectively hides the local read's
        // own cost entirely behind it, for every directory in the tree,
        // not just this one. futures_util::future::join, not try_join --
        // list_local and list_remote_with_recovery return genuinely
        // different error types (std::io::Error vs SyncError), so this
        // takes both raw Results and applies `?` to each separately
        // afterward, rather than needing one shared error type to
        // short-circuit through.
        let local_future = run_blocking(move || list_local(&local_dir_owned));
        let remote_future = list_remote_with_recovery(target, &remote_dir_url);
        let (local_result, remote_result) = futures_util::future::join(local_future, remote_future).await;
        let local_children = local_result?;
        let remote_children = remote_result?;
        let known_children = journal::direct_children(journal_conn, &target.connection_id, relative)?;

        // Temp-file cleanup, folded into this same walk rather than a
        // separate, dedicated pre-pass -- see SyncTarget::cleanup_temp_files'
        // own doc comment for why. Only ever looks at names already
        // sitting in local_children/remote_children from the enumeration
        // just above (no extra round-trip), and only when this is the
        // session's first round at all. Best-effort deletes -- a failure
        // here just means this one leftover survives to the next
        // session's own first round, which will try again.
        if target.cleanup_temp_files {
            for name in local_children.keys().chain(remote_children.keys()) {
                if !name.starts_with(exclude::TEMP_FILE_PREFIX) {
                    continue;
                }
                if let Some(local) = local_children.get(name) {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                    if now.saturating_sub(local.mtime) >= MIN_TEMP_FILE_AGE_SECS as i64 {
                        let _ = std::fs::remove_file(local_dir.join(name));
                    }
                }
                if let Some(remote) = remote_children.get(name) {
                    // A modification time we couldn't determine at all is
                    // treated as "too recent to touch," not "safe to
                    // delete" -- same forgiving-toward-caution default the
                    // local half above and the original cleanup sweep
                    // both already used.
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                    let old_enough = remote.mtime.is_some_and(|mtime| now.saturating_sub(mtime) >= MIN_TEMP_FILE_AGE_SECS as i64);
                    if old_enough {
                        let _permit = target.content_semaphore.acquire().await;
                        // See RemoteWriteGuard's own doc comment --
                        // included even for this rare, session-start-
                        // only cleanup: the server has no way to know
                        // this delete is just this app's own leftover
                        // junk rather than a real, user-meaningful
                        // change, so it still triggers the same
                        // notify_push echo any other delete would.
                        let _remote_write_guard = RemoteWriteGuard::new(events_tx);
                        let _ = target.webdav_delete(&webdav::build_url(&remote_dir_url, name)).await;
                    }
                }
            }
        }

        // .no-sync can live on either side, unioned together -- read
        // fresh each round rather than cached, so editing it (from
        // either side) takes effect on the very next sync round with no
        // restart needed. The remote read is skipped entirely unless
        // remote_children already shows a .no-sync entry -- that
        // enumeration just happened above, so checking is free, and it
        // means a directory without one never pays for a speculative
        // fetch. A read failure (network hiccup, permissions) just means
        // the remote list doesn't apply this round, same forgiving
        // default exclude::load already uses for a missing/unreadable
        // local file.
        let mut excluded = exclude::load(local_dir);
        if remote_children.contains_key(exclude::NO_SYNC_FILENAME) {
            let no_sync_url = webdav::build_url(&remote_dir_url, exclude::NO_SYNC_FILENAME);
            if let Ok(bytes) = target.webdav_download_to_memory(&no_sync_url).await {
                excluded.extend(exclude::parse(&String::from_utf8_lossy(&bytes)));
            }
        }

        let mut names: HashSet<String> = HashSet::new();
        names.extend(local_children.keys().cloned());
        names.extend(remote_children.keys().cloned());
        names.extend(known_children);

        // Report exclusions before dropping them -- only for names that
        // actually exist somewhere right now (a stale journal-only entry
        // for something long gone on both sides isn't worth a log line).
        // Deliberately sent every round for as long as the exclusion
        // holds, not just on the first sighting: the log is meant to be a
        // full, auditable record of every decision made on the user's
        // behalf, not a curated summary -- someone checking "why hasn't
        // this synced in the last hour" should see that it was skipped on
        // every single round, not just the first one.
        for name in &names {
            // NO_SYNC_FILENAME itself is always excluded by definition (see
            // exclude::exclusion_reason) -- reporting that specifically is
            // just the marker file announcing its own existence every
            // round, not useful information. Names it *lists* still get
            // reported below, every round, same as always.
            if name == exclude::NO_SYNC_FILENAME {
                continue;
            }
            let is_directory = local_children.get(name).map(|e| e.is_dir)
                .or_else(|| remote_children.get(name).map(|e| e.is_dir))
                .unwrap_or(false);
            let name_relative = join_relative(relative, name);
            let settings = target.current_settings();
            if let Some(reason) = exclude::exclusion_reason(&excluded, &settings.global_patterns, &settings.connection_patterns, &name_relative, name, is_directory) {
                // Same reasoning as NO_SYNC_FILENAME just above -- a
                // .ripples-partial-* temp file being excluded is this
                // app's own, internal in-progress-transfer bookkeeping
                // announcing itself, not something the user asked about
                // or needs surfaced every round.
                if reason == exclude::ExclusionReason::TempFile {
                    continue;
                }
                if local_children.contains_key(name) || remote_children.contains_key(name) {
                    let _ = events_tx.try_send(SyncEvent::Excluded {
                        connection: target.connection_name.clone(),
                        path: name_relative,
                        reason,
                    });
                }
            }
        }
        names.retain(|name| {
            let is_directory = local_children.get(name).map(|e| e.is_dir)
                .or_else(|| remote_children.get(name).map(|e| e.is_dir))
                .unwrap_or(false);
            let name_relative = join_relative(relative, name);
            let settings = target.current_settings();
            exclude::exclusion_reason(&excluded, &settings.global_patterns, &settings.connection_patterns, &name_relative, name, is_directory).is_none()
        });

        // See discovered_count's own doc comment -- a single, cheap
        // integer add covering everything this one directory's own
        // listing just confirmed exists (after exclusions), not a
        // per-item increment or event of any kind.
        target.discovered_count.set(target.discovered_count.get() + names.len() as u64);

        // Split into directories and files up front -- kept as two
        // separate lists (rather than one combined pass) so directories
        // can start their own recursive work before the file pass even
        // begins, not because they use different concurrency limits
        // anymore -- both now read the same Semaphore::MAX_PERMITS.
        // Directories go first: they're what actually contains the bulk
        // of the remaining, deeper enumeration work, so starting them
        // concurrently before the (typically much larger, in raw item
        // count) file pass gets that recursive work underway as early
        // as possible.
        let mut directory_names = Vec::new();
        let mut file_names = Vec::new();
        for name in names {
            let is_dir_for_split = local_children.get(&name).map(|e| e.is_dir)
                .or_else(|| remote_children.get(&name).map(|e| e.is_dir))
                .unwrap_or(false);
            if is_dir_for_split {
                directory_names.push(name);
            } else {
                file_names.push(name);
            }
        }

        // Both metadata and content operations now read the exact same
        // ceiling -- see Semaphore::MAX_PERMITS' own doc comment for why
        // there's no longer a separate, smaller metadata-specific number.
        let metadata_concurrency = Semaphore::MAX_PERMITS;
        let content_concurrency = Semaphore::MAX_PERMITS;

        // Each concurrent task below handles its own errors internally
        // (logging/emitting an event) rather than propagating with `?`
        // -- for_each_concurrent's own closure must return a plain,
        // infallible future (see futures_util's own signature), the same
        // constraint that already applied to sync_child_directory/
        // sync_child_file's own errors even in the original, sequential
        // version of this loop -- one broken item was never allowed to
        // take its siblings down with it, and that's unchanged here,
        // just now potentially several such items resolving at once
        // instead of strictly one after another.
        futures_util::stream::iter(directory_names).for_each_concurrent(Some(metadata_concurrency), |name| {
            let excluded = &excluded;
            let local_children = &local_children;
            let remote_children = &remote_children;
            let remote_dir_url = &remote_dir_url;
            async move {
                // Wrapped in its own inner block so processed_count (just
                // below) increments exactly once regardless of which of
                // this block's several early returns (exclusion,
                // symlink) or its normal completion actually happens --
                // simpler than repeating the increment at every one of
                // those exit points individually.
                (async {
                    let is_directory_for_exclusion_check = local_children.get(&name).map(|e| e.is_dir)
                        .or_else(|| remote_children.get(&name).map(|e| e.is_dir))
                        .unwrap_or(false);
                    let name_relative_for_exclusion_check = join_relative(relative, &name);
                    let settings = target.current_settings();
                    if exclude::exclusion_reason(excluded, &settings.global_patterns, &settings.connection_patterns, &name_relative_for_exclusion_check, &name, is_directory_for_exclusion_check).is_some() {
                        return;
                    }

                    let child_relative = join_relative(relative, &name);
                    let local_entry = local_children.get(&name);
                    let remote_entry = remote_children.get(&name);

                    if local_entry.map(|e| e.is_symlink).unwrap_or(false) {
                        return;
                    }

                    let result = sync_child_directory(journal_conn, target, local_dir, remote_dir_url, &child_relative, &name,
                        local_entry.is_some(), remote_entry.is_some(), remote_entry.and_then(|e| e.fileid.as_deref()), true, events_tx).await;
                    if let Err(e) = result {
                        if is_permission_error(&e) {
                            if let Err(je) = journal::mark_permission_error(journal_conn, &target.connection_id, &child_relative, true, &e.to_string(), now_unix()) {
                                // Not propagated with '?' -- see this loop's own
                                // move to for_each_concurrent, whose closures
                                // must return a plain, infallible future. A
                                // local journal-write failure here is its own,
                                // separate problem from the permission error
                                // being recorded in the first place; logged so
                                // it's not entirely silent, but deliberately
                                // not allowed to abort the rest of this round
                                // the way propagating it once would have.
                                eprintln!("sync[{}]: couldn't record permission error for {child_relative}: {je}", target.connection_id);
                            }
                            let _ = events_tx.try_send(SyncEvent::PermissionError {
                                connection: target.connection_name.clone(),
                                path: child_relative.clone(),
                            });
                        } else {
                            report_item_failed(target, events_tx, child_relative.clone(), &e);
                        }
                    } else {
                        // See webdav_failure_streak's own doc comment on
                        // SyncTarget for why a genuine success clears the
                        // streak rather than leaving a stale, elevated
                        // count for a future, unrelated failure to
                        // inherit.
                        target.webdav_failure_streak.borrow_mut().remove(&child_relative);
                    }
                }).await;
                target.processed_count.set(target.processed_count.get() + 1);
                // [debug] -- see Semaphore::in_use's own doc comment on
                // why this is a permanent facility, gated by --debug.
                if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] processed_count now {} (directory {name} just finished)", now_hms(), target.processed_count.get());
                }
            }
        }).await;

        futures_util::stream::iter(file_names).for_each_concurrent(Some(content_concurrency), |name| {
            let excluded = &excluded;
            let local_children = &local_children;
            let remote_children = &remote_children;
            let remote_dir_url = &remote_dir_url;
            async move {
                (async {
                    let is_directory_for_exclusion_check = local_children.get(&name).map(|e| e.is_dir)
                        .or_else(|| remote_children.get(&name).map(|e| e.is_dir))
                        .unwrap_or(false);
                    let name_relative_for_exclusion_check = join_relative(relative, &name);
                    let settings = target.current_settings();
                    if exclude::exclusion_reason(excluded, &settings.global_patterns, &settings.connection_patterns, &name_relative_for_exclusion_check, &name, is_directory_for_exclusion_check).is_some() {
                        return;
                    }

                    let child_relative = join_relative(relative, &name);
                    let local_entry = local_children.get(&name);
                    let remote_entry = remote_children.get(&name);

                    // Ignore symlinks entirely, matching the Qt client --
                    // resolving what they point to and all the edge cases
                    // that opens up is out of scope. Left present in
                    // local_children (not filtered out earlier) so its
                    // presence still counts for decide_action -- silently
                    // omitting it would make it look exactly like a
                    // deletion to anything that synced under this name
                    // before it became a symlink, and cascade an unwanted
                    // delete to the other side.
                    if local_entry.map(|e| e.is_symlink).unwrap_or(false) {
                        return;
                    }

                    // Scoped-round file-level gate -- same principle as
                    // sync_child_directory's own recursion gate, applied
                    // to files sitting directly in this directory rather
                    // than to whether to recurse into a subdirectory.
                    // Real, confirmed inefficiency without this: only
                    // directory RECURSION was ever scope-gated, so every
                    // file sitting in any ancestor directory along the
                    // path to a dirty target -- most visibly, everything
                    // at the connection's own root -- got a full
                    // decide_action re-evaluation (and its own diag log
                    // line) on every single scoped round, regardless of
                    // how narrow the actual change underneath it was.
                    // Still always evaluated regardless of scope when it
                    // exists on only one side (is_new below) -- covers
                    // both a genuinely brand-new file and one needing
                    // delete propagation, since scope's own ancestor
                    // expansion has no way to already know about
                    // something it's never seen, the same "let a new
                    // item through regardless" rule the directory gate
                    // already applies.
                    if let Some(scope) = &target.dirty_scope {
                        let is_new = local_entry.is_none() || remote_entry.is_none();
                        if !is_new && !scope.contains(relative) {
                            return;
                        }
                    }

                    let result = sync_child_file(journal_conn, target, local_dir, remote_dir_url, &child_relative, &name,
                        local_entry, remote_entry, false, events_tx).await;
                    if let Err(e) = result {
                        if is_permission_error(&e) {
                            if let Err(je) = journal::mark_permission_error(journal_conn, &target.connection_id, &child_relative, false, &e.to_string(), now_unix()) {
                                eprintln!("sync[{}]: couldn't record permission error for {child_relative}: {je}", target.connection_id);
                            }
                            let _ = events_tx.try_send(SyncEvent::PermissionError {
                                connection: target.connection_name.clone(),
                                path: child_relative,
                            });
                        } else {
                            report_item_failed(target, events_tx, child_relative.clone(), &e);
                        }
                    } else {
                        // See webdav_failure_streak's own doc comment on
                        // SyncTarget for why a genuine success clears the
                        // streak rather than leaving a stale, elevated
                        // count for a future, unrelated failure to
                        // inherit.
                        target.webdav_failure_streak.borrow_mut().remove(&child_relative);
                    }
                }).await;
                target.processed_count.set(target.processed_count.get() + 1);
                // [debug] -- see Semaphore::in_use's own doc comment on
                // why this is a permanent facility, gated by --debug.
                if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] processed_count now {} (file {name} just finished)", now_hms(), target.processed_count.get());
                }
            }
        }).await;

        Ok(())
    })
}

/// What's actually still left in a directory subtree that shows up on
/// only one side -- see is_fully_cleanable's own doc comment for the
/// bug its own recursion fixes. Three genuinely different outcomes, not
/// a plain bool, since two of them both mean "false" in the sense of
/// "don't delete" but need entirely different handling from the
/// caller: real content means recreate the missing side (this wasn't
/// actually a deletion), while a non-removable exclusion means preserve
/// the status quo exactly as-is, touching neither side at all.
enum Cleanability {
    FullyCleanable,
    RealContent,
    SomeNotAutoRemovable,
}

/// Recursively checks whether an entire directory subtree contains only
/// content this app already knows about from a previous sync (safe to
/// remove as part of propagating a whole-directory deletion) plus
/// auto-removable, excluded entries -- including nested subdirectories
/// that are themselves, recursively, fully cleanable this same way.
///
/// Confirmed as a real, reported bug without this recursion: the
/// original check only looked one level deep, so any non-excluded
/// subdirectory was immediately treated as "real content survived,
/// don't delete" -- even when that subdirectory was itself also, fully
/// empty. A tree like One/Two/Three/Four (each level containing nothing
/// but the next, with actual content -- if any -- only possibly at the
/// very bottom) would then only ever get its deepest level ("Four")
/// actually recognized as deletable; every intermediate level ("One",
/// "Two", "Three") would each, individually, see a "real" (non-excluded)
/// child directory and bail out, recreating the missing side instead of
/// letting the deletion propagate all the way up.
///
/// A second, more fundamental real bug fixed here: a non-excluded FILE
/// used to mean an unconditional, immediate RealContent -- "don't
/// delete, there's real content here." But the whole point of a normal,
/// ordinary whole-directory deletion is that the SURVIVING side still
/// has its real, original files sitting there untouched -- that's
/// simply what "hasn't been propagated yet" looks like, not evidence
/// this isn't a genuine deletion. Confirmed against a real report: an
/// entire directory, fully synced beforehand (three files, all
/// previously confirmed uploaded), deleted locally -- and every one of
/// those exact files, found still sitting on the surviving remote side,
/// tripped this old unconditional check and blocked the bulk delete,
/// falling through to recreating the directory locally and walking its
/// contents individually instead. The real, safe distinction isn't
/// "is there a real file here" (there almost always is, for an ordinary
/// deletion) -- it's "is this specific file already known to the
/// journal as previously synced under this connection." A previously-
/// synced file is exactly the kind of content a deletion is *supposed*
/// to remove; it's only a file the journal has never seen before that
/// genuinely might be new, independently-added content (someone else
/// sharing to this same remote folder, another client) that a blind
/// bulk delete would silently destroy -- that, and only that, is what
/// still earns RealContent here.
///
/// Deliberately read-only -- no deletions happen here at all. The
/// caller only actually deletes anything once this confirms the *entire*
/// subtree checks out; a single, genuinely-unknown file discovered
/// partway through a deep recursion means nothing anywhere in this
/// subtree should be touched, which isn't possible to safely guarantee
/// if this deleted things as it went and then had to unwind.
async fn is_fully_cleanable(
    journal_conn: &Connection,
    target: &SyncTarget,
    check_local: bool,
    local_path: &Path,
    remote_url: &str,
    relative_path: &str,
) -> Result<Cleanability, SyncError> {
    // Carries mtime+size+inode+etag alongside name/is_dir now -- see this
    // function's own doc comment's newest addition below for why "the
    // journal has a row for this path" alone is no longer enough to call
    // a file known. inode is only meaningful for the local side (Some
    // here); etag is the remote-side equivalent -- Nextcloud's own
    // content-identity fingerprint, changed by the server whenever the
    // underlying content actually changes, so a re-uploaded file at a
    // reused path gets a new one the same way a re-extracted local file
    // gets a new inode. mtime+size only remain as a last-resort
    // fallback for the rare case a server doesn't report an etag for
    // some entry. Remote entries without a reported mtime corroborate
    // as None too, which the check below treats the same as a genuine
    // mismatch -- deliberately conservative, since failing to
    // corroborate should mean "can't prove this is the same file," not
    // "assume it is."
    let names: Vec<(String, bool, Option<i64>, i64, Option<i64>, Option<String>)> = if check_local {
        let path_owned = local_path.to_path_buf();
        run_blocking(move || list_local(&path_owned)).await?.into_iter()
            .map(|(name, e)| (name, e.is_dir, Some(e.mtime), e.size, Some(e.inode), None)).collect()
    } else {
        // Acquired here, at this specific call site, rather than baked
        // into a shared wrapper -- this function is only ever called
        // from the normal recursive walk's own cleanup-decision logic,
        // never from a conflict-resolution action, so there's no risk
        // of gating the "resolutions bypass the queue" behavior. See
        // Semaphore's own doc comment for why metadata calls need this
        // now that they share the same ceiling as transfers/hashing.
        let _permit = target.content_semaphore.acquire().await;
        target.webdav_list(remote_url).await?.into_iter()
            .map(|(name, e)| (name, e.is_dir, e.mtime, e.size, None, e.etag)).collect()
    };
    // .no-sync entries are a per-directory local concept regardless of
    // which side is actually being enumerated here -- same reasoning as
    // sync_child_directory's own, original version of this check.
    let excluded_here = exclude::load(local_path);
    let settings = target.current_settings();

    for (name, is_dir, mtime, size, inode, etag) in &names {
        let child_rel = format!("{relative_path}/{name}");
        let reason = exclude::exclusion_reason(&excluded_here, &settings.global_patterns, &settings.connection_patterns, &child_rel, name, *is_dir);
        match reason {
            None => {
                if !*is_dir {
                    // Previously-synced content is exactly what a
                    // deletion is supposed to remove, not evidence
                    // against one -- see this function's own doc
                    // comment for the real bug this fixes. Only a file
                    // the journal genuinely has no record of syncing
                    // still counts as real, disqualifying content.
                    //
                    // "Has a row in the journal for this path" alone
                    // isn't enough, though -- confirmed against a real,
                    // reported second bug: quit the app, delete a
                    // directory on both sides by hand (so the journal's
                    // own rows for it are never cleared, since nothing
                    // was running to observe and reconcile that
                    // deletion), then recreate a directory with the same
                    // name and genuinely different content (extracting a
                    // fresh archive with the same target folder name).
                    // The journal still has the OLD file's row, still
                    // marked previously-synced, at that exact path -- so
                    // without corroboration this would call the brand
                    // new file "known, safe-to-clean residue" purely
                    // because something used to live at that path, and
                    // delete it right along with the real leftover
                    // directory it was mistaken for.
                    //
                    // inode (local) / etag (remote) are the primary
                    // checks here, not mtime+size -- a real, reported
                    // gap in an earlier version of this fix: re-
                    // extracting the exact same archive gives byte-
                    // identical content and, if the archive format
                    // preserves timestamps, an identical mtime too, so
                    // mtime+size alone would still wrongly call a
                    // brand-new file "known." A fresh local extraction
                    // always gets a fresh inode from the filesystem
                    // regardless of how identical its content or
                    // metadata are to what used to be there; a fresh
                    // remote upload gets a fresh etag from the server
                    // the same way -- both are the same "kernel-exact"
                    // (or server-exact) identity signal sync_child_file's
                    // own rename-pairing already trusts, just applied
                    // here too. Only falls back to mtime+size when
                    // neither is available for this specific entry.
                    let known = journal::get_item(journal_conn, &target.connection_id, &child_rel)?
                        .map(|item| !item.is_directory && item.last_synced_mtime.is_some() && match (inode, etag) {
                            (Some(current_inode), _) => item.last_synced_inode == Some(*current_inode),
                            (None, Some(current_etag)) => item.last_synced_etag.as_deref() == Some(current_etag.as_str()),
                            (None, None) => item.last_synced_mtime == *mtime && item.last_synced_size == Some(*size),
                        })
                        .unwrap_or(false);
                    if !known {
                        // Only logged for the notable case -- known=true
                        // (the overwhelming majority of entries in any
                        // real directory) carries no diagnostic value on
                        // its own; known=false is what actually explains
                        // why a directory didn't get cleaned up, worth
                        // keeping visible for that.
                        if crate::is_debug_enabled() {
                            eprintln!("[{}] [debug] is_fully_cleanable file {child_rel}: known=false (check_local={check_local}) -- blocks cleanup", now_hms());
                        }
                        return Ok(Cleanability::RealContent);
                    }
                    continue;
                }
                // A non-excluded directory isn't automatically "real
                // content" the way a file is -- recurse to check
                // whether *it* is also, itself, fully cleanable, rather
                // than bailing out immediately on its mere existence.
                // Boxed: async fns can't directly, recursively call
                // themselves without this, since the compiler can't
                // otherwise determine a finite size for the resulting
                // state machine.
                let child_local = local_path.join(name);
                let child_remote_url = webdav::build_url(remote_url, name);
                match Box::pin(is_fully_cleanable(journal_conn, target, check_local, &child_local, &child_remote_url, &child_rel)).await? {
                    Cleanability::FullyCleanable => {}
                    // Propagated up exactly as-is, not collapsed to a
                    // single "not cleanable" -- a nested
                    // SomeNotAutoRemovable several levels down still
                    // means the whole outer tree should be preserved
                    // untouched, same as if it were found at this level
                    // directly, not treated as equivalent to real
                    // content needing the missing side recreated.
                    other => return Ok(other),
                }
            }
            Some(exclude::ExclusionReason::GlobalPattern) => {
                match exclude::global_pattern_auto_removable(&settings.global_patterns, &child_rel, name, *is_dir) {
                    Some(true) => {}
                    _ => return Ok(Cleanability::SomeNotAutoRemovable),
                }
            }
            Some(exclude::ExclusionReason::NoSyncFile) | Some(exclude::ExclusionReason::TempFile) => {}
            Some(exclude::ExclusionReason::ConnectionPath) => return Ok(Cleanability::SomeNotAutoRemovable),
        }
    }
    Ok(Cleanability::FullyCleanable)
}

/// Deletes everything under `remote_dir` individually -- depth-first,
/// each file as it's found, each subdirectory only after its own
/// contents are already gone, then `remote_dir` itself last -- rather
/// than a single, top-level recursive DELETE (WebDAV's Depth: infinity,
/// the only depth a DELETE on a non-empty collection can even request).
/// See this function's own call site in sync_child_directory for the
/// real, repeatedly reproduced bug this exists to fix: the recursive
/// form kept failing with "Directory not empty" even immediately after
/// this app's own listing confirmed the collection held nothing but
/// already-verified content.
///
/// Makes no cleanability decisions of its own -- only ever called after
/// is_fully_cleanable has already confirmed the entire subtree safe to
/// remove, on that same confirmed-safe content, so this doesn't
/// re-check exclusions or journal state, just performs the removal a
/// different, more reliable way. Re-lists rather than reusing whatever
/// is_fully_cleanable already saw -- some real time has passed since
/// that check, and a plain, fresh listing right before acting is the
/// same discipline every other mutating path in this file already
/// follows.
async fn remote_delete_recursive(target: &SyncTarget, remote_dir_url: &str, events_tx: &async_channel::Sender<SyncEvent>) -> Result<(), SyncError> {
    // Acquired per individual call, not held across the whole recursive
    // deletion -- this function is only ever reached from the normal
    // walk's own DeleteRemote-folder handling, never from a conflict
    // resolution, so no risk of gating the "resolutions bypass the
    // queue" behavior. Per-call rather than one permit held for the
    // entire recursion so a large subtree being deleted doesn't starve
    // every other concurrent operation for as long as that takes.
    let children = {
        let _permit = target.content_semaphore.acquire().await;
        target.webdav_list(remote_dir_url).await?
    };
    for (name, entry) in children {
        let child_url = webdav::build_url(remote_dir_url, &name);
        if entry.is_dir {
            // Boxed for the same reason as is_fully_cleanable's own
            // recursive call -- async fns can't otherwise recurse into
            // themselves, since the compiler can't determine a finite
            // size for the resulting state machine.
            Box::pin(remote_delete_recursive(target, &child_url, events_tx)).await?;
        } else {
            let _permit = target.content_semaphore.acquire().await;
            // See RemoteWriteGuard's own doc comment -- one per
            // individual delete here, deliberately: a whole subtree
            // being deleted can fan out into many of these, and each
            // one still needs begin_remote_write/end_remote_write's own
            // pending_remote_writes count to correctly reflect it, even
            // though the batching this whole design relies on means
            // only the very first one across this entire round actually
            // fires a pre-flight check.
            let _remote_write_guard = RemoteWriteGuard::new(events_tx);
            target.webdav_delete(&child_url).await?;
        }
    }
    let _permit = target.content_semaphore.acquire().await;
    let _remote_write_guard = RemoteWriteGuard::new(events_tx);
    target.webdav_delete(remote_dir_url).await?;
    Ok(())
}

async fn sync_child_directory(
    journal_conn: &Connection,
    target: &SyncTarget,
    local_dir: &Path,
    remote_dir_url: &str,
    child_relative: &str,
    name: &str,
    exists_locally: bool,
    exists_remotely: bool,
    remote_fileid: Option<&str>,
    // true for every call from the ordinary recursive walk (sync_directory's
    // own per-child loop) -- unchanged behavior there. false only from
    // retry_single_item: a targeted, single-item retry from the resolver's
    // own Retry/Retry Now buttons has no business recursing into an entire
    // subtree just because the one item retried happened to be a directory.
    // Confirmed as a real, reported bug without this: a directory holding
    // several, separately-flagged PermissionError children (each its own
    // carousel entry) would have every one of them silently, surprisingly
    // "resolved" by retrying just the first, since the recursive walk this
    // triggered would re-attempt (and, for any still-broken child,
    // re-fail-but-swallow) every sibling too -- confusing on its own, and
    // specifically wrong when some of those siblings weren't the ones
    // retried at all. See this parameter's own use at both places that
    // recurse into sync_directory below.
    recurse: bool,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    // Already handled by a rename pairing found elsewhere THIS round --
    // see SyncTarget::renamed_away's own doc comment for why this check
    // has to come first. For a directory specifically this isn't just a
    // clean no-op: without it, the old (renamed-away) path's own
    // position here would see exists_locally=false (genuinely gone) but
    // exists_remotely=true (the pre-move remote snapshot, captured
    // before we moved it, hasn't caught up) and would actively recreate
    // the stale directory locally, then recurse into re-downloading
    // everything inside it from scratch -- not just wasted work, a
    // wrong one.
    if target.renamed_away.borrow().contains(child_relative) {
        return Ok(());
    }

    // Same skip as sync_child_file's own -- flagged from a previous
    // attempt as looking like a permission problem, so left alone until
    // something gives a real reason to believe it's changed (session
    // start, manual unpause, or a targeted retry from the carousel).
    if journal::get_item(journal_conn, &target.connection_id, child_relative)?.map(|i| i.permission_error).unwrap_or(false) {
        return Ok(());
    }

    let child_local = local_dir.join(name);
    let child_remote_url = webdav::build_url(remote_dir_url, name);

    // Rename/move pairing -- only a candidate when this directory exists
    // locally with nothing at the corresponding remote path yet
    // (otherwise it's not "new", it's an ordinary already-tracked
    // directory). Directories skip the size/mtime corroboration files
    // get below -- matching the real Nextcloud client's own reasoning
    // ("Directories and virtual files don't need size/mtime equality"):
    // a directory's own mtime reflects when ITS OWN contents last
    // changed, not whether it was renamed, so it's not a meaningful
    // signal here the way it is for a file. The inode match alone is
    // already kernel-exact.
    if exists_locally && !exists_remotely {
        if let Some(dir_inode) = local_inode(&child_local) {
            if let Some(matched) = journal::find_by_inode(journal_conn, &target.connection_id, dir_inode, true)? {
                let already_claimed = target.renamed_away.borrow().contains(&matched.relative_path);
                let old_still_exists_locally = target.local_root.join(&matched.relative_path).try_exists().unwrap_or(true);
                if matched.relative_path != child_relative && !already_claimed && !old_still_exists_locally {
                    let now = now_unix();
                    let old_remote_url = target.remote_url_for(&matched.relative_path);
                    // No transfer_with_stall_detection wrapper any more
                    // -- that mechanism exists to watch a byte-progress
                    // stream for a transfer actually moving content.
                    // MOVE isn't that: it's a single, metadata-only
                    // request the server completes without ever
                    // streaming this app anything to watch progress on,
                    // near-instant even for a large subtree (WebDAV
                    // MOVE defaults to Depth: infinity, handled
                    // entirely server-side).
                    let _permit = target.content_semaphore.acquire().await;
                    // See RemoteWriteGuard's own doc comment.
                    let _remote_write_guard = RemoteWriteGuard::new(events_tx);
                    match webdav_move_serialized(target, &old_remote_url, &child_remote_url, false, None).await {
                        Ok(()) => {
                            journal::rename_synced_prefix(journal_conn, &target.connection_id, &matched.relative_path, child_relative, now)?;
                            target.renamed_away.borrow_mut().insert(matched.relative_path.clone());
                            let _ = events_tx.try_send(SyncEvent::ItemRenamed {
                                connection: target.connection_name.clone(),
                                old_path: matched.relative_path,
                                new_path: child_relative.to_string(),
                            });
                            // Contents already exist at the new remote
                            // location via the MOVE itself (WebDAV MOVE
                            // defaults to Depth: infinity) -- just
                            // recurse to reconcile the journal against
                            // what's now actually there, not to
                            // re-transfer anything. The rename itself
                            // (this block's own actual work) already
                            // happened above regardless of recurse --
                            // only the follow-up reconcile pass over
                            // this directory's own children is skipped
                            // when a targeted, single-item retry doesn't
                            // want it; a later, ordinary walk reconciles
                            // them normally.
                            return if recurse {
                                sync_directory(journal_conn, target, &child_local, child_relative, events_tx).await
                            } else {
                                Ok(())
                            };
                        }
                        Err(e) => {
                            // Source vanished between the check above and
                            // now (a real but narrow race), or some other
                            // transient failure -- fall through to the
                            // ordinary "create a new directory" path
                            // below rather than propagating, since this
                            // is exactly the kind of single-item failure
                            // the round shouldn't abort over.
                            eprintln!("sync[{}]: rename pairing for {child_relative} failed, falling back to plain create: {e}", target.connection_id);
                        }
                    }
                }
            }
        }
    } else if exists_remotely && !exists_locally {
        // The mirror direction -- a directory renamed/moved on the
        // *server* (web UI, another synced device), not locally.
        // oc:fileid is what makes this direction detectable at all
        // (see idx_sync_items_fileid's own doc comment): unlike a local
        // rename, there's no inode to recognize here, and without a
        // stable server-assigned identity, a server-side rename is
        // completely indistinguishable from "old one deleted, unrelated
        // new one created," which is exactly what happens whenever this
        // block doesn't apply -- delete-then-download, no worse than the
        // pre-fileid behavior. Deliberately no server-side verification
        // that the old path is genuinely gone before proceeding, unlike
        // the local direction's own remote MOVE call -- the operation
        // here is a plain local rename, not a mutating network request,
        // so a false positive just means a local path briefly named
        // wrong, self-correcting on the very next ordinary reconcile
        // pass rather than something destructive worth an extra
        // round-trip to guard against.
        if let Some(fileid) = remote_fileid {
            if let Some(matched) = journal::find_by_fileid(journal_conn, &target.connection_id, fileid, true)? {
                let already_claimed = target.renamed_away.borrow().contains(&matched.relative_path);
                let old_local_path = target.local_root.join(&matched.relative_path);
                if matched.relative_path != child_relative && !already_claimed && old_local_path.try_exists().unwrap_or(false) {
                    let now = now_unix();
                    match std::fs::rename(&old_local_path, &child_local) {
                        Ok(()) => {
                            journal::rename_synced_prefix(journal_conn, &target.connection_id, &matched.relative_path, child_relative, now)?;
                            target.renamed_away.borrow_mut().insert(matched.relative_path.clone());
                            let _ = events_tx.try_send(SyncEvent::ItemRenamed {
                                connection: target.connection_name.clone(),
                                old_path: matched.relative_path,
                                new_path: child_relative.to_string(),
                            });
                            // Contents already exist at the new local
                            // location via the rename itself -- just
                            // recurse to reconcile the journal against
                            // what's now actually there, not to
                            // re-transfer anything. Same recurse-flag
                            // reasoning as the remote-direction block's
                            // own identical return just above.
                            return if recurse {
                                sync_directory(journal_conn, target, &child_local, child_relative, events_tx).await
                            } else {
                                Ok(())
                            };
                        }
                        Err(e) => {
                            // Same reasoning as the local-direction
                            // block's own Err arm -- fall through to the
                            // ordinary "download a new directory" path
                            // below rather than propagating.
                            eprintln!("sync[{}]: remote-side rename pairing for {child_relative} failed, falling back to plain download: {e}", target.connection_id);
                        }
                    }
                }
            }
        }
    }

    // Folder-cleanup check: only applies when this directory was
    // previously synced and now shows up on only one side -- a real
    // deletion candidate, not a brand-new folder needing plain
    // creation. Checked BEFORE recreating the missing side, not after
    // -- recreating first and checking after would put the "preserve"
    // outcome in exactly the wrong spot, with the missing side already
    // resurrected by the time anything decided it shouldn't exist. This
    // is the actual fix for the bug where a folder containing only an
    // excluded file had its removed remote counterpart silently
    // recreated empty rather than the deletion actually propagating.
    if exists_locally != exists_remotely {
        // [debug] -- see decide_action's own eprintln just above in this
        // file. Fires only when this specific directory shows up on
        // only one side -- exactly the ambiguous case ("is this a real
        // deletion, or genuinely new content") this whole block exists
        // to resolve, so this is the one place worth seeing the raw
        // inputs to that decision.
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] directory XOR check {child_relative}: exists_locally={exists_locally} exists_remotely={exists_remotely}", now_hms());
        }
        // Real, reported bug without the has_synced_descendant fallback:
        // this directory's own bare row can end up without
        // last_synced_mtime set for reasons that don't actually mean "this
        // was never synced" (a journal wiped mid-testing while the real
        // directory tree on both sides survived untouched, an unresolved
        // edge case from an earlier session) -- confirmed against a real
        // report where every individual FILE underneath a deleted
        // directory correctly showed its own previously-synced record
        // (each one propagated its own deletion correctly, one by one),
        // while the CONTAINER directory's own row didn't, so this check
        // alone concluded "not a real deletion" and fell through to
        // recreating the directory locally, then walking its
        // now-newly-"discovered" children individually instead of ever
        // taking the single bulk delete below. has_synced_descendant asks
        // the more robust question -- not just "does this exact row say
        // it was synced" but "does the journal have ANY evidence, anywhere
        // in this subtree, that this was known before" -- and a
        // previously-synced child is exactly that evidence, independent
        // of whatever state the parent's own bare row happens to be in.
        let previously_synced = journal::get_item(journal_conn, &target.connection_id, child_relative)?
            .map(|item| item.is_directory && item.last_synced_mtime.is_some())
            .unwrap_or(false)
            || journal::has_synced_descendant(journal_conn, &target.connection_id, child_relative)?;
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] {child_relative}: previously_synced={previously_synced}", now_hms());
        }

        if previously_synced {
            // Real, reported bug: with notify_push connected, ANY remote
            // change -- including the very flurry of ordinary per-file
            // deletes that leaves a directory like this one empty in the
            // first place -- forces an immediate full recheck (see
            // AppState::force_full_check in main.rs), so a directory
            // whose own cleanup delete fails was being re-attempted on
            // literally every one of those immediate rechecks, often just
            // a few seconds apart. Confirmed against a real log: dozens
            // of individual file deletes succeeding one after another,
            // immediately followed by the *parent* directory's own DELETE
            // failing with "Directory not empty" -- repeated identically
            // every ~5 seconds for over a minute straight, hammering the
            // server with a doomed request each time. The directory being
            // reported non-empty right after this app's own listing (via
            // is_fully_cleanable, just below) confirmed it empty points at
            // gvfsd-dav's own client-side directory-listing cache not yet
            // reflecting a delete flurry that only just finished through
            // that same mount -- the same class of GVfs staleness already
            // seen elsewhere in this app (see ensure_mounted's own
            // wedged-mount handling), not a bug in this app's own
            // cleanability logic. A short cooldown gives that cache a
            // realistic window to catch up, rather than either retrying
            // instantly forever or (the alternative this deliberately
            // avoids) treating a transient cache-staleness error as a
            // permanent one the way permission_error's own persistent,
            // session-spanning flag would.
            if let Some(last_attempt) = target.cleanup_retry_cooldown.borrow().get(child_relative) {
                if last_attempt.elapsed().as_secs() < CLEANUP_RETRY_COOLDOWN_SECS {
                    return Ok(());
                }
            }
            match is_fully_cleanable(journal_conn, target, exists_locally, &child_local, &child_remote_url, child_relative).await? {
                Cleanability::RealContent => {
                    // Real, non-excluded content survived somewhere in
                    // this subtree -- this isn't actually a full
                    // deletion after all. Fall through to the normal
                    // create-the-missing-side-and-reconcile handling
                    // below.
                }
                Cleanability::SomeNotAutoRemovable => {
                    // At least one remaining item, somewhere in this
                    // subtree, is excluded but not flagged
                    // auto_removable (or excluded some other way
                    // entirely) -- preserve everything exactly as-is,
                    // including not recreating the missing side.
                    return Ok(());
                }
                Cleanability::FullyCleanable => {
                    // See is_fully_cleanable's own doc comment --
                    // confirmed recursively safe to remove, all the way
                    // down, not just this one level.
                    //
                    // Not propagated with bare `?` any more -- a failure
                    // here needs to record a cooldown entry before
                    // returning, not just bail out immediately, so both
                    // arms are matched explicitly instead.
                    let outcome: Result<(), SyncError> = if exists_locally {
                        // remove_dir_all handles arbitrary depth
                        // natively and has never shown this problem --
                        // this side is untouched.
                        std::fs::remove_dir_all(&child_local).map_err(Into::into)
                    } else {
                        // NOT a single recursive delete_future() on the
                        // directory itself (WebDAV's Depth: infinity)
                        // any more -- real, repeatedly reported bug: this
                        // kept failing with "Directory not empty" even
                        // immediately after this app's own listing (the
                        // very same one is_fully_cleanable just used to
                        // confirm the whole subtree safe) reported
                        // nothing but known content -- confirmed
                        // reproducing seven separate times across 40
                        // minutes on one directory alone, including
                        // right after a forced remount, which ruled out
                        // a GVfs client-side mount cache as the cause.
                        // Since the listing and the delete both go
                        // through the same mount and the listing is
                        // provably accurate, whatever's inconsistent
                        // lives inside gvfsd-dav's own handling of a
                        // recursive collection DELETE specifically (or
                        // between it and the server), not in anything
                        // this app's own logic gets wrong. Deleting each
                        // item individually -- depth-first, files then
                        // the now-empty directory -- sidesteps that
                        // recursive codepath entirely: more requests,
                        // but every one of them a plain, single-item
                        // DELETE the same shape already used everywhere
                        // else in this app, on content is_fully_cleanable
                        // has already fully verified safe.
                        remote_delete_recursive(target, &child_remote_url, events_tx).await
                    };
                    if let Err(e) = outcome {
                        target.cleanup_retry_cooldown.borrow_mut().insert(child_relative.to_string(), std::time::Instant::now());
                        // Real gap this closes: without this, a failed
                        // cleanup delete doesn't mark anything dirty, so
                        // the only thing that can ever bring it back for
                        // a retry is the next full, unscoped periodic
                        // crawl -- which, on a large tree, can take
                        // minutes, and there's no user-facing manual
                        // retry for this failure type either (it never
                        // sets status=Conflict or permission_error, the
                        // only two things the Sync Issues carousel
                        // reads). Reusing had_freshness_deferral's own
                        // mechanism -- already shared, already exactly
                        // what main.rs's own trigger_sync_round watches
                        // for -- gets a real, scoped follow-up round in
                        // seconds instead, the same fast path a
                        // freshness deferral already gets. The cooldown
                        // above still applies on each of those follow-up
                        // attempts until it actually elapses, so this
                        // doesn't retry the delete itself any faster
                        // than CLEANUP_RETRY_COOLDOWN_SECS allows --
                        // it just means the round that's allowed to
                        // retry actually happens promptly once that
                        // window passes, rather than waiting on whatever
                        // the periodic schedule happens to be.
                        target.had_freshness_deferral.set(true);
                        target.dirty_dirs.borrow_mut()
                            .entry(target.connection_id.clone())
                            .or_default()
                            .insert(local_dir.to_path_buf());
                        return Err(tag_webdav_action(e, "Delete"));
                    }
                    // Clears any stale cooldown entry from an earlier,
                    // now-irrelevant failure -- harmless no-op if there
                    // was never one, via HashMap::remove's own contract.
                    target.cleanup_retry_cooldown.borrow_mut().remove(child_relative);
                    journal::forget_prefix(journal_conn, &target.connection_id, child_relative)?;
                    let _ = events_tx.try_send(SyncEvent::ItemDeleted {
                        connection: target.connection_name.clone(),
                        path: child_relative.to_string(),
                        local: exists_locally,
                    });
                    return Ok(());
                }
            }
        }
    }

    if !exists_locally {
        std::fs::create_dir_all(&child_local)?;
    }
    if !exists_remotely {
        let _permit = target.content_semaphore.acquire().await;
        // See RemoteWriteGuard's own doc comment.
        let _remote_write_guard = RemoteWriteGuard::new(events_tx);
        match target.webdav_mkcol(&child_remote_url).await {
            Ok(()) => {}
            // MKCOL on a collection that already exists -- servers
            // commonly report this as 405 (Method Not Allowed) or 409
            // (Conflict) rather than a dedicated status of its own;
            // either way this is the same "already there, fine" case
            // gio::IOErrorEnum::Exists used to cover.
            Err(webdav::WebDavError::UnexpectedStatus { status: 405 | 409, .. }) => {}
            Err(e) => return Err(tag_webdav_action(e.into(), "Upload")),
        }
    }

    let now = now_unix();
    journal::mark_synced(journal_conn, &target.connection_id, child_relative, true, now, 0, "", local_inode(&child_local), None, now)?;

    // Scoped-round recursion gate -- see SyncTarget::dirty_scope's own
    // doc comment for the full design. Only skips recursing when this
    // directory existed on BOTH sides already, before this call ever
    // touched it (is_new_directory below, computed from the exact
    // exists_locally/exists_remotely this function was called with, not
    // the params of the same name -- both mkdir calls above only ever
    // fire when one side was actually missing, so by the time execution
    // reaches here "new" and "was missing a moment ago" are the same
    // question) -- brand-new content was never covered by any previous
    // crawl, scoped or not, so it always needs the real, full recursive
    // walk regardless of what's in scope. An already-known directory
    // outside the expanded dirty set, by contrast, is exactly what
    // scoping exists to skip: nothing reported dirty anywhere in or
    // under it, so there's nothing here a full walk would find that
    // this round's own dirty-tracking wouldn't already have caught.
    let is_new_directory = !exists_locally || !exists_remotely;
    // Checked before dirty_scope's own gate, not folded into it -- a
    // targeted, single-item retry (recurse=false) skips recursing
    // unconditionally, regardless of whatever dirty_scope would
    // otherwise have decided; that gate's own logic only matters once
    // recursing is on the table at all.
    if !recurse {
        return Ok(());
    }
    if let Some(scope) = &target.dirty_scope {
        let in_scope = scope.contains(child_relative);
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] recursion gate {child_relative}: is_new_directory={is_new_directory} in_scope={in_scope} scope_len={}", now_hms(), scope.len());
        }
        if !is_new_directory && !in_scope {
            return Ok(());
        }
    }

    sync_directory(journal_conn, target, &child_local, child_relative, events_tx).await
}

async fn sync_child_file(
    journal_conn: &Connection,
    target: &SyncTarget,
    local_dir: &Path,
    remote_dir_url: &str,
    child_relative: &str,
    name: &str,
    local_entry: Option<&LocalEntry>,
    remote_entry: Option<&RemoteEntry>,
    // false for every call from the ordinary recursive walk (sync_directory's
    // own per-child loop) -- unchanged behavior there, every gate below
    // working exactly as designed across successive rounds. true only from
    // retry_single_item: a real, confirmed bug without this, found twice in
    // the same family. stability_streak/remote_stability_streak AND
    // deletion_pending are all keyed on a given SyncTarget's own, in-memory
    // map, and retry_single_item builds a fresh SyncTarget for every single
    // call (see build_sync_target_for_connection), so whatever streak this
    // same path's own prior rounds may have already accumulated is
    // invisible to it -- every single observation starts back at count=1,
    // which can never reach any of these gates' own required threshold
    // within one, single-shot call. The result, for either gate: a
    // "Retry"/"Retry Now" on an item actually needing an Upload/Download/
    // deletion silently, instantly deferred (returned Ok(()) with nothing
    // attempted at all, no network call made), the resolver reporting
    // success for something that was never actually retried, the same
    // underlying problem resurfacing on the very next ordinary round once
    // it finally did attempt it for real. A user explicitly clicking Retry
    // on one, specific, already-known item is a fundamentally different
    // situation from the ordinary walk blindly discovering a file that
    // might still be mid-write, or a path that might still be mid-replace
    // -- deferring silently in either case is actively misleading there,
    // not a safety net.
    skip_settling_checks: bool,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    // Already handled by a rename pairing found elsewhere THIS round --
    // see SyncTarget::renamed_away's own doc comment. Without this, the
    // old (renamed-away) path's own position here would decide
    // DeleteRemote against a pre-move remote snapshot that's since gone
    // stale -- not wrong exactly (the remote MOVE already relocated it,
    // so there's nothing left to delete), but a confusing, doomed
    // "not found" failure instead of the clean no-op this makes it.
    if target.renamed_away.borrow().contains(child_relative) {
        return Ok(());
    }

    let local_path = local_dir.join(name);
    let remote_url = webdav::build_url(remote_dir_url, name);

    let item = journal::get_or_new(journal_conn, &target.connection_id, child_relative, false)?;

    // Flagged from a previous attempt as looking like a permission
    // problem -- skip entirely rather than re-attempt (and re-fail, and
    // re-log) something with no reason to believe has changed. Cleared
    // at session start, on manual unpause, or by a targeted retry from
    // the Sync Issues carousel; see journal::clear_permission_error's
    // own doc comment.
    if item.permission_error {
        return Ok(());
    }

    let current_local = local_entry.map(|e| (e.mtime, e.size));
    let current_remote_etag = remote_entry.and_then(|e| e.etag.as_deref());

    // Stay frozen only while nothing has actually changed since the last
    // freeze -- compares against the snapshot mark_conflict_pending took
    // (item.local_mtime/local_size/remote_etag), not last_synced_*,
    // which is the true older common-ancestor and deliberately untouched
    // by a freeze. This replaces an unconditional freeze that never
    // looked again once flagged -- confirmed as a real bug: deleting the
    // local file after a conflict was flagged was invisible forever,
    // since nothing ever re-evaluated decide_action at all. Now:
    // identical state -> stay quiet (the original anti-spam intent,
    // preserved). Anything different -> fall through and actually
    // reconsider it, which naturally re-freezes with an updated snapshot
    // if it's still conflict-shaped (see the Conflict* arms below, which
    // already call mark_conflict_pending again), or resolves outright if
    // it isn't.
    if item.status == journal::SyncStatus::Conflict {
        let frozen_local = item.local_mtime.zip(item.local_size);
        let frozen_remote = item.remote_etag.as_deref();
        if current_local == frozen_local && current_remote_etag == frozen_remote {
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] {child_relative}: still frozen, no change since last conflict snapshot (local={current_local:?} remote_etag={current_remote_etag:?})", now_hms());
            }
            return Ok(());
        }
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] {child_relative}: conflict re-evaluating, snapshot changed (frozen_local={frozen_local:?} now={current_local:?}, frozen_remote={frozen_remote:?} now={current_remote_etag:?})", now_hms());
        }
    }

    let action = journal::decide_action(&item, current_local, local_entry.map(|e| e.inode), current_remote_etag, target.direction);
    if crate::is_debug_enabled() {
        eprintln!(
            "[{}] [debug] {child_relative}: decide_action -> {action:?} (never_synced={} local={current_local:?} remote_etag={current_remote_etag:?} last_synced_mtime={:?} last_synced_size={:?} last_synced_etag={:?})",
            now_hms(),
            item.last_synced_mtime.is_none() && item.last_synced_etag.is_none(),
            item.last_synced_mtime, item.last_synced_size, item.last_synced_etag,
        );
    }
    let now = now_unix();

    // Rename/move pairing -- only a candidate for Upload's specific
    // never-synced sub-case (current_remote_etag.is_none(): nothing
    // exists at the destination yet, so this isn't an overwrite of
    // something already tracked). Checked here, before the mtime-age
    // gate below, since a confirmed pairing already proves the content
    // is unchanged from its last successful sync (that's exactly what
    // the size+mtime corroboration below confirms) -- there's no
    // "possibly still mid-write" concern the age gate exists for once
    // that's established.
    if action == SyncAction::Upload && current_remote_etag.is_none() {
        if let Some(local_entry) = local_entry {
            if let Some(matched) = journal::find_by_inode(journal_conn, &target.connection_id, local_entry.inode, false)? {
                let already_claimed = target.renamed_away.borrow().contains(&matched.relative_path);
                let old_still_exists_locally = target.local_root.join(&matched.relative_path).try_exists().unwrap_or(true);
                // Files (unlike directories) need the secondary size/mtime
                // corroboration -- an inode match alone confirms "same
                // underlying file," not "content unchanged since last
                // sync." A mismatch here means the content changed too
                // (not a pure rename), so this deliberately falls through
                // to ordinary Upload-as-new + whatever the old path
                // separately, correctly decides (DeleteRemote) rather
                // than pairing them.
                let content_unchanged = matched.last_synced_mtime == Some(local_entry.mtime)
                    && matched.last_synced_size == Some(local_entry.size);

                if matched.relative_path != child_relative && !already_claimed && !old_still_exists_locally && content_unchanged {
                    let old_remote_url = target.remote_url_for(&matched.relative_path);
                    // No transfer_with_stall_detection wrapper -- see
                    // sync_child_directory's own identical rename-pairing
                    // block for why: MOVE is a single, metadata-only
                    // request with nothing to stream progress on, not a
                    // content transfer.
                    let move_permit = target.content_semaphore.acquire().await;
                    // See RemoteWriteGuard's own doc comment.
                    let _remote_write_guard = RemoteWriteGuard::new(events_tx);
                    match webdav_move_serialized(target, &old_remote_url, &remote_url, false, None).await {
                        Ok(()) => {
                            // Pool 3's own permit released explicitly,
                            // right here, rather than left held for the
                            // rest of this match arm by default -- the
                            // MOVE itself is already done; there's no
                            // reason to keep occupying a Pool 3 slot
                            // while the confirming stat below (Pool 2)
                            // runs. Pool 2 (confirm/detect) for that stat
                            // specifically, not another Pool 3 permit --
                            // see this session's own conversation for
                            // why: keeping this small, separate confirm
                            // step out from behind whatever else is
                            // currently saturating Pool 3 (a large
                            // transfer, a batch of hash verifications) is
                            // exactly what lets it resolve faster, rather
                            // than waiting in the same line as genuinely
                            // expensive work.
                            drop(move_permit);
                            let info = {
                                let _confirm_permit = target.confirm_pool.acquire(&target.content_semaphore, "Pool 2 (confirm/detect)").await;
                                target.webdav_stat(&remote_url).await?
                            };
                            let new_etag = info.etag.unwrap_or_default();
                            journal::rename_synced_path(
                                journal_conn, &target.connection_id, &matched.relative_path, child_relative,
                                &new_etag, local_entry.mtime, local_entry.size, local_entry.inode, now,
                            )?;
                            target.renamed_away.borrow_mut().insert(matched.relative_path.clone());
                            let _ = events_tx.try_send(SyncEvent::ItemRenamed {
                                connection: target.connection_name.clone(),
                                old_path: matched.relative_path,
                                new_path: child_relative.to_string(),
                            });
                            return Ok(());
                        }
                        Err(e) => {
                            // Source vanished between the check above and
                            // now, or some other transient failure -- fall
                            // through to the ordinary Upload path below
                            // rather than propagating; this is exactly the
                            // kind of single-item failure the round
                            // shouldn't abort over.
                            eprintln!("sync[{}]: rename pairing for {child_relative} failed, falling back to plain upload: {e}", target.connection_id);
                        }
                    }
                }
            }
        }
    }

    // The mirror direction -- a file renamed/moved on the *server*
    // (web UI, another synced device), not locally. Only a candidate
    // for Download's own never-synced case (this exact path has no
    // last_synced_mtime/last_synced_etag at all yet) -- an ordinary
    // Download of a path this journal already knows about is ALSO
    // decide_action::Download, but that's a genuine remote content
    // change to something already tracked, not a rename candidate; the
    // never-synced check is what tells the two apart, the same
    // distinction current_remote_etag.is_none() draws for the Upload
    // direction just above. See that block's own doc comment, and
    // sync_child_directory's identical remote-side block, for the full
    // reasoning on why oc:fileid is what makes this direction
    // detectable at all, and why no server-side verification of the old
    // path happens here either.
    if action == SyncAction::Download && item.last_synced_mtime.is_none() && item.last_synced_etag.is_none() {
        if let Some(fileid) = remote_entry.and_then(|e| e.fileid.as_deref()) {
            if let Some(matched) = journal::find_by_fileid(journal_conn, &target.connection_id, fileid, false)? {
                let already_claimed = target.renamed_away.borrow().contains(&matched.relative_path);
                let old_local_path = target.local_root.join(&matched.relative_path);
                if matched.relative_path != child_relative && !already_claimed && old_local_path.try_exists().unwrap_or(false) {
                    match std::fs::rename(&old_local_path, &local_path) {
                        Ok(()) => {
                            // Re-stat fresh rather than reusing matched's
                            // own last_synced_mtime/size/inode -- a plain
                            // local rename never touches any of them, so
                            // this is just confirming that invariant held,
                            // not expecting it to have changed.
                            let meta = std::fs::metadata(&local_path)?;
                            let mtime = meta.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                            let new_etag = current_remote_etag.unwrap_or_default();
                            journal::rename_synced_path(
                                journal_conn, &target.connection_id, &matched.relative_path, child_relative,
                                new_etag, mtime, meta.len() as i64, local_inode(&local_path).unwrap_or_default(), now,
                            )?;
                            target.renamed_away.borrow_mut().insert(matched.relative_path.clone());
                            let _ = events_tx.try_send(SyncEvent::ItemRenamed {
                                connection: target.connection_name.clone(),
                                old_path: matched.relative_path,
                                new_path: child_relative.to_string(),
                            });
                            return Ok(());
                        }
                        Err(e) => {
                            eprintln!("sync[{}]: remote-side rename pairing for {child_relative} failed, falling back to plain download: {e}", target.connection_id);
                        }
                    }
                }
            }
        }
    }

    // Skip uploading a file that was JUST modified locally -- decide_action
    // only returns Upload when local_changed is true, meaning the mtime
    // moved since the last successful sync, which is exactly the signal a
    // process might still be mid-write. A held-open file handle isn't
    // portably checkable on Linux via GIO (that's lsof/fuser territory),
    // so this is the practical alternative every other sync client uses
    // too: a plain minimum-age-since-mtime skip. Scoped to Upload
    // specifically, not every action -- it's the only one that actually
    // reads and transmits the local file's live byte content.
    // Download/DeleteLocal only ever fire when decide_action has already
    // confirmed local is UNCHANGED since the last successful sync, so a
    // recent local mtime there just reflects a recent prior sync, not an
    // in-progress write -- gating those too would incorrectly stall
    // legitimate downloads. Left completely alone this round (not marked
    // skipped, not logged as an error) -- there's nothing wrong here,
    // it's just not ready yet -- but had_freshness_deferral is set so
    // main.rs's trigger_sync_round schedules a real, prompt follow-up
    // round instead of leaving this to whatever round happens to run
    // next on its own schedule -- see that field's own doc comment.
    if action == SyncAction::Upload {
        if let Some((mtime, size)) = current_local {
            // mtime > now means the file's reported modification time is
            // in the future relative to this system's own clock -- a
            // real, confirmed occurrence, not just theoretical: a file
            // extracted from a ZIP archive can end up with a wildly
            // future-dated mtime because the ZIP format's own timestamp
            // field carries no timezone information at all -- it's a
            // bare "local time" value by convention. If whatever wrote
            // the archive and whatever extracted it disagree on which
            // timezone that means, the restored mtime lands offset by
            // exactly the difference between those zones. Confirmed
            // against a real report: two files extracted this way sat
            // permanently deferred for minutes straight, with the exact
            // same mtime logged on every single retry regardless of how
            // much real time passed -- the gap matched the system's own
            // UTC offset almost exactly. saturating_sub floors at 0 for
            // precisely this case (mtime > now), which this check would
            // otherwise read as "just modified, wait" -- forever, since
            // real elapsed time can never close a gap that was never
            // real to begin with. A mtime already in the future is never
            // legitimately "a process might still be mid-write" (nothing
            // sets its own mtime ahead of the clock while actively
            // writing), so it doesn't get the deferral at all.
            let age = now.saturating_sub(mtime);
            let cooldown_passed = !(mtime <= now && age < MIN_FILE_AGE_SECS);

            // Second, independent requirement alongside the cooldown
            // above -- see REQUIRED_STABILITY_STREAK's own doc comment
            // for why the age check alone isn't sufficient. Tracked
            // regardless of whether the cooldown has already passed, so
            // the streak keeps accumulating throughout the whole
            // cooldown window rather than only starting to count once it
            // clears -- a file that's been genuinely unchanged for the
            // entire cooldown period shouldn't also need to wait out
            // three more rounds on top of that once the cooldown itself
            // is satisfied.
            let current_snapshot = (mtime, size);
            let streak = {
                let mut streaks = target.stability_streak.borrow_mut();
                let count = match streaks.get(child_relative) {
                    Some((last_snapshot, count)) if *last_snapshot == current_snapshot => count + 1,
                    // Any actual change (or the first time this path has
                    // ever been observed here) resets to 1, not 0 -- this
                    // observation itself is the first data point of a
                    // new streak, not a zero-length one.
                    _ => 1,
                };
                streaks.insert(child_relative.to_string(), (current_snapshot, count));
                count
            };
            let stable_enough = streak >= REQUIRED_STABILITY_STREAK;

            if !skip_settling_checks && (!cooldown_passed || !stable_enough) {
                target.had_freshness_deferral.set(true);
                // Recorded into the exact same map (and by the exact
                // same shape -- absolute local path, keyed by
                // connection_id) a real watch.rs FileMonitor callback
                // would use -- see SyncTarget::dirty_dirs' own doc
                // comment for why this is what lets the follow-up round
                // scope down to just this directory instead of running
                // a full crawl.
                target.dirty_dirs.borrow_mut()
                    .entry(target.connection_id.clone())
                    .or_default()
                    .insert(local_dir.to_path_buf());
                return Ok(());
            }
            // Both requirements satisfied -- clears the tracked entry so
            // a future edit to this same file starts its own streak
            // fresh, rather than a stale, already-satisfied count from
            // this edit carrying over and letting a brand new change
            // through immediately on its very first observation.
            target.stability_streak.borrow_mut().remove(child_relative);
        }
    }

    // Download-side counterpart to the Upload gate just above -- same
    // two independent requirements (an age cooldown, plus a consecutive-
    // observation streak), same reasoning (don't act on a file that
    // might still be mid-write), now applied symmetrically rather than
    // only ever gating the local->remote direction. See
    // remote_stability_streak's own doc comment on SyncTarget for why
    // (etag, size) is the snapshot here rather than (mtime, size).
    if action == SyncAction::Download {
        if let Some(remote) = remote_entry {
            if let Some(etag) = &remote.etag {
                // mtime is optional here (a server simply may not report
                // one), unlike the local side where it's always present
                // -- when it's missing, the cooldown is treated as
                // already satisfied (there's no way to measure an age
                // that was never reported), and stability rests entirely
                // on the streak below instead. When it is present, same
                // future-mtime guard as the local side above for the
                // same reason: a server clock skewed ahead of this
                // system's own would otherwise read as "just modified,
                // wait" forever, since elapsed time can never close a
                // gap that isn't real.
                let cooldown_passed = match remote.mtime {
                    Some(mtime) => {
                        let age = now.saturating_sub(mtime);
                        !(mtime <= now && age < MIN_FILE_AGE_SECS)
                    }
                    None => true,
                };

                let current_snapshot = (etag.clone(), remote.size);
                let streak = {
                    let mut streaks = target.remote_stability_streak.borrow_mut();
                    let count = match streaks.get(child_relative) {
                        Some((last_snapshot, count)) if *last_snapshot == current_snapshot => count + 1,
                        // Same reasoning as the local side's own streak
                        // just above -- any actual change, or the first
                        // time this path has ever been observed here,
                        // starts a fresh streak at 1, not 0.
                        _ => 1,
                    };
                    streaks.insert(child_relative.to_string(), (current_snapshot, count));
                    count
                };
                let stable_enough = streak >= REQUIRED_STABILITY_STREAK;

                if !skip_settling_checks && (!cooldown_passed || !stable_enough) {
                    target.had_freshness_deferral.set(true);
                    target.dirty_dirs.borrow_mut()
                        .entry(target.connection_id.clone())
                        .or_default()
                        .insert(local_dir.to_path_buf());
                    return Ok(());
                }
                // Same reasoning as the local side's own removal just
                // above -- clears the tracked entry so a future, separate
                // edit to this same remote file starts its own streak
                // fresh rather than inheriting an already-satisfied count
                // from this one.
                target.remote_stability_streak.borrow_mut().remove(child_relative);
            }
            // No etag at all: can't corroborate stability this way,
            // same "None means can't corroborate" tolerance RemoteEntry::
            // etag's own doc comment already establishes for a plain
            // WebDAV server without oc: support -- proceeds without this
            // particular gate rather than blocking a legitimate download
            // forever on a signal that server will simply never provide.
        }
    }

    // ConflictDeleteVsChange included alongside DeleteLocal/DeleteRemote
    // now -- see deletion_pending's own doc comment on SyncTarget for
    // why this specific gap (this exact gate already existing, but never
    // actually applied to this one action) was confirmed as the real
    // cause of the resolver-facing version of the same narrow-window
    // replace race this gate has always guarded DeleteLocal/DeleteRemote
    // against.
    if matches!(action, SyncAction::DeleteLocal | SyncAction::DeleteRemote | SyncAction::ConflictDeleteVsChange) {
        let now_instant = std::time::Instant::now();
        let (first_seen, streak) = {
            let mut pending = target.deletion_pending.borrow_mut();
            let entry = match pending.get(child_relative) {
                Some((first_seen, streak)) => (*first_seen, streak + 1),
                None => (now_instant, 1),
            };
            pending.insert(child_relative.to_string(), entry);
            entry
        };

        // Own, dedicated constants now, not shared with the upload-
        // readiness gate above -- see DELETION_MIN_AGE_SECS/DELETION_
        // REQUIRED_STREAK's own doc comments for why a remote-side
        // settle genuinely needs more room than a local one. first_seen
        // is when this path was *first* observed missing, not a file's
        // own mtime (there isn't one for something that isn't there) --
        // the cooldown measures how long the absence itself has now
        // persisted.
        let cooldown_passed = first_seen.elapsed().as_secs() >= DELETION_MIN_AGE_SECS as u64;
        let stable_enough = streak >= DELETION_REQUIRED_STREAK;

        if !skip_settling_checks && (!cooldown_passed || !stable_enough) {
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] {child_relative}: deletion deferred (cooldown_passed={cooldown_passed} streak={streak}/{DELETION_REQUIRED_STREAK}) -- false-alarm guard against a transient atomic-save race", now_hms());
            }
            target.had_freshness_deferral.set(true);
            target.dirty_dirs.borrow_mut()
                .entry(target.connection_id.clone())
                .or_default()
                .insert(local_dir.to_path_buf());
            return Ok(());
        }
        if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] {child_relative}: deletion confirmed genuinely persistent (streak={streak}), proceeding with {action:?}", now_hms());
        }
        // Confirmed genuinely, persistently gone -- clears the tracked
        // entry so a future, unrelated deletion of a file later
        // recreated at this same path starts its own streak fresh,
        // rather than a stale, already-satisfied first_seen/count
        // letting that later, genuinely-new observation through on its
        // very first round.
        target.deletion_pending.borrow_mut().remove(child_relative);
    } else {
        // The path exists again (whatever the current action actually
        // is) -- clears any pending deletion tracking for it
        // unconditionally, not just when the gate above happens to pass.
        // Without this, a false alarm that resolved on its own (the file
        // reappeared before ever reaching DELETION_REQUIRED_STREAK)
        // would leave a stale entry sitting here indefinitely, ready to
        // incorrectly shortcut a later, completely unrelated deletion of
        // a file that just happens to get recreated at this same path.
        target.deletion_pending.borrow_mut().remove(child_relative);
    }

    // Size-gate anything that would actually move bytes if synced or
    // resolved. Actions that only touch metadata (Noop, deletes,
    // ForgetRow) never transfer content, so they're exempt -- there's no
    // upper-bound reason to block deleting or forgetting a huge file.
    //
    // The two Conflict* actions are NOT exempt, despite an earlier
    // version of this comment claiming they were (deferring the check to
    // "whichever resolve_* function actually performs a transfer, at
    // resolution time") -- confirmed that promise was never actually
    // kept: none of resolve_keep_local/resolve_keep_remote/
    // resolve_keep_both_rename_local/resolve_keep_both_rename_remote
    // reference max_file_size_bytes at all, so an oversized "conflict"
    // could be resolved straight through the limit entirely. Checking
    // here instead means an oversized file never reaches the
    // conflict-flagging step in the first place -- there's nothing
    // meaningful to resolve for a file that was never going to sync in
    // *either* direction anyway, so it's a quiet SkippedTooLarge instead
    // of asking the user to adjudicate something no resolution could
    // ever legally complete.
    let transfer_size: Option<i64> = match action {
        SyncAction::Upload => current_local.map(|(_, size)| size),
        SyncAction::Download => remote_entry.map(|e| e.size),
        // Checked against whichever side is larger, not just one --
        // resolving in EITHER direction (Keep Remote downloads, Keep
        // Local uploads, either Keep Both does both) needs to stay under
        // the limit, not just whichever direction happens to get picked
        // later. A deleted side contributes 0 (nothing to transfer for
        // it), so ConflictDeleteVsChange correctly only checks the side
        // that still has real content.
        SyncAction::ConflictBothChanged | SyncAction::ConflictDeleteVsChange => {
            let local_size = current_local.map(|(_, size)| size).unwrap_or(0);
            let remote_size = remote_entry.map(|e| e.size).unwrap_or(0);
            Some(local_size.max(remote_size))
        }
        _ => None,
    };

    if let Some(size) = transfer_size {
        let max_file_size_bytes = target.current_settings().max_file_size_bytes;
        if size as u64 > max_file_size_bytes {
            eprintln!(
                "sync[{}]: skipping {child_relative} ({size} bytes exceeds {} byte limit)",
                target.connection_id, max_file_size_bytes
            );
            journal::mark_skipped_too_large(journal_conn, &target.connection_id, child_relative, false, now)?;
            let _ = events_tx.try_send(SyncEvent::SkippedTooLarge {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
                size_bytes: size as u64,
                limit_bytes: max_file_size_bytes,
            });
            return Ok(());
        }
    }

    execute_action(action, journal_conn, target, &local_path, &remote_url, child_relative, current_local, current_remote_etag, remote_entry.and_then(|e| e.fileid.as_deref()), remote_entry.map(|e| e.size), remote_entry.and_then(|e| e.mtime), now, events_tx).await
        .map_err(|e| tag_webdav_action(e, action.label()))
}

/// RAII guard that emits TransferEnded on drop -- guarantees it fires
/// even if the enclosing future is itself dropped mid-transfer (e.g. by
/// pause's own cancellation race against the whole sync_target future
/// this is nested within -- see AppState's own doc comment on
/// active_sync_cancels), not only when execution reaches a normal,
/// sequential "emit TransferEnded" statement afterward. Confirmed as a
/// real, reported bug without this: a dropped future never resumes past
/// whatever await point it was suspended at, so any code written
/// "after" the transfer's own await -- including the old, plain
/// TransferEnded emission -- simply never ran when cancelled mid-flight,
/// leaving a permanent "ghost" entry in Active Transfers even though the
/// real work had already stopped.
struct TransferGuard<'a> {
    events_tx: &'a async_channel::Sender<SyncEvent>,
    connection: String,
    path: String,
}

impl<'a> Drop for TransferGuard<'a> {
    fn drop(&mut self) {
        let _ = self.events_tx.try_send(SyncEvent::TransferEnded {
            connection: std::mem::take(&mut self.connection),
            path: std::mem::take(&mut self.path),
        });
    }
}

/// RAII guard, same pattern as TransferGuard just above and for the
/// same reason -- guarantees end_remote_write (triggered on the main.rs
/// side by SyncEvent::RemoteWriteFinished) fires exactly once for every
/// begin_remote_write (SyncEvent::RemoteWriteStarted), regardless of
/// how the enclosing scope actually ends: success, an early `?` return
/// on error, or a panic unwinding through it. See RemoteWriteStarted's
/// own doc comment in status.rs for the full design this participates
/// in, and pending_remote_writes' own doc comment on AppState in
/// main.rs for why this exists at all. Constructed at the start of
/// every one of this app's own seven genuine remote-writing operations,
/// right before the actual write begins -- never around anything else.
struct RemoteWriteGuard<'a> {
    events_tx: &'a async_channel::Sender<SyncEvent>,
}

impl<'a> RemoteWriteGuard<'a> {
    fn new(events_tx: &'a async_channel::Sender<SyncEvent>) -> Self {
        let _ = events_tx.try_send(SyncEvent::RemoteWriteStarted);
        Self { events_tx }
    }
}

impl<'a> Drop for RemoteWriteGuard<'a> {
    fn drop(&mut self) {
        let _ = self.events_tx.try_send(SyncEvent::RemoteWriteFinished);
    }
}

async fn execute_action(
    action: SyncAction,
    journal_conn: &Connection,
    target: &SyncTarget,
    local_path: &Path,
    remote_file: &str,
    child_relative: &str,
    current_local: Option<(i64, i64)>,
    current_remote_etag: Option<&str>,
    current_remote_fileid: Option<&str>,
    remote_size: Option<i64>,
    remote_mtime: Option<i64>,
    now: i64,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    match action {
        SyncAction::Noop => {}

        SyncAction::Upload => {
            // Mirror of Download's own identical check just below --
            // same reasoning, same reused contents_match, applied here
            // to the case where local's own mtime/inode moved (a touch,
            // or a tool rewriting a file with unchanged content) but the
            // actual bytes didn't, and remote already has them. Records
            // the *existing*, unchanged remote ETag as the new last-
            // synced one -- there's genuinely nothing new to record from
            // remote's own side, since nothing was actually uploaded to
            // it -- alongside local's own new mtime/size, which did
            // change and do need recording.
            if let (Some(etag), Some((local_mtime, local_size))) = (current_remote_etag, current_local) {
                if Some(local_size) == remote_size {
                    let comparison = contents_match(target, child_relative, local_path, remote_file, local_size, target.current_settings().max_file_size_bytes).await;
                    if let ContentComparison::Match = comparison {
                        journal::mark_synced(journal_conn, &target.connection_id, child_relative, false, local_mtime, local_size, etag, local_inode(local_path), current_remote_fileid, now)?;
                        eprintln!("sync[{}]: {child_relative} looked like an upload was needed but content matched -- no transfer needed", target.connection_id);
                        let _ = events_tx.try_send(SyncEvent::TransferSkippedIdentical {
                            connection: target.connection_name.clone(),
                            path: child_relative.to_string(),
                        });
                        return Ok(());
                    }
                }
            }
            // Acquired before TransferStarted is even emitted, not after
            // -- see Semaphore's own doc comment for the bug this whole
            // mechanism exists to fix. Emitting TransferStarted first
            // would mean Active Transfers goes right back to showing
            // every item merely queued and waiting its turn, not just
            // the ones genuinely, actively transferring right now --
            // the exact problem this was built to solve in the first
            // place.
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] {child_relative}: acquiring permit", now_hms());
            }
            let _permit = target.content_semaphore.acquire().await;
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] {child_relative}: permit acquired, starting upload_temp_first", now_hms());
            }
            let _ = events_tx.try_send(SyncEvent::TransferStarted {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
                direction: TransferDirection::Uploading,
            });
            // Constructed right after TransferStarted, not a plain,
            // sequential TransferEnded emission afterward -- see
            // TransferGuard's own doc comment for the bug this fixes.
            // Held for the rest of this match arm, same lifetime as
            // _permit just above, so it's guaranteed to fire exactly
            // once this transfer is genuinely done settling, however
            // that happens.
            let _transfer_guard = TransferGuard {
                events_tx,
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
            };
            // See RemoteWriteGuard's own doc comment -- constructed
            // right before the actual write, held through the same
            // scope _transfer_guard is, so it settles at the same
            // moment regardless of how this arm finishes.
            let _remote_write_guard = RemoteWriteGuard::new(events_tx);
            let upload_result = upload_temp_first(target, local_path, remote_file, events_tx, child_relative).await;
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] {child_relative}: upload_temp_first returned {}", now_hms(), match &upload_result { Ok(_) => "Ok".to_string(), Err(e) => format!("Err({e})") });
            }
            // _permit (and _transfer_guard) still held through here
            // deliberately -- released only once this whole match arm
            // finally ends, well after this point, whether via the
            // early '?' below or falling through to the end. Ending it
            // any earlier would let a second transfer start before this
            // one has genuinely finished settling (the journal write
            // still below), not just before its bytes stopped moving.
            let etag = upload_result?;
            let (mtime, size) = current_local.expect("Upload implies local exists");
            journal::mark_synced(journal_conn, &target.connection_id, child_relative, false, mtime, size, &etag, local_inode(local_path), None, now)?;
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] {child_relative}: mark_synced committed, sending ItemUploaded", now_hms());
            }
            let _ = events_tx.try_send(SyncEvent::ItemUploaded {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
            });
        }

        SyncAction::Download => {
            // Hash-verified before actually transferring, when there's a
            // local file to compare against and its own size already
            // matches what's remote -- by request, same reasoning, the
            // same contents_match this app already had (not a
            // reimplementation), as ConflictBothChanged's own identical
            // check further below: an ETag change alone doesn't
            // necessarily mean the content actually differs (a
            // deterministic rebuild regenerating byte-identical output
            // is a real, confirmed case), and a remote overwrite that
            // briefly looked like a deletion (see DELETION_MIN_AGE_SECS's
            // own doc comment) reappearing as a fresh Download moments
            // later is exactly the other, concrete case this closes.
            // Sizes differing entirely is already a definite, known
            // difference -- no need to even ask -- same short-circuit
            // ConflictBothChanged already uses.
            if let Some((local_mtime, local_size)) = current_local {
                if Some(local_size) == remote_size {
                    let comparison = contents_match(target, child_relative, local_path, remote_file, local_size, target.current_settings().max_file_size_bytes).await;
                    if let ContentComparison::Match = comparison {
                        let etag = current_remote_etag.unwrap_or_default().to_string();
                        journal::mark_synced(journal_conn, &target.connection_id, child_relative, false, local_mtime, local_size, &etag, local_inode(local_path), current_remote_fileid, now)?;
                        eprintln!("sync[{}]: {child_relative} looked like a download was needed but content matched -- no transfer needed", target.connection_id);
                        let _ = events_tx.try_send(SyncEvent::TransferSkippedIdentical {
                            connection: target.connection_name.clone(),
                            path: child_relative.to_string(),
                        });
                        return Ok(());
                    }
                }
            }
            // Same reasoning as Upload's own permit acquisition above.
            let _permit = target.content_semaphore.acquire().await;
            let _ = events_tx.try_send(SyncEvent::TransferStarted {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
                direction: TransferDirection::Downloading,
            });
            // Same reasoning as Upload's own TransferGuard above.
            let _transfer_guard = TransferGuard {
                events_tx,
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
            };
            let preserve_mode = local_mode(local_path);
            let download_result = download_with_mode(target, remote_file, local_path, preserve_mode, events_tx, child_relative, current_remote_etag).await;
            // Prefers the ETag the download's own response actually
            // returned over current_remote_etag (known from the listing
            // that decided this file needed downloading in the first
            // place) -- same reasoning, same fix, as
            // resolve_keep_remote's own equivalent case: the response
            // is guaranteed to describe exactly the bytes just
            // received, closing the same narrow race a listing-then-
            // download gap could otherwise leave open if the remote
            // file changed again in between. Falls back to
            // current_remote_etag only if the response genuinely didn't
            // include one.
            let new_etag = download_result?;
            let meta = std::fs::metadata(local_path)?;
            let mtime = meta.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
            let etag = new_etag.or_else(|| current_remote_etag.map(String::from)).unwrap_or_default();
            journal::mark_synced(journal_conn, &target.connection_id, child_relative, false, mtime, meta.len() as i64, &etag, local_inode(local_path), current_remote_fileid, now)?;
            let _ = events_tx.try_send(SyncEvent::ItemDownloaded {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
            });
        }

        SyncAction::ConflictBothChanged => {
            // Sizes matching is a precondition GVfs already told us via
            // remote_entry -- if they differ, don't even bother reading
            // either side, they're provably different already. This
            // check is read-only (a content comparison, not a persisted
            // move) so it stays even under the new detect-only policy --
            // there's no reason to interrupt the user over two files
            // that turn out to be identical.
            let sizes_match = current_local.map(|(_, size)| size) == remote_size;

            // See SyncStatus::PendingVerification's own doc comment for
            // the full design reasoning. Both size AND mtime already
            // agreeing is a strong, cheap signal the files are likely
            // already identical -- strong enough to defer the real,
            // expensive content check rather than block this item's own
            // processing on it right now, but deliberately NOT strong
            // enough on its own to mark this Synced outright (a
            // background verification pass, not this metadata match
            // itself, is the only thing that ever does that -- see
            // main.rs). Sizes matching but mtimes differing is treated
            // exactly as it always was: still genuinely ambiguous, still
            // verified immediately below, not deferred -- this
            // optimization is specifically for the "everything already
            // agrees" case, not a general relaxation of when the real
            // check happens.
            let mtimes_also_match = sizes_match
                && current_local.map(|(mtime, _)| mtime) == remote_mtime;

            if mtimes_also_match {
                journal::mark_pending_verification(journal_conn, &target.connection_id, child_relative, false, current_local, current_remote_etag, now)?;
                eprintln!("sync[{}]: {child_relative} size+mtime match -- queued for background content verification", target.connection_id);
                // Deliberately no SyncEvent here -- this isn't a
                // user-facing outcome yet (neither confirmed synced nor
                // a real conflict), just an internal, deferred state
                // nobody outside this process needs to know about until
                // the background pass actually resolves it one way or
                // the other.
            } else {
                // Sizes not matching at all is already a definite,
                // known-for-certain difference -- no need to even call
                // contents_match to learn that. Sizes matching, on the
                // other hand, genuinely needs the real check to answer
                // the open question.
                let comparison = if sizes_match {
                    contents_match(target, child_relative, local_path, remote_file, current_local.map(|(_, s)| s).unwrap_or(0), target.current_settings().max_file_size_bytes).await
                } else {
                    ContentComparison::Different
                };

                match comparison {
                    ContentComparison::Match => {
                        let (mtime, size) = current_local.expect("ConflictBothChanged implies local exists");
                        let etag = current_remote_etag.unwrap_or_default().to_string();
                        journal::mark_synced(journal_conn, &target.connection_id, child_relative, false, mtime, size, &etag, local_inode(local_path), current_remote_fileid, now)?;
                        eprintln!("sync[{}]: {child_relative} looked conflicting but content matched -- no copy needed", target.connection_id);
                        let _ = events_tx.try_send(SyncEvent::ConflictResolvedIdentical {
                            connection: target.connection_name.clone(),
                            path: child_relative.to_string(),
                        });
                    }
                    ContentComparison::Different => {
                        // Detect and stop -- neither file is touched. No rename,
                        // no download. The user resolves this explicitly (see
                        // resolve_keep_local/resolve_keep_remote/
                        // resolve_keep_both_rename_local/resolve_keep_both_rename_remote
                        // below); until then this item is frozen by the
                        // status==Conflict short-circuit above sync_child_file's
                        // decide_action call.
                        journal::mark_conflict_pending(journal_conn, &target.connection_id, child_relative, false, current_local, current_remote_etag, now)?;
                        eprintln!("sync[{}]: conflict on {child_relative} -- both sides changed, left untouched pending resolution", target.connection_id);
                        let _ = events_tx.try_send(SyncEvent::Conflict {
                            connection: target.connection_name.clone(),
                            path: child_relative.to_string(),
                        });
                    }
                    // Confirmed as a real, reported problem otherwise:
                    // a connection blip during the content check (see
                    // ContentComparison's own doc comment) used to mean
                    // this fell through to the Different branch above by
                    // default, which could flood the resolver with many
                    // items at once that were never actually examined,
                    // not genuine conflicts. Deferred to
                    // PendingVerification instead -- same queue, same
                    // background retry, the mtimes_also_match branch
                    // above already uses -- rather than guessed at from
                    // a check that never completed.
                    ContentComparison::Indeterminate => {
                        journal::mark_pending_verification(journal_conn, &target.connection_id, child_relative, false, current_local, current_remote_etag, now)?;
                        eprintln!("sync[{}]: {child_relative} content check couldn't complete -- queued for another attempt rather than treated as a conflict", target.connection_id);
                    }
                }
            }
        }

        SyncAction::DeleteLocal => {
            if target.current_settings().move_deleted_to_trash {
                gio::File::for_path(local_path).trash_future(glib::Priority::DEFAULT).await?;
            } else {
                std::fs::remove_file(local_path)?;
            }
            journal::forget_item(journal_conn, &target.connection_id, child_relative)?;
            let _ = events_tx.try_send(SyncEvent::ItemDeleted {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
                local: true,
            });
        }

        SyncAction::DeleteRemote => {
            let _permit = target.content_semaphore.acquire().await;
            // See RemoteWriteGuard's own doc comment. `?` right after
            // this is exactly why it's a guard, not a plain increment/
            // decrement pair -- a failed delete still needs end_remote_
            // write to fire, and Drop guarantees that regardless of
            // this early return.
            let _remote_write_guard = RemoteWriteGuard::new(events_tx);
            target.webdav_delete(remote_file).await?;
            journal::forget_item(journal_conn, &target.connection_id, child_relative)?;
            let _ = events_tx.try_send(SyncEvent::ItemDeleted {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
                local: false,
            });
        }

        SyncAction::ConflictDeleteVsChange => {
            // Data-preserving policy unchanged: never let a delete
            // silently win over a concurrent edit on the other side.
            // What changed is *how* that's preserved -- previously this
            // resurrected the deleted side immediately; now it just
            // flags and stops, same as ConflictBothChanged, so the user
            // decides rather than the app silently undoing their delete.
            journal::mark_conflict_pending(journal_conn, &target.connection_id, child_relative, false, current_local, current_remote_etag, now)?;
            eprintln!("sync[{}]: conflict on {child_relative} -- deleted on one side, changed on the other, left untouched pending resolution", target.connection_id);
            let _ = events_tx.try_send(SyncEvent::Conflict {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
            });
        }

        SyncAction::ForgetRow => {
            journal::forget_item(journal_conn, &target.connection_id, child_relative)?;
        }
    }

    Ok(())
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

/// HH:MM:SS, local time -- exact same format as main.rs's own now_hms
/// (duplicated here rather than shared, since that one's private to
/// main.rs and this module has no other reason to depend on it). Exists
/// specifically so every [debug] eprintln! below carries a timestamp
/// matching the bracketed ones already on every line of the in-app
/// Status log -- confirmed as a real, reported gap without this: with
/// no way to correlate a raw stderr capture (which has no timestamps of
/// its own) against the Status log's own timestamped lines, there was
/// no way to tell which round a given [debug] line even belonged to
/// without guessing from surrounding context.
fn now_hms() -> String {
    glib::DateTime::now_local().ok()
        .and_then(|d| d.format("%H:%M:%S").ok())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// One remote item's current metadata -- for main.rs's own file-info
/// display specifically (the only caller outside this module that
/// needs a raw ETag/size/mtime rather than a full sync decision).
/// Thin public wrapper around SyncTarget's own private webdav_stat/
/// remote_url_for, which stay private since every other caller of
/// either lives inside this module already.
pub async fn remote_stat(target: &SyncTarget, relative_path: &str) -> Result<webdav::RemoteEntry, SyncError> {
    let url = target.remote_url_for(relative_path);
    target.webdav_stat(&url).await.map_err(Into::into)
}

/// "Keep Local" -- the local (conflicted, still-untouched) copy becomes
/// canonical: upload it, overwriting whatever's on the remote side, and
/// clear the conflict. Local file itself is untouched; only its content
/// gets pushed up.
pub async fn resolve_keep_local(
    journal_conn: &Connection,
    target: &SyncTarget,
    relative_path: &str,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    let local_path = target.local_root.join(relative_path);
    let remote_url = target.remote_url_for(relative_path);

    let etag = upload_temp_first(target, &local_path, &remote_url, events_tx, relative_path).await?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_local {relative_path}: uploaded, new etag={etag}", now_hms());
    }

    let meta = std::fs::metadata(&local_path)?;
    let mtime = meta.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    journal::mark_synced(journal_conn, &target.connection_id, relative_path, false, mtime, meta.len() as i64, &etag, local_inode(&local_path), None, now_unix())?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_local {relative_path}: journal updated, resolution complete", now_hms());
    }
    Ok(())
}

/// Generates a temp-file-first name for an in-flight transfer -- see
/// exclude::TEMP_FILE_PREFIX for the full reasoning on the naming
/// convention itself. Reuses glib::uuid_string_random rather than
/// pulling in a dedicated randomness crate -- already used elsewhere in
/// this codebase (connection IDs, main.rs) for exactly this "just need
/// enough entropy to avoid a same-round collision" purpose.
fn temp_file_name() -> String {
    format!("{}{}", exclude::TEMP_FILE_PREFIX, glib::uuid_string_random())
}

/// Current local permission bits at `local_path`, if anything's there.
/// `metadata()` follows symlinks, but that's moot here -- sync.rs
/// already treats symlinks as untouched/unsynced entirely (see
/// list_local's own is_symlink handling), so this is only ever called
/// against a real regular file or directory.
fn local_mode(local_path: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(local_path).ok().map(|m| m.permissions().mode())
}

/// Current inode of whatever's at `local_path`, if anything -- the
/// actual signal behind rename/move detection (see
/// journal::find_by_inode's own doc comment). `None` if nothing's there
/// right now, which mark_synced correctly records as "no inode to check
/// against" rather than a false zero.
fn local_inode(local_path: &Path) -> Option<i64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(local_path).ok().map(|m| m.ino() as i64)
}

/// Minimum time between two TransferProgress emissions for the same
/// transfer -- combined with the whole-percentage-point check below,
/// this is a deliberate, sensible double cap. A raw progress callback
/// can fire many times a second for a fast local read/write; reporting
/// every single tick would mean unnecessary event-channel traffic and
/// UI-refresh work for a change no one would actually perceive (a
/// percentage doesn't need hundredths-of-a-point precision, and a human
/// can't watch a number update faster than a few times a second
/// anyway).
const TRANSFER_PROGRESS_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Downloads remote_url's content to local_path, applying preserve_mode
/// to the result afterward if given. Simpler than this function's own
/// pre-migration version: webdav::download already writes to a sibling
/// temp file and renames into place internally (see that function's own
/// doc comment) -- there's no separate temp-file dance to repeat here
/// on top of it, just the transfer itself plus the mode restore.
///
/// WebDAV has no concept of unix permission bits at all -- there's no
/// protocol-level channel to sync a mode to or from the remote side, so
/// this can only ever reapply a mode that already existed somewhere on
/// this device, never propagate one across the wire. A brand-new file
/// (nothing existed at this path on this device before) just gets
/// whatever default permissions the OS/umask gives a fresh file --
/// there's no prior local value to preserve and no remote-side signal
/// to derive one from, so callers pass None rather than inventing a
/// heuristic. The chmod itself is best-effort: a failure here doesn't
/// fail the whole sync action over what's ultimately a secondary
/// property, not the file's actual content -- but it's still reported
/// via SyncEvent::PermissionRestoreFailed rather than silently
/// swallowed, since anything that touches the filesystem and doesn't do
/// exactly what was intended should be visible to the user.
async fn download_with_mode(
    target: &SyncTarget,
    remote_url: &str,
    local_path: &Path,
    preserve_mode: Option<u32>,
    events_tx: &async_channel::Sender<SyncEvent>,
    child_relative: &str,
    expected_etag: Option<&str>,
) -> Result<Option<String>, SyncError> {
    use std::os::unix::fs::PermissionsExt;

    let (username, password) = target.credentials();
    let local_path_owned = local_path.to_path_buf();
    let remote_url_owned = remote_url.to_string();
    let events_tx_owned = events_tx.clone();
    let connection_name = target.connection_name.clone();
    let child_relative_owned = child_relative.to_string();
    let expected_etag_owned = expected_etag.map(|s| s.to_string());
    let new_etag = run_blocking(move || {
        retry_transfer_on_permission_error(&remote_url_owned, &username, &password, || {
            // Reset fresh on every attempt -- same reasoning as the
            // identical choice in the upload call site further below in
            // this same file.
            let mut last_reported: Option<(u32, std::time::Instant)> = None;
            let events_tx_owned = events_tx_owned.clone();
            let connection_name = connection_name.clone();
            let child_relative_owned = child_relative_owned.clone();
            webdav::download(&remote_url_owned, &local_path_owned, &username, &password, expected_etag_owned.as_deref(), move |so_far, total| {
                if total == 0 {
                    return;
                }
                let percent = ((so_far as f64 / total as f64) * 100.0).min(100.0) as u32;
                let should_report = match last_reported {
                    None => true,
                    Some((last_percent, last_time)) => percent != last_percent && last_time.elapsed() >= TRANSFER_PROGRESS_MIN_INTERVAL,
                };
                if should_report {
                    let _ = events_tx_owned.try_send(SyncEvent::TransferProgress {
                        connection: connection_name.clone(),
                        path: child_relative_owned.clone(),
                        bytes_so_far: so_far as i64,
                        bytes_total: total as i64,
                    });
                    last_reported = Some((percent, std::time::Instant::now()));
                }
            })
        })
    }).await?;

    if let Some(mode) = preserve_mode {
        if let Err(e) = std::fs::set_permissions(local_path, std::fs::Permissions::from_mode(mode)) {
            let _ = events_tx.try_send(SyncEvent::PermissionRestoreFailed {
                connection: target.connection_name.clone(),
                path: child_relative.to_string(),
                error: e.to_string(),
            });
        }
    }
    Ok(new_etag)
}

/// Size threshold above which a large-file upload uses Nextcloud's own
/// chunking API (see upload_via_chunking) instead of a single PUT --
/// deliberately well above the API's own documented 5MB-per-chunk
/// floor: using chunking for something that could only ever amount to
/// one or two small chunks wouldn't gain anything real resumability is
/// for -- a genuinely interrupted transfer losing meaningful progress,
/// not just a retry that would restart in a few seconds regardless.
const CHUNKED_UPLOAD_THRESHOLD_BYTES: u64 = 50 * 1024 * 1024; // 50 MiB

/// Uploads local_path's content to remote_url, dispatching to
/// Nextcloud's own chunking API for large files (see
/// upload_via_chunking) when this account's been confirmed to support
/// it (target.chunking_base_url), falling back to a single, ordinary
/// PUT (upload_plain, just below) otherwise -- either because the file
/// is small enough that chunking wouldn't gain anything real, or
/// because this account/server was never confirmed Nextcloud/
/// ownCloud-capable in the first place (a generic WebDAV server, most
/// plainly -- see SyncTarget::chunking_base_url's own doc comment).
/// Returns the new ETag either way; callers don't need to know or care
/// which path actually ran.
///
/// Neither this function nor upload_plain/upload_via_chunking below
/// acquire a content_semaphore permit of their own, unlike every other
/// metadata-call site in this file -- deliberately, not an oversight.
/// This function is called from exactly two shapes of place: execute_
/// action's own Upload branch, which already holds a permit for the
/// entire duration of this call (see its own _permit binding), and the
/// conflict-resolution functions (resolve_keep_local, resolve_keep_
/// both_rename_remote), which deliberately hold no permit at all so a
/// resolution the user is actively waiting on never queues behind
/// background work (see Semaphore's own doc comment). Either way, the
/// right behavior for any metadata call nested inside this one (a
/// fallback stat, a temp-file move, a cleanup delete) is to inherit
/// whatever the caller already decided, not to make its own, separate
/// decision.
async fn upload_temp_first(target: &SyncTarget, local_path: &Path, remote_url: &str, events_tx: &async_channel::Sender<SyncEvent>, child_relative: &str) -> Result<String, SyncError> {
    let file_size = std::fs::metadata(local_path).map(|m| m.len()).unwrap_or(0);
    if file_size >= CHUNKED_UPLOAD_THRESHOLD_BYTES {
        if let Some(chunking_base_url) = target.chunking_base_url.borrow().clone() {
            return upload_via_chunking(target, local_path, remote_url, &chunking_base_url, events_tx, child_relative).await;
        }
    }
    upload_plain(target, local_path, remote_url, events_tx, child_relative).await
}

/// Uploads local_path via Nextcloud's own chunking API -- see
/// webdav::upload_chunked's own doc comment for the full protocol and
/// resumability design. The chunking folder name is deterministic,
/// derived from this connection's own ID and the file's own relative
/// path (SHA256, truncated to 8 bytes, hex-encoded) rather than a
/// fresh random ID per attempt -- so a retry of the same logical
/// upload (this exact connection, this exact path) always lands on the
/// same folder a previous, interrupted attempt used, letting
/// webdav::upload_chunked's own resumability actually find and reuse
/// whatever chunks already succeeded, instead of starting over from
/// nothing on every retry the way a random-per-attempt ID would force.
async fn upload_via_chunking(target: &SyncTarget, local_path: &Path, remote_url: &str, chunking_base_url: &str, events_tx: &async_channel::Sender<SyncEvent>, child_relative: &str) -> Result<String, SyncError> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(target.connection_id.as_bytes());
    hasher.update(b"\0"); // separator -- a connection_id/relative_path pair that could otherwise concatenate ambiguously (e.g. "ab"+"c" vs "a"+"bc") can't collide once a byte neither ID nor path could ever legally contain sits between them
    hasher.update(child_relative.as_bytes());
    let digest = hasher.finalize();
    let folder_name = format!("ripples-{:016x}", u64::from_be_bytes(digest[..8].try_into().expect("a SHA256 digest is always 32 bytes, well over the 8 needed here")));
    let chunk_folder_url = webdav::build_url(chunking_base_url, &folder_name);

    let (username, password) = target.credentials();
    let local_path_owned = local_path.to_path_buf();
    let remote_url_owned = remote_url.to_string();
    let events_tx_owned = events_tx.clone();
    let connection_name = target.connection_name.clone();
    let child_relative_owned = child_relative.to_string();
    let etag = run_blocking(move || {
        let mut last_reported: Option<(u32, std::time::Instant)> = None;
        webdav::upload_chunked(&local_path_owned, &chunk_folder_url, &remote_url_owned, &username, &password, move |so_far, total| {
            if total == 0 {
                return;
            }
            let percent = ((so_far as f64 / total as f64) * 100.0).min(100.0) as u32;
            let should_report = match last_reported {
                None => true,
                Some((last_percent, last_time)) => percent != last_percent && last_time.elapsed() >= TRANSFER_PROGRESS_MIN_INTERVAL,
            };
            if should_report {
                let _ = events_tx_owned.try_send(SyncEvent::TransferProgress {
                    connection: connection_name.clone(),
                    path: child_relative_owned.clone(),
                    bytes_so_far: so_far as i64,
                    bytes_total: total as i64,
                });
                last_reported = Some((percent, std::time::Instant::now()));
            }
        })
    }).await?;

    match etag {
        Some(etag) => Ok(etag),
        // Some servers don't return an ETag on the finalizing MOVE's own
        // response -- same fallback, same reasoning, as upload_plain's
        // own equivalent case just below.
        None => Ok(target.webdav_stat(remote_url).await?.etag.unwrap_or_default()),
    }
}

/// Uploads local_path's content to remote_url via a single, ordinary
/// PUT -- see upload_temp_first for the large-file chunked alternative
/// this dispatches to instead when applicable (not directly above this
/// function any more -- upload_via_chunking sits between them). Temp-
/// file-first on the remote side too -- a fresh temp object in the same
/// remote directory, only moved onto the real destination name via a
/// genuine WebDAV MOVE once the upload has fully completed. Confirmed
/// against real GVfs source (GVfsBackendDav's `do_move`, wired up in
/// `g_vfs_backend_dav_class_init` since before GVfs even added server-
/// side COPY support) that this uses the native WebDAV MOVE method, not
/// a copy-then-delete emulation -- so this is a single fast metadata
/// operation on the server, not a second slow byte-stream transfer.
/// Closes the same "cancelled/crashed transfer leaves a truncated file
/// at the real path" gap that download_with_mode closes locally, just
/// on the remote side.
///
/// Returns the new ETag on success -- pulled straight from the PUT's
/// own response headers when the server provides one (see webdav::
/// upload's own doc comment), falling back to a dedicated stat() call
/// only on the rare server that doesn't. This is what makes the
/// separate post-upload query_info_with_recovery this function's
/// callers used to need unnecessary -- the ETag comes back as part of
/// doing the upload itself, not a second round-trip after.
async fn upload_plain(target: &SyncTarget, local_path: &Path, remote_url: &str, events_tx: &async_channel::Sender<SyncEvent>, child_relative: &str) -> Result<String, SyncError> {
    let parent_url = webdav::parent_url(remote_url)
        .ok_or_else(|| SyncError::WebDav(webdav::WebDavError::UnexpectedStatus { path: remote_url.to_string(), status: 0 }, None))?;

    // Same bounded-retry reasoning download's own equivalent below
    // follows -- see that function's own doc comment for the full case.
    // No forced-remount recovery branch any more (see webdav.rs's own
    // top-level doc comment for why that whole class of recovery
    // doesn't apply here at all) -- just a plain bounded retry on the
    // narrow, genuinely transient cases: the temp object vanishing
    // between creating it and moving it (a real race, not a server
    // problem), and a bare transport-level failure (a dropped
    // connection, DNS hiccup), which is exactly what a client owning
    // its own retry policy directly -- rather than trusting an
    // intermediary daemon to get this right -- is for.
    for attempt in 0..=TEMP_FILE_RETRY_LIMIT {
        let temp_url = webdav::build_url(&parent_url, &temp_file_name());

        let (username, password) = target.credentials();
        let local_path_owned = local_path.to_path_buf();
        let temp_url_owned = temp_url.clone();
        let parent_url_owned = parent_url.clone();
        let events_tx_owned = events_tx.clone();
        let connection_name = target.connection_name.clone();
        let child_relative_owned = child_relative.to_string();
        let upload_result = run_blocking(move || {
            retry_transfer_on_permission_error(&parent_url_owned, &username, &password, || {
                // (last reported whole percent, when) -- reset fresh on
                // every attempt (including retry_transfer_on_permission_
                // error's own single retry, not just this loop's own
                // 404-triggered one), rather than carried over from a
                // previous attempt's own progress -- a retry's own first
                // report should read from wherever IT actually starts,
                // not resume percentages left over from an earlier,
                // separate attempt.
                let mut last_reported: Option<(u32, std::time::Instant)> = None;
                let events_tx_owned = events_tx_owned.clone();
                let connection_name = connection_name.clone();
                let child_relative_owned = child_relative_owned.clone();
                webdav::upload(&local_path_owned, &temp_url_owned, &username, &password, move |so_far, total| {
                    if total == 0 {
                        return;
                    }
                    let percent = ((so_far as f64 / total as f64) * 100.0).min(100.0) as u32;
                    let should_report = match last_reported {
                        None => true,
                        Some((last_percent, last_time)) => percent != last_percent && last_time.elapsed() >= TRANSFER_PROGRESS_MIN_INTERVAL,
                    };
                    if should_report {
                        let _ = events_tx_owned.try_send(SyncEvent::TransferProgress {
                            connection: connection_name.clone(),
                            path: child_relative_owned.clone(),
                            bytes_so_far: so_far as i64,
                            bytes_total: total as i64,
                        });
                        last_reported = Some((percent, std::time::Instant::now()));
                    }
                })
            })
        }).await;

        let put_etag = match upload_result {
            Ok(etag) => etag,
            Err(webdav::WebDavError::UnexpectedStatus { status: 404, .. }) if attempt < TEMP_FILE_RETRY_LIMIT => {
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        // Overwrite: T here, deliberately, unlike rename-pairing's own
        // webdav_move calls -- see move_item's own doc comment for why
        // these two genuinely need different values: this finalize step
        // is expected to replace an already-synced file's previous
        // content on every ordinary re-upload, not just the rare
        // brand-new-file case.
        //
        // mtime read fresh here, right before the call it's actually
        // for, rather than threaded in from further up -- real,
        // confirmed gap without this: upload's own X-OC-Mtime on the
        // PUT above does NOT survive this finalizing MOVE on its own
        // (confirmed directly, not assumed from the docs) -- the temp
        // object arrives with the right mtime, then loses it the
        // moment it's moved onto the real path. Sending it again here
        // is what actually makes it stick. A read failure here just
        // means None (no header sent, server falls back to its own
        // upload-time default) rather than failing the whole upload
        // over a detail that isn't essential to it.
        let mtime = std::fs::metadata(local_path).ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64);
        match webdav_move_serialized(target, &temp_url, remote_url, true, mtime).await {
            Ok(()) => {
                return match put_etag {
                    Some(etag) => Ok(etag),
                    // Some servers don't return an ETag on a PUT's own
                    // response -- confirmed as a real possibility, not
                    // just theoretical caution, hence this fallback
                    // rather than assuming every server behaves the
                    // same way push.rs's own PROPFIND-based checks
                    // already confirmed this app's real server does.
                    None => Ok(target.webdav_stat(remote_url).await?.etag.unwrap_or_default()),
                };
            }
            Err(webdav::WebDavError::UnexpectedStatus { status: 404, .. }) if attempt < TEMP_FILE_RETRY_LIMIT => {
                let _ = target.webdav_delete(&temp_url).await;
                continue;
            }
            Err(e) => {
                let _ = target.webdav_delete(&temp_url).await;
                return Err(e.into());
            }
        }
    }
    unreachable!("the loop's only continue branches are gated on attempt < TEMP_FILE_RETRY_LIMIT, so the final iteration always returns instead")
}

/// "Keep Remote" -- the remote copy becomes canonical: download it,
/// overwriting whatever's local, and clear the conflict. Mirror of
/// resolve_keep_local.
pub async fn resolve_keep_remote(
    journal_conn: &Connection,
    target: &SyncTarget,
    relative_path: &str,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    let local_path = target.local_root.join(relative_path);
    let remote_url = target.remote_url_for(relative_path);

    let preserve_mode = local_mode(&local_path);
    let new_etag = download_with_mode(target, &remote_url, &local_path, preserve_mode, events_tx, relative_path, None).await?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_remote {relative_path}: downloaded, response etag={new_etag:?}", now_hms());
    }

    let meta = std::fs::metadata(&local_path)?;
    let mtime = meta.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    // Still needs a stat() call for the fileid specifically -- oc:fileid
    // is a WebDAV property, only ever returned by PROPFIND, never by a
    // plain GET's own response headers the way ETag is. But the ETag
    // itself now comes straight from the download's own response (see
    // webdav::download's own doc comment) rather than this follow-up
    // stat -- closes a real, if narrow, race: this stat call happening
    // *after* the download means whatever it reports could describe a
    // version newer than what was actually just downloaded, if the
    // remote file changed again in the gap between the two. The
    // downloaded response's own ETag has no such gap -- it's exactly
    // the version whose bytes are now sitting on disk.
    let info = target.webdav_stat(&remote_url).await.ok();
    let etag = new_etag.or_else(|| info.as_ref().and_then(|i| i.etag.clone())).unwrap_or_default();
    let fileid = info.and_then(|i| i.fileid);
    journal::mark_synced(journal_conn, &target.connection_id, relative_path, false, mtime, meta.len() as i64, &etag, local_inode(&local_path), fileid.as_deref(), now_unix())?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_remote {relative_path}: journal updated (etag={etag} fileid={fileid:?}), resolution complete", now_hms());
    }
    Ok(())
}

/// "Keep Both (rename local)" -- the local copy moves aside to a
/// "(conflicted copy TIMESTAMP)" file, and the remote version is
/// downloaded to the canonical path. This is exactly what
/// ConflictBothChanged used to do automatically, pre-redesign -- now
/// it's one of four explicit choices instead of the only outcome.
pub async fn resolve_keep_both_rename_local(
    journal_conn: &Connection,
    target: &SyncTarget,
    relative_path: &str,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    let local_path = target.local_root.join(relative_path);
    let remote_url = target.remote_url_for(relative_path);

    // Captured BEFORE the rename below, not after -- once local_path is
    // renamed aside, nothing exists at that exact path anymore until the
    // download recreates it, so "what mode was already there" has to be
    // read from the file that's about to move, not from local_path
    // itself post-rename. The renamed-aside copy (`aside`) keeps its own
    // mode for free -- std::fs::rename never touches permission bits --
    // so no extra handling is needed for that side.
    let preserve_mode = local_mode(&local_path);

    let aside = conflicted_copy_path(&local_path);
    std::fs::rename(&local_path, &aside)?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_both_rename_local {relative_path}: local renamed aside to {}", now_hms(), aside.display());
    }

    let now = now_unix();

    let new_etag = download_with_mode(target, &remote_url, &local_path, preserve_mode, events_tx, relative_path, None).await?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_both_rename_local {relative_path}: downloaded to canonical path, response etag={new_etag:?}", now_hms());
    }

    let meta = std::fs::metadata(&local_path)?;
    let mtime = meta.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    // Same fix, same reasoning, as resolve_keep_remote's own equivalent
    // case -- see webdav::download's own doc comment. The stat call
    // still runs for the fileid (PROPFIND-only, not in a plain GET's
    // headers), but the ETag itself now prefers the download's own
    // response over this follow-up call, closing the same narrow
    // stat-happens-after-download race.
    let info = target.webdav_stat(&remote_url).await.ok();
    let etag = new_etag.or_else(|| info.as_ref().and_then(|i| i.etag.clone())).unwrap_or_default();
    let fileid = info.and_then(|i| i.fileid);
    journal::mark_synced(journal_conn, &target.connection_id, relative_path, false, mtime, meta.len() as i64, &etag, local_inode(&local_path), fileid.as_deref(), now)?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_both_rename_local {relative_path}: journal updated (etag={etag} fileid={fileid:?}), resolution complete", now_hms());
    }
    Ok(())
}

/// "Keep Both (rename remote)" -- mirror of resolve_keep_both_rename_local.
/// This app has exactly one mechanism for "keep both": a renamed artifact
/// that lives in the LOCAL tree (see conflicted_copy_path -- local-only,
/// there's no equivalent rename operation on the remote/WebDAV side
/// anywhere in this codebase).
/// So "rename remote" is interpreted as: the remote-originated content
/// becomes the local renamed copy, and the local edit becomes canonical
/// everywhere (uploaded, overwriting remote) -- rather than literally
/// renaming anything on the server.
///
/// Confirmed correct against the actual UI, not just left as an assumed
/// reading: this app's own design puts each resolution button directly
/// under the column it acts on (see the dialog's own comment on why),
/// and "Rename Remote" sits under the Remote column -- so a user reading
/// it means "take the remote one and set it aside," which is exactly
/// what this function does. Symmetric with resolve_keep_both_rename_local
/// doing the mirror-image thing under the Local column.
pub async fn resolve_keep_both_rename_remote(
    journal_conn: &Connection,
    target: &SyncTarget,
    relative_path: &str,
    events_tx: &async_channel::Sender<SyncEvent>,
) -> Result<(), SyncError> {
    let local_path = target.local_root.join(relative_path);
    let remote_url = target.remote_url_for(relative_path);

    // Pull remote's content down into a new renamed local artifact --
    // the canonical local path isn't touched by this step, so the
    // caller's own local edit is still sitting there untouched. No
    // preserve_mode -- this is a brand-new local artifact with no prior
    // mode of its own to preserve, the same reasoning download_with_mode's
    // own doc comment already gives for a genuinely new file.
    let aside = conflicted_copy_path(&local_path);
    download_with_mode(target, &remote_url, &aside, None, events_tx, relative_path, None).await?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_both_rename_remote {relative_path}: remote downloaded aside to {}", now_hms(), aside.display());
    }

    let now = now_unix();

    // Now push the local edit up as canonical, same as resolve_keep_local.
    let etag = upload_temp_first(target, &local_path, &remote_url, events_tx, relative_path).await?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_both_rename_remote {relative_path}: local uploaded as canonical, new etag={etag}", now_hms());
    }

    let meta = std::fs::metadata(&local_path)?;
    let mtime = meta.modified()?.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    journal::mark_synced(journal_conn, &target.connection_id, relative_path, false, mtime, meta.len() as i64, &etag, local_inode(&local_path), None, now)?;
    if crate::is_debug_enabled() {
        eprintln!("[{}] [debug] resolve_keep_both_rename_remote {relative_path}: journal updated, resolution complete", now_hms());
    }
    Ok(())
}

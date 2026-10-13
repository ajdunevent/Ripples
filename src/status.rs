// src/status.rs
//
// Structured status/log event bus. Previously sync.rs only `eprintln!`d
// on anything interesting, which meant the UI had no way to show
// anything beyond "syncing / not syncing" -- this is the piece the TODO
// flagged as blocking the richer Status tab (worst-first state plus a
// capped scrolling log). sync.rs emits SyncEvent values on an
// async_channel; main.rs owns the receiving end, folds them into a
// SyncState and a StatusLog, and reflects both in the Status tab.
//
// Deliberately NOT a general logging framework -- just enough structure
// to drive the two things the Status tab actually needs to show.

use std::collections::VecDeque;

/// Worst-first ordering: Disconnected > Syncing > Scanning > Idle.
/// Derives Ord so "the worse of these two states" is just `.max()` --
/// that's the whole mechanism behind combining multiple connections'
/// states into one overall status for the round. No Conflict variant
/// here on purpose -- a conflict doesn't mean the app has stopped doing
/// useful work, and letting it outrank Syncing/Scanning/Idle used to
/// hide whether the app was actually connecting/transferring/idle for
/// as long as any conflict sat unresolved. Conflicts are surfaced
/// instead via the always-visible Conflicts list and the log (see
/// SyncEvent::Conflict's log_line and implied_state below).
///
/// Idle, not Monitoring -- confirmed this was a real, not just cosmetic,
/// mismatch: what this state actually represents is a FileMonitor
/// passively listening for a filesystem event (see watch.rs), or
/// waiting on the remote poll timer, not any kind of active watching
/// happening on its own. Scanning is new: previously trigger_sync_round
/// set sync_state straight to Syncing for a round's entire duration,
/// including the walk-and-compare phase before anything was actually
/// found to transfer -- so "Syncing…" was ambiguous between "actively
/// uploading/downloading right now" and "just checking, might find
/// nothing at all," which is a real source of confusion (a fast round
/// that finds nothing to do still flashed "Syncing…" with no way to
/// tell that apart from a real transfer in progress). Scanning is the
/// new baseline a round starts at; a real transfer event promotes it to
/// Syncing the same worst-first way anything else does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyncState {
    Idle,
    Scanning,
    Syncing,
    Disconnected,
}

impl SyncState {
    pub fn label(self) -> &'static str {
        match self {
            SyncState::Idle => "Idle",
            SyncState::Scanning => "Scanning\u{2026}",
            SyncState::Syncing => "Syncing\u{2026}",
            SyncState::Disconnected => "Disconnected",
        }
    }
}

/// Upload vs download, for the Active Transfers row's own child rows --
/// deliberately its own, small, dedicated type rather than reusing
/// SyncState, since that enum's whole design is worst-first, app-wide
/// severity (Idle < Scanning < Syncing < Disconnected), not "which
/// direction is this one specific item's own transfer going."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    Uploading,
    Downloading,
}

impl TransferDirection {
    pub fn label(self) -> &'static str {
        match self {
            TransferDirection::Uploading => "Uploading",
            TransferDirection::Downloading => "Downloading",
        }
    }
}

/// One noteworthy thing that happened during a sync round. Deliberately
/// connection-and-item-level only -- round start/finish is a main.rs-level
/// concept (it's the one calling run_sync_round in a loop), so main.rs
/// sets SyncState::Syncing itself around that call rather than this enum
/// carrying round-lifecycle variants.
#[derive(Debug, Clone)]
pub enum SyncEvent {
    MountFailed { connection: String, error: String },
    ConnectionFailed { connection: String, error: String },
    /// Fired right before the actual transfer begins, and again (as
    /// TransferEnded) right after it finishes either way -- purely live,
    /// ephemeral UI state for the Active Transfers row, not a permanent
    /// record the way every other SyncEvent is. Deliberately excluded
    /// from status_log/log_line entirely (see apply_status_event) --
    /// ItemUploaded/ItemDownloaded already cover the "this transfer
    /// completed" log entry; a second, separate "started" line for the
    /// same transfer would just be noise, not new information.
    TransferStarted { connection: String, path: String, direction: TransferDirection },
    /// Same "purely live, ephemeral UI state" reasoning as TransferStarted
    /// -- fired at most once per whole-percentage-point change (see
    /// transfer_with_stall_detection's own doc comment in sync.rs for the
    /// actual throttling), never logged, and never fired at all if
    /// bytes_total is ever 0 or unknown -- the Active Transfers row shows
    /// no progress suffix at all in that case, rather than a meaningless
    /// "0/0".
    TransferProgress { connection: String, path: String, bytes_so_far: i64, bytes_total: i64 },
    /// See TransferStarted's own doc comment. Fired on completion
    /// *or* failure -- either way, this specific transfer is no longer
    /// active, which is the only thing the Active Transfers row itself
    /// needs to know; ItemFailed (a real, logged event) still covers
    /// reporting *why* separately.
    TransferEnded { connection: String, path: String },
    ItemUploaded { connection: String, path: String },
    ItemDownloaded { connection: String, path: String },
    ItemDeleted { connection: String, path: String, local: bool },
    Conflict { connection: String, path: String },
    /// Fired instead of a real conflict when both sides looked changed
    /// but turned out byte-identical -- not something the user needs to
    /// act on, but worth a line so "why didn't X show up as a conflict"
    /// has an answer.
    ConflictResolvedIdentical { connection: String, path: String },
    /// Fired instead of a real Upload/Download when the transfer that
    /// decide_action called for turned out unnecessary -- content on
    /// both sides was already byte-identical, an ETag/mtime change alone
    /// having implied otherwise (a deterministic rebuild regenerating
    /// the exact same bytes is a real, confirmed case). Distinct from
    /// ConflictResolvedIdentical just above: that one means "this looked
    /// like a real conflict, decide_action already having produced
    /// ConflictBothChanged, and turned out not to be one at all";  this
    /// one is the equivalent check applied to an ordinary, one-sided
    /// Upload/Download that was never a conflict candidate in the first
    /// place, so the two shouldn't share a single label.
    TransferSkippedIdentical { connection: String, path: String },
    /// An explicit user choice (Keep Local/Remote/Rename) from the
    /// conflict resolution dialog actually landed. `resolution` is
    /// ConflictAction::label()'s output -- already a fixed, human-
    /// readable string, so log_line just drops it in directly rather
    /// than re-deriving it from an enum this module doesn't otherwise
    /// need to know about.
    ConflictResolved { connection: String, path: String, resolution: &'static str },
    /// The chosen resolution failed to actually apply (mount/network
    /// error, etc) -- the dialog already cleared this item from its own
    /// list optimistically (see apply_conflict_resolution's doc comment
    /// in main.rs), so this log line is the only remaining record that
    /// it didn't really succeed.
    ConflictResolutionFailed { connection: String, path: String, error: String },
    /// Matched one of the three exclusion mechanisms (see
    /// exclude::ExclusionReason) -- the direct answer to "why isn't this
    /// file syncing" for anyone who forgot they (or a past-them, or the
    /// app's own default pattern list) excluded it. `reason` says which
    /// of the three, so the log line can point at exactly which place to
    /// go check instead of a generic "it's excluded, somewhere."
    Excluded { connection: String, path: String, reason: crate::exclude::ExclusionReason },
    /// Present on the side that would transfer, but past the configured
    /// size limit -- see config::DEFAULT_MAX_FILE_SIZE_BYTES.
    SkippedTooLarge { connection: String, path: String, size_bytes: u64, limit_bytes: u64 },
    /// A local rename/move was detected (same inode, previously synced
    /// at the old path, confirmed genuinely gone from there) and handled
    /// via a single remote MOVE instead of a delete+reupload pair. See
    /// journal::find_by_inode's own doc comment for the actual
    /// detection mechanism.
    ItemRenamed { connection: String, old_path: String, new_path: String },
    /// One item failed (disk full mid-write, a transient network
    /// hiccup, etc) without taking the rest of the round down with it --
    /// see sync_directory's per-item error handling in sync.rs.
    /// Previously any such error propagated all the way up and aborted
    /// the entire connection's round, repeating on every future round
    /// against the same one broken file for as long as it stayed
    /// broken, with everything else in that connection silently starved
    /// in the meantime. Now it's isolated to just this one item; the
    /// rest of the tree keeps syncing normally. Deliberately excludes
    /// what looks like a permission error specifically -- see
    /// PermissionError below, which gets its own, quieter handling
    /// rather than repeating this same log line every round forever.
    ItemFailed { connection: String, path: String, error: String },
    /// The throttled counterpart to ItemFailed above, specifically for
    /// WebDAV/HTTP/transport failures (see sync::tag_webdav_action's own
    /// doc comment on why only that error kind qualifies) -- fired only
    /// once every WEBDAV_FAILURE_LOG_INTERVAL consecutive failures at
    /// the same path, rather than every single one. By explicit
    /// request: a persistently unreachable server previously meant the
    /// same failure repeating in the log on every round indefinitely,
    /// full error text and all. tries is always a multiple of
    /// sync::WEBDAV_FAILURE_LOG_INTERVAL when this fires (never the raw,
    /// unthrottled count) -- action is SyncAction::label()'s own output
    /// ("Upload"/"Download"/"Delete"), a fixed, human-readable string
    /// the same way ConflictResolved's own `resolution` field already is
    /// above, not something this module re-derives from an enum it
    /// doesn't otherwise need to know about.
    WebDavFailureThrottled { connection: String, action: &'static str, tries: u32, path: String },
    /// Distinct from ItemFailed: this specific failure looks like a
    /// permission problem (see sync::is_permission_error), on either
    /// side. The journal itself is flagged (journal::mark_permission_error)
    /// so the normal walk skips this item entirely on future rounds,
    /// rather than re-attempting -- and re-logging -- the same doomed
    /// operation every time, until something actually gives the app a
    /// reason to believe it might have changed (session start, manual
    /// unpause, or a user-initiated retry from the Sync Issues carousel).
    PermissionError { connection: String, path: String },
    /// The transfer itself succeeded, but restoring the file's prior
    /// local permission bits afterward failed -- see
    /// download_with_mode's own doc comment in sync.rs for why this is
    /// attempted at all. Deliberately non-fatal to the sync action (the
    /// file's actual content is correct either way), but still worth
    /// surfacing: anything that touches the filesystem and doesn't do
    /// exactly what was intended should be visible, not just silently
    /// swallowed.
    PermissionRestoreFailed { connection: String, path: String, error: String },
    /// Whether notify_push is currently usable for this connection --
    /// logged once per session (the initial discovery/exchange) and
    /// again any time it actually changes (a WebSocket disconnect, a
    /// successful reconnect) -- deliberately doubles as the "tier
    /// changed" signal for this dimension, since the effective tier is
    /// just a function of this and EtagCapability below, so a separate,
    /// generic tier-change event would only repeat the same information.
    /// Not logged on every individual push message received -- see this
    /// event's own design discussion for why that would risk the same
    /// unbounded-log-growth shape the log-rendering fix earlier this
    /// project specifically addressed; a push-triggered round is already
    /// distinguishable the same way a local-watch-triggered one is.
    NotifyPushCapability { connection: String, available: bool },
    /// Whether the ETag tier is usable for this connection -- gated on
    /// confirmed Nextcloud/ownCloud identity specifically, not "any
    /// WebDAV server," since the property this tier relies on (a
    /// directory's own ETag reflecting changes anywhere in its whole
    /// subtree) is documented Nextcloud/ownCloud server behavior, not a
    /// generic WebDAV guarantee. Logged once per session and again if it
    /// ever changes.
    EtagCapability { connection: String, available: bool },
    /// Fired immediately before, and immediately after, every one of
    /// this app's own genuine remote-writing operations -- uploads,
    /// deletes, MOVEs (rename-pairing), MKCOL, and conflict-resolution
    /// writes, everywhere target.webdav_* or upload_temp_first/upload_
    /// via_chunking actually writes to the server. Purely internal
    /// bookkeeping for pending_remote_writes' own design (see that
    /// field's own doc comment on AppState) -- not logged, not shown in
    /// any UI, the same "purely live, ephemeral" treatment TransferStarted/
    /// TransferEnded already get above, for the same reason: this isn't
    /// new information a person would want to read, it's the mechanism
    /// deciding whether a later notify_push can be trusted as "probably
    /// just us."
    ///
    /// Routed through this same events_tx channel, not a separate one,
    /// specifically so sync.rs itself never needs to know anything
    /// about AppState, journal, or account credentials at all -- the
    /// actual pre-flight/post-flight ETag logic these two events drive
    /// lives entirely in main.rs, where all of that is already
    /// available; sync.rs's own job is just marking when a write starts
    /// and ends, the same boundary every other SyncEvent already
    /// respects.
    RemoteWriteStarted,
    /// See RemoteWriteStarted's own doc comment just above.
    RemoteWriteFinished,
    /// Watchdog report (see watchdog.rs); logged only.
    Diagnostic { message: String },
}

impl SyncEvent {
    /// The state this event implies on its own. Combined by the caller
    /// against whatever's already been seen this round via `.max()` --
    /// an event only ever pushes the round's state *worse*, never better;
    /// recovering back to Idle only happens at the end of a clean
    /// round (see main.rs's trigger_sync_round).
    pub fn implied_state(&self) -> SyncState {
        match self {
            SyncEvent::MountFailed { .. }
            | SyncEvent::ConnectionFailed { .. } => SyncState::Disconnected,
            SyncEvent::ItemUploaded { .. }
            | SyncEvent::ItemDownloaded { .. }
            | SyncEvent::ItemRenamed { .. }
            | SyncEvent::ItemDeleted { .. }
            // Starting, not just finishing, is real I/O happening right
            // now -- promotes sync_state the moment a transfer actually
            // begins, not only once it completes.
            | SyncEvent::TransferStarted { .. }
            // A resolution actually moving/keeping a file is real I/O,
            // same category as the plain transfer events above -- unlike
            // the conflict's initial detection, this is work happening,
            // not just something noted for later.
            | SyncEvent::ConflictResolved { .. } => SyncState::Syncing,
            // Purely informational -- none of these mean the app has
            // stopped doing useful work, so none of them move the
            // overall status needle on their own. A conflict is real and
            // needs the user's attention eventually, but it's tracked in
            // the Conflicts list and logged below, not by stalling the
            // Status row on "Conflict" for as long as it stays
            // unresolved. A failed resolution attempt is the same story
            // -- the item's still sitting there needing attention, same
            // as before the attempt; it doesn't newly mean the
            // connection itself is down.
            SyncEvent::Conflict { .. }
            | SyncEvent::ConflictResolvedIdentical { .. }
            | SyncEvent::TransferSkippedIdentical { .. }
            | SyncEvent::ConflictResolutionFailed { .. }
            | SyncEvent::Excluded { .. }
            | SyncEvent::TransferEnded { .. }
            | SyncEvent::TransferProgress { .. }
            // Deliberately NOT Disconnected, even though these are real
            // errors -- that's the whole point of isolating them: one
            // bad file doesn't mean the connection itself is down, it's
            // still doing useful work on everything else in the tree.
            | SyncEvent::ItemFailed { .. }
            | SyncEvent::WebDavFailureThrottled { .. }
            | SyncEvent::PermissionError { .. }
            | SyncEvent::PermissionRestoreFailed { .. }
            | SyncEvent::NotifyPushCapability { .. }
            | SyncEvent::EtagCapability { .. }
            | SyncEvent::SkippedTooLarge { .. }
            // Never actually promotes sync_state on its own -- apply_
            // status_event skips both of these before ever consulting
            // implied_state at all (same "purely live, ephemeral"
            // early-return treatment TransferStarted/TransferEnded
            // already get above). Still needs a real arm here since the
            // match itself must be exhaustive.
            | SyncEvent::RemoteWriteStarted
            | SyncEvent::RemoteWriteFinished
            | SyncEvent::Diagnostic { .. } => SyncState::Idle,
        }
    }

    /// One human-readable line for the scrolling log. Not timestamped
    /// here -- sync.rs has no reason to depend on a clock beyond what it
    /// already needs, so the receiver (main.rs) stamps it on arrival.
    pub fn log_line(&self) -> String {
        match self {
            SyncEvent::MountFailed { connection, error } => format!("{connection}: Mount failed - {error}"),
            // Never actually reached -- apply_status_event skips both
            // of these before ever calling log_line at all (see
            // TransferStarted/TransferEnded's own doc comments for why).
            // Still needs a real arm here since the match itself must be
            // exhaustive.
            SyncEvent::TransferStarted { connection, path, direction } => format!("{connection}: {} @ {path}", direction.label()),
            SyncEvent::TransferProgress { connection, path, bytes_so_far, bytes_total } => format!("{connection}: Transfer progress @ {path} ({bytes_so_far}/{bytes_total})"),
            SyncEvent::TransferEnded { connection, path } => format!("{connection}: Transfer ended @ {path}"),
            SyncEvent::ConnectionFailed { connection, error } => format!("{connection}: Connection failed - {error}"),
            SyncEvent::ItemUploaded { connection, path } => format!("{connection}: Upload succeeded @ {path}"),
            SyncEvent::ItemRenamed { connection, old_path, new_path } => format!("{connection}: Rename succeeded @ {old_path} -> {new_path}"),
            SyncEvent::ItemDownloaded { connection, path } => format!("{connection}: Download succeeded @ {path}"),
            SyncEvent::ItemDeleted { connection, path, local } => if *local {
                format!("{connection}: Local deletion succeeded @ {path}")
            } else {
                format!("{connection}: Remote deletion succeeded @ {path}")
            },
            SyncEvent::Conflict { connection, path } => format!("{connection}: Conflict identified @ {path}"),
            SyncEvent::ConflictResolvedIdentical { connection, path } =>
                format!("{connection}: Match confirmed, no conflict @ {path}"),
            SyncEvent::TransferSkippedIdentical { connection, path } =>
                format!("{connection}: Contents matched, transfer skipped @ {path}"),
            SyncEvent::ConflictResolved { connection, path, resolution } =>
                format!("{connection}: Resolved conflict @ {path} - {resolution}"),
            SyncEvent::ConflictResolutionFailed { connection, path, error } =>
                format!("{connection}: Resolution failed @ {path} - {error}"),
            SyncEvent::Excluded { connection, path, reason } => match reason {
                // Only reached for an item actually *listed* within a
                // .no-sync file -- the marker file itself is suppressed
                // entirely at the emission site (see sync.rs), since
                // that's this app's own bookkeeping announcing itself,
                // not a user decision worth logging. An item a user
                // explicitly listed is a real, deliberate exclusion,
                // same as the other two reasons below.
                crate::exclude::ExclusionReason::NoSyncFile => format!("{connection}: Skipped, .no-sync item matched @ {path}"),
                crate::exclude::ExclusionReason::GlobalPattern => format!("{connection}: Skipped, ignored filename pattern @ {path}"),
                crate::exclude::ExclusionReason::ConnectionPath => format!("{connection}: Skipped, excluded path @ {path}"),
                // Never actually reached -- suppressed entirely at the
                // emission site, same reasoning as NoSyncFile's own
                // marker-file case above (see sync.rs). Still needs a
                // real arm here since the match itself must be
                // exhaustive.
                crate::exclude::ExclusionReason::TempFile => format!("{connection}: Skipped, in-progress transfer @ {path}"),
            },
            SyncEvent::SkippedTooLarge { connection, path, size_bytes, limit_bytes } => format!(
                "{connection}: Skipped, {} exceeds {} limit @ {path}",
                format_bytes(*size_bytes), format_bytes(*limit_bytes)
            ),
            SyncEvent::ItemFailed { connection, path, error } =>
                format!("{connection}: Failure @ {path} - {error}"),
            SyncEvent::WebDavFailureThrottled { connection, action, tries, path } =>
                format!("{connection}: {action} failed ({tries} tries) @ {path}"),
            SyncEvent::PermissionError { connection, path } =>
                format!("{connection}: Failure, permission denied @ {path}"),
            SyncEvent::PermissionRestoreFailed { connection, path, error } =>
                format!("{connection}: Synced, permission restoration failed @ {path} - {error}"),
            SyncEvent::NotifyPushCapability { connection, available } => if *available {
                format!("{connection}: Remote notify_push capability confirmed")
            } else {
                format!("{connection}: Remote notify_push capability unavailable")
            },
            SyncEvent::EtagCapability { connection, available } => if *available {
                format!("{connection}: Remote ETag precheck capability confirmed")
            } else {
                format!("{connection}: Remote ETag precheck capability unavailable")
            },
            // Never actually reached -- apply_status_event skips both
            // of these before ever calling log_line at all (see
            // RemoteWriteStarted's own doc comment for why). Still
            // needs a real arm here since the match itself must be
            // exhaustive.
            SyncEvent::RemoteWriteStarted => String::new(),
            SyncEvent::RemoteWriteFinished => String::new(),
            SyncEvent::Diagnostic { message } => message.clone(),
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const MB: u64 = 1024 * 1024;
    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else {
        format!("{bytes} bytes")
    }
}

/// Ring buffer of already-formatted, timestamped log lines, feeding the
/// in-app Session tab's own live display. Deliberately small now, not a
/// full-session history -- disk_log.rs's own on-disk, per-session file
/// is the actual, permanent, canonical record (see
/// flush_disk_log_now/export_log_button's own handler in main.rs, which
/// exports that file directly rather than re-serializing from this
/// structure). This only ever needs to hold "recent enough to be useful
/// to glance at live," not "everything that's happened this session" --
/// 999 lines is generous headroom over what's actually visible on
/// screen at once even on a large display, with plenty of room to
/// scroll back a little further. Previously much larger (was raised to
/// 20000 at one point, during a search for a UI-responsiveness cause
/// that turned out to be unrelated to this cap entirely -- see
/// run_sync_round's own notify_push/tier-multiplier fixes for the real
/// cause), back when this was still the only place any session history
/// existed at all; now that disk_log.rs exists as the genuine, complete
/// record, keeping a second, large, separately-capped copy in memory
/// alongside it was pure redundancy, not a real safeguard.
pub struct StatusLog {
    lines: VecDeque<String>,
}

pub const STATUS_LOG_CAPACITY: usize = 9_999;

impl StatusLog {
    pub fn new() -> Self {
        Self { lines: VecDeque::with_capacity(STATUS_LOG_CAPACITY) }
    }

    pub fn push(&mut self, line: String) {
        if self.lines.len() >= STATUS_LOG_CAPACITY {
            self.lines.pop_front();
        }
        self.lines.push_back(line);
    }

    /// Clears all lines -- the whole log, not a partial trim. Used by
    /// the Status tab's own "clear log" button; a fresh start for the
    /// in-app view, distinct from exporting (which reads the log
    /// without touching it at all).
    pub fn clear(&mut self) {
        self.lines.clear();
    }

    /// Most-recent-first, for display.
    pub fn recent(&self) -> impl Iterator<Item = &String> {
        self.lines.iter().rev()
    }
}

// src/main.rs

mod config;
mod disk_log;
mod exclude;
mod folder_picker;
mod goa;
mod journal;
mod nextcloud_login;
mod push;
mod status;
mod sync;
mod tray;
mod watch;
mod webdav;

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
// See sync.rs's own comment on the identical import -- needed
// specifically for .for_each_concurrent() in the background
// verification pass below.
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::UNIX_EPOCH;

use gtk4::prelude::*;
use libadwaita::prelude::*;
use glib::clone;

use goa::{GoaAccountInfo, GoaWatcher};

/// Unifies a GOA account and a direct (non-GOA) Nextcloud account behind
/// one type -- the explicit-enum design settled on over the alternative
/// (constructing a synthetic GoaAccountInfo for a direct account so it
/// could slot into the same, unchanged account list everywhere): that
/// "faking it" approach would have needed its own, separate lifecycle
/// management wherever the app currently assumes every account came
/// from GOA and is watched via real D-Bus events (most of goa.rs's own
/// event handling), since a synthetic account would never actually fire
/// any of those. An explicit enum is more honest about the real
/// distinction, at the cost of needing to be threaded through the
/// account-management surface directly -- accepted as the right
/// trade-off given how much of that surface (SyncConnection's own
/// account_id, SyncTarget, credentials()) was already source-agnostic
/// before this even existed.
#[derive(Debug, Clone)]
pub enum AccountSource {
    Goa(GoaAccountInfo),
    Direct(nextcloud_login::DirectNextcloudAccount),
}

impl AccountSource {
    pub fn account_id(&self) -> &str {
        match self {
            AccountSource::Goa(a) => &a.account_id,
            AccountSource::Direct(a) => &a.account_id,
        }
    }

    /// A short, human-readable label -- GoaAccountInfo's own
    /// presentation_identity for a GOA account, DirectNextcloudAccount's
    /// own display_name() (same shape, "login_name@host") for a direct
    /// one. Deliberately not adding the "GOA: "/"Nextcloud: " source
    /// prefix here -- that's specifically a Connections-list-only
    /// convention (see build_connections_tab's own use of this), not
    /// something every caller of this method wants baked in (the
    /// account-selector row in Settings, for instance, doesn't want it
    /// repeated on every row when the row's own icon/section already
    /// makes the source obvious).
    pub fn presentation_identity(&self) -> String {
        match self {
            AccountSource::Goa(a) => a.presentation_identity.clone(),
            AccountSource::Direct(a) => a.display_name(),
        }
    }

    /// Whether this account needs the user's attention before it can
    /// sync -- a GOA account's own attention_needed (GOA itself sets
    /// this when re-authentication is required, surfaced via GNOME
    /// Online Accounts). A direct account has no GOA-managed analogue to
    /// that specific state; always false here -- not because a direct
    /// account's own app password could never be revoked server-side
    /// (it can), but because detecting that would require an active
    /// failed request first, not something inferable up front the way
    /// GOA's own flag already is. A future improvement, not a gap this
    /// type needs to paper over with a guess.
    pub fn attention_needed(&self) -> bool {
        match self {
            AccountSource::Goa(a) => a.attention_needed,
            AccountSource::Direct(_) => false,
        }
    }

    /// Whether this account is currently usable for syncing at all --
    /// a GOA account needs files_uri populated (GOA hasn't finished
    /// resolving Files support yet otherwise); a direct account is
    /// always ready the moment it exists, since Login Flow v2 doesn't
    /// complete at all without already having everything (server,
    /// login_name) needed to sync.
    pub fn is_ready(&self) -> bool {
        match self {
            AccountSource::Goa(a) => a.files_uri.is_some(),
            AccountSource::Direct(_) => true,
        }
    }

    /// The bare server root, no path at all -- GoaAccountInfo's own
    /// server_url for a GOA account (already optional there, since GOA
    /// itself hasn't always resolved it), DirectNextcloudAccount's own
    /// server for a direct one (always present -- Login Flow v2 can't
    /// complete without it). Distinct from the WebDAV base URL
    /// (credentials()'s own third return value, which has "/remote.php/
    /// webdav/" or similar appended) -- this is specifically for the few
    /// places that need the bare root itself, like SyncTarget::
    /// chunking_base_url's own construction.
    pub fn server_url(&self) -> Option<String> {
        match self {
            AccountSource::Goa(a) => a.server_url.clone(),
            AccountSource::Direct(a) => Some(a.server.clone()),
        }
    }

    /// The (base_url, username, password) triple every WebDAV operation
    /// ultimately needs, regardless of which kind of account this is --
    /// the one method meant to let most callers (build_sync_target_for_
    /// connection, remote_browse_location) stay entirely unaware of the
    /// distinction, rather than matching on this enum themselves at
    /// every credential-fetching call site. None on any failure (GOA
    /// account not ready, credential fetch failed either way) -- every
    /// existing caller already treats a missing remote root as "nothing
    /// to browse/sync yet," the same tolerance this preserves. Password
    /// returned already wrapped in Zeroizing, not a plain String this
    /// caller would need to remember to wrap itself -- matches
    /// SyncTarget::password's own field type directly, and keeps the
    /// value protected for its whole lifetime rather than only from
    /// whatever point a caller happens to wrap it at.
    pub async fn credentials(&self, watcher: &GoaWatcher) -> Option<(String, String, zeroize::Zeroizing<String>)> {
        match self {
            AccountSource::Goa(a) => {
                let files_uri = a.files_uri.as_deref()?;
                let base_url = webdav::davs_to_https(files_uri)?;
                let username = webdav::extract_username(files_uri)?;
                let password = watcher.get_password_for_http_auth(&a.account_id).await.ok()?;
                Some((base_url, username, password))
            }
            AccountSource::Direct(a) => {
                let password = nextcloud_login::get_app_password(&a.account_id).await.ok()??;
                Some((a.webdav_base_url(), a.login_name.clone(), password))
            }
        }
    }
}

const APP_ID: &str = "net.dunevent.Ripples";

/// Set exactly once, in main(), before anything else runs -- see that
/// call site's own doc comment for why this is a global static rather
/// than a threaded-through parameter. OnceLock rather than a plain
/// static bool: this is written from a single, one-time call in main(),
/// but read from many places across both this file and sync.rs
/// (including from spawned OS threads via run_blocking), so it needs to
/// actually be Sync -- a plain `static mut` wouldn't be, and safe
/// interior mutability is exactly what OnceLock already provides for a
/// "write once, read many times, from anywhere" value like this one.
static DEBUG_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Whether `--debug` was passed on the command line -- see DEBUG_ENABLED's
/// own doc comment. Deliberately a CLI flag, not a config file or
/// persistent setting: the whole point is that it has to be re-typed
/// every single launch to turn on, so it's a genuine hassle to
/// accidentally leave enabled rather than something that could silently
/// keep logging verbosely for weeks after whatever it was needed for is
/// long resolved. get() rather than get_or_init(): by the time anything
/// could call this, main() has already set it unconditionally as the
/// very first thing it does -- if that hasn't happened yet, something
/// has gone wrong enough that a panic here is the right, loud signal,
/// not silently defaulting to false and hiding it.
pub fn is_debug_enabled() -> bool {
    *DEBUG_ENABLED.get().expect("main() sets this unconditionally before anything else runs")
}

struct WindowWidgets {
    window: libadwaita::ApplicationWindow,
    // Rebuilt fresh on every refresh_account_ui call, same "rebuild
    // rather than incrementally diff" approach connections_list below
    // already uses -- holds either a single row for the active account
    // (with its own edit pencil) or the "Add Account +" row, never both.
    account_section: gtk4::ListBox,
    // Same staleness problem apply_server_theme itself had -- its own
    // visibility was originally checked once, at Settings-page build
    // time, using whatever was known about account capability at that
    // moment. Stored here so refresh_account_ui (the one place that
    // reliably knows when that's actually settled) can update it
    // directly, the same way it now keeps the theme itself in sync.
    theming_row: libadwaita::SwitchRow,
    // The Sync Connections list -- no longer includes the account
    // itself as its own first row (see account_section above, now a
    // separate section entirely, by request).
    connections_list: gtk4::ListBox,
    status_row: libadwaita::ActionRow,
    status_log_label: gtk4::TextView,
    active_transfers_row: libadwaita::ExpanderRow,
    conflicts_row: libadwaita::ActionRow,
    conflicts_icon: gtk4::Image,
    export_log_button: gtk4::Button,
    clear_log_button: gtk4::Button,
    sync_now_button: gtk4::Button,
    pause_button: gtk4::Button,
    // Needed by apply_server_theme to switch top_bar_style between
    // Raised (header bar gets its own distinct painted background,
    // needed when it's a different color from the window) and Flat
    // (the default -- no background of its own, just shows the window's
    // background through it) depending on whether server theming is
    // currently toggled on. Was previously set once, unconditionally,
    // at window construction time.
    toolbar_view: libadwaita::ToolbarView,
    // Shown/hidden by update_status_ui itself, alongside status_row's
    // own subtitle -- see paused_reason_restored_account's own doc
    // comment on AppState for what this actually communicates.
    pause_banner: libadwaita::Banner,
}

/// One entry in the Active Transfers row's own expanded child list.
struct ActiveTransfer {
    connection: String,
    path: String,
    direction: status::TransferDirection,
    // (bytes_so_far, bytes_total) -- None until a real
    // SyncEvent::TransferProgress actually arrives for this transfer, not
    // populated with a placeholder like (0, 0) in the meantime. The
    // Active Transfers row shows no progress suffix at all while this is
    // None, rather than a meaningless "0%" or "0/0 bytes".
    progress: Option<(i64, i64)>,
}

/// A genuine three-state machine, not a bool -- confirmed as a much
/// cleaner fit than an earlier, separate "was unavailable actually
/// announced" tracking set layered on top of a plain connected/
/// disconnected bool, which needed its own, awkward extra bookkeeping to
/// distinguish "never connected at all yet" from "was connected, then
/// genuinely, sustainedly went down." A proper third state doesn't need
/// any of that: Retrying IS the "known down, but not yet worth
/// bothering the user about" state on its own.
///
/// Retrying is entered the moment push::run_listener's own reconnect
/// loop reports a drop -- not immediately announced as Disconnected,
/// since that reconnect loop can drop and re-establish within moments
/// under perfectly ordinary conditions (see push::run_listener's own
/// 30-second retry sleep), and each one used to produce its own
/// "unavailable ... confirmed" pair in the log for a blip that was never
/// a real, sustained outage. Only escalates to Disconnected -- and only
/// then actually logged -- if NOTIFY_PUSH_UNAVAILABLE_DEBOUNCE_SECS
/// passes while still stuck in Retrying, checked once via a single,
/// delayed task rather than any kind of repeated polling (see the
/// Disconnected match arm in the push-event handler).
///
/// Reconnecting out of Retrying (never having reached Disconnected at
/// all) announces nothing -- there was nothing the user was ever told
/// went down, so there's nothing to announce recovering either.
/// Reconnecting out of Disconnected does announce "available again",
/// since that's the one case where the user actually saw a prior outage
/// message. Treated the same as Disconnected specifically for the
/// "should this connection announce on its very first successful
/// connect" question -- there's no real state before this at all (the
/// account simply has no entry in this map yet), but a first-ever
/// success should still be worth telling the user about, the same as
/// this app's own, original, pre-debounce behavior always did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotifyPushStatus {
    Connected,
    Retrying,
    Disconnected,
}

impl NotifyPushStatus {
    /// Whether this status should count as "connected" for anything
    /// that actually needs to know real, current connectivity right now
    /// -- e.g. poll_interval_multiplier's own tier decision. Retrying
    /// counts as not-connected here even though it isn't announced to
    /// the user yet: the connection genuinely, actually is down at this
    /// moment, and a functional decision like polling frequency should
    /// reflect that reality directly, regardless of whether the debounce
    /// window covering the user-facing announcement has elapsed yet.
    fn is_connected(self) -> bool {
        self == NotifyPushStatus::Connected
    }
}

struct AppState {
    config: RefCell<config::AppConfig>,
    journal: rusqlite::Connection,
    window_widgets: RefCell<Option<WindowWidgets>>,
    started: RefCell<bool>,
    // Created once here, registered to the display once the main window
    // actually exists (see build_or_present_window), then reloaded (not
    // re-created) every time apply_server_theme runs -- GtkCssProvider
    // supports reloading its own content live, which re-styles anything
    // already using it automatically, so there's no need to
    // remove/re-add a fresh provider on every theme change.
    theme_css_provider: gtk4::CssProvider,
    // Consumed (set false) the moment the session's first real sync
    // round actually completes -- see trigger_sync_round's own use of
    // this. Threaded into SyncTarget::cleanup_temp_files at construction
    // so the temp-file sweep only ever runs folded into that one, first
    // walk, not the dedicated, blocking pre-pass this replaced.
    first_round_this_session: Cell<bool>,
    // Mirrors what refresh_account_ui's own is_ready() filter last found
    // for the currently-active account specifically -- kept here so
    // update_status_ui can distinguish "actively idle, everything's
    // fine" from "the active account itself isn't reachable right now"
    // without needing its own watcher/all_accounts() call at every one
    // of its own 10+ call sites just to answer that one question.
    // Starts true (not false) deliberately -- matches update_status_ui's
    // own existing early checks (active_account_id.is_none(), etc.),
    // which already correctly cover the very first paint before
    // refresh_account_ui has ever run once; this only needs to become
    // meaningful once an account is actually configured, at which point
    // refresh_account_ui will have already updated it before anything
    // reads it.
    active_account_ready: Cell<bool>,
    // Bumped every time the global exclude-patterns list actually
    // changes (add, remove, reset to defaults -- see each call site's
    // own comment). Rc<Cell<>>, not a plain Cell, since SyncTarget holds
    // a clone of this exact same counter -- see its own doc comment for
    // why: a sync round already in progress needs to notice a mid-round
    // change, not just the next round's own fresh SyncTarget. Confirmed
    // as a real gap otherwise: an exclusion added while a directory was
    // already being walked wouldn't take effect until the *next* round
    // started, potentially uploading a substantial, unwanted amount in
    // the meantime.
    settings_version: Rc<Cell<u64>>,
    // Must be kept alive for as long as we want the app to keep running
    // without a window -- ApplicationHoldGuard releases the hold on Drop,
    // so a bare `app.hold();` (discarding the guard) undoes itself
    // immediately at the end of that statement. Confirmed against gio
    // 0.22.8's actual source: `ApplicationHoldGuard` wraps a WeakRef and
    // its Drop impl is what calls g_application_release.
    hold_guard: RefCell<Option<gio::ApplicationHoldGuard>>,
    // Backing data for the Status tab's Sync Issues row (count in its
    // subtitle, hidden entirely at zero) and what show_sync_issues_dialog
    // opens against when that row is activated. refresh_conflicts_ui
    // fully replaces this Vec's contents from the journal on every status
    // refresh -- the dialog holds a clone of this same Rc<RefCell<>>, not
    // an independent snapshot, so a resolve/retry happening inside it and
    // a background refresh happening outside it stay consistent with
    // each other rather than racing. Combines two genuinely different
    // kinds of item (SyncIssue::Conflict, SyncIssue::PermissionError) --
    // see SyncIssue's own doc comment for why one shared list/carousel
    // rather than two separate ones.
    pending_issues: Rc<RefCell<Vec<SyncIssue>>>,
    // (connection, path) pairs already logged as Excluded or
    // SkippedTooLarge this session -- see apply_status_event's own use
    // of this. Confirmed as a real, reported preference: these are
    // static, per-path facts governed by settings that don't change
    // mid-session, so re-logging the identical line every single round
    // (the crawl re-walks the whole tree regardless of whether anything
    // actually changed) was pure, repetitive noise with no new
    // information in it. If an exclusion is later lifted (a setting
    // changes), nothing here needs to notice that specially -- the item
    // just gets picked up and actually transferred on some future round,
    // which logs its own, real ItemUploaded/ItemDownloaded line the
    // normal way; this set staying stale for a path that's no longer
    // excluded is harmless, since nothing ever reads it to decide
    // whether to *act*, only whether to log again.
    logged_exclusions: Rc<RefCell<std::collections::HashSet<(String, String)>>>,
    // Some once spawn_tray_if_enabled actually spawns the tray thread
    // (tray_enabled setting on); stays None otherwise, in which case
    // update_status_ui's own tray-update block is just a cheap no-op.
    // Sends a plain owned icon-name String across, not a shared Rc --
    // this is the one channel in the app that genuinely crosses an OS
    // thread boundary, not just a same-thread cooperative one, so
    // nothing Rc-based can go through it. See tray.rs's own module doc.
    tray_update_tx: RefCell<Option<async_channel::Sender<tray::TrayUpdate>>>,
    // Local-change watching: every FileMonitor callback across every
    // watched directory calls straight into on_local_change (see that
    // function's own doc comment) rather than flipping a bare polled
    // flag -- this is what that callback actually accumulates into.
    // Keyed by connection_id, since a scoped rescan (see
    // sync::SyncTarget::dirty_scope) needs to know which directories
    // belong to which connection, not just that *something* somewhere
    // changed. Absolute local paths, matching watch.rs's own -- converted
    // to the relative_path convention sync.rs uses only at the point a
    // round actually drains this (see run_sync_round's own use).
    dirty_dirs: Rc<RefCell<HashMap<String, HashSet<PathBuf>>>>,
    // The one outstanding "settle" timer armed by on_local_change, if
    // any -- see that function's own doc comment. Re-armed (old one
    // removed, new one scheduled) on every single FileMonitor event
    // rather than left to fire on a fixed schedule, which is the actual
    // debounce: a burst of writes keeps pushing this back until the
    // writes actually stop, rather than firing partway through a burst
    // the way a fixed 2s/3s poll-then-settle pairing could. Cell, not
    // RefCell -- glib::SourceId is Copy-free but this only ever needs
    // "is one currently pending, and if so, remove it," never a
    // borrowed reference into it.
    watch_debounce_timer: Cell<Option<glib::SourceId>>,
    watch_manager: watch::WatchManager,
    // Per-connection, persistent across rounds within one session (not
    // rebuilt fresh each round the way renamed_away is) -- see
    // sync::CLEANUP_RETRY_COOLDOWN_SECS' own doc comment for the real,
    // reported bug this exists to fix. Cloned into each connection's own
    // SyncTarget at construction time (see build_sync_target_for_connection
    // and run_sync_round's own target construction).
    cleanup_retry_cooldown: Rc<RefCell<HashMap<String, std::time::Instant>>>,
    stability_streak: Rc<RefCell<HashMap<String, ((i64, i64), u32)>>>,
    remote_stability_streak: Rc<RefCell<HashMap<String, ((String, i64), u32)>>>,
    deletion_pending: Rc<RefCell<HashMap<String, (std::time::Instant, u32)>>>,
    webdav_failure_streak: Rc<RefCell<HashMap<String, u32>>>,
    // Same session-wide, cloned-into-every-SyncTarget lifecycle as
    // webdav_failure_streak just above -- see sync::SyncTarget::
    // move_locks' own doc comment (and sync::webdav_move_serialized's)
    // for the full, confirmed reasoning this exists for.
    move_locks: Rc<RefCell<HashMap<String, Rc<sync::Semaphore>>>>,
    // Shared with every SyncTarget's own had_freshness_deferral field for
    // the round currently in flight -- see that field's own doc comment
    // in sync.rs. Reset to false at the start of every round (alongside
    // discovered_count/processed_count); trigger_sync_round checks it
    // once the round finishes and schedules a short follow-up round if
    // it's true.
    had_freshness_deferral_this_round: Rc<Cell<bool>>,
    // Guards against the periodic timer and the watch-triggered debounce
    // loop both deciding to run a round at once -- see trigger_sync_round.
    syncing: Cell<bool>,
    // Status/log event bus: sync.rs sends structured SyncEvents here as it
    // works; a background task in run_background folds them into
    // sync_state + status_log below and refreshes the Status tab. The
    // receiver lives in a RefCell<Option<..>> purely so it can be moved
    // out (taken) once at startup -- same pattern as hold_guard.
    status_events_tx: async_channel::Sender<status::SyncEvent>,
    status_events_rx: RefCell<Option<async_channel::Receiver<status::SyncEvent>>>,
    status_log: RefCell<status::StatusLog>,
    // Current session's own on-disk log file -- see disk_log.rs. A
    // RefCell<PathBuf>, not a one-time-computed constant, since
    // disk_log::should_rotate can mean this changes mid-session (a new,
    // fresh file, still logically the same session -- see
    // new_session_path's own doc comment for why nothing marks rotated
    // files as continuations of each other).
    disk_log_path: RefCell<PathBuf>,
    // Lines accumulated since the last actual flush to disk_log_path --
    // drained (not just read) every time the flush timer below fires.
    // Deliberately separate from status_log's own Vec/VecDeque, not an
    // index into it, since status_log's own ring-buffer eviction would
    // otherwise invalidate any such index the moment the cap is hit.
    disk_log_pending: RefCell<Vec<String>>,
    // Whether a flush timer is currently counting down -- see
    // apply_status_event's own use of this. Sole purpose: a burst of
    // many new lines in quick succession should still result in exactly
    // one spawned timer, not one per line, and importantly, a quiet
    // stretch with nothing new to log should mean the timer simply never
    // gets armed again at all until the next real line arrives, rather
    // than free-running on a fixed interval forever regardless of
    // whether there's ever anything to flush -- the same "don't wake up
    // for no reason" reasoning already applied elsewhere in this app
    // (see Semaphore's own doc comment in sync.rs).
    disk_log_flush_armed: Cell<bool>,
    // Worst-first across the current/most recent round (Disconnected >
    // Syncing > Scanning > Idle) -- see status.rs. Conflicts don't
    // factor in here; see status.rs's SyncState doc comment for why.
    sync_state: Cell<status::SyncState>,
    last_checked: RefCell<Option<String>>,
    // Set/cleared by trigger_sync_round's metered-connection check --
    // deliberately NOT folded into sync_state itself. sync_state's whole
    // design is worst-first severity (Disconnected > Syncing > Scanning
    // > Idle, see status.rs), and "paused because of a setting the
    // user turned on" isn't a severity level, it's a different axis
    // entirely -- treating it as a SyncState variant would mean deciding
    // whether it outranks Idle for no good reason. update_status_ui
    // checks this directly instead, taking priority over sync_state's own
    // label when true.
    paused_for_metered: Cell<bool>,
    // Same reasoning and pattern as paused_for_metered just above -- see
    // gio::PowerProfileMonitor's own usage (right where paused_for_metered
    // uses gio::NetworkMonitor) for how this is actually detected, and
    // trigger_sync_round's own use of this alongside
    // pause_on_power_save_mode.
    paused_for_power_save: Cell<bool>,
    // Lazily established on first use (see on_battery_power), then reused
    // for every subsequent battery check rather than reconnecting to the
    // system bus on every single trigger_sync_round call -- zbus's own
    // Connection is cheap to clone (internally Arc-wrapped) once it
    // exists, but the initial handshake itself isn't free, and this can
    // run as often as any other trigger_sync_round check. The SYSTEM
    // bus specifically, not the session bus -- confirmed as a real,
    // reported bug from using the wrong one originally: UPower's own
    // documentation states directly that its service is available via
    // "the system message bus," not the session bus check_tray_available
    // correctly uses for org.kde.StatusNotifierWatcher (a genuinely
    // session-level service). The session bus has no such service
    // registered on it at all, so every on_battery_power call silently
    // failed and returned None on every machine, not something specific
    // to any one device -- this row just hadn't been tested live yet
    // before that surfaced. Power Save Mode detection deliberately
    // doesn't share this connection -- see gio::PowerProfileMonitor's own
    // usage for why it needs no manual D-Bus connection management at
    // all, unlike UPower here, which this app talks to directly.
    battery_system_bus: RefCell<Option<zbus::Connection>>,
    // Deliberately separate from paused_for_metered above, not a shared
    // flag -- these are two genuinely different reasons to pause, and
    // folding them into one bool would mean the automatic metered-
    // detection logic (trigger_sync_round) and this manual toggle could
    // each clear a pause the other one set for its own reason. Same
    // "not a SyncState variant" reasoning as paused_for_metered's own
    // comment applies here too.
    paused_by_user: Cell<bool>,
    // Purely informational -- NOT a fourth gating flag alongside the
    // three above. By request: this new kind of pause should always be
    // overridable by the same pause/resume button the other three
    // already are, and toggle_pause's own logic unconditionally flips
    // paused_by_user itself (see its own doc comment) -- a genuinely
    // separate gate would mean clicking resume while ONLY this flag was
    // set would flip paused_by_user to true instead of actually
    // resuming, since it was false going in. Reusing paused_by_user
    // itself as the real pause mechanism sidesteps that entirely: this
    // flag only ever distinguishes WHY paused_by_user is true, for the
    // Status tab's own subtitle, the pause banner, and the log message
    // -- toggle_pause's own resume branch clears it alongside
    // paused_by_user, same moment, every time, so it can never
    // meaningfully outlive the actual pause it's describing.
    paused_reason_restored_account: Cell<bool>,
    // The shared, round-wide progress counters -- see
    // sync::SyncTarget::discovered_count/processed_count's own doc
    // comments for the full reasoning. Reset to 0 at the start of each
    // round (see trigger_sync_round); the same Rc<Cell<>> is cloned into
    // every connection's own SyncTarget constructed within that one
    // round, so they add up to one, single, app-wide total rather than
    // several separate per-connection ones.
    discovered_count: Rc<Cell<u64>>,
    processed_count: Rc<Cell<u64>>,
    // Confirmed as a real, serious bug: the background verification
    // task (see trigger_sync_round's own spawn of it) had no
    // reentrancy protection at all, unlike the main round's own
    // `syncing` flag above. Every trigger_sync_round call that got
    // past its own `syncing` guard -- including many rapid, separate
    // ones, e.g. from notify_push firing repeatedly -- spawned a
    // completely independent verification task, with no coordination
    // between them. Multiple overlapping tasks then each queried the
    // same PendingVerification queue, each redid the full content hash
    // for the same files, and each called mark_conflict_pending/
    // emitted the event on a mismatch -- real, repeated file reads and
    // hashing, not just duplicate log lines. This flag gates spawning
    // a new task at all while one is already active; a newer round's
    // own newly-discovered PendingVerification items are simply picked
    // up by the next verification pass once the current one finishes,
    // not left permanently stuck.
    verifying: Cell<bool>,
    // Live, ephemeral state for the Active Transfers row -- populated by
    // SyncEvent::TransferStarted, cleared by SyncEvent::TransferEnded
    // (see apply_status_event). Keyed by "{connection}:{path}" rather
    // than just path, since two different connections could otherwise
    // collide on the same relative path.
    active_transfers: Rc<RefCell<HashMap<String, ActiveTransfer>>>,
    // The ExpanderRow's own currently-displayed child rows -- tracked
    // separately since, unlike a plain ListBox, ExpanderRow has no
    // generic "enumerate/clear all children" API, only add_row/remove
    // for individual rows. Rebuilt from scratch on every
    // refresh_active_transfers_ui call: each existing row removed via
    // this list first, then fresh ones added from the current
    // active_transfers map.
    active_transfer_rows: RefCell<Vec<libadwaita::ActionRow>>,
    // App-wide, not per-connection -- see sync::Semaphore's own doc
    // comment for what this actually bounds. Fixed-size now, built once
    // for the app's whole lifetime -- there's no longer a per-round
    // recalculation to keep current, since the value it bounds is a
    // plain constant rather than something derived from live conditions.
    // Constructed once, here, and shared (via Rc::clone) into every
    // connection's own SyncTarget each round, rather than each
    // connection getting its own, independent semaphore -- confirmed as
    // a real, reported problem otherwise: several folder connections
    // under one account each drawing from their own, separate budget
    // could still add up to far more simultaneous load against the same
    // underlying mount than any single one's own limit suggested.
    content_semaphore: Rc<sync::Semaphore>,
    discovery_pool: Rc<sync::HybridPool>,
    confirm_pool: Rc<sync::HybridPool>,
    // A genuinely separate, dedicated Semaphore instance (capacity 1),
    // by request -- not a HybridPool's own reserve, and deliberately
    // never shared with content_semaphore above at all. The reason:
    // retry_single_item/apply_conflict_resolution both make several,
    // independent content_semaphore acquisitions internally (one per
    // real network operation, not just one for the whole call), so a
    // single outer "privileged permit" wrapping the whole call wouldn't
    // actually help -- the inner calls would still block on a saturated
    // shared pool regardless of whatever outer permit was held. Giving
    // one specific queued resolution its own, fully separate SyncTarget
    // (built with this Semaphore in place of content_semaphore -- see
    // run_sync_round's own queued-action drain loop) means every one of
    // its own internal acquisitions draws from this dedicated capacity-1
    // pool instead, genuinely never contending with the shared one at
    // all, not just holding one extra ticket into the same line.
    resolver_privileged_semaphore: Rc<sync::Semaphore>,
    // Filled in once run_background successfully connects to GOA -- None
    // only during the brief startup window before that. Exists so code
    // that builds/refreshes UI (the conflicts list's Resolve buttons, in
    // particular) can reach the watcher without it being threaded through
    // update_status_ui/apply_status_event's signatures, which run from
    // several call sites that don't all have it in scope already.
    watcher: RefCell<Option<Rc<GoaWatcher>>>,
    // One entry per connection currently mid-round in run_sync_round,
    // inserted right before that connection's sync::sync_target call and
    // removed right after (success, failure, or cancelled -- all three).
    // Closing the sender is how the connection-removal handler actually
    // interrupts an in-flight round for it rather than merely wishing it
    // would stop: run_sync_round races sync_target against this
    // channel's recv() via futures_util::future::select, and gio-rs's
    // _future() wrappers cancel their underlying GIO operation when
    // dropped (confirmed against gio 0.22.8's GioFuture, which holds an
    // internal Cancellable it cancels on Drop) -- so losing the select
    // genuinely aborts whatever single copy/delete/enumerate call was
    // in flight at that moment, not just skips the next one.
    active_sync_cancels: RefCell<HashMap<String, async_channel::Sender<()>>>,
    // Per-connection tier state for the notify_push/ETag/full-crawl
    // design -- see run_sync_round's own per-connection scheduling logic
    // and effective_poll_interval_secs for how these two combine into
    // the actual multiplier applied to remote_poll_interval_minutes.
    // HashMap<account_id, bool>, not connection_id -- confirmed as a
    // real, reported inefficiency otherwise: notify_push, ETag support,
    // and the credential/HTTP work to discover them are all genuinely
    // facts about the underlying server an account represents, not
    // about any one specific folder connection. Every connection sharing
    // one account necessarily shares the same server -- keying by
    // connection_id meant redundantly re-discovering the identical
    // answer per connection, including, worst of all, spawning a fully
    // separate, duplicate notify_push WebSocket connection per
    // connection rather than the one the account actually needed.
    // Different *accounts* can still genuinely have different
    // capabilities (one on a Nextcloud server with notify_push
    // installed, another on a generic WebDAV server with neither).
    // HashMap<account_id, NotifyPushStatus> -- see that enum's own doc
    // comment for the full state machine and why it replaced an earlier,
    // separate bool-plus-tracking-set design. Confirmed as a real,
    // reported problem this whole design exists to fix: notify_push's
    // own reconnect loop (see push::run_listener) can drop and
    // re-establish a connection within moments even under perfectly
    // ordinary conditions, and each one used to produce its own
    // "unavailable ... confirmed" pair in the log for a blip that was
    // never a real, sustained outage. Rather than pausing the listener
    // itself (a real, separate idea that was deliberately set aside --
    // policing it would mean frequent wakeups of its own, defeating the
    // same "reduce wakeups" purpose notify_push exists for in the first
    // place), this instead debounces the user-facing announcement only.
    notify_push_status: Rc<RefCell<HashMap<String, NotifyPushStatus>>>,
    // Set true specifically when push::PushMessage::AuthenticationFailed
    // arrives for an account (the pre_auth exchange got a 401/403, not
    // a generic network/server error) -- distinct from
    // notify_push_status being anything other than Connected, which covers every
    // reason an account might not currently be up. Consulted by the
    // Disconnected status row's own subtitle so a genuinely actionable
    // problem (re-authenticate this account) doesn't look identical to
    // an ordinary, self-resolving network blip. Keyed by account_id, not
    // connection_id -- see notify_push_status's own doc comment for
    // why.
    push_auth_failed: Rc<RefCell<HashMap<String, bool>>>,
    // Keyed by account_id -- see notify_push_status's own doc comment.
    etag_capable: Rc<RefCell<HashMap<String, bool>>>,
    // When this connection's own tier-adjusted interval was last
    // actually satisfied (an ETag check ran, or a real crawl ran) --
    // compared against effective_poll_interval_secs' own result inside
    // run_sync_round's per-connection loop to decide whether this
    // connection's turn has actually come up yet on *this* round, given
    // its own multiplier, rather than the base interval every
    // connection would otherwise share regardless of its own tier.
    // Deliberately still keyed by connection_id, not account_id, unlike
    // notify_push_status/etag_capable/capabilities_checked just above
    // -- each connection genuinely has its own, independent sync
    // schedule even when it shares an account with another (one folder
    // could just have finished a full crawl while a sibling folder under
    // the same account hasn't been checked in a while), so there's
    // nothing to consolidate here the way there was for those.
    // boot_time::Instant, not std::time::Instant -- see Cargo.toml's own
    // comment on the boot-time dependency for the full reasoning
    // (CLOCK_MONOTONIC not advancing during suspend, confirmed as a
    // real, reported bug this specifically fixes).
    last_full_check: Rc<RefCell<HashMap<String, boot_time::Instant>>>,
    // Connection IDs whose own tier-multiplier gate (see
    // run_sync_round's own use of this) should be bypassed on their very
    // next turn -- populated when notify_push reports a real, known
    // change (see PushMessage::FileChanged's own handler), consumed
    // (removed) the moment that connection's own gate check actually
    // honors it. Confirmed as a real, reported bug without this:
    // trigger_sync_round had no way to distinguish "the routine,
    // periodic poll fired again" from "notify_push just told us
    // something definitely changed" -- both funneled through the exact
    // same run_sync_round, meaning notify_push's entire point (immediate
    // notice, not waiting for the next slow, tier-multiplied interval)
    // was being silently defeated by the same gate that exists
    // specifically to reduce needless polling when nothing's actually
    // changed. Still connection-keyed, same reasoning as
    // last_full_check just above -- this is about which connection's own
    // next crawl should be forced, not a fact about the account itself.
    force_full_check: Rc<RefCell<HashSet<String>>>,
    // Distinct from force_full_check just above -- that one still means
    // "bypass try_etag_skip entirely," which is exactly right for Sync
    // Now (a direct, explicit user request for a real, thorough check --
    // see trigger_sync_round's own doc comment on why that one needs
    // this) but wrong for an unresolved notify_push event, which only
    // ever means "something changed somewhere in the account," not
    // "something changed in THIS connection's own remote root" -- a
    // question try_etag_skip itself is already positioned to answer
    // cheaply and correctly. This field bypasses only the tier-multiplier
    // scheduling gate in run_sync_round (guaranteeing the connection is
    // actually looked at this round rather than skipped outright by its
    // own schedule), while still letting try_etag_skip run normally.
    notify_push_pending: Rc<RefCell<HashSet<String>>>,
    // How many genuine remote-writing operations (uploads, deletes,
    // MOVEs/rename-pairing, MKCOL, and conflict-resolution writes --
    // everywhere target.webdav_* or upload_temp_first/upload_via_
    // chunking actually writes to the server; see RemoteWriteStarted/
    // RemoteWriteFinished's own doc comments in status.rs for the full,
    // enumerated list) are currently in flight, across every connection
    // under the active account at once. Plain global counter, not keyed
    // by connection_id or account_id -- only one account is ever active
    // in this app at all, and the design below treats "any write from
    // any connection, for any reason" as one, single batch rather than
    // separate ones per connection or per trigger, since the whole
    // point is bounding the window where a false "just an echo"
    // conclusion could be reached, not tracking each write's own,
    // individual provenance.
    //
    // This used to only ever cover conflict-resolution writes
    // specifically (ConflictAction::KeepLocal/KeepBothRenameRemote);
    // extended to cover every remote-writing operation after a real,
    // reported bug: adding even a single, tiny file to a synced folder
    // triggered a full, disproportionate remote rescan, congesting the
    // reporting user's own WiFi network badly enough to disrupt other
    // devices. Root cause: a remote write this app makes itself
    // legitimately causes Nextcloud's own notify_push service to notify
    // every connected client about it, including this one, about its
    // own just-completed activity -- and that event is coarse-grained
    // (see push::PushMessage::FileChanged's own doc comment: it doesn't
    // say what changed, only that something did, somewhere in the
    // account), so without this mechanism, the only way to find out
    // what changed is a full, unscoped crawl, every single time, no
    // matter how small the actual change was.
    //
    // Pairs with dirty_connections and coalesced_notify_push_pending
    // (both just below) to implement a "pre-flight, batch, post-flight"
    // design, arrived at over an extended design discussion:
    //   - The count transitioning from 0 to nonzero (i.e. whichever
    //     write happens to be the first to start a new batch, not every
    //     write individually) fires a pre-flight check: for every
    //     currently-active connection, fetch the live root ETag and
    //     compare it against journal's own, already-stored baseline. A
    //     mismatch here means the baseline was already untrustworthy
    //     before this batch even started -- likely a genuinely
    //     different, external write whose own notify_push got dropped
    //     (no delivery guarantee) at some point before now -- and marks
    //     that connection dirty.
    //   - Anything that starts writing while the count is already
    //     nonzero just joins the batch already in progress; no
    //     redundant pre-flight check of its own, since one already
    //     covers the whole batch.
    //   - The count transitioning back to 0 (every write from every
    //     source has now settled) fires the post-flight side: for any
    //     connection marked dirty, or if coalesced_notify_push_pending
    //     is set (something arrived mid-batch that could be a genuine,
    //     concurrent change, not provably just this app's own echo),
    //     skip straight to a real, immediate check rather than trusting
    //     anything about the batch just finished. Otherwise, capture
    //     the live root ETag now and store it as the new, trusted
    //     journal baseline -- so a notify_push arriving after this
    //     point compares against a value known to reflect only what
    //     this app just, definitely did, letting a genuine echo be
    //     recognized and safely skipped.
    //
    // Deliberately not per-write pre/post-flight brackets -- a single
    // logical operation (deleting a whole folder locally, for instance)
    // can fan out into dozens of individual remote writes; bracketing
    // each one individually would reproduce the exact wasteful-burst
    // problem this design exists to fix. A mid-batch ETag re-check was
    // considered and deliberately rejected too: while ETags are never
    // "torn" or meaningless mid-transfer (Nextcloud updates them
    // synchronously, as part of the same database transaction that
    // commits each write -- confirmed against a real Nextcloud server
    // bug report about database lock contention from concurrent writes
    // to the same directory's own ETag row), a mid-batch reading still
    // can't be decomposed into "my own writes so far" versus "something
    // else, blended in" -- there's nothing meaningful to compare it
    // against until the batch is either not yet started or fully
    // finished.
    pending_remote_writes: Rc<Cell<u32>>,
    // Connection ids whose pre-flight check (this batch) found the
    // journal's stored root ETag already didn't match live server state
    // -- see pending_remote_writes' own doc comment just above for the
    // full design. Cleared once the batch settles (the count drops back
    // to 0), regardless of which branch (dirty vs clean) each
    // connection actually took.
    dirty_connections: Rc<RefCell<HashSet<String>>>,
    // Whether a notify_push event arrived while pending_remote_writes
    // was still nonzero -- checked, and acted on, only once that count
    // actually drops back to zero (see pending_remote_writes' own doc
    // comment above). Not suppressed outright: a notify_push event
    // landing in that window could still genuinely include someone
    // else's real, concurrent change, not just this app's own echo, so
    // it's coalesced into exactly one, deferred decision once the dust
    // settles, rather than dropped entirely.
    coalesced_notify_push_pending: Rc<Cell<bool>>,
    // Whether push::discover_capabilities has already been run for this
    // account this session -- once per account per session, not once
    // per connection or once per round, since a server's own
    // capabilities don't change during a single run of this app, and
    // every connection under one account necessarily shares that same
    // server. See notify_push_status's own doc comment for the
    // redundancy this consolidation fixes.
    capabilities_checked: Rc<RefCell<std::collections::HashSet<String>>>,
    // Given to every spawned push::run_listener thread (one per
    // connection with a live notify_push connection) so it can request a
    // fresh password from the main, glib-driven thread -- see
    // push::PasswordRequest's own doc comment for why this round-trip
    // exists. None only during the brief startup window before the
    // password-request-handling task itself has been spawned.
    push_password_requests_tx: RefCell<Option<async_channel::Sender<push::PasswordRequest>>>,
    // Given to every spawned push::run_listener thread as its own `tx`
    // parameter -- the shared channel every connection's own listener
    // sends (account_id, PushMessage) through, read by the single
    // handler task in run_background. None only during the same brief
    // startup window as push_password_requests_tx above.
    push_events_tx: RefCell<Option<async_channel::Sender<(String, push::PushMessage)>>>,
    /// Queued actions from the Sync Issues carousel -- either kind of
    /// item's own action (see QueuedSyncAction), deferred specifically
    /// because a broader round happened to already be in flight, or the
    /// connection was paused and the user didn't check "Apply
    /// immediately, even while paused." Drained right after
    /// trigger_sync_round's own round finishes, not polled on any timer;
    /// this only ever needs to run the moment the thing it was waiting
    /// on (state.syncing going back to false, which for a pause
    /// specifically only happens once it's actually unpaused) happens.
    pending_sync_actions: RefCell<Vec<QueuedSyncAction>>,
}

fn journal_db_path() -> PathBuf {
    let mut path = glib::user_data_dir();
    path.push("ripples");
    std::fs::create_dir_all(&path).ok();
    path.push("journal.sqlite3");
    path
}

fn autostart_desktop_path() -> PathBuf {
    let mut path = glib::user_config_dir();
    path.push("autostart");
    path.push(format!("{APP_ID}.desktop"));
    path
}

/// Writes or removes the XDG autostart entry to match `enabled`, called
/// both from the Settings toggle (for immediate effect) and once at
/// every launch (for self-healing -- see main()'s own call site). No
/// external packaged .desktop file: this app generates its own at
/// runtime, since a .desktop entry is simple enough not to need a
/// build-time asset, and generating it fresh every time means Exec=
/// never goes stale if the executable gets moved or reinstalled
/// somewhere else.
///
/// `NoDisplay=true` keeps it out of application menus/launchers -- this
/// entry exists purely for session-startup autostart, not as a second,
/// redundant way to launch the app a user would actually pick from a
/// list. `--hidden` on the Exec= line is what makes this actually usable
/// as an autostart entry in the first place -- without it, every login
/// would pop the window open unprompted, which isn't what "launch in
/// the background" should mean.
fn ensure_autostart(enabled: bool) {
    let path = autostart_desktop_path();
    if !enabled {
        // Not finding it is fine -- already off, nothing to remove.
        let _ = std::fs::remove_file(&path);
        return;
    }

    let Some(parent) = path.parent() else { return };
    if let Err(e) = std::fs::create_dir_all(parent) {
        eprintln!("couldn't create autostart directory: {e}");
        return;
    }

    let Ok(exe) = std::env::current_exe() else {
        eprintln!("couldn't determine own executable path for autostart entry");
        return;
    };

    let contents = format!(
        "[Desktop Entry]\nType=Application\nVersion=1.0\nName=Ripples\nComment=Sync client for Nextcloud, ownCloud, and WebDAV\nExec={} --hidden\nX-GNOME-Autostart-enabled=true\nNoDisplay=true\nTerminal=false\n",
        exe.display()
    );
    if let Err(e) = std::fs::write(&path, contents) {
        eprintln!("couldn't write autostart entry: {e}");
    }
}

fn main() -> glib::ExitCode {
    // Stripped out of argv BEFORE it ever reaches GApplication's own
    // commandline parsing (app.run_with_args below), rather than left in
    // and trusting GLib's default option handling to ignore an
    // unrecognized flag gracefully -- that behavior isn't something
    // worth relying on unverified for a flag whose entire purpose is
    // making autostart launch cleanly. See activate()'s own doc comment
    // for what this actually gates (only the process's own first
    // activation, not every subsequent one).
    let raw_args: Vec<String> = std::env::args().collect();
    let hidden = raw_args.iter().any(|a| a == "--hidden");
    // Deliberately a global static, not passed down as a parameter the
    // way `hidden` is to activate() -- debug logging is scattered across
    // both this file and sync.rs, at call depths far removed from
    // main()'s own argument parsing, so threading an explicit bool
    // through every intervening function signature just to reach a
    // handful of eprintln! calls isn't worth the churn. Set exactly
    // once, here, before anything else runs; see is_debug_enabled's own
    // doc comment for the read side.
    DEBUG_ENABLED.set(raw_args.iter().any(|a| a == "--debug")).expect("set exactly once, here, before any other code could have called is_debug_enabled() and raced this");
    let filtered_args: Vec<String> = raw_args.into_iter().filter(|a| a != "--hidden" && a != "--debug").collect();

    let app = libadwaita::Application::builder().application_id(APP_ID).build();

    // A lazily-populated holder, not AppState constructed directly here
    // -- see ensure_state_initialized's own doc comment for the real,
    // reported crash this fixes. Everything that used to sit here
    // unconditionally (journal::open, the rest of AppState's own
    // construction, ensure_autostart) now only ever runs inside
    // activate(), on this process's own genuine first activation --
    // never merely on process launch, which used to happen even for a
    // second, redundant process that gio would otherwise hand off to an
    // already-running instance via D-Bus and exit immediately, with
    // *this* process never becoming the real, primary one at all.
    let state_holder: Rc<RefCell<Option<Rc<AppState>>>> = Rc::new(RefCell::new(None));

    app.connect_activate(clone!(
        #[strong] state_holder,
        move |app| {
            let state = ensure_state_initialized(&state_holder);
            activate(app, &state, hidden)
        }
    ));

    // A real "completely exit" path, distinct from just closing the
    // window -- this app deliberately holds itself open in the
    // background (see activate()'s app.hold() below) so closing the
    // window keeps syncing running, which is the whole point most of
    // the time. Without this, the only way to actually stop the process
    // is `kill`. Dropping the hold guard first, then quit(), rather than
    // just quit() alone: gio::Application::quit() does immediately
    // terminate regardless of outstanding holds, so the drop isn't
    // strictly required for the process to exit -- but leaving the
    // guard sitting in state.hold_guard as if the app were still meant
    // to be held open, on the one code path whose entire job is "stop
    // holding it open," reads as leftover state rather than a clean
    // handoff. Wired to a real GAction (app.quit) rather than just a
    // button-click closure so it also picks up the standard Ctrl+Q
    // accelerator below for free.
    //
    // Captures state_holder, not state directly -- state itself doesn't
    // exist yet at the point this action is registered (AppState is now
    // only ever constructed inside activate(), see above), though by the
    // time a person could actually trigger Quit at all, activate() must
    // already have run at least once, so .expect() here is safe, not
    // just hopeful.
    let quit_action = gio::SimpleAction::new("quit", None);
    quit_action.connect_activate(clone!(
        #[strong] state_holder,
        #[strong] app,
        move |_, _| {
            let state = state_holder.borrow().clone().expect("Quit can only be triggered after activate() has already run at least once");
            state.hold_guard.borrow_mut().take();
            app.quit();
        }
    ));
    app.add_action(&quit_action);
    app.set_accels_for_action("app.quit", &["<Ctrl>Q"]);

    app.run_with_args(&filtered_args)
}

/// Constructs AppState -- including opening the sync journal, pruning
/// old on-disk session logs, and self-healing the autostart entry --
/// the first time this is ever called for a given process, then simply
/// returns a clone of the same, already-constructed Rc<AppState> on
/// every call after that.
///
/// Confirmed as a real, reported crash without this: journal::open used
/// to run unconditionally at the very top of main(), before
/// app.run_with_args() -- meaning even a second, redundant launch while
/// an instance was already running (which gio's own D-Bus activation
/// handling is specifically designed to detect and hand off to that
/// existing instance, rather than let a second process do any real work
/// at all) still tried to open the exact same journal.sqlite3 a second,
/// competing time before gio ever got the chance to intervene --
/// panicking with "database is locked" (SQLITE_BUSY), since the
/// already-running instance still held its own, real connection open.
/// Moving this to only run here, inside activate() itself, and only on
/// that process's own genuine first activation (the same
/// once-ever distinction activate() already, correctly draws for
/// starting the background sync loop -- see its own doc comment) means
/// a second, handed-off process's own connect_activate closure calls
/// this too, but the handoff itself is what gio's own machinery inside
/// app.run_with_args() is responsible for -- by the time a truly
/// separate, second process's own activate() might otherwise run, gio
/// has already forwarded the request to the existing instance over
/// D-Bus and this second process exits, never reaching this function at
/// all.
fn ensure_state_initialized(state_holder: &Rc<RefCell<Option<Rc<AppState>>>>) -> Rc<AppState> {
    if let Some(existing) = state_holder.borrow().as_ref() {
        return existing.clone();
    }

    let journal = journal::open(&journal_db_path()).expect("failed to open sync journal");
    let (status_events_tx, status_events_rx) = async_channel::unbounded();
    let initial_config = config::AppConfig::load();
    let initial_content_concurrency = sync::Semaphore::MAX_PERMITS;
    eprintln!("File transfer concurrency limit: {initial_content_concurrency}");
    // Pruned before this session's own log file is even created, so the
    // file about to be written never risks being the one pruned -- see
    // disk_log::prune_old_sessions' own doc comment.
    disk_log::prune_old_sessions();
    let state = Rc::new(AppState {
        config: RefCell::new(initial_config),
        journal,
        window_widgets: RefCell::new(None),
        started: RefCell::new(false),
        theme_css_provider: gtk4::CssProvider::new(),
        first_round_this_session: Cell::new(true),
        active_account_ready: Cell::new(true),
        settings_version: Rc::new(Cell::new(0)),
        hold_guard: RefCell::new(None),
        pending_issues: Rc::new(RefCell::new(Vec::new())),
        logged_exclusions: Rc::new(RefCell::new(std::collections::HashSet::new())),
        tray_update_tx: RefCell::new(None),
        dirty_dirs: Rc::new(RefCell::new(HashMap::new())),
        watch_debounce_timer: Cell::new(None),
        cleanup_retry_cooldown: Rc::new(RefCell::new(HashMap::new())),
        stability_streak: Rc::new(RefCell::new(HashMap::new())),
        remote_stability_streak: Rc::new(RefCell::new(HashMap::new())),
        deletion_pending: Rc::new(RefCell::new(HashMap::new())),
        webdav_failure_streak: Rc::new(RefCell::new(HashMap::new())),
        move_locks: Rc::new(RefCell::new(HashMap::new())),
        had_freshness_deferral_this_round: Rc::new(Cell::new(false)),
        watch_manager: watch::WatchManager::new(),
        syncing: Cell::new(false),
        status_events_tx,
        status_events_rx: RefCell::new(Some(status_events_rx)),
        status_log: RefCell::new(status::StatusLog::new()),
        disk_log_path: RefCell::new(disk_log::new_session_path()),
        disk_log_pending: RefCell::new(Vec::new()),
        disk_log_flush_armed: Cell::new(false),
        sync_state: Cell::new(status::SyncState::Idle),
        last_checked: RefCell::new(None),
        paused_for_metered: Cell::new(false),
        paused_for_power_save: Cell::new(false),
        battery_system_bus: RefCell::new(None),
        paused_by_user: Cell::new(false),
        paused_reason_restored_account: Cell::new(false),
        discovered_count: Rc::new(Cell::new(0)),
        processed_count: Rc::new(Cell::new(0)),
        verifying: Cell::new(false),
        active_transfers: Rc::new(RefCell::new(HashMap::new())),
        active_transfer_rows: RefCell::new(Vec::new()),
        content_semaphore: Rc::new(sync::Semaphore::new()),
        // 2 each, per this session's own conversation -- enough to keep
        // discovery/confirm-detect making genuine progress even under
        // full Pool 3 saturation, without needing to be large given how
        // brief these calls actually are.
        discovery_pool: Rc::new(sync::HybridPool::new(2)),
        confirm_pool: Rc::new(sync::HybridPool::new(2)),
        resolver_privileged_semaphore: Rc::new(sync::Semaphore::with_capacity(1)),
        watcher: RefCell::new(None),
        active_sync_cancels: RefCell::new(HashMap::new()),
        notify_push_status: Rc::new(RefCell::new(HashMap::new())),
        push_auth_failed: Rc::new(RefCell::new(HashMap::new())),
        etag_capable: Rc::new(RefCell::new(HashMap::new())),
        last_full_check: Rc::new(RefCell::new(HashMap::new())),
        force_full_check: Rc::new(RefCell::new(HashSet::new())),
        notify_push_pending: Rc::new(RefCell::new(HashSet::new())),
        pending_remote_writes: Rc::new(Cell::new(0)),
        dirty_connections: Rc::new(RefCell::new(HashSet::new())),
        coalesced_notify_push_pending: Rc::new(Cell::new(false)),
        capabilities_checked: Rc::new(RefCell::new(std::collections::HashSet::new())),
        push_password_requests_tx: RefCell::new(None),
        push_events_tx: RefCell::new(None),
        pending_sync_actions: RefCell::new(Vec::new()),
    });

    // Self-healing, every launch, not just when the Settings toggle
    // itself is flipped -- handles both "the .desktop file was removed
    // externally" (a desktop's own Startup Applications tool, e.g.) and
    // "the executable moved" (Exec= is regenerated fresh from
    // std::env::current_exe() every time, not written once and left
    // stale) the same way the rest of this app prefers recomputing
    // ground truth over trusting a value written once.
    ensure_autostart(state.config.borrow().launch_on_startup);
    // Same "apply the loaded value once, here, at startup" reasoning --
    // the Settings row's own handler only fires on a subsequent user
    // edit, so whatever was actually loaded from disk needs this too,
    // or a saved non-zero limit would silently do nothing until the
    // user happened to revisit Settings and nudge the spinner.
    let bandwidth_kbps = state.config.borrow().bandwidth_limit_kbps;
    webdav::set_bandwidth_limit(if bandwidth_kbps == 0 { None } else { Some(bandwidth_kbps as u64 * 1024) });

    *state_holder.borrow_mut() = Some(state.clone());
    state
}

/// `hidden` only ever suppresses window presentation on THIS process's
/// own first activation (e.g. an autostart-triggered launch) -- gated on
/// the same `!*state.started.borrow()` check that already exists for
/// the one-time background-loop start, not applied unconditionally.
/// Every later activation of this same long-running instance (someone
/// opening the app normally afterward, forwarded via D-Bus to this
/// already-running process) is a genuine request to see the UI and
/// always gets one, regardless of how the process originally started --
/// `hidden` being true at launch doesn't mean "never show a window,"
/// just "don't show one unprompted this one time."
fn activate(app: &libadwaita::Application, state: &Rc<AppState>, hidden: bool) {
    // connect_activate can fire more than once (secondary D-Bus
    // activation) -- only start the background loop the first time.
    let first_activation = !*state.started.borrow();
    if first_activation {
        *state.started.borrow_mut() = true;
        // This is a background sync app first, a window second: hold the
        // app open so closing the window doesn't quit the process and
        // stop syncing. The guard has to be stored somewhere -- dropping
        // it (including via the compiler's own suggested `let _ = ...`)
        // releases the hold immediately, undoing this entirely.
        *state.hold_guard.borrow_mut() = Some(app.hold());
        spawn_tray_if_enabled(app, state);
        glib::MainContext::default().spawn_local(clone!(
            #[strong] state,
            async move { run_background(state).await }
        ));
    }

    if !(first_activation && hidden) {
        build_or_present_window(app, state);
    }
}

/// Checks whether any StatusNotifierHost (a tray implementation) is
/// actually registered and running right now, via a direct read of
/// org.kde.StatusNotifierWatcher's own IsStatusNotifierHostRegistered
/// property -- the standard, protocol-level way to ask this, confirmed via
/// the real freedesktop.org StatusNotifierWatcher spec (that bus name is
/// used across desktops generally, not just KDE ones -- confirmed
/// against real reports from Waybar/sway and XFCE setups using the
/// identical name). Not exposed through ksni itself -- see this
/// function's own Cargo.toml comment on zbus for why a separate,
/// direct connection is needed here.
///
/// Any failure here (no such service on the bus at all, connection
/// error, method call error) is treated as "not available" -- by far
/// the most common failure mode is simply that nothing on this desktop
/// implements the Watcher side of the protocol at all, which is exactly
/// the case this function exists to detect.
///
/// One real, confirmed caveat, not just a theoretical one: a documented
/// startup race exists where the Watcher service can already be on the
/// bus before an actual Host has registered with it, so a check
/// performed immediately at process launch could return a false
/// negative for an implementation that's about to become available a
/// moment later (seen in real bug reports against Waybar's own tray
/// module). This is called from Settings-page construction, not at
/// startup, which happens well after any such race would have already
/// resolved in practice -- not a complete fix for the race, but a
/// reasonable, low-complexity way to sidestep it rather than adding a
/// live signal subscription for a plain Settings toggle.
async fn check_tray_available() -> bool {
    // Confirmed as a real false negative on at least one real, working
    // setup (GNOME Shell's own AppIndicator/KStatusNotifierItem Support
    // extension) -- the tray demonstrably works there, but this check
    // said otherwise. Logging each step's own error rather than
    // collapsing everything to a silent `false` is what actually makes
    // this diagnosable instead of guessing blindly at which of three
    // genuinely different failure points (no session bus, no such
    // service registered at all, or the service existing under a
    // different interface/method shape than assumed) is the real one.
    let connection = match zbus::Connection::session().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("tray availability check: couldn't connect to session bus: {e}");
            return false;
        }
    };
    let proxy = match zbus::Proxy::new(
        &connection,
        "org.kde.StatusNotifierWatcher",
        "/StatusNotifierWatcher",
        "org.kde.StatusNotifierWatcher",
    ).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("tray availability check: couldn't build a StatusNotifierWatcher proxy: {e}");
            return false;
        }
    };
    match proxy.get_property::<bool>("IsStatusNotifierHostRegistered").await {
        Ok(registered) => registered,
        Err(e) => {
            eprintln!("tray availability check: IsStatusNotifierHostRegistered read failed: {e}");
            false
        }
    }
}

/// Queries UPower over D-Bus for whether the system is currently
/// running on battery power -- None if the query itself failed for any
/// reason (no UPower running, no system bus, etc), Some(true/false) for
/// a real answer either way. Used only for the on-battery polling
/// multiplier now (see run_sync_round's own use) -- the previous
/// battery-percentage-threshold pause feature this also used to serve
/// is gone, replaced by Power Save Mode detection (see
/// gio::PowerProfileMonitor's own usage), which reacts to the system's
/// own power posture directly rather than a battery-percentage
/// threshold. This dropped needing anything from UPower's own
/// DisplayDevice object (Percentage, IsPresent) as a real, confirmed
/// simplification once that was the only remaining reason to query it
/// at all -- OnBattery alone, from the top-level org.freedesktop.UPower
/// object, answers everything this still needs, one D-Bus proxy and one
/// property read instead of two proxies and three reads.
///
/// Property name confirmed directly against the actual UPower D-Bus
/// reference (upower.freedesktop.org/docs/UPower.html), not assumed --
/// an exact-match, case-sensitive D-Bus property name, and a wrong one
/// here would fail silently at runtime rather than at compile time.
/// Specifically, explicitly documented as "provided for convenience" to
/// answer "is the system as a whole currently running on battery
/// power" -- more robust than inferring this from a specific device's
/// own, more granular charging/discharging State enum, which this app
/// has no real need to reason about directly.
///
/// Races the actual query against a timeout -- confirmed as a real,
/// reported regression without this: the underlying D-Bus call had no
/// timeout of its own at all, unlike every other external call this
/// session (ensure_mounted_with_recovery, list_remote_with_recovery,
/// query_info_with_recovery, transfer_with_stall_detection). D-Bus
/// service activation can genuinely hang rather than fail fast when a
/// service is declared activatable but never actually starts or
/// responds -- plausible on a system where UPower either isn't installed
/// or isn't behaving as expected. 10 seconds, not the 60 used for actual
/// GVfs/network operations elsewhere -- this is meant to be a fast,
/// purely-local D-Bus round trip, so a real answer should arrive almost
/// immediately if UPower is working at all; timing out on this specific
/// check taking unusually long is itself diagnostic; a genuine 60-second
/// wait would mean an entire round stalls for a full minute over what's
/// meant to be a minor, best-effort convenience feature.
async fn on_battery_power(state: &Rc<AppState>) -> Option<bool> {
    match futures_util::future::select(
        std::pin::pin!(on_battery_power_inner(state)),
        std::pin::pin!(glib::timeout_future(std::time::Duration::from_secs(10))),
    ).await {
        futures_util::future::Either::Left((result, _)) => result,
        futures_util::future::Either::Right(_) => {
            eprintln!("battery check: timed out after 10s -- treating as unavailable for this check");
            None
        }
    }
}

async fn on_battery_power_inner(state: &Rc<AppState>) -> Option<bool> {
    // Split into two statements deliberately, not combined into a single
    // `if let Some(existing) = state.battery_system_bus.borrow().clone()
    // { ... } else { ... }` -- confirmed as a real, reported panic
    // without this split: the temporary Ref guard from .borrow() has its
    // own lifetime extended to cover the *entire* if-let/else construct,
    // including the else branch, even though .clone() already produced
    // an independent, owned value that no longer needs it. The else
    // branch's own .borrow_mut() a few lines below then panics with
    // "already borrowed," since the first borrow is still technically
    // alive at that point. Ending the first borrow's own statement here
    // guarantees it's fully dropped before anything else touches this
    // same RefCell.
    let cached = state.battery_system_bus.borrow().clone();
    let connection = if let Some(existing) = cached {
        existing
    } else {
        let conn = match zbus::Connection::system().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("battery check: couldn't connect to system bus: {e}");
                return None;
            }
        };
        *state.battery_system_bus.borrow_mut() = Some(conn.clone());
        conn
    };

    let upower_proxy = match zbus::Proxy::new(
        &connection,
        "org.freedesktop.UPower",
        "/org/freedesktop/UPower",
        "org.freedesktop.UPower",
    ).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("battery check: couldn't build a UPower proxy: {e}");
            return None;
        }
    };
    match upower_proxy.get_property("OnBattery").await {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("battery check: OnBattery read failed: {e}");
            None
        }
    }
}

/// Spawns the system tray (see tray.rs) if config::AppConfig::
/// tray_enabled is on -- a no-op otherwise, leaving state.tray_update_tx
/// as None, which update_status_ui's own tray block already treats as
/// "nothing to do." Called both at the process's own first activation
/// AND live from the Settings toggle -- confirmed as a real bug that it
/// only did the former originally: toggling the switch on with the app
/// already running did nothing until a restart, which isn't the
/// behavior "Launch on Startup" and "System Tray" should actually share.
/// Launch on Startup governs a FUTURE login, so "not live" was the only
/// option there; System Tray describes THIS session's current behavior,
/// so it has no equivalent excuse.
///
/// Guarded against double-spawning (tray_update_tx already Some) --
/// matters now that this can be called more than once in the same
/// session, not just once at startup.
fn spawn_tray_if_enabled(app: &libadwaita::Application, state: &Rc<AppState>) {
    if !state.config.borrow().tray_enabled || state.tray_update_tx.borrow().is_some() {
        return;
    }

    let (update_tx, update_rx) = async_channel::unbounded::<tray::TrayUpdate>();
    let (command_tx, command_rx) = async_channel::unbounded::<tray::TrayCommand>();
    *state.tray_update_tx.borrow_mut() = Some(update_tx);

    let no_account_or_connections = state.config.borrow().active_account_id.is_none()
        || !state.active_account_ready.get()
        || active_connections(state).is_empty();
    let needs_attention = !state.pending_issues.borrow().is_empty();
    let initial_status = tray::select_status(
        no_account_or_connections,
        needs_attention,
        state.paused_for_metered.get() || state.paused_for_power_save.get() || state.paused_by_user.get(),
        state.sync_state.get(),
    );
    let initial_icon_dark = state.config.borrow().tray_icon_dark;
    // Same conditions as the Status tab's own Sync Now button -- see
    // RippleTray::sync_now_enabled's own doc comment for why this can't
    // just be derived from initial_status above.
    let initial_sync_now_enabled = !no_account_or_connections && !state.paused_by_user.get() && state.sync_state.get() == status::SyncState::Idle;
    // Same three conditions already folded into initial_status's own
    // third argument above -- see RippleTray::paused's own doc comment
    // for why this needs to be its own, separate value rather than
    // derived from initial_status.
    let initial_paused = state.paused_for_metered.get() || state.paused_for_power_save.get() || state.paused_by_user.get();
    let initial_window_visible = app.active_window().is_some_and(|w| w.is_visible());

    // Own dedicated OS thread -- see tray.rs's own module doc for why
    // ksni's async-io feature still needs one rather than running
    // directly on glib's MainContext. Nothing Rc-based crosses into
    // this closure: command_tx (async_channel, genuinely Send/Sync) and
    // plain owned values are all that move, not `state` itself.
    std::thread::spawn(move || {
        use ksni::TrayMethods; // provides the .spawn() method
        async_io::block_on(async move {
            let tray = tray::RippleTray {
                status: initial_status,
                icon_dark: initial_icon_dark,
                sync_now_enabled: initial_sync_now_enabled,
                paused: initial_paused,
                window_visible: initial_window_visible,
                command_tx,
            };
            let handle = match tray.spawn().await {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("tray: couldn't start system tray service: {e}");
                    return;
                }
            };
            // Ends either when the Settings toggle is switched off
            // (Shutdown, explicit) or update_tx is dropped entirely
            // (the app shutting down, implicit) -- either way, nothing
            // more to do once this loop exits, the thread ends
            // naturally along with the tray service.
            loop {
                match update_rx.recv().await {
                    Ok(tray::TrayUpdate::Icon(new_status)) => {
                        handle.update(|t: &mut tray::RippleTray| t.status = new_status).await;
                    }
                    Ok(tray::TrayUpdate::IconColor(dark)) => {
                        handle.update(|t: &mut tray::RippleTray| t.icon_dark = dark).await;
                    }
                    Ok(tray::TrayUpdate::SyncNowEnabled(enabled)) => {
                        handle.update(|t: &mut tray::RippleTray| t.sync_now_enabled = enabled).await;
                    }
                    Ok(tray::TrayUpdate::Paused(paused)) => {
                        handle.update(|t: &mut tray::RippleTray| t.paused = paused).await;
                    }
                    Ok(tray::TrayUpdate::WindowVisibility(visible)) => {
                        handle.update(|t: &mut tray::RippleTray| t.window_visible = visible).await;
                    }
                    Ok(tray::TrayUpdate::Shutdown) | Err(_) => {
                        handle.shutdown().await;
                        break;
                    }
                }
            }
        });
    });

    // Commands (Open/Quit) are handled back on the MAIN thread's own
    // glib loop, never from the tray's own thread directly -- presenting
    // a window or touching AppState only ever happens here.
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] app,
        async move {
            while let Ok(command) = command_rx.recv().await {
                match command {
                    tray::TrayCommand::ToggleVisibility => {
                        match app.active_window() {
                            Some(window) if window.is_visible() => window.set_visible(false),
                            // Covers both "window exists but is
                            // currently hidden" (present() both shows
                            // and focuses it) and "window doesn't exist
                            // at all yet" (build_or_present_window's own
                            // app.active_window() check handles that
                            // case by building fresh).
                            _ => build_or_present_window(&app, &state),
                        }
                    }
                    tray::TrayCommand::PauseToggle => toggle_pause(&state),
                    tray::TrayCommand::SyncNow => {
                        // Same state.watcher.borrow().clone() accessor
                        // already used everywhere else a command handler
                        // needs it outside run_background's own scope
                        // (see the Status tab's own Sync Now button) --
                        // silently does nothing if the watcher isn't
                        // ready yet, same tolerance that button already
                        // has, rather than this command queueing up or
                        // erroring.
                        if let Some(watcher) = state.watcher.borrow().clone() {
                            let state = state.clone();
                            glib::MainContext::default().spawn_local(async move {
                                trigger_sync_round(&state, &watcher, false, true).await;
                            });
                        }
                    }
                    tray::TrayCommand::Quit => {
                        state.hold_guard.borrow_mut().take();
                        app.quit();
                    }
                }
            }
        }
    ));
}

/// Tears down a running tray live -- the Settings toggle's off path.
/// Takes tray_update_tx out of AppState entirely (not just sends
/// Shutdown and leaves it Some) so update_status_ui's own tray block
/// correctly goes back to treating the tray as absent immediately, and
/// so a later re-enable via spawn_tray_if_enabled isn't blocked by its
/// own double-spawn guard. A no-op if the tray wasn't running to begin
/// with (tray_update_tx already None) -- e.g. toggling the setting off
/// when it was never on this session.
fn shutdown_tray(state: &Rc<AppState>) {
    if let Some(tx) = state.tray_update_tx.borrow_mut().take() {
        let _ = tx.try_send(tray::TrayUpdate::Shutdown);
    }
}

/// PreferencesGroup/PreferencesPage don't have safe `.add()` wrappers in
/// current gtk4-rs/libadwaita-rs (checked against both 0.7.0 and the
/// newest 0.9.2 on crates.io) -- the underlying C functions exist
/// (confirmed in libadwaita-sys), they're just not wrapped yet. This is a
/// normal, narrow unsafe call for a binding gap, not a workaround to be
/// nervous about; signatures confirmed against libadwaita-sys 0.7.0's
/// actual declarations before writing this.
fn preferences_group_add(group: &libadwaita::PreferencesGroup, child: &impl IsA<gtk4::Widget>) {
    let child: &gtk4::Widget = child.upcast_ref();
    unsafe {
        libadwaita::ffi::adw_preferences_group_add(
            glib::translate::ToGlibPtr::to_glib_none(group).0,
            glib::translate::ToGlibPtr::to_glib_none(child).0,
        );
    }
}

fn preferences_page_add(page: &libadwaita::PreferencesPage, group: &libadwaita::PreferencesGroup) {
    unsafe {
        libadwaita::ffi::adw_preferences_page_add(
            glib::translate::ToGlibPtr::to_glib_none(page).0,
            glib::translate::ToGlibPtr::to_glib_none(group).0,
        );
    }
}

/// Same binding gap as the two above -- ActionRow::add_suffix has no
/// safe wrapper either (checked against libadwaita-sys 0.9.2's actual
/// declarations before writing this, same as the others). pub(crate),
/// not private -- folder_picker.rs's row-building code needs this same
/// wrapper too, and duplicating an unsafe FFI call in a second place
/// isn't better than sharing the one that's already been checked
/// against the real bindings.
pub(crate) fn action_row_add_suffix(row: &impl IsA<libadwaita::ActionRow>, child: &impl IsA<gtk4::Widget>) {
    let row: &libadwaita::ActionRow = row.upcast_ref();
    let child: &gtk4::Widget = child.upcast_ref();
    unsafe {
        libadwaita::ffi::adw_action_row_add_suffix(
            glib::translate::ToGlibPtr::to_glib_none(row).0,
            glib::translate::ToGlibPtr::to_glib_none(child).0,
        );
    }
}

/// adw_action_row_add_prefix has no safe wrapper in libadwaita-sys 0.9.2.
pub(crate) fn action_row_add_prefix(row: &impl IsA<libadwaita::ActionRow>, child: &impl IsA<gtk4::Widget>) {
    let row: &libadwaita::ActionRow = row.upcast_ref();
    let child: &gtk4::Widget = child.upcast_ref();
    unsafe {
        libadwaita::ffi::adw_action_row_add_prefix(
            glib::translate::ToGlibPtr::to_glib_none(row).0,
            glib::translate::ToGlibPtr::to_glib_none(child).0,
        );
    }
}

/// Same binding-gap pattern as the *_add helpers above -- ViewStackPage's
/// icon isn't set through the builder, so this is a plain two-call safe
/// wrapper (add_titled + set_icon_name are both real safe methods on
/// AdwViewStack/AdwViewStackPage, no FFI needed here, unlike the
/// PreferencesGroup/ActionRow gaps above).
fn view_stack_add(
    view_stack: &libadwaita::ViewStack,
    child: &impl IsA<gtk4::Widget>,
    name: &str,
    title: &str,
    icon_name: &str,
) {
    let page = view_stack.add_titled(child, Some(name), title);
    page.set_icon_name(Some(icon_name));
}

fn build_status_tab() -> (gtk4::ScrolledWindow, libadwaita::ActionRow, gtk4::TextView, libadwaita::ExpanderRow, libadwaita::ActionRow, gtk4::Image, gtk4::Button, gtk4::Button, gtk4::Button, gtk4::Button) {
    // Plain row, not clickable -- there's nothing to expand/collapse
    // anymore now that the log always shows in its own section below,
    // so this is just a title+subtitle summary.
    let status_row = libadwaita::ActionRow::builder()
        .title("Starting\u{2026}")
        .build();
    // Always activatable, even at zero conflicts -- simpler than toggling
    // it on every refresh, and harmless: opening the resolver with
    // nothing pending is just a no-op (see
    // show_sync_issues_dialog's own early return). Subtitle text
    // is the only thing refresh_conflicts_ui actually needs to update.
    //
    // Suffix icon is window-new-symbolic -- reads unambiguously as "opens
    // in another window," distinct from a menu trigger (the earlier
    // view-more-symbolic read as a hamburger/kebab menu, not a dialog
    // launcher).
    // Always visible now, not hidden at zero -- see refresh_active_transfers_ui's
    // own "None" subtitle at zero for why: an always-visible row that
    // sometimes reads "None" rather than blipping in and out of the
    // status page was a deliberate, requested change.
    let active_transfers_row = libadwaita::ExpanderRow::builder()
        .title("Active File Transfers")
        .build();

    let conflicts_row = libadwaita::ActionRow::builder()
        .title("Sync Issues")
        .activatable(true)
        // Always visible now, not hidden at zero -- see
        // refresh_conflicts_ui's own "None" subtitle at zero for why.
        .build();
    // window-new-symbolic (the previous icon here) evidently renders in
    // at least some icon themes as a rectangle with a plus sign -- reads
    // as "add a conflict" rather than "open the resolver," backwards
    // from the intent. object-select-symbolic (a plain checkmark) is a
    // long-standing, near-universal Adwaita/GNOME icon name; unlike
    // library APIs, icon-theme membership isn't something checkable
    // against crate source, so this is a best-effort pick worth
    // confirming on a real render rather than something verified the
    // way everything else in this file has been.
    //
    // Opacity-dimmed at zero, not full-opacity regardless of count -- see
    // refresh_conflicts_ui's own comment on this: the row itself stays
    // visible even with nothing to resolve (showing "None"), so the icon
    // needs its own way to distinguish that from the "issues need
    // attention" state. The click itself is still safely gated
    // regardless -- see show_sync_issues_dialog's own early return on an
    // empty list.
    let conflicts_icon = gtk4::Image::from_icon_name("object-select-symbolic");
    conflicts_icon.set_tooltip_text(Some("Resolve"));
    action_row_add_suffix(&conflicts_row, &conflicts_icon);

    // Plain ListBox + "boxed-list" instead of AdwPreferencesGroup/
    // PreferencesPage -- the same pattern already used for the
    // connections/exclusions lists elsewhere in this file. Switched to
    // deliberately, not just for consistency: PreferencesPage has its
    // own internal scrolling built in, sized on its own terms, which
    // turned out to still let Status/Conflicts get compressed under the
    // log even wrapped in an outer ScrolledWindow -- a plain ListBox's
    // minimum size more directly reflects its rows' actual minimum
    // height, with no second, less-transparent scrolling behavior
    // fighting the outer one for control of the space.
    let status_title = gtk4::Label::builder()
        .label("Status")
        .xalign(0.0)
        .css_classes(["heading"])
        .margin_start(12).margin_end(12).margin_top(12)
        .build();
    let status_list = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .margin_start(12).margin_end(12).margin_bottom(12)
        .build();
    status_list.append(&status_row);
    status_list.append(&active_transfers_row);
    status_list.append(&conflicts_row);

    // Sync Now, as an icon suffix directly on status_row, immediately to
    // the left of pause_button below (added first, since ActionRow lays
    // out suffixes left-to-right in the order they're added) -- a plain,
    // explicit "check right now" alongside the existing pause/resume
    // toggle, rather than only ever waiting on whatever the next
    // scheduled round's own timing happens to be.
    let sync_now_button = gtk4::Button::builder()
        .icon_name("view-refresh-symbolic")
        .valign(gtk4::Align::Center)
        .css_classes(["flat"])
        .tooltip_text("Sync now")
        .build();
    action_row_add_suffix(&status_row, &sync_now_button);

    // Manual pause, as an icon-toggle suffix directly on status_row --
    // not a separate row (an earlier version added one; this replaces
    // it per feedback that it belonged on the same row as the status
    // text itself, e.g. "Idle"). Icon alone toggles between
    // pause/play, updated in update_status_ui alongside status_row's
    // own title/subtitle rather than a text label needing its own
    // separate update site.
    let pause_button = gtk4::Button::builder()
        .icon_name("media-playback-pause-symbolic")
        .valign(gtk4::Align::Center)
        .css_classes(["flat"])
        .tooltip_text("Pause syncing")
        .build();
    action_row_add_suffix(&status_row, &pause_button);

    // TextView, not Label -- the more appropriate widget for read-only,
    // potentially-large, multi-line text that the user should be able
    // to select and copy from (e.g. to share a line or two directly).
    // Label has no text-selection support at all; TextView is GTK4's
    // own, genuinely intended widget for exactly this shape of content,
    // not just a Label with a property flipped on.
    let log_label = gtk4::TextView::builder()
        // No wrapping: log lines are full of long unbroken paths, and
        // wrapping them (even at WordChar granularity) turns one logical
        // line into several visual ones, which is worse to scan than
        // scrolling right for the ones that need it.
        .wrap_mode(gtk4::WrapMode::None)
        .editable(false)
        // No blinking text-entry cursor in a read-only view -- text
        // selection itself (click-drag, double-click-word, Ctrl+A, etc.)
        // works the same regardless of this.
        .cursor_visible(false)
        .left_margin(12)
        .right_margin(12)
        .top_margin(6)
        .bottom_margin(6)
        .css_classes(["monospace", "sync-log-text"])
        .build();
    apply_log_text_css();

    let log_scroller = gtk4::ScrolledWindow::builder()
        .child(&log_label)
        .hscrollbar_policy(gtk4::PolicyType::Automatic)
        .vscrollbar_policy(gtk4::PolicyType::Automatic)
        .min_content_height(60)
        .vexpand(true)
        .build();

    let export_log_button = gtk4::Button::builder()
        .icon_name("document-save-symbolic")
        .valign(gtk4::Align::Center)
        .tooltip_text("Save log to file")
        .css_classes(["flat"])
        .sensitive(false) // nothing logged yet -- update_status_ui enables it once there's real content
        .build();
    let clear_log_button = gtk4::Button::builder()
        .icon_name("edit-delete-symbolic")
        .valign(gtk4::Align::Center)
        .tooltip_text("Clear log")
        .css_classes(["flat"])
        .sensitive(false) // nothing logged yet -- update_status_ui enables it once there's real content
        .build();

    // "Log" now sits outside the card, as its own heading -- a true
    // sibling of the card, the same way "Status" sits above status_list
    // rather than inside it.
    let log_title = gtk4::Label::builder()
        .label("Log")
        .xalign(0.0)
        .css_classes(["heading"])
        .margin_start(12).margin_end(12).margin_top(12)
        .build();

    let log_footer_row = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Horizontal)
        .halign(gtk4::Align::End)
        .margin_end(6).margin_bottom(6)
        .build();
    log_footer_row.append(&clear_log_button);
    log_footer_row.append(&export_log_button);

    // "card" is a standard Adwaita style class (rounded corners, subtle
    // background) -- CSS, not something checkable against the Rust API
    // surface the way widget methods are, but it's the conventional way
    // to give a plain GtkBox the same "settings section" look
    // PreferencesGroup gives its own rows for free. Only the log
    // contents and export button live inside it now -- the heading is a
    // sibling above, not a child.
    let log_section = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .css_classes(["card"])
        .margin_start(12).margin_end(12).margin_bottom(12)
        .vexpand(true)
        .build();
    log_section.append(&log_scroller);
    log_section.append(&log_footer_row);

    let status_tab = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).spacing(6).build();
    status_tab.append(&status_title);
    status_tab.append(&status_list);
    status_tab.append(&log_title);
    status_tab.append(&log_section);

    // A plain GtkBox compresses ALL its children, including non-expand
    // ones, once there isn't enough height for everyone's natural size --
    // "non-expand" only means "don't grow to claim extra space," not
    // "never shrink below natural size." This outer ScrolledWindow is
    // the safety net: above the combined minimum height, everything
    // behaves exactly as before (log dynamically fills remaining space);
    // below it, the page scrolls instead of squeezing Status/Conflicts
    // into overlap.
    let status_scroller = gtk4::ScrolledWindow::builder()
        .child(&status_tab)
        .hscrollbar_policy(gtk4::PolicyType::Never)
        .vscrollbar_policy(gtk4::PolicyType::Automatic)
        .vexpand(true)
        .build();

    (status_scroller, status_row, log_label, active_transfers_row, conflicts_row, conflicts_icon, export_log_button, clear_log_button, sync_now_button, pause_button)
}

/// .pill turned out to be documented as a BUTTON-only style class in
/// libadwaita's own reference (listed alongside .flat/.raised/
/// .suggested-action, all button-specific) -- applying it to a plain
/// Label does nothing, since the underlying CSS selector is almost
/// certainly scoped to buttons specifically. This is the real fix: a
/// small custom rule giving the label actual rounded-pill geometry,
/// independent of that button-only class. Same "applied once per
/// process, GTK dedupes an identical provider added twice" pattern as
/// apply_log_text_css below -- colors are a first-pass guess (accent-
/// tinted, theme-adaptive via named colors rather than a hardcoded
/// value) not something rendered and checked visually here.
fn apply_auto_removable_badge_css() {
    let provider = gtk4::CssProvider::new();
    provider.load_from_string(
        ".auto-removable-badge { border-radius: 999px; padding: 1px 9px; font-size: 0.85em; background-color: alpha(@accent_bg_color, 0.15); color: @accent_color; }"
    );
    gtk4::style_context_add_provider_for_display(
        &gtk4::gdk::Display::default().expect("a display exists by the time any widget is being built"),
        &provider,
        gtk4::STYLE_PROVIDER_PRIORITY_APPLICATION,
    );
}

/// "caption" is the smallest built-in Adwaita text style class; there's
/// no smaller one to reach for, so getting the log a size below that
/// needs a real CSS provider. Applied once per process (GTK dedupes an
/// identical provider added twice, but there's no reason to even try --
/// this only ever needs adding once, at the point the label asking for
/// it is first built). 0.8em turned out too aggressive in practice --
/// 0.95em keeps it slightly denser than body text (still useful for a
/// log full of long paths) without being hard to read.
fn apply_log_text_css() {
    let provider = gtk4::CssProvider::new();
    provider.load_from_string(".sync-log-text { font-size: 0.95em; }");
    gtk4::style_context_add_provider_for_display(
        &gtk4::gdk::Display::default().expect("a display exists by the time any widget is being built"),
        &provider,
        gtk4::STYLE_PROVIDER_PRIORITY_APPLICATION,
    );
}

/// Displays a path with the user's own home directory abbreviated to
/// "~", the same, familiar convention as any Unix shell -- only when the
/// path is genuinely under the home directory; shown in full,
/// unchanged, otherwise. pub(crate), not private -- folder_picker.rs's
/// own display_path reuses this directly for the local side's live
/// breadcrumb, rather than a second, duplicate implementation of the
/// same convention that could quietly drift from this one over time.
///
/// Always trailed with "/", confirmed as a real, deliberate request --
/// this only ever displays folders (never a file), so a trailing slash
/// unambiguously marks it as one on sight, the same convention already
/// familiar from a shell's own directory listings.
pub(crate) fn display_path_with_tilde(path: &Path) -> String {
    let home = glib::home_dir();
    match path.strip_prefix(&home) {
        Ok(relative) if relative.as_os_str().is_empty() => "~/".to_string(),
        Ok(relative) => format!("~/{}/", relative.display()),
        // trim_end_matches('/') guards specifically against path itself
        // already being the bare filesystem root ("/") -- an unlikely
        // but real edge case (nothing stops a user from choosing it as
        // a connection's own local root) that would otherwise double up
        // into "//" here.
        Err(_) => format!("{}/", path.display().to_string().trim_end_matches('/')),
    }
}

/// Displays a connection's own remote subpath with a leading "/" --
/// every remote path is relative to the Nextcloud account's own root,
/// so the slash makes that explicit rather than leaving a bare,
/// ambiguous-looking relative path. Guards against a doubled "//" if the
/// stored value somehow already has its own leading slash (see
/// resolve_relative_path's own trim_start_matches('/') elsewhere in this
/// file, which suggests this isn't purely hypothetical).
/// Always both leading- and trailing-slashed -- this only ever displays
/// a folder (never a file), so a trailing slash unambiguously marks it
/// as one on sight, the same convention display_path_with_tilde's own
/// doc comment gives for the local side. trim_matches('/') first, not
/// just trim_start, so a subpath that already happens to carry a
/// trailing slash of its own doesn't double up into "//" here -- and
/// the root case (an empty subpath) collapses to that trimmed-to-empty
/// string, correctly producing a bare "/" rather than "//" once the
/// leading and trailing slashes are added back around it.
fn display_remote_subpath(subpath: &str) -> String {
    let trimmed = subpath.trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}/")
    }
}

/// Finds an existing, unrelated connection whose own local_path either
/// contains, or is contained by, `local_path` -- scoped to connections
/// under the same account specifically, matching find_overlapping_
/// remote_connection's own, identical reasoning: only one account is
/// ever active at a time in this app (active_connections filters to
/// just that one), so a connection belonging to a different account can
/// never actually walk at all -- let alone race with anything -- until
/// that other account itself becomes the active one, at which point
/// this account's own connections are the ones that stop walking
/// instead. The race this check exists to prevent (below) requires both
/// connections to be actively walking at the same moment, which is
/// structurally impossible across two different accounts given that
/// architecture; confirmed as a real, reported bug without this scoping
/// -- an inactive, dormant connection under a completely different,
/// unrelated account could block a brand new connection from ever being
/// created at all, for a local path that account was never going to
/// touch regardless.
///
/// Deliberately still a hard block, not just a warning, within the same
/// account -- confirmed as a real, deliberate decision, not an
/// oversight: two independently-walking sync_directory connections with
/// overlapping roots would mean the same local files tracked by two
/// separate, uncoordinated journals (keyed by (connection_id,
/// relative_path)), each capable of acting on them without the other
/// ever knowing -- a real race, not just redundant work. The official
/// Nextcloud desktop client's own history is the concrete cautionary
/// example: it only warns rather than blocks, and a real, reported bug
/// (nextcloud/desktop#6442) resulted -- a user proceeding past that
/// warning ended up with a child folder's own synced content looping
/// back and duplicating inside the parent's own sync target.
///
/// Path::starts_with, not a naive string-prefix check -- component-
/// aware specifically so "/home/user/Documents" and
/// "/home/user/DocumentsBackup" are correctly treated as unrelated
/// siblings, not a false-positive overlap a plain string comparison
/// would wrongly flag.
fn find_overlapping_local_connection<'a>(connections: &'a [config::SyncConnection], account_id: &str, local_path: &Path) -> Option<&'a config::SyncConnection> {
    connections.iter()
        .filter(|c| c.account_id == account_id)
        .find(|c| c.local_path.starts_with(local_path) || local_path.starts_with(&c.local_path))
}

/// Same reasoning and same mechanism as find_overlapping_local_connection
/// just above, for the remote side -- also scoped to connections under
/// the same account (that function's own doc comment covers why both
/// need this now, not just this one): two different accounts' own
/// remote namespaces are completely independent servers (or at least
/// completely independent storage within the same one), so a
/// "Documents" subpath on one account has no relationship at all to a
/// "Documents" subpath on another.
///
/// remote_subpath is a plain String, not a real filesystem Path -- but
/// wrapping it as one here is a deliberate, safe reuse of Path::
/// starts_with's own already-correct, already-tested component-aware
/// comparison, rather than reimplementing the same "split and compare
/// component by component" logic by hand just because the string in
/// question doesn't happen to describe a location on this machine's own
/// filesystem.
fn find_overlapping_remote_connection<'a>(connections: &'a [config::SyncConnection], account_id: &str, remote_subpath: &str) -> Option<&'a config::SyncConnection> {
    let candidate = Path::new(remote_subpath);
    connections.iter()
        .filter(|c| c.account_id == account_id)
        .find(|c| {
            let existing = Path::new(&c.remote_subpath);
            existing.starts_with(candidate) || candidate.starts_with(existing)
        })
}

fn build_connections_tab() -> (libadwaita::PreferencesPage, gtk4::ListBox, gtk4::ListBox) {
    // Rebuilt fresh by refresh_account_ui on every call -- starts empty,
    // filled in there. See account_section's own doc comment on
    // WindowWidgets. A ListBox with the same "boxed-list" styling
    // connections_list below already uses, not a plain Box -- by
    // request, so the active account (or the Add Account row) reads as
    // a real, set-and-changeable thing on its own rounded card, the same
    // established visual language every other list in this app already
    // uses, rather than looking like plain text sitting on the
    // background.
    let account_section = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    let account_group = libadwaita::PreferencesGroup::builder().title("Account").build();
    preferences_group_add(&account_group, &account_section);

    let connections_list = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    let connections_group = libadwaita::PreferencesGroup::builder().title("Sync Connections").build();
    preferences_group_add(&connections_group, &connections_list);

    let preferences_page = libadwaita::PreferencesPage::new();
    preferences_page_add(&preferences_page, &account_group);
    preferences_page_add(&preferences_page, &connections_group);

    (preferences_page, account_section, connections_list)
}

/// Rebuilds patterns_list's rows from config::effective_exclude_patterns
/// (built-in defaults minus any the user's explicitly removed, plus
/// whatever they've added), in display order. Every row gets a remove
/// button now -- removing a default doesn't erase it from
/// DEFAULT_EXCLUDE_PATTERNS (that stays hardcoded, immutable, hence
/// unconditionally reappearing on a fresh install or after Reset), it
/// just records the decision to skip it in the separate custom-patterns
/// file (CustomExcludePatterns::removed_defaults), the same file a
/// user's own additions already live in.
fn refresh_patterns_list(state: &Rc<AppState>, patterns_list: &gtk4::ListBox) {
    while let Some(row) = patterns_list.row_at_index(0) {
        patterns_list.remove(&row);
    }
    let defaults = config::default_exclude_patterns();
    let patterns = config::effective_exclude_patterns();
    for pattern in patterns {
        let is_default = defaults.iter().any(|d| d.pattern == pattern.pattern);
        let row_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(6)
            .margin_start(12).margin_end(12).margin_top(6).margin_bottom(6)
            .build();
        let label = gtk4::Label::builder()
            .label(&pattern.pattern)
            .xalign(0.0)
            .hexpand(true)
            .ellipsize(gtk4::pango::EllipsizeMode::End)
            .build();
        row_box.append(&label);
        // Indicator only, not a selection mechanism -- editing this
        // flag means removing and re-adding the pattern via the Add
        // Pattern dialog's own checkbox, not toggling it in place here.
        // A plain text badge rather than an icon -- a trash-can read as
        // an action ("click to delete this pattern"), not the state
        // this is actually showing, and an unlock-style icon needs its
        // own surrounding context to be legible, which a single list
        // row doesn't have room to provide. Text has no such ambiguity.
        if pattern.auto_removable {
            apply_auto_removable_badge_css();
            let badge = gtk4::Label::builder()
                .label("Auto-removable")
                .css_classes(["caption", "auto-removable-badge"])
                .valign(gtk4::Align::Center)
                .tooltip_text("Allows matching files to be automatically removed if their containing folder is removed from the other side")
                .build();
            row_box.append(&badge);
        }
        // Per-row remove button, not a separate "remove the selected
        // row" button acting on ListBox selection -- a selectable
        // ListBox row is a real, documented source of exactly this bug
        // on touchscreens (confirmed via a real report elsewhere: a
        // GtkListBox's own row-select gesture can consume a touch-drag
        // before the parent ScrolledWindow's kinetic-scroll gesture ever
        // sees it, making the list unscrollable by touch specifically --
        // not a mouse/scrollwheel problem, which is why this wasn't
        // caught earlier). Removing selection from the picture entirely
        // is the reliable fix, not trying to out-maneuver GTK's own
        // gesture disambiguation. Matches by the pattern string itself,
        // not row position -- simpler than threading an index through,
        // and patterns are already unique (Add Pattern's own validation
        // rejects a duplicate against the full effective list).
        let remove_button = gtk4::Button::builder()
            .icon_name("user-trash-symbolic")
            .valign(gtk4::Align::Center)
            .css_classes(["flat"])
            .tooltip_text(if is_default { "Remove this built-in pattern" } else { "Remove this pattern" })
            .build();
        remove_button.connect_clicked(clone!(
            #[strong] state,
            #[strong] patterns_list,
            #[strong] pattern,
            move |_| {
                let mut custom = config::load_custom_exclude_patterns();
                if is_default {
                    if !custom.removed_defaults.iter().any(|r| r == &pattern.pattern) {
                        custom.removed_defaults.push(pattern.pattern.clone());
                    }
                } else {
                    custom.added.retain(|p| p != &pattern);
                }
                let _ = config::save_custom_exclude_patterns(&custom);
                state.settings_version.set(state.settings_version.get() + 1);
                reevaluate_pending_issues(&state);
                refresh_patterns_list(&state, &patterns_list);
            }
        ));
        row_box.append(&remove_button);
        patterns_list.append(&row_box);
    }
}

/// Small prompt dialog for one new pattern -- replaces what used to be a
/// permanently-visible Entry + Add button sitting above the list at all
/// times. Same libadwaita::Window + ToolbarView + HeaderBar construction
/// the Add Folder and Resolve Conflict dialogs already use elsewhere in
/// this file, just much smaller, since there's only one field here.
fn show_add_pattern_dialog(state: Rc<AppState>, patterns_list: gtk4::ListBox) {
    let parent_window = state.window_widgets.borrow().as_ref().map(|w| w.window.clone());

    let entry = gtk4::Entry::builder()
        .placeholder_text("*.tmp, node_modules/, ...")
        .margin_top(12).margin_bottom(6).margin_start(12).margin_end(12)
        .build();
    // Nextcloud Desktop's own idea (their sync-exclude.lst `]`-prefix),
    // adopted properly this time -- see config::ExcludePattern's own
    // doc comment for why an earlier pass here rejected it and why that
    // rejection doesn't actually apply now. Off by default for a
    // pattern someone adds here themselves -- matches this app's
    // general stance of never opting someone into a destructive
    // behavior they didn't ask for (a few of the bundled DEFAULT_
    // EXCLUDE_PATTERNS entries -- pure OS/filesystem litter, never
    // meaningful content on its own -- do ship pre-marked auto_removable, but
    // that's a deliberate, documented choice about specific patterns,
    // not the default for whatever a user types in here).
    // A plain .label() string on CheckButton doesn't wrap, and this
    // label is long -- worth keeping wrapped regardless of the dialog-
    // level fix below, since a wide, non-resizable-adjacent window is
    // still worth keeping readable at any width. .child() with an
    // explicitly-wrapping Label handles that. (The dialog's own
    // resizable(false), further down, turned out to be the actual root
    // cause of the reported "won't fit 360px" bug -- see its own
    // comment; this wrap setting alone wasn't and isn't the fix for
    // that specific problem, just good practice regardless.)
    let auto_removable_label = gtk4::Label::builder()
        .label("Allow matching files to be automatically removed if their containing folder is removed from the other side")
        .wrap(true)
        .wrap_mode(gtk4::pango::WrapMode::WordChar)
        .xalign(0.0)
        .build();
    let auto_removable_check = gtk4::CheckButton::builder()
        .child(&auto_removable_label)
        .margin_start(12).margin_end(12).margin_bottom(6)
        .build();
    let error_label = gtk4::Label::builder()
        .css_classes(["error"])
        .wrap(true)
        .xalign(0.0)
        .visible(false)
        .margin_start(12).margin_end(12).margin_bottom(12)
        .build();
    let content_box = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).build();
    content_box.append(&entry);
    content_box.append(&auto_removable_check);
    content_box.append(&error_label);

    let cancel_button = gtk4::Button::builder().label("Cancel").build();
    // Starts insensitive -- nothing typed yet -- rather than letting an
    // empty Add/Enter silently do nothing the way the old inline entry
    // did; update_validity below keeps it in sync with the entry live,
    // same check commit uses right before actually adding anything (the
    // button's own sensitivity doesn't stop Enter from firing activate).
    let add_button = gtk4::Button::builder()
        .label("Add")
        .css_classes(["suggested-action"])
        .sensitive(false)
        .build();

    let header_bar = libadwaita::HeaderBar::builder()
        .title_widget(&libadwaita::WindowTitle::new("Add Pattern", ""))
        .show_start_title_buttons(false)
        .show_end_title_buttons(false)
        .build();
    header_bar.pack_start(&cancel_button);
    header_bar.pack_end(&add_button);

    let toolbar_view = libadwaita::ToolbarView::builder().content(&content_box).build();
    toolbar_view.add_top_bar(&header_bar);

    // resizable(false) removed -- almost certainly the actual root
    // cause of the whole "won't fit 360px" problem, not anything about
    // the label's own wrap settings above. A non-resizable window
    // generally locks to its natural/preferred size and never gets
    // asked to shrink below it, which would explain why fixing the
    // label's MINIMUM size calculation (twice) had no visible effect --
    // resizable(false) means GTK never even consults it. Also directly
    // matches the original complaint: a window locked to its natural
    // desktop-sized width can never adapt down to a small screen no
    // matter what else is fixed inside it.
    let mut dialog_builder = libadwaita::Window::builder()
        .title("Add Pattern")
        .default_width(360)
        .modal(true)
        .content(&toolbar_view);
    if let Some(parent) = parent_window.as_ref() {
        dialog_builder = dialog_builder.transient_for(parent);
    }
    let dialog = dialog_builder.build();
    // Same widget name as the main window -- GTK's CSS engine matches
    // ID selectors structurally, not enforcing DOM-style uniqueness the
    // way a browser would, so multiple widgets sharing this name all
    // match #ripples-themed-window simultaneously. The theme provider is
    // already registered application-wide, so this alone is enough to
    // pick it up; no separate provider or selector needed for this
    // window specifically. Was deliberately left unthemed before, by
    // original design -- now brought in by request.
    dialog.set_widget_name("ripples-themed-window");
    // None means "fine to add" -- also what an untouched/empty field
    // reports, so it shares the same disabled-Add path as a real problem
    // without flashing an error message for simply not having typed
    // anything yet. Duplicate-checking lives here (needs live config
    // state); shape checking (a pattern that can never match anything --
    // see exclude::validate_pattern's own doc comment) is exclude.rs's
    // job, same split main.rs already uses elsewhere for exclude.rs.
    let pattern_problem = move |text: &str| -> Option<String> {
        if text.is_empty() {
            return None;
        }
        let existing = config::effective_exclude_patterns();
        if existing.iter().any(|p| p.pattern == text) {
            return Some("Already in the list.".to_string());
        }
        exclude::validate_global_pattern(text).err()
    };

    let update_validity = clone!(
        #[strong] entry,
        #[strong] add_button,
        #[strong] error_label,
        #[strong] pattern_problem,
        move || {
            let text = entry.text().trim().to_string();
            match if text.is_empty() { None } else { pattern_problem(&text) } {
                None => {
                    add_button.set_sensitive(!text.is_empty());
                    error_label.set_visible(false);
                }
                Some(msg) => {
                    add_button.set_sensitive(false);
                    error_label.set_label(&msg);
                    error_label.set_visible(true);
                }
            }
        }
    );
    entry.connect_changed(clone!(#[strong] update_validity, move |_| update_validity()));

    let commit = clone!(
        #[strong] state,
        #[strong] patterns_list,
        #[strong] entry,
        #[strong] auto_removable_check,
        #[strong] dialog,
        #[strong] pattern_problem,
        move || {
            let text = entry.text().trim().to_string();
            if text.is_empty() || pattern_problem(&text).is_some() {
                // Enter fires activate regardless of add_button's
                // sensitivity -- this is what actually stops a bad
                // pattern from getting added that way. The error
                // message is already showing via update_validity;
                // nothing more to say here.
                return;
            }
            let mut custom = config::load_custom_exclude_patterns();
            custom.added.push(config::ExcludePattern { pattern: text, auto_removable: auto_removable_check.is_active() });
            let _ = config::save_custom_exclude_patterns(&custom);
            state.settings_version.set(state.settings_version.get() + 1);
            reevaluate_pending_issues(&state);
            refresh_patterns_list(&state, &patterns_list);
            dialog.close();
        }
    );
    add_button.connect_clicked(clone!(#[strong] commit, move |_| commit()));
    entry.connect_activate(move |_| commit());
    cancel_button.connect_clicked(clone!(#[strong] dialog, move |_| dialog.close()));

    dialog.present();
    entry.grab_focus();
}

fn build_settings_tab(app: &libadwaita::Application, state: &Rc<AppState>) -> (libadwaita::PreferencesPage, libadwaita::SwitchRow) {
    let page = libadwaita::PreferencesPage::new();

    // --- General ---
    let general_group = libadwaita::PreferencesGroup::builder().title("General").build();

    let startup_row = libadwaita::SwitchRow::builder()
        .title("Launch on system startup")
        .active(state.config.borrow().launch_on_startup)
        .build();
    startup_row.connect_active_notify(clone!(
        #[strong] state,
        move |row| {
            let enabled = row.is_active();
            state.config.borrow_mut().launch_on_startup = enabled;
            save_config(&state);
            // Immediate effect, not just "takes effect next launch" --
            // same idempotent function main() itself calls at startup.
            ensure_autostart(enabled);
        }
    ));
    preferences_group_add(&general_group, &startup_row);

    let theming_row = libadwaita::SwitchRow::builder()
        .title("Apply server theming")
        .active(state.config.borrow().apply_server_theming)
        // Disabled (not hidden) whenever the active account isn't
        // confirmed Nextcloud/ownCloud-capable -- see AppConfig::
        // apply_server_theming's own doc comment for why: a generic
        // WebDAV server has no theming endpoint this could ever read
        // from, so offering the toggle switched on would be misleading,
        // but the row itself staying put (by request) means it doesn't
        // silently vanish and reappear as accounts/capabilities change.
        // Checked once, here, at Settings-page build time -- refined
        // shortly after by refresh_account_ui/the capabilities check
        // itself, once a live answer is actually known.
        //
        // A cached theme from a previous session counts as "safe to
        // enable immediately" on its own, even before this session's own
        // live verification has completed -- a fresh capabilities check
        // could only ever confirm the same account is still
        // Nextcloud/ownCloud-capable that it was the last time this
        // exact theme got cached, or fail entirely and change nothing
        // either way (see push::discover_capabilities' own doc comment
        // on why failures never propagate as errors). There's no
        // realistic path where a cached theme existing was itself wrong
        // -- it's not worth a momentary disabled state at startup just
        // to wait on confirming something a real cached value already
        // implies.
        //
        // has_cached_theme alone, not also etag_capable as a fallback --
        // that flag answers a different question (is the ETag tier
        // eligible for this account), true for any confirmed Nextcloud/
        // ownCloud server regardless of whether it actually has a real
        // "theming" app installed (ownCloud's own capabilities response
        // never includes one at all -- see discover_capabilities' own
        // has_real_theming). Using it here would enable this toggle for
        // an ownCloud account with no real theme data to ever apply.
        // cached_server_themes' own entry for the active account, not
        // just "is the map non-empty" -- a different, previously-active
        // account's own leftover entry shouldn't make this toggle look
        // sensitive for an account that's never actually had its own
        // theme confirmed.
        .sensitive({
            let config = state.config.borrow();
            config.active_account_id.as_deref().is_some_and(|id| config.cached_server_themes.contains_key(id))
        })
        .build();
    theming_row.connect_active_notify(clone!(
        #[strong] state,
        move |row| {
            let now_active = row.is_active();
            state.config.borrow_mut().apply_server_theming = now_active;
            save_config(&state);
            // Applies whatever's cached immediately, same as always --
            // not deferred until the re-query below completes.
            apply_server_theme(&state);

            if now_active {
                // Also re-queries the server in the background, re-
                // applying again once fresh data actually arrives -- by
                // request. A briefly stale cached theme showing first,
                // then updating once the fetch completes, is accepted
                // here deliberately: it's the same precedent this app's
                // own startup already relies on (apply_server_theme's
                // early call applies whatever's cached before any fresh
                // capabilities check has had a chance to run), not a new
                // kind of inconsistency being introduced.
                let Some(watcher) = state.watcher.borrow().clone() else { return };
                let Some(account_id) = state.config.borrow().active_account_id.clone() else { return };
                let Some(account) = all_accounts(&state, &watcher).into_iter().find(|a| a.account_id() == account_id) else { return };
                let Some(server_url) = account.server_url() else { return };
                let state = state.clone();
                glib::MainContext::default().spawn_local(async move {
                    let Some((_, username, password)) = account.credentials(&watcher).await else {
                        if is_debug_enabled() {
                            eprintln!("[{}] [debug] theme re-fetch (settings toggle): couldn't get credentials, aborting", now_hms());
                        }
                        return;
                    };
                    let password_str = password.as_str().to_string();
                    let capabilities = push::run_blocking(move || push::discover_capabilities(&server_url, &username, &password_str)).await;
                    if is_debug_enabled() {
                        eprintln!("[{}] [debug] theme re-fetch (settings toggle): server returned theme={:?}", now_hms(), capabilities.theme);
                    }
                    if let Some(theme) = capabilities.theme {
                        state.config.borrow_mut().cached_server_themes.insert(account_id, theme);
                        save_config(&state);
                    }
                    apply_server_theme(&state);
                });
            }
        }
    ));
    preferences_group_add(&general_group, &theming_row);

    let tray_row = libadwaita::SwitchRow::builder()
        .title("Use system tray")
        .active(state.config.borrow().tray_enabled)
        .build();
    tray_row.connect_active_notify(clone!(
        #[strong] state,
        #[strong] app,
        move |row| {
            let enabled = row.is_active();
            state.config.borrow_mut().tray_enabled = enabled;
            save_config(&state);
            // Live, not next-launch-only -- confirmed as a real bug
            // that it wasn't originally: unlike Launch on Startup
            // (which genuinely can't take effect until a future login),
            // System Tray describes THIS session's current behavior,
            // so requiring a restart to see it wasn't defensible the
            // same way.
            if enabled {
                spawn_tray_if_enabled(&app, &state);
            } else {
                shutdown_tray(&state);
            }
        }
    ));
    preferences_group_add(&general_group, &tray_row);

    // Checked once, here at Settings-page construction (see
    // check_tray_available's own doc comment on why this timing, not
    // startup, sidesteps the real registration race it's otherwise
    // exposed to). Doesn't touch tray_enabled itself either way -- only
    // grays out the row and explains why, leaving the underlying
    // setting untouched rather than silently flipping it off just
    // because this particular launch couldn't confirm a host.
    glib::MainContext::default().spawn_local(clone!(
        #[strong] tray_row,
        async move {
            if !check_tray_available().await {
                tray_row.set_sensitive(false);
                tray_row.set_subtitle("No system tray detected on this desktop");
            }
        }
    ));

    // No subtitle -- deliberately kept plain, unlike most rows in this
    // tab. Sensitivity bound directly to tray_row's own active state
    // (not a manually-maintained duplicate check) so it goes grayed out
    // the instant the tray itself is switched off, and back live the
    // instant it's switched back on, with no risk of the two drifting
    // out of sync with each other.
    let icon_color_row = libadwaita::SwitchRow::builder()
        .title("Dark icon")
        .active(state.config.borrow().tray_icon_dark)
        .sensitive(state.config.borrow().tray_enabled)
        .build();
    tray_row.bind_property("active", &icon_color_row, "sensitive").sync_create().build();
    icon_color_row.connect_active_notify(clone!(
        #[strong] state,
        move |row| {
            let dark = row.is_active();
            state.config.borrow_mut().tray_icon_dark = dark;
            save_config(&state);
            // Live, same reasoning as tray_row's own toggle just above --
            // a no-op if the tray isn't currently running at all (e.g.
            // toggled while System Tray is off, though the row is
            // grayed out then anyway) since there's nothing listening
            // on the channel in that case.
            if let Some(tx) = state.tray_update_tx.borrow().as_ref() {
                let _ = tx.try_send(tray::TrayUpdate::IconColor(dark));
            }
        }
    ));
    preferences_group_add(&general_group, &icon_color_row);
    preferences_page_add(&page, &general_group);

    // --- Network ---
    let network_group = libadwaita::PreferencesGroup::builder().title("Network").build();

    let bandwidth_adjustment = gtk4::Adjustment::new(
        state.config.borrow().bandwidth_limit_kbps as f64,
        0.0, 1_000_000.0, // 0 = unlimited; a 1 GB/s ceiling is generously past anything a real connection would need, just bounding the spinner's own scroll range to something sane
        64.0, 1024.0, 0.0,
    );
    let bandwidth_row = libadwaita::SpinRow::builder()
        // Word joiners (U+2060, zero-width, invisible) around the slash
        // -- Pango's line-breaking treats '/' as a valid break
        // opportunity by default (the same reason a URL can wrap
        // mid-path), so "(KB/s)" could otherwise split across lines as
        // "(KB/" and "s)". This suppresses that specific break point
        // without changing the visible text at all.
        .title("Bandwidth limit (KB\u{2060}/\u{2060}s)")
        .subtitle("Set to 0 for no restriction")
        .adjustment(&bandwidth_adjustment)
        .build();
    bandwidth_adjustment.connect_value_changed(clone!(
        #[strong] state,
        move |adj| {
            let kbps = adj.value().round() as u32;
            state.config.borrow_mut().bandwidth_limit_kbps = kbps;
            save_config(&state);
            // Applied immediately, mid-transfer if one's in progress --
            // webdav::throttle's own doc comment covers why this is
            // safe: a change doesn't reset whatever credit's already
            // banked, it just starts refilling at the new rate from
            // this point on.
            webdav::set_bandwidth_limit(if kbps == 0 { None } else { Some(kbps as u64 * 1024) });
        }
    ));
    preferences_group_add(&network_group, &bandwidth_row);

    let current_minutes = state.config.borrow().remote_poll_interval_minutes;
    let poll_adjustment = gtk4::Adjustment::new(
        current_minutes as f64,
        config::MIN_REMOTE_POLL_INTERVAL_MINUTES as f64,
        config::MAX_REMOTE_POLL_INTERVAL_MINUTES as f64,
        1.0, 5.0, 0.0,
    );
    let poll_row = libadwaita::SpinRow::builder()
        .title("Minimum polling interval (minutes)")
        .subtitle("Time between full remote re-scans")
        .adjustment(&poll_adjustment)
        .build();
    poll_adjustment.connect_value_changed(clone!(
        #[strong] state,
        move |adj| {
            state.config.borrow_mut().remote_poll_interval_minutes = adj.value().round() as u32;
            save_config(&state);
            // Deliberately not woken up immediately -- the running
            // timeout_future_seconds sleep just finishes on its old
            // duration once, then picks up the new value next loop
            // iteration. A user dragging this slider isn't expecting
            // sub-second precision.
        }
    ));
    preferences_group_add(&network_group, &poll_row);

    let metered_row = libadwaita::SwitchRow::builder()
        .title("Pause on metered connections")
        .active(state.config.borrow().pause_on_metered)
        .build();
    metered_row.connect_active_notify(clone!(
        #[strong] state,
        move |row| {
            let enabled = row.is_active();
            state.config.borrow_mut().pause_on_metered = enabled;
            save_config(&state);
            let currently_metered = gio::NetworkMonitor::default().is_network_metered();
            if enabled && currently_metered {
                // Confirmed as a real gap: turning this on while already
                // on a metered connection didn't pause anything until
                // the next scheduled round happened to run and check for
                // itself. Set/logged directly here rather than waiting
                // for that -- same log line trigger_sync_round's own
                // metered check would produce, since this is the same
                // underlying condition, just noticed at a different
                // moment (the setting changing, not the network).
                if !state.paused_for_metered.get() {
                    state.paused_for_metered.set(true);
                    log_and_persist(&state, format!("[{}] Sync paused -- metered connection detected", now_hms()));
                }
                // Also confirmed as a real gap, distinct from the above:
                // this alone only prevents the NEXT round from starting;
                // a round already mid-flight when the setting was
                // switched on kept running to completion regardless.
                // Same cancellation mechanism the manual pause button
                // and connection-removal handler already use -- see
                // active_sync_cancels' own doc comment on AppState.
                state.active_sync_cancels.borrow_mut().clear();
                update_status_ui(&state, true);
            } else if !enabled && state.paused_for_metered.get() {
                // Confirmed as a real gap in the other direction:
                // turning this off while already paused for a metered
                // connection left it paused until the next scheduled
                // round, rather than resuming immediately -- the whole
                // point of turning the setting off.
                state.paused_for_metered.set(false);
                log_and_persist(&state, format!("[{}] Sync resumed -- metered pausing disabled", now_hms()));
                update_status_ui(&state, true);
                // Safe to call immediately, not just schedule for later:
                // paused_for_metered being true means state.syncing was
                // never set for that skipped round (trigger_sync_round's
                // own metered check runs before it claims the syncing
                // lock), so there's nothing in-flight this could
                // conflict with.
                if let Some(watcher) = state.watcher.borrow().clone() {
                    glib::MainContext::default().spawn_local(clone!(
                        #[strong] state,
                        async move { trigger_sync_round(&state, &watcher, false, false).await; }
                    ));
                }
            }
        }
    ));
    preferences_group_add(&network_group, &metered_row);

    let power_save_row = libadwaita::SwitchRow::builder()
        .title("Pause on Power Save Mode")
        .active(state.config.borrow().pause_on_power_save_mode)
        .build();
    power_save_row.connect_active_notify(clone!(
        #[strong] state,
        move |row| {
            let enabled = row.is_active();
            state.config.borrow_mut().pause_on_power_save_mode = enabled;
            save_config(&state);
            // Same "apply immediately, don't wait for the next scheduled
            // round" reasoning as metered_row's own handler -- see its
            // own doc comments for the two, separate gaps (turning a
            // pause condition on vs. off) this same shape closes. Fully
            // synchronous, same as metered_row's own gio::NetworkMonitor
            // check -- gio::PowerProfileMonitor handles its own D-Bus
            // connection internally, so there's no async round trip this
            // handler needs to wait on at all.
            let currently_power_saving = gio::PowerProfileMonitor::get_default().is_power_saver_enabled();
            if enabled && currently_power_saving {
                if !state.paused_for_power_save.get() {
                    state.paused_for_power_save.set(true);
                    log_and_persist(&state, format!("[{}] Sync paused -- Power Save Mode is active", now_hms()));
                    state.active_sync_cancels.borrow_mut().clear();
                    update_status_ui(&state, true);
                }
            } else if state.paused_for_power_save.get() {
                state.paused_for_power_save.set(false);
                log_and_persist(&state, format!("[{}] Sync resumed -- Power Save Mode is no longer active", now_hms()));
                update_status_ui(&state, true);
                if let Some(watcher) = state.watcher.borrow().clone() {
                    glib::MainContext::default().spawn_local(clone!(
                        #[strong] state,
                        #[strong] watcher,
                        async move { trigger_sync_round(&state, &watcher, false, false).await; }
                    ));
                }
            }
        }
    ));
    preferences_group_add(&network_group, &power_save_row);

    preferences_page_add(&page, &network_group);

    // --- Storage ---
    let storage_group = libadwaita::PreferencesGroup::builder().title("Storage").build();

    let current_max_mb = (state.config.borrow().max_file_size_bytes / (1024 * 1024)).max(1);
    let size_adjustment = gtk4::Adjustment::new(current_max_mb as f64, 1.0, 100_000.0, 10.0, 100.0, 0.0);
    let size_row = libadwaita::SpinRow::builder()
        .title("Skip files larger than (MB)")
        .adjustment(&size_adjustment)
        .build();
    size_adjustment.connect_value_changed(clone!(
        #[strong] state,
        move |adj| {
            let previous_max = state.config.borrow().max_file_size_bytes;
            let new_max = (adj.value().round() as u64) * 1024 * 1024;
            state.config.borrow_mut().max_file_size_bytes = new_max;
            save_config(&state);
            // Only the increase direction -- see retry_skipped_too_large's
            // own doc comment for why a decrease needs no action here at
            // all (it's already handled, separately and immediately, by
            // save_config's own call to reevaluate_pending_issues).
            if new_max > previous_max {
                retry_skipped_too_large(&state);
            }
        }
    ));
    preferences_group_add(&storage_group, &size_row);

    let trash_row = libadwaita::SwitchRow::builder()
        .title("Move removed files to Trash")
        .active(state.config.borrow().move_deleted_to_trash)
        .build();
    trash_row.connect_active_notify(clone!(
        #[strong] state,
        move |row| {
            state.config.borrow_mut().move_deleted_to_trash = row.is_active();
            save_config(&state);
        }
    ));
    preferences_group_add(&storage_group, &trash_row);
    preferences_page_add(&page, &storage_group);

    // --- Ignored Files ---
    // Global, cross-cutting complement to .no-sync above -- one list,
    // not per-directory. Seeded from DEFAULT_EXCLUDE_PATTERNS (mirrors
    // Nextcloud Desktop's own sync-exclude.lst, minus their auto-delete
    // semantics -- see config.rs), but fully editable here, unlike
    // Nextcloud's own locked defaults.
    //
    // Deliberately plain compact rows (a bare Label, not a full
    // AdwActionRow) per pattern, since with 13 defaults plus whatever's
    // been added, full-size rows added up to a lot of vertical space.
    // No longer wrapped in its own nested ScrolledWindow, though --
    // that was the original plan (cap this list's own height so the
    // rest of Settings doesn't get pushed too far down), but it turned
    // out to be a real, confirmed, upstream GTK bug on touchscreens:
    // nested ScrolledWindows have a genuine gesture-state-cancellation
    // bug (GNOME/gtk#4387) where the OUTER scroller denying a drag (it
    // does this whenever it doesn't itself have a scrollbar -- i.e.
    // whenever the rest of Settings already fits without scrolling,
    // which is common on a small phone screen) leaves the INNER
    // scroller's own gesture stuck rather than properly handing off to
    // it. AdwPreferencesPage already provides its own single, outer
    // scroll area for the whole page -- this list is a plain, un-
    // scrolled ListBox now, relying entirely on that same one scroll
    // region rather than nesting a second one inside it. Trades away
    // the height cap (a long pattern list means more scrolling to reach
    // the rest of Settings) for actually being scrollable by touch at
    // all, which matters more given this app's own primary mobile
    // target is touchscreen-only.
    let patterns_list = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    refresh_patterns_list(state, &patterns_list);

    // Add/Reset as one row of icon buttons -- Remove used to live here
    // too (a third icon acting on whichever row was selected), but that
    // selection mechanism is what broke touch-scrolling (see
    // refresh_patterns_list); each row now carries its own remove
    // button instead. .linked groups these two into a single
    // connected-looking cluster, the same visual pattern GTK uses for
    // things like GtkSpinButton's own +/- pair. tooltip_text is the
    // whole mechanism GTK needs for a hover explanation -- no manual
    // popover/timer wiring required, it's a plain widget property.
    let add_pattern_button = gtk4::Button::builder()
        .icon_name("list-add-symbolic")
        .tooltip_text("Add a pattern")
        .build();
    let reset_button = gtk4::Button::builder()
        .icon_name("view-refresh-symbolic")
        .tooltip_text("Reset to default patterns")
        .build();

    let patterns_toolbar = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Horizontal)
        .css_classes(["linked"])
        .halign(gtk4::Align::Start)
        .margin_top(6).margin_bottom(6).margin_start(12).margin_end(12)
        .build();
    patterns_toolbar.append(&add_pattern_button);
    patterns_toolbar.append(&reset_button);


    // Opens show_add_pattern_dialog rather than keeping a permanently-
    // visible Entry sitting above the list -- one more thing trimmed off
    // what's otherwise a fairly tall settings page.
    add_pattern_button.connect_clicked(clone!(
        #[strong] state,
        #[strong] patterns_list,
        move |_| show_add_pattern_dialog(state.clone(), patterns_list.clone())
    ));

    reset_button.connect_clicked(clone!(
        #[strong] state,
        #[strong] patterns_list,
        move |_| {
            // Clears both halves of the custom-patterns file: any
            // removed_defaults overrides (so every built-in default
            // reappears) and any of the user's own additions (so this
            // is a genuine return to "just the defaults," not only a
            // partial reset of one half).
            let _ = config::save_custom_exclude_patterns(&config::CustomExcludePatterns::default());
            state.settings_version.set(state.settings_version.get() + 1);
            reevaluate_pending_issues(&state);
            refresh_patterns_list(&state, &patterns_list);
        }
    ));

    let patterns_box = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).build();
    patterns_box.append(&patterns_list);
    patterns_box.append(&patterns_toolbar);

    let patterns_group = libadwaita::PreferencesGroup::builder()
        .title("Ignored filename patterns")
        .description(
            "Supports \"*\" and \"?\" wildcards. A trailing \"/\" matches directories only."
        )
        .build();
    preferences_group_add(&patterns_group, &patterns_box);
    preferences_page_add(&page, &patterns_group);

    (page, theming_row)
}

/// Toggles paused_by_user and everything that needs to happen alongside
/// it -- shared by the Status tab's own pause button and the tray
/// menu's "Pause Syncing"/"Resume Syncing" item, so both trigger
/// identical behavior rather than two, subtly-diverging copies of the
/// same logic.
fn toggle_pause(state: &Rc<AppState>) {
    let now_paused = !state.paused_by_user.get();
    state.paused_by_user.set(now_paused);
    if now_paused {
        log_and_persist(state, format!("[{}] Sync paused -- paused by user", now_hms()));
        // Without this, Pause only prevented the NEXT round from
        // starting -- a round already mid-flight when the button
        // was clicked kept running to completion regardless,
        // which read as "Pause did nothing" even though it was
        // working exactly as designed at the time. Clearing
        // every entry here is the same mechanism the connection-
        // removal handler already uses (see active_sync_cancels'
        // own doc comment on AppState): dropping each sender
        // wakes run_sync_round's own recv() for that connection,
        // which drops whatever sync_target future was in flight,
        // which cancels the underlying GIO operation via its
        // Cancellable-on-Drop behavior -- a real, immediate stop
        // for every currently-active connection at once, not
        // just the next round.
        state.active_sync_cancels.borrow_mut().clear();
    } else {
        log_and_persist(state, format!("[{}] Sync resumed -- resumed by user", now_hms()));
        // Cleared here, alongside paused_by_user itself -- see this
        // flag's own doc comment on AppState for why it can never
        // meaningfully outlive the pause it's describing.
        state.paused_reason_restored_account.set(false);
        // Distinct from the metered-detection log line itself --
        // this isn't trigger_sync_round noticing a NEW metered
        // transition, it's clicking Resume while a separate,
        // still-active metered pause means the sync isn't
        // actually about to resume at all. Without this, the log
        // just said "resumed by user" while nothing happened,
        // with no visible reason why -- confirmed as a real,
        // reported gap, not a hypothetical one.
        if state.paused_for_metered.get() {
            log_and_persist(state, format!("[{}] Sync remains paused -- metered connection still detected", now_hms()));
        }
        // Manual unpause is one of the two global triggers for
        // clearing every permission_error flag at once (the
        // other is session start, in run_background) --
        // deliberately NOT the metered-connection pause/unpause,
        // which has no bearing on whether a permission actually
        // changed. Same flat, whole-journal sweep either way.
        if let Err(e) = journal::clear_all_permission_errors(&state.journal) {
            eprintln!("couldn't clear permission-error flags on unpause: {e}");
        }
    }
    update_status_ui(state, true);
    // Immediately catch up on anything that would have synced
    // while paused, rather than waiting for the next scheduled
    // round -- same reasoning as the network_changed handler
    // above. state.watcher, not a local `watcher` -- GoaWatcher
    // is created once in run_background and stored there, not
    // threaded through every function as an explicit parameter;
    // None here just means it hasn't finished connecting to GOA
    // yet, in which case there's nothing to catch up on anyway.
    if !now_paused {
        if let Some(watcher) = state.watcher.borrow().clone() {
            glib::MainContext::default().spawn_local(clone!(
                #[strong] state,
                async move { trigger_sync_round(&state, &watcher, false, false).await; }
            ));
        }
    }
}

fn build_or_present_window(app: &libadwaita::Application, state: &Rc<AppState>) {
    if let Some(window) = app.active_window() {
        window.present();
        return;
    }

    let (status_page, status_row, status_log_label, active_transfers_row, conflicts_row, conflicts_icon, export_log_button, clear_log_button, sync_now_button, pause_button) = build_status_tab();
    let (connections_page, account_section, connections_list) = build_connections_tab();
    let (settings_page, theming_row) = build_settings_tab(app, state);

    let view_stack = libadwaita::ViewStack::new();
    view_stack_add(&view_stack, &status_page, "status", "Session", "network-transmit-receive-symbolic");
    view_stack_add(&view_stack, &connections_page, "connections", "Connections", "folder-symbolic");
    view_stack_add(&view_stack, &settings_page, "settings", "Settings", "preferences-system-symbolic");

    // AdwViewSwitcherTitle is deprecated as of libadwaita 1.4 in favor of
    // an explicit AdwBreakpoint driving a plain AdwViewSwitcher + a
    // WindowTitle fallback. Same adaptive behavior as before -- header
    // bar shows the switcher on wide windows, falls back to a plain
    // title with the switcher bar taking over below the breakpoint --
    // just wired explicitly instead of via the old widget's internal
    // title-visible logic.
    let view_switcher = libadwaita::ViewSwitcher::builder()
        .stack(&view_stack)
        .policy(libadwaita::ViewSwitcherPolicy::Wide)
        .build();
    let window_title = libadwaita::WindowTitle::new("Ripples", "");
    let view_switcher_bar = libadwaita::ViewSwitcherBar::builder().stack(&view_stack).build();

    let header_bar = libadwaita::HeaderBar::builder().title_widget(&view_switcher).build();

    // Left side (pack_start), not right -- the right edge already has
    // real window controls (minimize/maximize/close), and stacking a
    // fourth thing there got visibly crowded. Left is otherwise empty at
    // any window width, so this is a straightforward move, not a
    // trade-off.
    let primary_menu = gio::Menu::new();
    primary_menu.append(Some("About Ripples"), Some("app.about"));
    // Just "Quit" here -- in a menu, next to "About," the context already
    // makes it obvious this is app-level, not a plain window close. If
    // this ever also becomes a standalone button (Settings tab, or a
    // future tray icon), that's the point to say more, e.g. "Quit
    // Ripples" or similar -- a bare button out of context needs the
    // extra word a menu item doesn't.
    primary_menu.append(Some("Quit"), Some("app.quit"));
    let menu_button = gtk4::MenuButton::builder()
        .icon_name("open-menu-symbolic")
        .menu_model(&primary_menu)
        .tooltip_text("Main Menu")
        .build();
    header_bar.pack_start(&menu_button);

    let toolbar_view = libadwaita::ToolbarView::builder().content(&view_stack).build();
    // top_bar_style is no longer set here -- moved into apply_server_theme
    // so it can react live to the server-theming toggle (Raised when
    // theming is on and the header bar has its own distinct color;
    // Flat, the plain default, when theming is off), rather than being
    // fixed once at construction time regardless of the toggle's state.
    let pause_banner = libadwaita::Banner::builder()
        .title("Ripples has been paused to allow the newly restored account\u{2019}s Sync Connections to be verified. Click the \u{25b6} button when ready.")
        .build();
    toolbar_view.add_top_bar(&header_bar);
    toolbar_view.add_top_bar(&pause_banner);
    toolbar_view.add_bottom_bar(&view_switcher_bar);

    let window = libadwaita::ApplicationWindow::builder()
        .application(app)
        .title("Ripples")
        .default_width(360)
        .default_height(600)
        .content(&toolbar_view)
        .build();
    // Reports every actual visibility change to the tray, regardless of
    // what triggered it -- the tray's own ToggleVisibility command, this
    // window's own close button (see connect_close_request below, which
    // hides rather than destroys), anything else. connect_visible_notify
    // matches the same connect_<property>_notify pattern already used
    // elsewhere in this app (e.g. StyleManager's own connect_dark_notify)
    // for a GObject boolean property's notify signal. Keeps the tray's
    // Show/Hide Ripples label correct no matter which path actually
    // changed visibility, rather than only the tray's own toggle command
    // knowing to report it.
    {
        let state = state.clone();
        window.connect_visible_notify(move |w| {
            if let Some(tx) = state.tray_update_tx.borrow().as_ref() {
                let _ = tx.try_send(tray::TrayUpdate::WindowVisibility(w.is_visible()));
            }
        });
    }
    // set_widget_name + a CSS ID selector (#ripples-themed-window),
    // rather than a CSS class -- this follows a proven pattern from a
    // sister project (Cardigan) that does the same kind of per-widget
    // dynamic CSS override without ever touching the widget's class
    // list at all. A widget's *name* (used for ID selectors, id="..."
    // in CSS terms) is a completely separate GTK property from its
    // *classes* (class="..."), so nothing that manipulates CSS classes
    // -- now, or anything added later -- can ever collide with or
    // silently replace it. This replaces an earlier fix that switched
    // .css_classes([...]) on the builder (which replaces a widget's
    // entire class list wholesale) to add_css_class (which appends) --
    // that was a real, likely-correct fix for a genuine, reproducible
    // window-transparency bug (suspected cause: ApplicationWindow
    // relying on some default internal class for its own opaque
    // background painting, which .css_classes([...]) was silently
    // wiping out), but switching to an ID selector removes the class of
    // bug entirely rather than just avoiding this one instance of it --
    // there's no "replace vs. append" footgun left to step on at all,
    // since nothing here touches classes anymore.
    window.set_widget_name("ripples-themed-window");
    // Scopes apply_server_theme's own CSS override to just this window
    // -- a plain :root-level override (the shape libadwaita's own docs
    // demonstrate) would also bleed into every other top-level window
    // this app opens (the folder pickers, the New Folder dialog), which
    // was a deliberate, agreed design constraint from the start, not an
    // afterthought. The unique widget name provides this same scoping
    // on its own now, without needing a class for it.

    // Applies whatever was cached from a previous session immediately,
    // including registering state.theme_css_provider to the display for
    // the first time -- see AppConfig::cached_server_themes' own doc
    // comment for why this can't just wait for a fresh capabilities
    // check to complete first, and apply_server_theme's own doc comment
    // for why registration doesn't need a separate call site of its own
    // here anymore.
    apply_server_theme(state);

    // Re-applies whenever dark/light appearance changes live -- needed
    // now that apply_server_theme's card-bg/fg values depend on it
    // (previously nothing in this app reacted to appearance changes at
    // all, since nothing used to depend on it). connect_dark_notify
    // matches the same connect_<property>_notify pattern already used
    // elsewhere in this app (e.g. theming_row's own connect_active_notify
    // above) for a GObject boolean property's notify signal.
    {
        let state = state.clone();
        libadwaita::StyleManager::default().connect_dark_notify(move |_| {
            apply_server_theme(&state);
        });
    }

    // application_icon references the app's own icon-theme name --
    // installed under that name via the hicolor theme layout in
    // assets/icons/app/ (see Cargo.toml's [package.metadata.deb] assets
    // list for the actual install-path mapping); this dialog just needs
    // the name, not a path, since icon-theme lookup resolves it at
    // runtime the same way the window manager/app grid do.
    // AdwAboutDialog, not the deprecated AdwAboutWindow -- confirmed via
    // pkg-config on both target devices (FLX1s and the NixOS desktop:
    // GTK 4.22.4, libadwaita 1.9.0) that the real requirement here
    // (roughly libadwaita 1.6+/GTK 4.18+, see Cargo.toml's "gtk_v4_18"
    // feature comment) is comfortably cleared with real margin on both,
    // not just technically present. AboutDialog is the current,
    // non-deprecated widget going forward.
    let about_action = gio::SimpleAction::new("about", None);
    about_action.connect_activate(clone!(
        #[strong] window,
        move |_, _| {
            let about = libadwaita::AboutDialog::builder()
                .application_name("Ripples")
                .application_icon("net.dunevent.Ripples")
                .version(env!("CARGO_PKG_VERSION")) // reads straight from Cargo.toml -- can't drift out of sync with the real build
                .comments("An adaptive Nextcloud sync client built for GNOME Shell and Phosh")
                .website("https://dev.dunevent.net/aj/Ripples")
                // Ripples itself is GPLv3 -- gtk4::License::Gpl30
                // gives AdwAboutDialog's own built-in license display
                // (standard boilerplate + full text on request), not
                // something this code needs to reproduce itself.
                .license_type(gtk4::License::Gpl30)
                // AdwAboutDialog auto-collapses to one page when the
                // Details page would only ever hold the website link,
                // but the Donate/Fediverse links below push it into a
                // real two-page (main + Details) layout instead -- see
                // the widget's own doc comment on add_link for that
                // exact "collapses to one page if there's nothing more
                // than website" rule. Given that, the default content
                // height (sized comfortably for the short main page)
                // left the Details page's three link rows looking
                // cramped. A taller fixed height fixes both pages at
                // once, since it's one shared dialog size, not a
                // per-page one.
                .content_height(480)
                .build();
            about.add_link("Donate", "https://paypal.me/ajdunevent");
            about.add_link("Fediverse", "https://social.dunevent.net/@aj");
            // MoreWaita's own license (GPLv3) requires attribution on
            // redistribution -- these are their actual files (see
            // tray.rs), not something adapted/modified here.
            about.add_credit_section(
                Some("Credits"),
                &["Tray status icons from the MoreWaita icon theme, licensed under GPLv3 https://github.com/somepaulo/MoreWaita"],
            );
            about.add_acknowledgement_section(
                None,
                &[
                    "Several design decisions were informed by studying the Nextcloud desktop client's own open-source implementation.",
                ],
            );
            about.present(Some(&window));
        }
    ));
    app.add_action(&about_action);

    export_log_button.connect_clicked(clone!(
        #[strong] state,
        #[strong] window,
        move |_| {
            // Flushed synchronously, right here, before the file picker
            // even opens -- not left to whatever's already pending for
            // the next timer fire. See flush_disk_log_now's own doc
            // comment for why: this on-disk file is now the single,
            // canonical session record (not a separate copy assembled
            // from an in-memory structure), so exporting it without
            // this would risk handing over a file missing up to the
            // last 30 seconds of activity.
            //
            // One known, deliberately-accepted gap: disk_log_path can
            // itself rotate mid-session (see disk_log::should_rotate)
            // for a session that runs long enough to cross that
            // threshold -- rare in practice (20MB of log text), but in
            // that case this exports only the current, most-recent
            // segment, not earlier, already-rotated-out files from
            // the same logical session. Not handled here (e.g. by
            // concatenating every session-log file) since it's enough
            // of an edge case not to be worth the added complexity
            // right now.
            flush_disk_log_now(&state);
            let path = state.disk_log_path.borrow().clone();

            let file_dialog = gtk4::FileDialog::builder()
                .title("Save Log")
                .initial_name("sync-log.txt")
                .build();
            glib::MainContext::default().spawn_local(clone!(
                #[strong] window,
                async move {
                    let Ok(dest) = file_dialog.save_future(Some(&window)).await else { return }; // cancelled -- fine
                    // Read as plain bytes from the real path rather than
                    // going through gio for the source side -- this is
                    // always a local file this process itself just
                    // wrote, never anything remote/virtual, so there's
                    // nothing gio's own File abstraction would add here.
                    let contents = match std::fs::read(&path) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("couldn't read on-disk session log for export: {e}");
                            return;
                        }
                    };
                    if let Err((_, e)) = dest.replace_contents_future(
                        contents, None, false,
                        gio::FileCreateFlags::NONE,
                    ).await {
                        eprintln!("couldn't save log: {e}");
                    }
                }
            ));
        }
    ));

    clear_log_button.connect_clicked(clone!(
        #[strong] state,
        #[strong] clear_log_button,
        move |_| {
            // Confirmed as a real gap: this button is icon-only, with
            // its explanation living solely in a tooltip -- which
            // doesn't exist at all on a touchscreen with no hover.
            // Someone could tap an unlabeled button and irreversibly
            // wipe the whole session log with no idea what it even did.
            // AlertDialog, not a Popover, for the confirmation itself --
            // a Popover has to fit within the screen space around its
            // own anchor widget, and that's a real, already-confirmed
            // source of a different bug this session (the folder
            // picker's path label growing a Popover past what a small
            // screen could actually show); AlertDialog is a proper
            // modal dialog with no such constraint. Clearly labeled
            // responses ("Cancel"/"Clear Log"), not a bare "Yes"/"No" --
            // less ambiguous out of context, and matches this app's own
            // established HIG-following style elsewhere.
            let dialog = libadwaita::AlertDialog::new(
                Some("Clear Session Log?"),
                None,
            );
            dialog.add_responses(&[("cancel", "Cancel"), ("clear", "Clear Log")]);
            dialog.set_response_appearance("clear", libadwaita::ResponseAppearance::Destructive);
            // Dismissing via Escape or tapping outside counts as Cancel,
            // not Clear -- the safe, non-destructive default either way
            // someone backs out without picking a button explicitly.
            dialog.set_close_response("cancel");
            dialog.set_default_response(Some("cancel"));

            glib::MainContext::default().spawn_local(clone!(
                #[strong] state,
                #[strong] clear_log_button,
                async move {
                    let response = dialog.choose_future(Some(&clear_log_button)).await;
                    if response == "clear" {
                        state.status_log.borrow_mut().clear();
                        // update_status_ui no longer touches the log
                        // TextView's own content at all (see append_
                        // status_log_line's own doc comment) -- this has
                        // to explicitly reset it back to the empty-log
                        // placeholder itself, the one other legitimate
                        // full-buffer replace in this file besides
                        // populate_status_log_initial's own, equally
                        // one-time use at window construction.
                        if let Some(status_log_label) = state.window_widgets.borrow().as_ref().map(|w| w.status_log_label.clone()) {
                            status_log_label.buffer().set_text("Nothing logged yet.");
                        }
                        update_status_ui(&state, true);
                    }
                }
            ));
        }
    ));

    sync_now_button.connect_clicked(clone!(
        #[strong] state,
        move |_| {
            let Some(watcher) = state.watcher.borrow().clone() else { return }; // not connected yet
            let state = state.clone();
            glib::MainContext::default().spawn_local(async move {
                trigger_sync_round(&state, &watcher, false, true).await;
            });
        }
    ));
    pause_button.connect_clicked(clone!(
        #[strong] state,
        move |_| {
            toggle_pause(&state);
        }
    ));

    // Same 550sp threshold the old ViewSwitcherTitle used internally:
    // below it, swap the header bar's title widget for a plain title and
    // reveal the bottom switcher bar instead.
    let breakpoint = libadwaita::Breakpoint::new(libadwaita::BreakpointCondition::new_length(
        libadwaita::BreakpointConditionLengthType::MaxWidth,
        550.0,
        libadwaita::LengthUnit::Sp,
    ));
    breakpoint.add_setter(&header_bar, "title-widget", Some(&window_title.to_value()));
    breakpoint.add_setter(&view_switcher_bar, "reveal", Some(&true.to_value()));
    window.add_breakpoint(breakpoint);

    *state.window_widgets.borrow_mut() = Some(WindowWidgets {
        window: window.clone(),
        account_section,
        theming_row: theming_row.clone(),
        connections_list,
        status_row,
        status_log_label,
        active_transfers_row,
        conflicts_row: conflicts_row.clone(),
        conflicts_icon,
        export_log_button,
        clear_log_button,
        sync_now_button,
        pause_button,
        toolbar_view,
        pause_banner,
    });

    // apply_server_theme's own earlier call (see above, before this
    // window existed at all) ran before window_widgets was populated,
    // so its top_bar_style logic -- which needs widgets.toolbar_view to
    // exist -- safely no-op'd rather than actually applying anything.
    // Re-running now, with window_widgets actually in place, is what
    // makes the header bar's style correct from first launch instead of
    // only catching up whenever some later trigger (toggling theming,
    // a dark/light change) happens to call this again.
    apply_server_theme(state);

    // Opens the resolver against whatever's currently in
    // state.pending_issues -- always wired, since the row itself is
    // always activatable (see build_status_tab); with zero conflicts
    // pending this is just a no-op via the dialog's own early return.
    conflicts_row.connect_activated(clone!(
        #[strong] state,
        move |_| {
            let Some(watcher) = state.watcher.borrow().clone() else { return }; // not connected yet
            show_sync_issues_dialog(state.clone(), watcher, state.pending_issues.borrow().clone(), 0);
        }
    ));

    // Closing the window normally just closes the window -- app.hold()
    // in activate() keeps the process (and background sync loop)
    // running with zero windows open, by design, since this is meant to
    // be a background sync app first. But that only makes sense if
    // there's actually something to sync in the background: with no
    // active account, an active account with no enabled folder
    // connections, or an active account that exists but currently isn't
    // ready (Files disabled in Online Accounts, most concretely -- see
    // AccountSource::is_ready's own doc comment), run_background has
    // nothing to do and there's no reason to leave a window-less process
    // sitting around. In that case, release the hold here so the window
    // closing behaves like an ordinary app quit instead --
    // Propagation::Proceed lets the window actually close; releasing the
    // hold first is what additionally lets the app quit once it does.
    // With active connections on a ready account, the window is hidden
    // instead (see the handler's own doc comment on why
    // Propagation::Proceed alone was a real, live bug here, not just a
    // stale comment).
    //
    // Deliberately only checked here, at the moment the user actually
    // tries to close the window -- not continuously watched while
    // already hidden. An account going from ready to not-ready while the
    // window is already closed (Files switched off mid-session, say)
    // doesn't retroactively quit an already-backgrounded app -- by
    // request: a system setting changing out from under a background
    // process shouldn't silently kill it, the tray icon already
    // reflects the problem (see update_status_ui's own tray-icon logic),
    // and that's enough. This is the exact same, already-accepted
    // scope/limitation active_connections().is_empty() alone already had
    // here before this account_ready check existed, not a new one.
    window.connect_close_request(clone!(
        #[strong] state,
        move |window| {
            if active_connections(&state).is_empty() || !state.active_account_ready.get() {
                *state.hold_guard.borrow_mut() = None;
                return glib::Propagation::Proceed;
            }
            // Hides rather than lets the close proceed -- confirmed as a
            // real, live bug without this: Propagation::Proceed always
            // let the window actually destroy, contradicting this exact
            // comment (which described the *intended* behavior, not
            // what the code actually did). Destroying rather than hiding
            // meant window.connect_visible_notify's own tray-label sync
            // never fired for this specific path -- the tray's own Hide/
            // Show label went stale, since as far as it knew the window
            // was still there. set_visible(false) here fixes that
            // directly: it's the exact same signal build_or_present_
            // window's own present()/hide() calls already use, so it's
            // indistinguishable to the rest of the app from any other
            // hide, tray-triggered or otherwise.
            window.set_visible(false);
            glib::Propagation::Stop
        }
    ));

    // Confirmed as a real bug: a freshly-built window (this whole
    // function, not the early-return present() above) starts out
    // showing nothing but blank/default placeholder state -- nothing
    // here was pushing real data into it. It only got populated once
    // something ELSE happened to trigger update_status_ui/
    // refresh_account_ui on its own (a sync round completing, a GOA
    // event). On first launch that's nearly instant, since
    // run_background's poll loop runs its very first round immediately,
    // before its first sleep -- but reopening via the tray without
    // quitting means the background loop is already mid-wait on its own
    // schedule, so the new window could sit there showing nothing real
    // for however long was left on that wait, up to the full poll
    // interval. Pushing both refreshes here, right after the widgets
    // exist, means the window always opens showing current reality
    // immediately, regardless of where the background loop happens to
    // be in its own cycle.
    update_status_ui(state, true);
    if let Some(watcher) = state.watcher.borrow().clone() {
        refresh_account_ui(state, &watcher);
    }
    // Same reasoning, same fix, as the two calls just above -- the log
    // TextView needs its own, one-time initial population here too,
    // since build_status_tab itself has no access to state at all (it
    // takes no parameters) and couldn't have set real content when it
    // first built the widget. Every update after this point is append_
    // status_log_line's own, single-line prepend instead -- this is the
    // only other place a full buffer replace is legitimate.
    if let Some(status_log_label) = state.window_widgets.borrow().as_ref().map(|w| w.status_log_label.clone()) {
        populate_status_log_initial(state, &status_log_label);
    }

    window.present();
}

/// Spawns gnome-control-center directly to the Online Accounts panel.
/// This is deliberately simple *because* we're a plain .deb, not a
/// Flatpak -- no sandbox to punch a D-Bus-activation hole through, so a
/// normal subprocess spawn is the right tool, not the org.gnome.
/// ControlCenter D-Bus Activate dance a sandboxed build would need.
fn open_online_accounts_panel() {
    match std::process::Command::new("gnome-control-center").arg("online-accounts").spawn() {
        Ok(_) => {}
        Err(e) => eprintln!("couldn't launch gnome-control-center: {e}"),
    }
}

async fn run_background(state: Rc<AppState>) {
    let (tx, rx) = async_channel::unbounded();
    let watcher = match GoaWatcher::new(tx).await {
        Ok(w) => Rc::new(w),
        Err(e) => {
            eprintln!("couldn't connect to GNOME Online Accounts: {e}");
            return;
        }
    };
    *state.watcher.borrow_mut() = Some(watcher.clone());

    refresh_account_ui(&state, &watcher);

    // Confirmed as a real, reported gap: update_status_ui is otherwise
    // only ever called in response to a real SyncEvent -- but both
    // SyncAction::Noop (already in sync, the single most common outcome
    // for a large, previously-synced tree) and the size+mtime deferral
    // path (see SyncStatus::PendingVerification's own doc comment)
    // deliberately emit no event at all. That meant the "discovered;
    // processed" progress display could go stale for long stretches
    // during exactly the scenario it exists to make visible -- a huge
    // tree, mostly already in sync, moving through it with real progress
    // the UI simply never got told about.
    //
    // This used to be a permanent loop living right here, waking every
    // 500ms forever and checking sync_state to decide whether there was
    // anything worth doing -- meaning the app kept polling on a fixed
    // schedule for the entire life of the process, even across long
    // stretches with no round running at all. It's spawned fresh from
    // inside trigger_sync_round now instead, only for the lifetime of
    // one actual round -- see that function's own spawn of
    // spawn_progress_refresh_loop for where this moved to.

    // Also one-time, also deliberately here rather than in any recurring
    // loop: session start is one of the two global triggers for
    // clearing every permission_error flag at once (the other is manual
    // unpause -- see pause_button's own handler). Neither is about any
    // one specific item, so this is a flat sweep across the whole
    // journal, not scoped to a connection. A fresh launch is exactly the
    // kind of "something might have changed since we last checked"
    // moment worth spending one real retry per still-broken item on --
    // if a permission genuinely hasn't changed, is_permission_error will
    // just flag it right back on the very next attempt.
    if let Err(e) = journal::clear_all_permission_errors(&state.journal) {
        eprintln!("couldn't clear permission-error flags at startup: {e}");
    }

    // Status/log event bus: sync.rs (via run_sync_round) sends SyncEvents
    // on state.status_events_tx as it works; this loop is the one place
    // that folds them into sync_state + status_log and repaints the
    // Status tab. Receiver is taken once here -- see status_events_rx's
    // own doc comment on AppState.
    let status_rx = state.status_events_rx.borrow_mut().take()
        .expect("status_events_rx taken exactly once, at startup");
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] watcher,
        async move {
            while let Ok(event) = status_rx.recv().await {
                apply_status_event(&state, &watcher, event);
            }
        }
    ));

    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] watcher,
        async move {
            while let Ok(_event) = rx.recv().await {
                // Every event type ends up needing the same thing: re-read
                // watcher.accounts() and rebuild the list. No need to
                // branch on GoaEvent's variant for that.
                refresh_account_ui(&state, &watcher);
                // Also fires an immediate round, not just a UI refresh --
                // confirmed as a real, reported gap without this: an
                // account going from not-ready back to ready (Files
                // re-enabled in Online Accounts, most concretely) only
                // ever updated the account list, leaving the account
                // showing correctly as selectable/ready again while
                // actually syncing nothing until whatever periodic timer
                // happened to fire next -- which could be minutes away,
                // not the immediate response most people would expect
                // right after fixing the thing that was blocking sync.
                // Safe to call unconditionally, the same "don't branch on
                // GoaEvent's variant" reasoning as refresh_account_ui's
                // own call just above -- trigger_sync_round's own busy
                // guard and active_connections()/is_ready() checks
                // already make this a cheap, correct no-op for every
                // event that doesn't actually mean "something new is
                // ready," not something this handler needs to work out
                // for itself first.
                spawn_immediate_sync_round(&state, &watcher);
            }
        }
    ));

    // notify_push's own password-request bridge -- see
    // push::PasswordRequest's own doc comment for why this round-trip
    // exists at all. One shared channel for every connection's own push
    // listener thread; this loop answers each request by calling the
    // async, glib-driven get_password_for_http_auth directly, which only
    // actually completes when polled here, on this MainContext, not from
    // any of those listener threads themselves.
    let (password_requests_tx, password_requests_rx) = async_channel::unbounded::<push::PasswordRequest>();
    *state.push_password_requests_tx.borrow_mut() = Some(password_requests_tx);
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] watcher,
        async move {
            while let Ok(request) = password_requests_rx.recv().await {
                // By request -- see push::PasswordRequestError's own doc
                // comment for the full reasoning. Checked here, not as a
                // separate signal threaded in from wherever an account
                // might become inactive: this reconnect attempt was
                // already going to ask the main thread for a password
                // regardless, so answering "you're not active anymore"
                // here -- instead of either a real password or a generic
                // failure -- costs nothing extra, and is what actually
                // lets that listener thread's own loop notice and exit
                // for good, rather than retrying forever against an
                // account nothing here will ever act on again.
                let is_active = state.config.borrow().active_account_id.as_deref() == Some(request.account_id.as_str());
                let result = if !is_active {
                    Err(push::PasswordRequestError::AccountNoLongerActive)
                } else {
                    // all_accounts + AccountSource::credentials(), not
                    // watcher.get_password_for_http_auth alone --
                    // confirmed as a real, reported bug without this,
                    // the same class as sync_target's own, already-fixed
                    // one: get_password_for_http_auth is a GOA-only
                    // D-Bus call, so this request could never succeed
                    // for a direct account's own listener thread, on any
                    // reconnect attempt, ever -- driving it into
                    // permanent retry and eventually reporting notify_
                    // push as unavailable even on a server where it
                    // genuinely, confirmedly works (the same server, the
                    // same remote account, just reached through this
                    // app's own GOA-only credential path instead of the
                    // correct, already-established account-agnostic
                    // one).
                    match all_accounts(&state, &watcher).into_iter().find(|a| a.account_id() == request.account_id) {
                        Some(account) => account.credentials(&watcher).await
                            .map(|(_, _, password)| password)
                            .ok_or_else(|| push::PasswordRequestError::Other("account's credentials aren't ready yet".to_string())),
                        None => Err(push::PasswordRequestError::Other("account no longer known".to_string())),
                    }
                };
                let _ = request.respond_to.send(result).await;
            }
        }
    ));

    // notify_push's own event stream -- one shared channel every
    // connection's own listener thread (spawned in run_sync_round, once
    // capabilities are discovered) sends (account_id, PushMessage)
    // through. Connected/Disconnected update notify_push_status and
    // log the capability change; FileChanged triggers an immediate round
    // for every connection actually using that account, the same way a
    // local FileMonitor callback already does for the local side.
    let (push_events_tx, push_events_rx) = async_channel::unbounded::<(String, push::PushMessage)>();
    *state.push_events_tx.borrow_mut() = Some(push_events_tx);
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] watcher,
        async move {
            while let Ok((account_id, message)) = push_events_rx.recv().await {
                let connections_for_account: Vec<(String, String, PathBuf)> = state.config.borrow().connections.iter()
                    .filter(|c| c.account_id == account_id)
                    .map(|c| (c.id.clone(), c.name.clone(), c.local_path.clone()))
                    .collect();

                match message {
                    push::PushMessage::Connected => {
                        // Treated the same as Disconnected for this
                        // specific question -- see NotifyPushStatus's own
                        // doc comment on why a first-ever success (no
                        // prior entry in this map at all) should still
                        // announce, same as this app's own, original,
                        // pre-debounce behavior always did.
                        let previous = state.notify_push_status.borrow().get(&account_id).copied();
                        state.notify_push_status.borrow_mut().insert(account_id.clone(), NotifyPushStatus::Connected);
                        // A successful connection means any prior auth
                        // problem, if there was one, is resolved --
                        // whatever credential this account succeeded
                        // with is clearly fine now, regardless of
                        // whether it changed or GOA's own stored value
                        // was simply refreshed by something else in the
                        // meantime.
                        state.push_auth_failed.borrow_mut().insert(account_id.clone(), false);
                        if !matches!(previous, Some(NotifyPushStatus::Retrying)) {
                            // all_accounts, not watcher.accounts() alone
                            // -- confirmed as a real, reported bug
                            // without this: watcher.accounts() only ever
                            // returns GOA accounts (see all_accounts' own
                            // implementation), so this lookup could never
                            // find a match for a direct account, always
                            // falling back to the raw, machine-readable
                            // account_id string (e.g. "direct:https://
                            // example.com:username") instead of a real,
                            // human-readable identity.
                            let presentation_identity = all_accounts(&state, &watcher).into_iter()
                                .find(|a| a.account_id() == account_id)
                                .map(|a| a.presentation_identity())
                                .unwrap_or_else(|| account_id.clone());
                            let _ = state.status_events_tx.try_send(status::SyncEvent::NotifyPushCapability {
                                connection: presentation_identity,
                                available: true,
                            });
                        }
                    }
                    push::PushMessage::Disconnected => {
                        let previous = state.notify_push_status.borrow().get(&account_id).copied();
                        // Only a genuine, first transition away from
                        // Connected (or the very first event ever for
                        // this account) actually starts the debounce
                        // window -- an already-Retrying or
                        // already-Disconnected account just stays
                        // exactly where it is; there's no new timer to
                        // spawn, since one's either already counting
                        // down (Retrying) or already fired and announced
                        // (Disconnected).
                        if !matches!(previous, Some(NotifyPushStatus::Retrying) | Some(NotifyPushStatus::Disconnected)) {
                            state.notify_push_status.borrow_mut().insert(account_id.clone(), NotifyPushStatus::Retrying);
                            const NOTIFY_PUSH_UNAVAILABLE_DEBOUNCE_SECS: u64 = 60;
                            glib::MainContext::default().spawn_local(clone!(
                                #[strong] state,
                                #[strong] watcher,
                                #[strong] account_id,
                                async move {
                                    glib::timeout_future(std::time::Duration::from_secs(NOTIFY_PUSH_UNAVAILABLE_DEBOUNCE_SECS)).await;
                                    // Only escalates if still genuinely
                                    // stuck in Retrying -- if this
                                    // reconnected in the meantime (now
                                    // Connected), or somehow already
                                    // escalated some other way, this
                                    // check does nothing at all.
                                    let still_retrying = state.notify_push_status.borrow().get(&account_id).copied() == Some(NotifyPushStatus::Retrying);
                                    if still_retrying {
                                        state.notify_push_status.borrow_mut().insert(account_id.clone(), NotifyPushStatus::Disconnected);
                                        // all_accounts, not watcher.accounts()
                                        // alone -- see the Connected arm's own
                                        // identical fix, just above, for the
                                        // full reasoning.
                                        let presentation_identity = all_accounts(&state, &watcher).into_iter()
                                            .find(|a| a.account_id() == account_id)
                                            .map(|a| a.presentation_identity())
                                            .unwrap_or_else(|| account_id.clone());
                                        let _ = state.status_events_tx.try_send(status::SyncEvent::NotifyPushCapability {
                                            connection: presentation_identity,
                                            available: false,
                                        });
                                    }
                                }
                            ));
                        }
                    }
                    push::PushMessage::AuthenticationFailed => {
                        state.push_auth_failed.borrow_mut().insert(account_id.clone(), true);
                    }
                    push::PushMessage::FileChanged => {
                        if !connections_for_account.is_empty() {
                            // Coalesced, not acted on immediately, while
                            // a remote write is genuinely in flight (any
                            // account -- only one is ever active in this
                            // app, so there's nothing to gain from
                            // checking which one) -- see pending_
                            // remote_writes' own doc comment on AppState
                            // for the real, reported burst this avoids.
                            // The decrement site (the global count
                            // dropping back to zero, in end_remote_
                            // write) is what actually triggers the
                            // deferred check this event represents.
                            if state.pending_remote_writes.get() > 0 {
                                state.coalesced_notify_push_pending.set(true);
                                if is_debug_enabled() {
                                    eprintln!("[{}] [debug] notify_push FileChanged ({account_id}) coalesced -- remote write(s) still in flight", now_hms());
                                }
                                continue;
                            }
                            // notify_push_pending, not force_full_check --
                            // this event alone doesn't say anything
                            // changed in THIS connection's own remote
                            // root specifically, only somewhere in the
                            // account; try_etag_skip in run_sync_round is
                            // what actually settles that, cheaply, rather
                            // than this handler assuming a full crawl is
                            // already warranted. See notify_push_pending's
                            // own doc comment on AppState.
                            for (connection_id, _connection_name, _local_path) in &connections_for_account {
                                state.notify_push_pending.borrow_mut().insert(connection_id.clone());
                            }
                            if is_debug_enabled() {
                                eprintln!("[{}] [debug] round triggered: notify_push FileChanged ({account_id})", now_hms());
                            }
                            spawn_immediate_sync_round(&state, &watcher);
                        }
                    }
                    push::PushMessage::FileIdsChanged(ids) => {
                        if !connections_for_account.is_empty() {
                            // Same coalescing as FileChanged just above,
                            // for the same reason -- see pending_remote_
                            // writes' own doc comment. Coalesced
                            // wholesale, not just the notify_push_
                            // pending fallback further down: even the
                            // dirty_dirs-scoped path below would still
                            // redundantly re-verify a directory this app
                            // just wrote to itself, if one of these IDs
                            // happens to be that same write.
                            if state.pending_remote_writes.get() > 0 {
                                state.coalesced_notify_push_pending.set(true);
                                if is_debug_enabled() {
                                    eprintln!("[{}] [debug] notify_push FileIdsChanged ({account_id}) coalesced -- remote write(s) still in flight", now_hms());
                                }
                                continue;
                            }
                            // Per connection, not per ID globally --
                            // find_by_fileid is itself scoped to one
                            // connection_id's own journal rows (the
                            // same fileid could plausibly collide across
                            // two entirely unrelated connections, even
                            // under the same account, if they cover
                            // different remote subpaths), and a single
                            // pushed batch of IDs can easily span content
                            // relevant to more than one connection at
                            // once.
                            for (connection_id, _connection_name, local_path) in &connections_for_account {
                                let mut any_resolved = false;
                                for fileid in &ids {
                                    // Don't know upfront whether a given
                                    // ID belongs to a file or a directory
                                    // -- check both, same uncertainty
                                    // find_by_fileid's own signature
                                    // already requires an answer to.
                                    let matched = journal::find_by_fileid(&state.journal, connection_id, fileid, false).ok().flatten()
                                        .or_else(|| journal::find_by_fileid(&state.journal, connection_id, fileid, true).ok().flatten());
                                    if let Some(item) = matched {
                                        any_resolved = true;
                                        let parent_relative = item.relative_path.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
                                        let dirty_local_dir = if parent_relative.is_empty() {
                                            local_path.clone()
                                        } else {
                                            local_path.join(parent_relative)
                                        };
                                        // Same map, same shape, a real
                                        // watch.rs FileMonitor callback
                                        // already writes to -- see
                                        // SyncTarget::dirty_dirs' own doc
                                        // comment for why this is what
                                        // lets the follow-up round scope
                                        // down to just this directory
                                        // instead of running a full crawl.
                                        state.dirty_dirs.borrow_mut()
                                            .entry(connection_id.clone())
                                            .or_default()
                                            .insert(dirty_local_dir);
                                    }
                                }
                                // Every resolved ID above already scoped
                                // this connection's own next round down
                                // to specific directories via dirty_dirs
                                // -- notify_push_pending is only for the
                                // IDs that came back completely unknown
                                // (a genuinely new file, or a connection
                                // that's simply never synced whatever
                                // this is), the same fallback FileChanged
                                // already uses for the connection as a
                                // whole when nothing more specific is
                                // known at all.
                                if !any_resolved {
                                    state.notify_push_pending.borrow_mut().insert(connection_id.clone());
                                }
                            }
                            if is_debug_enabled() {
                                eprintln!("[{}] [debug] round triggered: notify_push FileIdsChanged ({account_id}, {} ids)", now_hms(), ids.len());
                            }
                            spawn_immediate_sync_round(&state, &watcher);
                        }
                    }
                }
            }
        }
    ));

    // Reactive metered-connection detection -- confirmed as a real gap:
    // trigger_sync_round's own metered check (below) only ever ran
    // inside a sync round, which meant a connection becoming metered
    // mid-session (switching from Wi-Fi to mobile data, say) went
    // unnoticed until whatever remote_poll_interval_minutes happened to
    // have left to run -- a real, "am I actually paused right now"
    // anxiety gap, not just a cosmetic one, and the same applied in
    // reverse (a connection becoming un-metered again). NetworkMonitor's
    // own network-changed signal fires immediately on either transition;
    // reusing trigger_sync_round's own logic directly here (rather than
    // extracting/duplicating the metered-check-and-log part into its own
    // function) means this reacts instantly while the actual pause/
    // resume decision and its logging/UI update still live in exactly
    // one place. The bool parameter (general network availability, not
    // specifically metered status) isn't needed here -- the signal
    // firing at all is the only thing this cares about, since
    // trigger_sync_round re-checks is_network_metered() itself either
    // way.
    gio::NetworkMonitor::default().connect_network_changed(clone!(
        #[strong] state,
        #[strong] watcher,
        move |_, _| {
            glib::MainContext::default().spawn_local(clone!(
                #[strong] state,
                #[strong] watcher,
                async move { trigger_sync_round(&state, &watcher, false, false).await; }
            ));
        }
    ));

    // Same reactive-detection reasoning, same fix for the same class of
    // gap, as the network_changed handler just above -- Power Save Mode
    // changing mid-session (the desktop's own idle/low-battery policy
    // switching it on, or a person toggling it manually) previously went
    // unnoticed the same way a connection becoming metered did, until
    // whatever remote_poll_interval_minutes happened to have left to
    // run. gio::PowerProfileMonitor's own notify::power-saver-enabled
    // signal fires immediately on either transition. Reuses
    // trigger_sync_round's own pause-check logic directly, the same as
    // network_changed does, rather than duplicating the decision here.
    gio::PowerProfileMonitor::get_default().connect_power_saver_enabled_notify(clone!(
        #[strong] state,
        #[strong] watcher,
        move |_| {
            glib::MainContext::default().spawn_local(clone!(
                #[strong] state,
                #[strong] watcher,
                async move { trigger_sync_round(&state, &watcher, false, false).await; }
            ));
        }
    ));

    // Local-change-triggered syncing used to live here as its own
    // poll-and-debounce loop, waking every 2 seconds just to check a bare
    // flag. It's gone entirely now -- every FileMonitor callback
    // (watch.rs's own on_change) calls straight into arm_watch_debounce,
    // which schedules a one-shot timer directly, re-armed on every event
    // rather than polled for on a fixed schedule. See arm_watch_debounce
    // and watch_on_change_handler's own doc comments for the full
    // mechanism, and AppState::dirty_dirs for what actually gets
    // accumulated in the meantime.

    // The remote side has no push notifications, so this is the one
    // genuinely fixed-interval timer left -- its cadence is user-
    // configurable (Settings tab / remote_poll_interval_minutes) and it
    // doubles as the periodic full local re-verification the local
    // watch alone can't guarantee (a dropped watch, a directory that
    // existed before the app started, etc). The very first iteration
    // runs immediately, before the first sleep, which is what gives every
    // startup a fresh full crawl of both sides with no special-casing.
    loop {
        if is_debug_enabled() {
            eprintln!("[{}] [debug] round triggered: periodic poll", now_hms());
        }
        trigger_sync_round(&state, &watcher, false, false).await;

        // Polls in short ticks and re-reads the interval fresh each
        // time, rather than committing to one long, uninterruptible
        // sleep -- confirmed as a real bug: changing the interval
        // mid-wait had no effect until the ORIGINAL (stale) interval
        // finished, since glib::timeout_future_seconds can't be
        // shortened or cancelled once it's running. 30s tick: the
        // setting's own minimum is 60s, so a 30s tick keeps worst-case
        // overshoot at the tightest setting to at most half the
        // interval, not up to a full extra interval the way a
        // same-size tick would risk. Not meaningfully more expensive
        // than one long sleep either -- each tick is just a RefCell
        // borrow, an integer comparison, and a boot_time::Instant::
        // elapsed() call, all on the already-running glib main loop,
        // not anything waking the system from a deeper sleep state --
        // see Cargo.toml's own comment on the boot-time dependency for
        // why this reads boot_time::Instant specifically (a real,
        // reported bug: std::time::Instant's own CLOCK_MONOTONIC
        // basis doesn't advance during suspend, so a device suspended
        // for most of an idle period could sit well past its actual,
        // configured interval without this loop ever recognizing it).
        let round_finished = boot_time::Instant::now();
        loop {
            glib::timeout_future_seconds(30).await;
            let target_secs = state.config.borrow().remote_poll_interval_secs();
            if round_finished.elapsed().as_secs() >= target_secs as u64 {
                break;
            }
        }
    }
}

/// Spawned fresh from inside trigger_sync_round, right as a round
/// actually starts (state.syncing already true, sync_state already set
/// to Scanning) -- keeps the "discovered; processed" progress display
/// live during a round even though SyncAction::Noop and the size+mtime
/// deferral path (see SyncStatus::PendingVerification's own doc comment)
/// deliberately emit no SyncEvent of their own to hang a UI refresh off
/// of. Used to be a permanent loop living in run_background instead,
/// waking every 500ms for the entire life of the process regardless of
/// whether a round was even running -- this is the same 500ms cadence,
/// just scoped to exactly one round's own lifetime: the loop below ends
/// itself the moment state.syncing goes false, rather than needing an
/// external cancellation handle at all. A fresh task per round (instead
/// of one long-lived task checking a flag) costs one small heap
/// allocation per round, not a hot path worth avoiding that simplicity
/// for.
fn spawn_progress_refresh_loop(state: &Rc<AppState>) {
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        async move {
            while state.syncing.get() {
                glib::timeout_future(std::time::Duration::from_millis(500)).await;
                // Re-checked after the sleep, not just before it -- the
                // round can finish while this task is asleep, and
                // there's no reason to render one final, unnecessary
                // frame after it already has.
                if state.syncing.get() {
                    update_status_ui(&state, false);
                }
            }
        }
    ));
}


/// with no further local changes, before actually triggering a scoped
/// sync round -- matches the old poll loop's own "3s settle" window, the
/// same rough order of magnitude sync.rs's own MIN_FILE_AGE_SECS check
/// already assumes a watch-triggered round gets.
const WATCH_DEBOUNCE_SETTLE_SECS: u64 = 3;

/// How long schedule_freshness_retry waits before its follow-up round --
/// strictly longer than sync::MIN_FILE_AGE_SECS (5s) so anything deferred
/// this round is guaranteed to have aged past that window by the time
/// the follow-up actually runs, not just probably have.
const FRESHNESS_RETRY_DELAY_SECS: u64 = 8;

/// Spawns a one-shot, *scoped* follow-up round a short delay from now --
/// see had_freshness_deferral's own doc comment in sync.rs for the real
/// gap this closes. Scoped, not a full crawl: the deferral itself
/// already recorded exactly which directory needs a follow-up look
/// directly into dirty_dirs (see SyncTarget::dirty_dirs' own doc
/// comment) at the moment it deferred, using the exact same map a real
/// local-watch event populates -- by the time this fires, run_sync_round's
/// existing per-connection drain-and-scope logic finds those entries
/// waiting and restricts the walk to just them, the same as it would for
/// any other watch-triggered round.
fn schedule_freshness_retry(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>) {
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] watcher,
        async move {
            glib::timeout_future(std::time::Duration::from_secs(FRESHNESS_RETRY_DELAY_SECS)).await;
            if is_debug_enabled() {
                eprintln!("[{}] [debug] round triggered: freshness retry (8s)", now_hms());
            }
            trigger_sync_round(&state, &watcher, true, false).await;
        }
    ));
}


/// by run_sync_round whenever it removed a connection's own dirty
/// directories up front but then decided, further down, not to actually
/// give this round to that connection after all (the tier-multiplier
/// gate hasn't elapsed, or try_etag_skip says the remote side is
/// unchanged). A plain `insert` would silently clobber anything a
/// FileMonitor callback has *already* added back for this same
/// connection while this round's own several `.await` points (a
/// password fetch, an HTTP round-trip) gave the glib main loop a chance
/// to run something else -- `entry(...).or_default().extend(dirs)`
/// merges instead, so a real, concurrent local change reported during
/// that narrow window survives right alongside whatever's being
/// restored, rather than either one winning at the other's expense. A
/// no-op on an empty set (nothing to restore, and no reason to insert an
/// empty HashMap entry that would just sit there doing nothing).
fn restore_dirty_dirs(state: &Rc<AppState>, connection_id: &str, dirs: HashSet<PathBuf>) {
    if dirs.is_empty() {
        return;
    }
    state.dirty_dirs.borrow_mut().entry(connection_id.to_string()).or_default().extend(dirs);
}

/// Builds the callback watch::WatchManager::sync_watches hands to every
/// FileMonitor it sets up -- called fresh at each sync_watches call
/// site (a few times per round, not a hot path) rather than built once
/// and stored on AppState itself, since it needs to close over an owned
/// `Rc<AppState>` clone to satisfy FileMonitor's own 'static callback
/// requirement, and that clone isn't available until after AppState's
/// own Rc::new.
///
/// Two jobs, every time any watched directory reports a change: (1)
/// record exactly which directory, under which connection, into
/// state.dirty_dirs -- see that field's own doc comment for why this is
/// per-connection rather than one flat set; (2) (re-)arm the debounce
/// timer via arm_watch_debounce. Deliberately does NOT trigger a round
/// directly itself -- a burst of several files saved in quick
/// succession would otherwise fire one round per file instead of
/// collapsing into one, which is exactly the bug the old "poll every
/// 2s, then wait 3s to settle" loop existed to avoid; the debounce timer
/// is what preserves that same settling behavior without needing to
/// poll for it.
fn watch_on_change_handler(state: &Rc<AppState>) -> Rc<dyn Fn(&str, &Path)> {
    let state = state.clone();
    Rc::new(move |connection_id: &str, dir: &Path| {
        state.dirty_dirs.borrow_mut()
            .entry(connection_id.to_string())
            .or_default()
            .insert(dir.to_path_buf());
        arm_watch_debounce(&state);
    })
}

/// Cancels whatever debounce timer is currently pending (if any) and
/// schedules a fresh one, WATCH_DEBOUNCE_SETTLE_SECS out -- called on
/// every single local-change event via watch_on_change_handler above, so
/// a burst of writes keeps pushing this back rather than firing partway
/// through it, the same "let it settle" behavior the old poll loop's own
/// 3-second sleep provided, just event-driven instead of polled for.
///
/// Once this actually fires (nothing re-armed it in the meantime), it
/// runs a *scoped* sync round -- see trigger_sync_round's and
/// run_sync_round's own `scoped` parameter for what that means:
/// dirty_dirs gets drained per-connection into each SyncTarget's
/// dirty_scope, so only the reported-dirty directories (and their
/// ancestors, and any genuinely new content) actually get walked,
/// instead of a full recursive crawl of every connection's entire tree.
fn arm_watch_debounce(state: &Rc<AppState>) {
    if let Some(previous) = state.watch_debounce_timer.take() {
        previous.remove();
    }
    let source_id = glib::timeout_add_local_once(
        std::time::Duration::from_secs(WATCH_DEBOUNCE_SETTLE_SECS),
        clone!(
            #[strong] state,
            move || {
                // Cleared before spawning, not after -- a change arriving
                // during the round below should arm a fresh timer of its
                // own once this one's callback returns, not find one
                // that looks like it's still "pending" from this firing.
                state.watch_debounce_timer.set(None);
                let Some(watcher) = state.watcher.borrow().clone() else { return };
                if is_debug_enabled() {
                    eprintln!("[{}] [debug] round triggered: local watch debounce settled", now_hms());
                }
                glib::MainContext::default().spawn_local(clone!(
                    #[strong] state,
                    async move { trigger_sync_round(&state, &watcher, true, false).await; }
                ));
            }
        ),
    );
    state.watch_debounce_timer.set(Some(source_id));
}

/// Runs one sync round, unless one's already in progress -- the periodic
/// timer and the watch-triggered debounce above both call this, and
/// letting two rounds overlap would mean two crawls sharing the one
/// journal `Connection` concurrently, and doubled network/mount traffic
/// for no benefit. After the round, watches are reconciled against
/// whatever's now configured, so a folder added/removed/disabled takes
/// effect on the very next round with no separate "rebuild watches"
/// trigger needed.
///
/// `scoped`: true only for the watch-debounce path above -- see
/// run_sync_round's own doc comment on its own `scoped` parameter for
/// what this actually changes. Every other caller (the periodic timer,
/// a manual "Sync Now," a settings change, notify_push's own
/// force_full_check path) passes false, preserving the full-crawl
/// behavior this app always had -- the periodic timer in particular
/// needs to stay a real, unconditional full crawl regardless of
/// anything dirty_dirs currently holds, since it's the safety net for
/// whatever the local watch itself might have dropped (a watch that
/// failed to set up, a directory that existed before this session
/// started).
/// `force`: skips past the metered/low-battery checks' own early
/// return, for Sync Now's own deliberate "I know this connection is
/// metered/low, I still want this one sync right now" case -- the
/// pause state itself still gets detected, tracked, and logged exactly
/// the same either way (a forced sync while metered still means the
/// connection genuinely *is* metered, worth still reflecting in the
/// UI), only the "and therefore skip this round" consequence is
/// bypassed. Does NOT bypass paused_by_user below, deliberately: that's
/// a direct, explicit "stop everything" from the user, a fundamentally
/// different kind of pause than an automatic policy protecting against
/// unwanted cost or battery drain -- overriding an automatic policy for
/// one deliberate reason makes sense; silently overriding an explicit
/// stop the user just asked for would not.
async fn trigger_sync_round(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>, scoped: bool, force: bool) {
    if state.syncing.get() {
        return;
    }
    // Generic safety net, on top of the source-specific logs at each
    // individual call site (arm_watch_debounce, the two notify_push
    // handlers, schedule_freshness_retry, the periodic poll loop) --
    // this one fires regardless of which of trigger_sync_round's several
    // callers reached it, including the ones not individually labeled
    // (network/pause/power-saver transitions, Sync Now, account
    // switches). If this fires noticeably more often than the sum of
    // those labeled sources, that's the signal one of the un-labeled
    // callers is the real one -- scoped/force here narrow it further,
    // since each caller passes its own, fixed combination of the two.
    if is_debug_enabled() {
        eprintln!("[{}] [debug] trigger_sync_round entered (scoped={scoped}, force={force})", now_hms());
    }

    // force means one complete, coherent thing wherever it's used, not
    // just "bypass the metered/battery pause gates below" -- real,
    // reported bug without this: those two gates were the only thing
    // force originally touched, but there's a second, entirely separate
    // optimization (the ETag tier, gated by bypass_etag_check/force_full_check
    // in run_sync_round below) that can *also* make a round do
    // effectively nothing -- a connection whose stored root ETag still
    // matches gets skipped there even when nothing paused it at all.
    // Sync Now (both the Status tab button and the tray menu item) is
    // explicitly asking for a real, thorough check right now; a user
    // watching the status blink to Scanning and immediately back to
    // Idle, with nothing actually checked, is exactly what happened
    // without this. Populated here, once, rather than each caller of
    // force=true needing to remember to also touch force_full_check
    // itself -- every current and future force=true caller gets the
    // complete behavior automatically.
    if force {
        for connection in active_connections(state) {
            state.force_full_check.borrow_mut().insert(connection.id);
        }
    }

    // Checked first, ahead of the metered check below -- a deliberate,
    // manual pause should take precedence if both happen to be true at
    // once. No log line or UI update needed here the way the metered
    // check below has one: a manual pause is a direct user action,
    // already logged and reflected in the UI at the moment the button
    // itself was clicked, not something this function needs to notice
    // and announce after the fact.
    if state.paused_by_user.get() {
        let patterns = config::effective_exclude_patterns();
        state.watch_manager.sync_watches(&active_watch_targets(state), &patterns, &watch_on_change_handler(state));
        return;
    }

    // Checked before claiming the syncing lock, and before touching the
    // network at all -- a credential fetch and the first request of a
    // round would each be a real network operation even with nothing
    // to actually transfer, and the whole point here is to skip that
    // too, not just file transfers. Watches
    // still get reconciled even while paused: that's local-only,
    // costs nothing over the network, and keeps dirty_dirs accurate so
    // a change made while paused triggers a round the moment the
    // connection stops being metered, rather than being silently missed.
    // Evaluated once into a plain bool, not re-checked implicitly below --
    // real, confirmed bug without this: with force=true and the
    // connection still genuinely metered, the block below (meant only to
    // fire once no longer metered) would immediately see
    // paused_for_metered freshly set to true just above and clear it
    // right back, logging a false "no longer metered" the same instant
    // "metered connection detected" was logged. The two used to be
    // mutually exclusive for free, since the early return below meant
    // execution could never reach the second block while still metered --
    // once force could skip that return, they needed to be made
    // explicitly, structurally exclusive instead.
    let currently_metered = state.config.borrow().pause_on_metered && gio::NetworkMonitor::default().is_network_metered();
    if currently_metered {
        if !state.paused_for_metered.get() {
            state.paused_for_metered.set(true);
            log_and_persist(state, format!("[{}] Sync paused -- metered connection detected", now_hms()));
            update_status_ui(state, true);
        }
        if !force {
            let patterns = config::effective_exclude_patterns();
            state.watch_manager.sync_watches(&active_watch_targets(state), &patterns, &watch_on_change_handler(state));
            return;
        }
    } else if state.paused_for_metered.get() {
        state.paused_for_metered.set(false);
        log_and_persist(state, format!("[{}] Sync resumed -- connection no longer metered", now_hms()));
        update_status_ui(state, true);
    }

    // Same restructuring, same reasoning, as the metered check just
    // above -- see its own doc comment for the exact bug this avoids.
    // Fully synchronous, same as the metered check's own
    // gio::NetworkMonitor call -- gio::PowerProfileMonitor handles its
    // own D-Bus connection internally, gracefully degrading to a plain
    // `false` rather than an explicit "unavailable" state on a system
    // without power-profiles-daemon, the same tolerance
    // is_network_metered() already has for a system with no
    // NetworkMonitor backend. No separate "couldn't be read" case to
    // handle here as a result -- there's no such outcome this API can
    // actually produce.
    let currently_power_saving = state.config.borrow().pause_on_power_save_mode
        && gio::PowerProfileMonitor::get_default().is_power_saver_enabled();
    if currently_power_saving {
        if !state.paused_for_power_save.get() {
            state.paused_for_power_save.set(true);
            log_and_persist(state, format!("[{}] Sync paused -- Power Save Mode is active", now_hms()));
            update_status_ui(state, true);
        }
        if !force {
            let patterns = config::effective_exclude_patterns();
            state.watch_manager.sync_watches(&active_watch_targets(state), &patterns, &watch_on_change_handler(state));
            return;
        }
    } else if state.paused_for_power_save.get() {
        state.paused_for_power_save.set(false);
        log_and_persist(state, format!("[{}] Sync resumed -- Power Save Mode is no longer active", now_hms()));
        update_status_ui(state, true);
    }

    state.syncing.set(true);
    // Baseline for this round -- any MountFailed/Conflict event that
    // arrives while this round is in flight can only push sync_state
    // *worse* than this (see SyncEvent::implied_state); recovering back
    // down to Idle only happens below, once the round finishes without
    // ending on Disconnected. Scanning, not Syncing -- this is the walk-
    // and-compare phase, before anything's necessarily been found to
    // actually transfer; a real transfer event (ItemUploaded and
    // friends) promotes this to Syncing the same worst-first way
    // anything else does, via apply_status_event's own `.max()` logic.
    // Previously this started straight at Syncing, which meant a round
    // that found nothing to do still showed "Syncing…" for its entire
    // walk, indistinguishable from an actual transfer in progress.
    state.sync_state.set(status::SyncState::Scanning);
    state.discovered_count.set(0);
    state.processed_count.set(0);
    state.had_freshness_deferral_this_round.set(false);
    update_status_ui(state, true);

    spawn_progress_refresh_loop(state);
    let any_check_happened = run_sync_round(state, watcher, scoped, force).await;
    // Cleared only now, after the round genuinely ran (not before it, and
    // not merely on being called) -- run_sync_round's own SyncTarget
    // construction reads this same flag per connection, so flipping it
    // any earlier would mean the temp-file sweep this flag exists to gate
    // never actually happens at all.
    state.first_round_this_session.set(false);
    let patterns = config::effective_exclude_patterns();
    state.watch_manager.sync_watches(&active_watch_targets(state), &patterns, &watch_on_change_handler(state));

    state.syncing.set(false);
    // Checked right after the round is genuinely marked over, not
    // before -- schedule_freshness_retry's own spawned task checks
    // state.syncing itself when it eventually fires, so there's no risk
    // of it racing this round's own tail. See had_freshness_deferral's
    // own doc comment in sync.rs for the real gap this closes: without
    // this, a deferred upload was only ever picked back up by whatever
    // round happens to run next on its own separate schedule, which
    // could be a very long wait.
    if state.had_freshness_deferral_this_round.get() {
        schedule_freshness_retry(state, watcher);
    }
    // Checked before the Idle reset below, not after -- see the
    // background verification task's own doc comment for why: a round
    // that discovered new PendingVerification items this pass shouldn't
    // drop to Idle just because its own crawl finished, when there's
    // real, visible-to-the-journal work still queued right behind it.
    let verification_pending: i64 = active_connections(state).iter()
        .filter_map(|c| journal::count_in_status(&state.journal, &c.id, journal::SyncStatus::PendingVerification).ok())
        .sum();
    if verification_pending == 0 && state.sync_state.get() != status::SyncState::Disconnected {
        // Neither "stayed at the Scanning baseline" (nothing to do) nor
        // "got promoted to Syncing" (some successful transfers) is an
        // ongoing problem worth leaving visible once the round's done --
        // only Disconnected persists, since that's the one state that
        // still means something's actually wrong.
        state.sync_state.set(status::SyncState::Idle);
    }
    if any_check_happened {
        *state.last_checked.borrow_mut() = Some(now_hms());
    }
    update_status_ui(state, true);

    // Drain anything queued while this round was in flight, or while
    // paused -- see retry_defer_reason and apply_conflict_resolution's
    // own callers for how each variant gets queued in the first place.
    // Taken (not just read) so an action that itself queues nothing
    // further doesn't loop; a connection that's since been removed, or
    // an account that's no longer available, is silently skipped rather
    // than erroring, same "don't resurrect what's gone" policy used
    // elsewhere in this app.
    //
    // Concurrent now, not sequential -- confirmed as a real, reported
    // problem without this: a plain, one-at-a-time loop meant every
    // queued action fully finished (including up to RESOLVER_RETRY_
    // ATTEMPTS retries, each with its own cooldown) before the next one
    // even started, using only one of content_semaphore's own 6 lanes
    // regardless of how many were actually free. Limited to MAX_PERMITS
    // "in progress" at once, same reasoning as verify_pending_item's own
    // batch just above -- nothing to gain from spawning more outer tasks
    // than the shared semaphore could ever actually let run at once
    // anyway.
    let queued = state.pending_sync_actions.borrow_mut().drain(..).collect::<Vec<_>>();
    let content_concurrency = sync::Semaphore::MAX_PERMITS;
    futures_util::stream::iter(queued).for_each_concurrent(Some(content_concurrency), |action| {
        let state = state.clone();
        let watcher = watcher.clone();
        async move {
            let (connection_id, _relative_path) = match &action {
                QueuedSyncAction::RetryPermissionError { connection_id, relative_path, .. } => (connection_id, relative_path),
                QueuedSyncAction::ResolveConflict { connection_id, relative_path, .. } => (connection_id, relative_path),
            };
            // active_connections, not just "does this connection still
            // exist in config at all" -- the same class of gap just
            // fixed in the background verification task (see its own
            // doc comment): deactivating an account doesn't delete its
            // connections, so the old check would still find and
            // process a queued action for one, even though its account
            // was no longer the active one.
            let Some(connection) = active_connections(&state).into_iter().find(|c| &c.id == connection_id) else {
                return;
            };
            // Non-blocking probe, immediately released -- decides which
            // Semaphore this one action's own SyncTarget gets built
            // with, not a permit this task holds onto itself. See
            // AppState::resolver_privileged_semaphore's own doc comment
            // for why this decision has to be made here, before target
            // construction, rather than as an outer wrapper around the
            // whole call: retry_single_item/apply_conflict_resolution
            // both make several, independent acquisitions internally,
            // not just one. A small, deliberately accepted race: the
            // shared pool's own state could change between this probe
            // and this action's first real acquisition moments later --
            // worst case, it just blocks on the shared pool like
            // normal, exactly the only behavior that existed here
            // before this feature at all, not a new failure mode.
            let use_privileged = state.content_semaphore.try_acquire().is_none();
            let Ok(mut target) = build_sync_target_for_connection(&state, &watcher, &connection).await else {
                return;
            };
            if use_privileged {
                target.content_semaphore = state.resolver_privileged_semaphore.clone();
            }
            match action {
                QueuedSyncAction::RetryPermissionError { relative_path, is_directory, .. } => {
                    let _ = retry_resolution(|| async {
                        sync::retry_single_item(&state.journal, &target, &relative_path, is_directory, &state.status_events_tx).await.map_err(|e| e.to_string())
                    }).await;
                }
                QueuedSyncAction::ResolveConflict { relative_path, action, .. } => {
                    if let Some(item) = journal::get_item(&state.journal, &connection.id, &relative_path).unwrap_or_default() {
                        // Discarded deliberately, not an oversight --
                        // success/failure is already surfaced via the
                        // ConflictResolved/ConflictResolutionFailed
                        // events apply_conflict_resolution sends
                        // internally regardless of who calls it, the
                        // same reasoning retry_single_item's own
                        // identical `let _ =` just above already
                        // follows.
                        let content_semaphore_override = if use_privileged {
                            Some(state.resolver_privileged_semaphore.clone())
                        } else {
                            None
                        };
                        // Same in-flight tracking as the resolver
                        // dialog's own direct Keep/Rename click handler
                        // -- a queued resolution reaching this point is
                        // still exactly the same, real remote write when
                        // it's KeepLocal/KeepBothRenameRemote, still
                        // just as capable of triggering the same
                        // self-echo burst this whole mechanism exists to
                        // coalesce, regardless of whether it took the
                        // immediate or the queued path to get here.
                        let writes_remote = matches!(action, ConflictAction::KeepLocal | ConflictAction::KeepBothRenameRemote);
                        if writes_remote {
                            begin_remote_write(&state, &watcher);
                        }
                        let _ = retry_resolution(|| apply_conflict_resolution(&state, &watcher, &connection, &item, action, content_semaphore_override.as_ref())).await;
                        if writes_remote {
                            end_remote_write(&state, &watcher);
                        }
                    }
                }
            }
        }
    }).await;
    update_status_ui(state, true);

    // Background verification pass -- see sync::verify_pending_item's own
    // doc comment for the full design. Deliberately spawned as its own,
    // separate task rather than awaited inline here: the whole point of
    // the size+mtime deferral this feeds from is that the round itself
    // shouldn't have to wait on this expensive, background-appropriate
    // work to actually finish.
    //
    // Doesn't touch sync_state at all during the work itself -- see
    // trigger_sync_round's own PendingVerification check, right before
    // its Idle reset, for how the round's own title (Scanning/Syncing)
    // already stays visible for as long as this pass still has work
    // queued, without this task needing to set anything itself. Only
    // exception: once this pass finishes and the count is confirmed back
    // to zero, this is the one place responsible for the final Idle
    // transition -- nothing else would ever notice verification finished
    // otherwise, since the round itself already returned long before this
    // task's own work was done.
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] watcher,
        async move {
            // Guards the entire task, not just its own work -- see
            // AppState's own doc comment on `verifying` for the full
            // reasoning: without this, a newer round starting while a
            // previous verification pass was still active would spawn a
            // second, fully independent task, with both then processing
            // the same queued items concurrently and redundantly. If
            // one's already running, this round's own newly-discovered
            // PendingVerification items (if any) are simply left for the
            // next verification pass to pick up once the current one
            // finishes, rather than being processed twice over.
            if state.verifying.get() {
                return;
            }
            state.verifying.set(true);

            (async {
                let mut work: Vec<(config::SyncConnection, Vec<journal::SyncItem>)> = Vec::new();
                for connection in active_connections(&state) {
                    if let Ok(pending) = journal::items_in_status(&state.journal, &connection.id, journal::SyncStatus::PendingVerification) {
                        if !pending.is_empty() {
                            work.push((connection, pending));
                        }
                    }
                }
                if work.is_empty() {
                    return;
                }

                let verified_since_last_refresh = Rc::new(Cell::new(0u32));
                for (connection, pending) in work {
                    // Checked before starting each connection's own batch --
                    // see the per-item check just below for why this task
                    // needed its own, separate pause handling at all: the
                    // existing active_sync_cancels mechanism (see
                    // AppState's own doc comment on it) only ever covered
                    // the main crawl's own sync_target future -- this task
                    // is a genuinely separate, independently-spawned one,
                    // never registered there, so it kept running to
                    // completion regardless of Pause being clicked. Confirmed
                    // as a real, reported case: exactly the "files still
                    // being hashed after Pause" symptom.
                    if state.paused_by_user.get() {
                        return;
                    }
                    // Same reasoning as the pause check just above, for a
                    // different way this connection can stop being valid
                    // mid-pass: `work` is a snapshot taken once, at the
                    // very start of this task, before the account this
                    // connection belongs to could have been unchecked in
                    // the Connections tab. Confirmed as a real, reported
                    // case: deactivating an account didn't stop this
                    // task's own, already-queued items for it from
                    // continuing to process and log results, even though
                    // the Status tab itself had already moved on to "No
                    // account selected."
                    if !active_connections(&state).iter().any(|c| c.id == connection.id) {
                        continue;
                    }
                let Ok(target) = build_sync_target_for_connection(&state, &watcher, &connection).await else {
                    continue;
                };
                // Same shared concurrency ceiling the main walk itself
                // uses (see sync::Semaphore::MAX_PERMITS' own doc
                // comment) -- this is exactly the same kind of
                // potentially-large, variable-size content work that
                // limit exists to gate, so it gets the same treatment
                // here rather than running unbounded just because it
                // happens to be in the background.
                let content_concurrency = sync::Semaphore::MAX_PERMITS;
                let target = &target;
                let verified_since_last_refresh = &verified_since_last_refresh;
                let batch_future = std::pin::pin!(futures_util::stream::iter(pending).for_each_concurrent(Some(content_concurrency), |item| {
                    let state = state.clone();
                    async move {
                        // Checked per item too, not just once per
                        // connection above -- a single connection's own
                        // batch can itself be thousands of items, and
                        // Pause should take effect promptly, not only at
                        // the next connection boundary. Items skipped
                        // here are left untouched in PendingVerification
                        // -- not marked, not consumed -- so they're
                        // naturally picked up again by a future pass
                        // once resumed, exactly as if this task had
                        // simply not gotten to them yet.
                        if state.paused_by_user.get() {
                            return;
                        }
                        // Same reasoning as the connection-level check
                        // above, for the same reason the per-item pause
                        // check just above exists at all: the account
                        // could be deactivated partway through this
                        // specific connection's own batch, not only
                        // before it started. Left untouched in
                        // PendingVerification, same as the pause case --
                        // naturally picked up again if this connection's
                        // account is ever re-enabled.
                        if !active_connections(&state).iter().any(|c| c.id == target.connection_id) {
                            return;
                        }
                        let _ = sync::verify_pending_item(&state.journal, target, &item, &state.status_events_tx).await;
                        // Throttled to every 50th item, not every single
                        // one -- at the scale this queue can reach
                        // (thousands of items, concurrently processed),
                        // refreshing on every completion would mean many
                        // rapid, redundant calls in quick succession, the
                        // same kind of unbounded per-item cost already
                        // found and fixed twice elsewhere this session.
                        // The count itself (via count_in_status) is still
                        // always accurate whenever it IS refreshed --
                        // only the refresh cadence is coarser, not the
                        // number shown.
                        let count = verified_since_last_refresh.get() + 1;
                        verified_since_last_refresh.set(count);
                        if count % 50 == 0 {
                            update_status_ui(&state, false);
                        }
                    }
                }));
                // One shared cancellation check for this whole batch,
                // not one per concurrently-in-flight item -- see
                // verification_should_cancel's own doc comment for why
                // this needed to be a real race at all (the per-item
                // checks just above only ever catch a condition already
                // true before a given item started, not one that arises
                // *during* verify_pending_item's own real network read
                // and hashing). Racing the whole for_each_concurrent
                // future here, rather than each item individually,
                // means a phone on battery pays for one, infrequent poll
                // per connection's own batch, not up to
                // content_concurrency redundant ones checking the exact
                // same thing at once -- cancelling drops every
                // currently in-flight item in this batch together, which
                // is the actual, desired outcome regardless (if this
                // connection's own work is being abandoned, it should
                // all stop, not just whichever items happen to notice
                // individually).
                let cancel_future = std::pin::pin!(verification_should_cancel(&state, &target.connection_id));
                let _ = futures_util::future::select(batch_future, cancel_future).await;
                }

                // Re-checked fresh here, not assumed from the work this task
                // itself just finished -- a newer round could have queued
                // more PendingVerification items while this pass was still
                // working (this flag being set doesn't prevent that round's
                // own crawl from running, only a second, competing
                // verification task for the same items) -- those get
                // picked up by the next verification pass instead, once
                // this one finishes and the flag clears. Same "don't
                // override Disconnected" reasoning as the main round's own
                // reset.
                let still_pending: i64 = active_connections(&state).iter()
                    .filter_map(|c| journal::count_in_status(&state.journal, &c.id, journal::SyncStatus::PendingVerification).ok())
                    .sum();
                if still_pending == 0 && state.sync_state.get() != status::SyncState::Disconnected {
                    state.sync_state.set(status::SyncState::Idle);
                }
                update_status_ui(&state, true);
            }).await;

            state.verifying.set(false);
        }
    ));
}

/// Entry point for the Sync Issues carousel's Retry ("force" false) and
/// Retry Now ("force" true) actions.
///
/// Plain Retry respects both state.syncing and either kind of pause --
/// if either holds, this queues rather than running alongside an
/// Retries the resolver dialog's own real, immediate attempt, when one
/// happens at all -- None specifically means "queued for later, no
/// result to report yet" (the connection is paused, or a round is
/// already mid-flight), not "failed silently." A queued retry only
/// actually runs once whatever round eventually drains
/// pending_sync_actions completes, entirely outside this call's own
/// awareness; the same queue, drained only at the end of a round that
/// actually runs to completion (see trigger_sync_round's own drain
/// logic), naturally handles both the paused and mid-round cases with
/// no extra bookkeeping -- a paused connection has no round running at
/// all until it's unpaused, so a queued retry simply waits for whatever
/// round eventually runs, which for a pause specifically only happens
/// once the user actually unpauses.
///
/// Retry Now bypasses the pause checks entirely (an explicit, once-off,
/// user-consented exception -- see this app's own design discussion on
/// why a global "ignore metered pause" toggle was rejected in favor of
/// exactly this, scoped, single-item override instead) but still
/// respects state.syncing, since two things touching the same journal/
/// files at once is never sound regardless of why either one is
/// running.
///
/// async now, not fire-and-forget with its own internal spawn_local --
/// confirmed as a real, reported bug without this: the resolver dialog's
/// own Retry button used to tag an item Succeeded the instant it was
/// clicked, before this function's own real attempt (or lack of one) had
/// even started, meaning "Resolution succeeded!" was never actually tied
/// to whether the retry worked at all. Returning the real outcome here
/// lets that caller await it and tag based on what genuinely happened,
/// the same as every other resolver action already does.
/// Whether an immediate retry attempt should be deferred right now, and
/// why -- checked synchronously by the resolver dialog's own button
/// handler, before anything spawns, so this decision can never race a
/// pause event landing on the main loop in the gap between a click and
/// a spawned task actually starting. See IssueResolution::Paused's own
/// doc comment for the full reasoning on why neither apply_conflict_
/// resolution nor retry_single_item ever re-check pause state mid-
/// execution, which is exactly what makes a synchronous, before-spawn
/// decision sufficient here -- once something's actually running, no
/// later pause can touch it either way.
enum RetryDefer {
    /// Paused (and Retry Now wasn't the one clicked) -- stuck until the
    /// connection actually resumes. IssueResolution::Paused, not Pending
    /// -- see that variant's own doc comment for why the distinction
    /// matters.
    Paused,
    /// A round is already mid-flight (state.syncing) -- genuinely
    /// transient, not a real pause, so IssueResolution::Pending instead.
    Busy,
}
fn retry_defer_reason(state: &Rc<AppState>, force: bool) -> Option<RetryDefer> {
    if state.syncing.get() {
        return Some(RetryDefer::Busy);
    }
    if !force && (state.paused_by_user.get() || state.paused_for_metered.get() || state.paused_for_power_save.get()) {
        return Some(RetryDefer::Paused);
    }
    None
}

/// How many times the resolver dialog itself retries a failed
/// resolution attempt before finally tagging the item Failed, and how
/// long it waits between each -- by request: a single failed attempt
/// doesn't necessarily mean the underlying problem is still there (a
/// momentary network blip is enough to fail one attempt outright, as a
/// real, reported case confirmed -- the very next click succeeded with
/// nothing else having changed), so declaring failure after the first
/// try alone was too eager. 2 seconds, not something shorter or longer:
/// enough of a real gap for a transient hiccup to plausibly clear
/// without making an already-waiting, watching user feel like the
/// dialog has stalled.
const RESOLVER_RETRY_ATTEMPTS: u32 = 3;
const RESOLVER_RETRY_COOLDOWN_SECS: u64 = 2;

/// Wraps a single resolver action -- apply_conflict_resolution or
/// attempt_retry_now/sync::retry_single_item, all plain, single-attempt
/// functions otherwise unaware of any of this -- with up to
/// RESOLVER_RETRY_ATTEMPTS tries. Returns the very first success
/// immediately, without exhausting whatever attempts remain -- nothing
/// to gain from continuing to retry something that already worked. Only
/// the very last failure's own message is what the caller ever sees;
/// the earlier ones were, by definition, not the one that actually
/// stuck.
///
/// Deliberately a wrapper around every call site, not folded into any
/// underlying function itself. Used at all of them, not just the
/// resolver dialog's own, direct, click-triggered attempts -- a
/// resolution queued from the dialog while paused or busy, and only
/// actually run later by the drain loop, is still a resolver-initiated
/// resolution in every way that matters here; there was never a real
/// reason for "the resolver retries failures" to stop applying just
/// because the attempt itself happened to run on a delay. (An earlier
/// version of this comment argued the opposite -- that was wrong, not a
/// deliberate distinction worth keeping.)
async fn retry_resolution<F, Fut>(mut attempt: F) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let mut last_error = String::new();
    for attempt_number in 1..=RESOLVER_RETRY_ATTEMPTS {
        match attempt().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_error = e;
                if attempt_number < RESOLVER_RETRY_ATTEMPTS {
                    glib::timeout_future(std::time::Duration::from_secs(RESOLVER_RETRY_COOLDOWN_SECS)).await;
                }
            }
        }
    }
    Err(last_error)
}

/// itself is handled directly by the caller now (a plain, synchronous
/// push to pending_sync_actions needs no async function of its own at
/// all), so this only ever runs once retry_defer_reason has already
/// confirmed there's no reason to defer. async, not fire-and-forget with
/// its own internal spawn_local -- confirmed as a real, reported bug
/// without this: the resolver dialog's own Retry button used to tag an
/// item Succeeded the instant it was clicked, before any real attempt
/// had even started, meaning "Resolution succeeded!" was never actually
/// tied to whether the retry worked at all. Returning the real outcome
/// here lets the caller await it and tag based on what genuinely
/// happened, the same as every other resolver action already does.
async fn attempt_retry_now(state: &Rc<AppState>, connection_id: String, relative_path: String, is_directory: bool) -> Result<(), String> {
    let Some(watcher) = state.watcher.borrow().clone() else {
        return Err("No account available to retry against".to_string());
    };
    // active_connections, not just "does this connection still exist in
    // config" -- same reasoning as the other fixes this session (see
    // the background verification task's own doc comment): the account
    // could have been deactivated, or this specific connection removed,
    // in the time between this dialog opening and the user actually
    // clicking Retry on one of its own pages.
    let Some(connection) = active_connections(state).into_iter().find(|c| c.id == connection_id) else {
        return Err("This connection is no longer active".to_string());
    };
    let Ok(target) = build_sync_target_for_connection(state, &watcher, &connection).await else {
        return Err("Account's remote location isn't ready yet".to_string());
    };
    let result = sync::retry_single_item(&state.journal, &target, &relative_path, is_directory, &state.status_events_tx).await;
    update_status_ui(state, true);
    result.map_err(|e| e.to_string())
}

/// Raising "Skip files larger than (MB)" doesn't just affect what a
/// *future* crawl decides -- every item already sitting in the journal
/// as status=skipped_too_large from a previous round, under the old
/// (lower) limit, deserves one real retry against the new one too,
/// rather than waiting for its own directory to happen to get
/// re-walked. That wait could now be a genuinely long one: with the
/// targeted, scoped rescanning the local watch does (see
/// sync::SyncTarget::dirty_scope), a directory nothing else has touched
/// since might not get walked again for a long time, so a skipped item
/// sitting quietly in an otherwise-untouched directory could easily
/// outlive several settings changes before the ordinary walk ever
/// reconsiders it on its own.
///
/// Only wired to the increase direction -- a *lower* limit needs no
/// action here at all: the plain skipped_too_large items were already
/// correctly skipped, and stay correctly skipped; the separate,
/// already-existing reevaluate_pending_issues (called from save_config
/// unconditionally, both directions) is what handles a lower limit
/// newly disqualifying something that's also flagged as a real
/// Conflict/PermissionError issue.
///
/// Spawned rather than run inline -- same reasoning as
/// attempt_retry_now's own spawn: this is real, potentially
/// multi-item, multi-connection network I/O (a mount per connection, a
/// real transfer per item that now fits under the raised limit), not
/// something a settings-row callback should block the UI thread on.
fn retry_skipped_too_large(state: &Rc<AppState>) {
    let Some(watcher) = state.watcher.borrow().clone() else { return };
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        async move {
            for connection in active_connections(&state) {
                let items = journal::items_in_status(&state.journal, &connection.id, journal::SyncStatus::SkippedTooLarge)
                    .unwrap_or_default();
                if items.is_empty() {
                    continue; // no mount/target worth building for a connection with nothing to retry
                }
                let Ok(target) = build_sync_target_for_connection(&state, &watcher, &connection).await else {
                    continue;
                };
                for item in items {
                    let _ = sync::retry_single_item(&state.journal, &target, &item.relative_path, item.is_directory, &state.status_events_tx).await;
                }
            }
            update_status_ui(&state, true);
        }
    ));
}

/// Folds one SyncEvent from sync.rs into sync_state (worst-first) and
/// status_log, then refreshes the Status tab. Runs on the main loop, so
/// it's always safe to touch widgets directly here.
fn apply_status_event(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>, event: status::SyncEvent) {
    // Handled entirely separately from everything below -- these are
    // purely live, ephemeral UI state for the Active Transfers row, not
    // permanent log entries the way every other SyncEvent is (see
    // TransferStarted/TransferEnded's own doc comments in status.rs).
    // Still promotes sync_state via implied_state below, same as any
    // other event -- just skips the status_log push and the
    // conflict/permission-error bookkeeping that follows, none of which
    // applies to a transfer simply starting or ending.
    match &event {
        status::SyncEvent::TransferStarted { connection, path, direction } => {
            state.sync_state.set(std::cmp::max(state.sync_state.get(), event.implied_state()));
            state.active_transfers.borrow_mut().insert(
                format!("{connection}:{path}"),
                ActiveTransfer { connection: connection.clone(), path: path.clone(), direction: *direction, progress: None },
            );
            update_status_ui(state, false);
            return;
        }
        status::SyncEvent::TransferProgress { connection, path, bytes_so_far, bytes_total } => {
            if let Some(transfer) = state.active_transfers.borrow_mut().get_mut(&format!("{connection}:{path}")) {
                transfer.progress = Some((*bytes_so_far, *bytes_total));
            }
            update_status_ui(state, false);
            return;
        }
        status::SyncEvent::TransferEnded { connection, path } => {
            state.active_transfers.borrow_mut().remove(&format!("{connection}:{path}"));
            update_status_ui(state, false);
            return;
        }
        // Purely internal bookkeeping, same treatment as the three
        // Transfer* events just above -- see RemoteWriteStarted's own
        // doc comment in status.rs for the full design. This is the one
        // place every one of this app's own seven remote-writing sites
        // actually reaches begin_remote_write/end_remote_write: those
        // sites themselves (in sync.rs) only ever send the event, never
        // call these functions directly, since sync.rs has no access to
        // AppState, the journal, or account credentials at all -- this
        // status_events_tx channel is deliberately the one, sole
        // boundary crossing between the two.
        status::SyncEvent::RemoteWriteStarted => {
            begin_remote_write(state, watcher);
            return;
        }
        status::SyncEvent::RemoteWriteFinished => {
            end_remote_write(state, watcher);
            return;
        }
        _ => {}
    }

    let implied = event.implied_state();
    if implied > state.sync_state.get() {
        state.sync_state.set(implied);
    }
    // Real, previously-tracked gap: excluded_subpaths entries are exact-
    // string root-anchored paths (see config::SyncConnection's own doc
    // comment), not attached to the folder they came from -- without
    // this, renaming/moving an excluded folder (now detectable on both
    // the local and remote side, see sync.rs's own rename-pairing)
    // silently stopped the exclusion from matching anything at all,
    // with no warning. Matched by name, not connection_id -- ItemRenamed
    // only ever carried a display name (see status.rs), and there's no
    // existing name->id lookup this app already does elsewhere worth
    // deviating from for just this one caller.
    if let status::SyncEvent::ItemRenamed { connection, old_path, new_path } = &event {
        let old_prefix = format!("/{old_path}");
        let new_prefix = format!("/{new_path}");
        let mut config = state.config.borrow_mut();
        if let Some(conn) = config.connections.iter_mut().find(|c| &c.name == connection) {
            let mut changed = false;
            for entry in conn.excluded_subpaths.iter_mut() {
                if *entry == old_prefix {
                    *entry = new_prefix.clone();
                    changed = true;
                } else if let Some(suffix) = entry.strip_prefix(&format!("{old_prefix}/")) {
                    *entry = format!("{new_prefix}/{suffix}");
                    changed = true;
                }
            }
            if changed {
                drop(config);
                save_config(state);
            }
        }
    }
    // First time this specific (connection, path) has been seen as
    // Excluded/SkippedTooLarge this session -- log it and remember it;
    // every subsequent round hitting the same, unchanged fact about the
    // same path is silently skipped from here on. See
    // logged_exclusions' own doc comment on AppState for the full
    // reasoning and why nothing needs to specially "un-remember" a path
    // if its exclusion is later lifted.
    if let status::SyncEvent::Excluded { connection, path, .. } | status::SyncEvent::SkippedTooLarge { connection, path, .. } = &event {
        let key = (connection.clone(), path.clone());
        if !state.logged_exclusions.borrow_mut().insert(key) {
            return; // already logged this one this session
        }
    }
    let formatted = format!("[{}] {}", now_hms(), event.log_line());
    // ConnectionPath skips (an explicit, user-configured folder
    // exclusion) still go to the permanent, on-disk session log below,
    // same as always -- just never to the in-app view. By explicit
    // request: the user already, deliberately told this app not to
    // touch that folder, so reminding them of that choice on every
    // distinct path adds nothing they don't already know, even though
    // it's still worth having in the on-disk record for later
    // troubleshooting.
    let show_in_app = !matches!(&event, status::SyncEvent::Excluded { reason: crate::exclude::ExclusionReason::ConnectionPath, .. });
    if show_in_app {
        // Captured before the push just below, not after -- this is
        // specifically "was the log empty before this line," which
        // append_status_log_line needs to know whether to replace the
        // still-showing 'Nothing logged yet.' placeholder outright
        // versus genuinely prepend to real content that's already there.
        let was_empty = state.status_log.borrow().recent().next().is_none();
        state.status_log.borrow_mut().push(formatted.clone());
        append_status_log_line(state, &formatted, was_empty);
    }
    state.disk_log_pending.borrow_mut().push(formatted);
    if !state.disk_log_flush_armed.get() {
        state.disk_log_flush_armed.set(true);
        spawn_disk_log_flush_timer(state);
    }
    // Only these two events always, unconditionally mean a row just
    // entered the conflict/permission-error state -- everything else
    // either never touches that state at all (an upload, a download, an
    // exclusion) or only *might* clear it (ConflictResolvedIdentical,
    // ConflictResolved, ConflictResolutionFailed), and for the dominant
    // real-world case -- a fresh journal's first round, where nothing
    // was ever frozen as a conflict to begin with -- never actually does.
    // Skipping those means the row/count can very rarely lag by one
    // (over-counting something already resolved, never under-counting
    // and hiding something that needs attention) until the next
    // Conflict/PermissionError event or another update_status_ui caller
    // catches it up -- a far better direction to be occasionally wrong
    // in than the reverse, and a real, structural reduction in how often
    // this expensive rebuild runs, not just a rate limit sitting on top
    // of the same total cost.
    let refresh_issues = matches!(event, status::SyncEvent::Conflict { .. } | status::SyncEvent::PermissionError { .. });
    update_status_ui(state, refresh_issues);
}

/// Spawns a one-shot, 30-second timer that flushes disk_log_pending to
/// disk_log_path -- see disk_log_flush_armed's own doc comment on
/// AppState for why this is armed once per burst of activity rather than
/// a fixed, always-running periodic timer. The file write itself is
/// synchronous, not spawned to a separate thread -- a small, infrequent,
/// append-only write of at most a few seconds' worth of log lines is
/// cheap enough that a separate thread would be unnecessary complexity
/// for no real benefit, same reasoning already applied to
/// journal::mark_synced and every other SQLite write throughout this
/// app. Best-effort throughout, same as disk_log.rs's own functions: a
/// failed write here is logged but not retried -- this is a diagnostic
/// convenience layered on top of the app's actual sync correctness, not
/// something anything else depends on existing.
fn spawn_disk_log_flush_timer(state: &Rc<AppState>) {
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        async move {
            glib::timeout_future(std::time::Duration::from_secs(30)).await;
            flush_disk_log_now(&state);
            state.disk_log_flush_armed.set(false);
        }
    ));
}

/// Writes every line currently sitting in disk_log_pending to
/// disk_log_path (draining the pending buffer), then rotates to a fresh
/// file if that write pushed the current one past disk_log::
/// should_rotate's own threshold. Factored out of
/// spawn_disk_log_flush_timer specifically so export_log_button's own
/// handler can call this too, synchronously, right before it hands off
/// the actual on-disk file -- otherwise an export could hand over a file
/// missing whatever's accumulated in the last (up to 30-second) window
/// since the timer's own last fire, which defeats the entire point of
/// exporting "the session log" as the one, single, genuinely canonical
/// record rather than a second, separately-assembled copy of it.
fn flush_disk_log_now(state: &Rc<AppState>) {
    let lines: Vec<String> = state.disk_log_pending.borrow_mut().drain(..).collect();
    if !lines.is_empty() {
        let path = state.disk_log_path.borrow().clone();
        if let Err(e) = disk_log::append_lines(&path, &lines) {
            eprintln!("couldn't write to on-disk session log: {e}");
        }
        if disk_log::should_rotate(&path) {
            *state.disk_log_path.borrow_mut() = disk_log::new_session_path();
        }
    }
}

/// Saves state.config to disk and bumps settings_version together --
/// centralized here specifically so the two can never happen separately,
/// the way they easily could if each of the many individual settings
/// handlers throughout this file remembered (or forgot) to bump the
/// counter itself. settings_version's whole purpose is to mean "the
/// on-disk settings sync.rs might be caching have changed" -- see
/// SyncTarget::current_settings' own doc comment in sync.rs -- so every
/// single settings change needs to bump it, not just the ones that
/// happen to be exclude-pattern-related.
fn save_config(state: &Rc<AppState>) {
    if let Err(e) = state.config.borrow().save() {
        eprintln!("couldn't save config: {e}");
    }
    state.settings_version.set(state.settings_version.get() + 1);
    reevaluate_pending_issues(state);
}

/// Loads (or reloads) state.theme_css_provider's own content -- see
/// AppState::theme_css_provider's own doc comment for why this is a
/// reload of one persistent provider, not a fresh one each time. Safe
/// to call before any window exists at all (this only ever touches the
/// provider's own content, not the display registration, which
/// build_or_present_window handles separately and only once) -- and
/// safe to call repeatedly with no window ever having registered the
/// provider yet, since GtkCssProvider itself doesn't require being
/// attached to a display to have its content set.
///
/// Empty CSS (a plain, valid no-op reload) whenever apply_server_theming
/// is off or nothing's cached yet -- explicit, not just "skip the call
/// site" -- so turning the setting off actually reverts a previously-
/// applied theme immediately, rather than leaving whatever was loaded
/// last in place until something else happens to overwrite it.
fn apply_server_theme(state: &Rc<AppState>) {
    let config = state.config.borrow();
    let should_apply = config.apply_server_theming && config.active_account_id.is_some();
    // Looked up regardless of should_apply, not just when it's true --
    // the debug log just below wants to distinguish "no theme cached
    // for this account" from "a theme IS cached, just not being
    // applied right now" (apply_server_theming off, most commonly),
    // which collapsing this into should_apply's own gate would lose.
    let cached_for_account = config.active_account_id.as_deref().and_then(|id| config.cached_server_themes.get(id)).cloned();
    let theme = if should_apply { cached_for_account.clone() } else { None };
    if is_debug_enabled() {
        eprintln!(
            "[{}] [debug] apply_server_theme: apply_server_theming={} active_account_id={:?} cached_theme_present={} -> theme_active={}",
            now_hms(), config.apply_server_theming, config.active_account_id, cached_for_account.is_some(), theme.is_some(),
        );
    }
    drop(config);

    // Raised (header bar paints its own distinct background) when a
    // theme is actually active, Flat (the plain default -- no
    // background of its own, just shows the window's through it) when
    // it isn't. Was previously fixed to Raised unconditionally at
    // window construction time; moved here so toggling server theming
    // off actually returns the header bar to a normal, undecorated
    // look instead of staying visually "themed" with nothing behind it.
    // No-op harmlessly if the window doesn't exist yet (state.window_
    // widgets is genuinely optional at this point in the app's
    // lifecycle -- see its own doc comment).
    if let Some(widgets) = state.window_widgets.borrow().as_ref() {
        widgets.toolbar_view.set_top_bar_style(if theme.is_some() {
            libadwaita::ToolbarStyle::Raised
        } else {
            libadwaita::ToolbarStyle::Flat
        });
    }

    // Whether the app is currently in dark appearance -- needed now
    // that both card-bg/fg (fixed constants) and the body/sidebar-style
    // color's mix target/ratio (see below) depend on it, confirmed
    // directly against Nextcloud's own real theming source rather than
    // guessed.
    let dark_mode = libadwaita::StyleManager::default().is_dark();
    let (card_bg, card_fg, body_mix_ratio) = if dark_mode {
        // All three confirmed directly against Nextcloud's own
        // DarkTheme.php source ($colorMainBackground = '#171717',
        // $colorMainText = '#EBEBEB', --color-main-background-blur:
        // 85%) -- the blur ratio specifically is also confirmed by an
        // exact match against one real measured server sidebar color at
        // this same ratio, not just the source reading alone.
        ("#171717", "#EBEBEB", 0.85)
    } else {
        // All three confirmed directly against Nextcloud's own
        // DefaultTheme.php source (--color-main-background: #ffffff,
        // --color-main-text: #222222, --color-main-background-blur:
        // 80%) -- the blur ratio here also independently matches an
        // earlier empirical confirmation against two real background/
        // sidebar color pairs from a real server, before this file was
        // available to check directly.
        ("#ffffff", "#222222", 0.8)
    };

    let css = match &theme {
        Some(t) => {
            // Body/sidebar-style color, computed fresh here rather than
            // cached on ServerTheme -- Nextcloud's own real sidebar
            // formula (confirmed against DarkTheme.php/DefaultTheme.php
            // and an exact real-server measurement match) is the
            // current mode's own main-background color mixed toward the
            // server's background color at the current mode's own
            // ratio, not a fixed formula independent of light/dark
            // state the way it was treated before this file was
            // available to check. body_fg reuses card_fg directly --
            // both the body and the widget backgrounds are governed by
            // the same light/dark-appropriate pairing, so there's no
            // separate value to compute for it.
            let body_bg = push::mix_toward(&t.background, card_bg, body_mix_ratio)
                .unwrap_or_else(|| t.background.clone());
            let body_fg = card_fg;
            // --headerbar-backdrop-color: load-bearing, not just a
            // hedge -- its default alias is --window-bg-color, which is
            // the body color here, not the header bar's own background.
            // Header bar and window are different colors in this
            // scheme, so without setting this explicitly, losing focus
            // would silently swap the header bar to the body color
            // instead of staying on background -- the exact bug this
            // variable exists to prevent (see the original diagnosis, a
            // few iterations back, for the fuller reasoning).
            format!(
                "#ripples-themed-window {{ \
                    --window-bg-color: {body_bg}; --window-fg-color: {body_fg}; \
                    --headerbar-bg-color: {hbg}; --headerbar-fg-color: {hbg_text}; \
                    --headerbar-backdrop-color: {hbg}; \
                    --accent-bg-color: {pri}; --accent-fg-color: {pri_text}; \
                    --card-bg-color: {card_bg}; --card-fg-color: {card_fg}; \
                }}",
                hbg = t.background, hbg_text = t.background_text,
                pri = t.primary, pri_text = t.primary_text,
            )
        }
        None => String::new(),
    };
    state.theme_css_provider.load_from_string(&css);

    // Belt-and-suspenders alongside the reload above, not a replacement
    // for it: explicitly adds or removes the provider from the display
    // entirely based on whether a theme should currently be active,
    // rather than trusting an empty-string reload alone to fully
    // invalidate every already-computed style on its own.
    //
    // gdk4::Display::default() rather than going through a window (the
    // gtk4::prelude::WidgetExt::display(&window)/RootExt::display
    // approach this used before, and needed explicit disambiguation
    // between the two to even compile) -- confirmed directly against
    // gdk4's own real source that this exists and is exactly what it
    // sounds like: the default display for this process, independent of
    // any specific widget or window. This means this function no longer
    // needs a window to exist at all (state.window_widgets is genuinely
    // irrelevant here now) -- it correctly registers the provider the
    // very first time this runs, at app startup, before
    // build_or_present_window has built anything, rather than needing
    // window construction to happen first and a separate one-time
    // registration call site of its own.
    if let Some(display) = gtk4::gdk::Display::default() {
        // Removed unconditionally first (best-effort; harmless if it
        // was never actually registered) rather than only removing in
        // the "should be off" branch -- this guarantees the provider is
        // never registered more than once regardless of how many times
        // this function has already run, instead of depending on GTK's
        // own internal add_provider_for_display bookkeeping to dedupe a
        // repeat add of the same instance correctly on its own.
        gtk4::style_context_remove_provider_for_display(&display, &state.theme_css_provider);
        if theme.is_some() {
            // STYLE_PROVIDER_PRIORITY_USER (GTK4's highest available
            // level), not APPLICATION as before -- a real inconsistency
            // was reported: the accent color correctly picks up the
            // server theme on Phosh/mobile, but stays at GNOME Shell's
            // own system accent color on desktop. libadwaita's own 1.6
            // release notes are explicit that app CSS is supposed to
            // always take priority over the system accent -- so this
            // isn't a contradiction of that, but APPLICATION sits below
            // USER in GTK4's own priority ordering (FALLBACK < THEME <
            // SETTINGS < APPLICATION < USER), and a standard GNOME Shell
            // session's system-accent-color integration (which Phosh
            // doesn't have) may apply its own override at a priority
            // above APPLICATION specifically -- not a contradiction of
            // "CSS wins," just a case where this app's own CSS wasn't
            // registered at a high enough level to be the CSS that
            // wins. USER is the highest level GTK4 exposes, so nothing
            // can outrank it regardless of what level that turns out to
            // be. Confirmed working -- accent color now correctly picks
            // up the server theme on both Phosh and standard GNOME
            // Shell desktop sessions.
            gtk4::style_context_add_provider_for_display(&display, &state.theme_css_provider, gtk4::STYLE_PROVIDER_PRIORITY_USER);
        }
    }
}

/// Whether `relative_path` (a file if `is_directory` is false, else a
/// directory) is excluded either directly, or because *any ancestor
/// directory* along its path is -- a pattern like ".cargo/" matches the
/// directory itself, but a conflict is always flagged on a *file*
/// somewhere inside it, never the directory, so checking only the
/// item's own leaf name would never catch this. Checks every prefix of
/// the path in turn, each with the correct is_directory (true for every
/// ancestor, the real value only for the final, leaf component).
/// Deliberately passes an empty .no-sync set -- see
/// reevaluate_pending_issues' own doc comment for why.
fn path_or_ancestor_excluded(global_patterns: &[config::ExcludePattern], connection_patterns: &[String], relative_path: &str, is_directory: bool) -> bool {
    let no_sync_names = std::collections::HashSet::new();
    let components: Vec<&str> = relative_path.split('/').collect();
    let mut prefix = String::new();
    for (i, component) in components.iter().enumerate() {
        if i > 0 {
            prefix.push('/');
        }
        prefix.push_str(component);
        let this_is_directory = i + 1 < components.len() || is_directory;
        if exclude::exclusion_reason(&no_sync_names, global_patterns, connection_patterns, &prefix, component, this_is_directory).is_some() {
            return true;
        }
    }
    false
}

/// Re-evaluates every currently-pending conflict/issue against the
/// *current* settings, right after they've actually changed -- called
/// from save_config (every settings change that goes through it) and
/// the exclude-pattern add/remove/reset handlers (which save to their
/// own, separate file instead). An item that now matches an exclusion
/// (a global pattern, or this connection's own excluded_subpaths --
/// checked against every ancestor directory in its path, not just its
/// own name, see path_or_ancestor_excluded), or whose local size now
/// exceeds a since-lowered max_file_size_bytes, is forgotten from the
/// journal and dropped from the list entirely. There's nothing
/// meaningful left for the user to resolve once either becomes true --
/// leaving it sitting in the Sync Issues carousel would just be asking
/// them to adjudicate something the app itself has already decided
/// isn't relevant anymore, and neither retry_single_item nor any of the
/// four resolve_* functions check either condition themselves. This is
/// the single, centralized place that check needed to live, rather than
/// duplicated across every execution path that could act on a pending
/// item.
///
/// Deliberately checks .no-sync as an always-empty set, not a real one:
/// .no-sync is a per-directory filesystem convention, not an app
/// setting -- it isn't part of settings_version at all, and re-reading
/// every ancestor directory's own .no-sync file (local *and* remote)
/// for every pending item on every settings change would turn this
/// cheap, immediate cleanup into a real, possibly-slow set of network
/// round-trips for something .no-sync's own existing, per-round check
/// already covers on its own schedule regardless.
///
/// Size check is local_size only, not "either side" -- remote_size
/// isn't stored in the journal at all (only remote_etag), and querying
/// it fresh for every pending item on every settings change would mean
/// a real network round-trip per item too. This is a real, honest
/// approximation, not a full guarantee: a conflict whose local side is
/// small but whose remote side has since grown past the limit wouldn't
/// be caught here. The normal walk's own size check (which does compare
/// both sides) remains the actual, correct enforcement point the next
/// time this item is genuinely revisited -- this is purely a proactive
/// UI cleanup on top of that, not a replacement for it.
fn reevaluate_pending_issues(state: &Rc<AppState>) {
    let max_file_size_bytes = state.config.borrow().max_file_size_bytes;
    let global_patterns = config::effective_exclude_patterns();

    let mut still_valid = Vec::new();
    for issue in state.pending_issues.borrow().iter() {
        let connection = issue.connection();
        let item = issue.item();
        let is_excluded = path_or_ancestor_excluded(&global_patterns, &connection.excluded_subpaths, &item.relative_path, item.is_directory);
        let is_too_large = item.local_size.is_some_and(|size| size as u64 > max_file_size_bytes);

        if is_excluded || is_too_large {
            let _ = journal::forget_item(&state.journal, &connection.id, &item.relative_path);
        } else {
            still_valid.push(issue.clone());
        }
    }
    *state.pending_issues.borrow_mut() = still_valid;
}

fn now_hms() -> String {
    glib::DateTime::now_local().ok()
        .and_then(|d| d.format("%H:%M:%S").ok())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Pushes `line` to both the in-app StatusLog (for the visible Status
/// tab) *and* the on-disk session log (for Export Log and general
/// persistence) -- see disk_log.rs's own module doc comment for why
/// those are two genuinely separate things, not one piece of state with
/// two views onto it. This is the ONLY correct way to log a line that
/// should survive an export: a bare `state.status_log.borrow_mut().push(...)`
/// only ever updates the in-app view.
///
/// Confirmed as a real, reported bug without this: `line` isn't the
/// only thing that already needed this -- every pause/resume message in
/// this file (metered, low battery, manual pause) was already calling
/// the bare, display-only push directly, meaning none of them were ever
/// actually exportable, only visible in the running app's own Status
/// tab. This function exists specifically so that mistake has nowhere
/// left to be made again -- every call site below has been switched to
/// use it instead of the bare push.
fn log_and_persist(state: &Rc<AppState>, line: String) {
    state.status_log.borrow_mut().push(line.clone());
    state.disk_log_pending.borrow_mut().push(line);
    if !state.disk_log_flush_armed.get() {
        state.disk_log_flush_armed.set(true);
        spawn_disk_log_flush_timer(state);
    }
}

/// Enabled connections under the currently-active account -- the same
/// filter run_sync_round applies before it touches the network, factored
/// out so the watch manager can target exactly the same set without
/// duplicating the account/enabled logic.
/// Resolves as soon as any of: paused (either kind), the account
/// deselected, or this specific connection removed -- used to race
/// against an in-flight verify_pending_item call (see its own call site
/// in the background verification task) so it's actually abandoned
/// mid-flight rather than allowed to run to completion once started.
///
/// Deliberately polling, not event-based like active_sync_cancels --
/// that mechanism is safe specifically because it's one cancel channel
/// per connection, held by that connection's own single, sequential
/// main-round future. Here, many items for the *same* connection can be
/// verified concurrently (see content_concurrency's own use just below
/// this function's call site), so a shared cancel channel keyed the
/// same way would mean concurrent items overwriting each other's
/// registration -- and worse, colliding with the main round's own entry
/// for that same connection if both happened to be active at once. A
/// short, cheap poll of state already sitting in memory sidesteps all of
/// that for a negligible responsiveness cost.
async fn verification_should_cancel(state: &Rc<AppState>, connection_id: &str) {
    loop {
        if state.paused_by_user.get() || state.paused_for_metered.get() || state.paused_for_power_save.get() {
            return;
        }
        if !active_connections(state).iter().any(|c| c.id == connection_id) {
            return;
        }
        // Seconds, not milliseconds -- this is one, shared check for an
        // entire connection's own batch (see this function's own call
        // site), not something needing sub-second responsiveness. A user
        // pausing or deselecting an account doesn't need to be noticed
        // instantly; a couple of seconds' delay before an in-flight batch
        // actually stops costs nothing real in practice, while waking the
        // CPU 8x less often than a sub-second interval would matters
        // measurably on a battery-powered device.
        glib::timeout_future(std::time::Duration::from_secs(2)).await;
    }
}

/// Marks one more genuine remote write as starting -- called from
/// every one of this app's own remote-writing operations, right before
/// that write begins. See pending_remote_writes' own doc comment on
/// AppState for the full design this participates in and why it exists.
///
/// Fires the pre-flight check, once per batch, only for whichever
/// write happens to be the first to transition the count from 0 to
/// nonzero -- anything that starts while the count is already nonzero
/// just joins the batch already in progress, with no redundant check of
/// its own. Synchronous, no await between reading and incrementing the
/// count -- this app runs on a single-threaded main context, so this
/// check-then-mutate step can't race against another call to this same
/// function; whichever call's own synchronous code happens to run first
/// is unambiguously the one that started the batch, regardless of which
/// of the seven different call sites it came from.
fn begin_remote_write(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>) {
    let was_zero = state.pending_remote_writes.get() == 0;
    state.pending_remote_writes.set(state.pending_remote_writes.get() + 1);
    if !was_zero {
        return;
    }
    if is_debug_enabled() {
        eprintln!("[{}] [debug] remote-write batch starting -- pre-flight ETag check", now_hms());
    }
    for connection in active_connections(state) {
        glib::MainContext::default().spawn_local(clone!(
            #[strong] state,
            #[strong] watcher,
            async move {
                let Some(account) = all_accounts(&state, &watcher).into_iter().find(|a| a.account_id() == connection.account_id) else { return };
                let Some(remote_root_url) = connection_remote_root_url(&account, &connection) else { return };
                let Some((_, username, password)) = account.credentials(&watcher).await else { return };
                let Ok(Some(stored_etag)) = journal::get_root_etag(&state.journal, &connection.id) else { return };
                let password_str = password.as_str().to_string();
                // Pool 2 (confirm/detect) -- same reasoning as try_etag_
                // skip's own, identical acquisition: this is exactly the
                // same shape of work as sync.rs's own Pool 2 calls (a
                // single stat-like check confirming whether something
                // changed), just structurally separate for reasons that
                // predate this pool hierarchy entirely.
                let _permit = state.confirm_pool.acquire(&state.content_semaphore, "Pool 2 (confirm/detect)").await;
                let unchanged = push::run_blocking(move || push::root_etag_unchanged(&remote_root_url, &username, &password_str, &stored_etag)).await;
                if !unchanged {
                    if is_debug_enabled() {
                        eprintln!("[{}] [debug] pre-flight ETag mismatch @ {} -- marked dirty", now_hms(), connection.id);
                    }
                    state.dirty_connections.borrow_mut().insert(connection.id);
                }
            }
        ));
    }
}

/// Marks one genuine remote write as finished, success or failure --
/// called from every one of this app's own remote-writing operations,
/// right after that write completes. See pending_remote_writes' own
/// doc comment on AppState for the full design this participates in.
///
/// Decrements the global in-flight count; once it reaches zero (every
/// write from every source has now settled), fires the post-flight
/// side: any connection marked dirty by begin_remote_write's own pre-
/// flight check, or -- if coalesced_notify_push_pending is set -- every
/// active connection, skips straight to a real, immediate check rather
/// than trusting anything about the batch just finished. Every other
/// connection gets the cheap path instead: capture the live root ETag
/// now and store it as the new, trusted journal baseline, so a later
/// notify_push comparing against it can be safely recognized as just an
/// echo and skipped.
///
/// No account_id parameter, deliberately -- active_connections below is
/// what actually needs to be right, not which account triggered the
/// original increment. Even the rare case of switching accounts while a
/// batch is still in flight resolves correctly this way: active_
/// connections always reflects whichever account is active by the time
/// this actually fires, not whichever was active when the batch began.
fn end_remote_write(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>) {
    let count = state.pending_remote_writes.get().saturating_sub(1);
    state.pending_remote_writes.set(count);
    if count > 0 {
        return;
    }
    let coalesced = state.coalesced_notify_push_pending.replace(false);
    let dirty = state.dirty_connections.replace(HashSet::new());
    if is_debug_enabled() {
        eprintln!("[{}] [debug] remote-write batch settled (coalesced={coalesced}, dirty={})", now_hms(), dirty.len());
    }
    for connection in active_connections(state) {
        if coalesced || dirty.contains(&connection.id) {
            // Skips the post-flight capture entirely for this
            // connection -- deliberately: doing it anyway would just
            // produce a baseline blending this batch's own writes
            // together with whatever else might have happened, which
            // is exactly the un-decomposable snapshot this whole design
            // exists to avoid relying on. The real check about to run
            // (via notify_push_pending/spawn_immediate_sync_round
            // below) is what actually discovers what changed, and its
            // own, natural completion re-establishes a trustworthy
            // baseline the normal way (see journal::set_root_etag's own
            // one call site, in run_sync_round).
            state.notify_push_pending.borrow_mut().insert(connection.id);
            continue;
        }
        glib::MainContext::default().spawn_local(clone!(
            #[strong] state,
            #[strong] watcher,
            async move {
                let Some(account) = all_accounts(&state, &watcher).into_iter().find(|a| a.account_id() == connection.account_id) else { return };
                let Some(remote_root_url) = connection_remote_root_url(&account, &connection) else { return };
                let Some((_, username, password)) = account.credentials(&watcher).await else { return };
                let password_str = password.as_str().to_string();
                let _permit = state.confirm_pool.acquire(&state.content_semaphore, "Pool 2 (confirm/detect)").await;
                let Some(etag) = push::run_blocking(move || push::fetch_root_etag(&remote_root_url, &username, &password_str)).await else { return };
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                if let Err(e) = journal::set_root_etag(&state.journal, &connection.id, &etag, now) {
                    eprintln!("couldn't store post-flight root ETag for {}: {e}", connection.id);
                }
            }
        ));
    }
    if coalesced || !dirty.is_empty() {
        spawn_immediate_sync_round(state, watcher);
    }
}

fn active_connections(state: &AppState) -> Vec<config::SyncConnection> {
    let config = state.config.borrow();
    let Some(active_id) = config.active_account_id.as_deref() else { return Vec::new() };
    config.connections.iter()
        .filter(|c| c.enabled && c.account_id == active_id)
        .cloned()
        .collect()
}

/// Filters out Download-only connections entirely -- by explicit
/// request, a connection where local changes can never win anything at
/// all has no reason to pay for a local FileMonitor watch on any of its
/// own directories. sync_watches's own "anything not in `wanted` is
/// dropped entirely" contract (see its own doc comment) is what makes
/// this a plain filter here rather than needing any separate teardown
/// logic of its own: a connection that flips to DownloadOnly, or gets
/// disabled, or removed entirely, are all just "no longer in this list"
/// to sync_watches, indistinguishable from each other and handled the
/// exact same way.
fn active_watch_targets(state: &AppState) -> Vec<(String, PathBuf, Vec<String>)> {
    active_connections(state).into_iter()
        .filter(|c| c.direction != config::SyncDirection::DownloadOnly)
        .map(|c| (c.id, c.local_path, c.excluded_subpaths))
        .collect()
}

/// Refreshes the Status tab's row (worst-first title + last-checked
/// subtitle) and the always-visible Log section below it. Called both
/// after every sync round and every time a SyncEvent arrives mid-round,
/// so the UI never lags behind sync_state/status_log by more than one
/// event.

/// `refresh_issues`: whether to also run refresh_conflicts_ui this call --
/// true for essentially every caller (a button click, a settings change,
/// anything low-frequency, where refreshing immediately is both cheap in
/// practice and the right, expected behavior). The one exception is
/// apply_status_event, which passes this based on the specific SyncEvent
/// just received rather than unconditionally -- see its own call site for
/// why: refresh_conflicts_ui's own DB queries + full clone + rebuild
/// running on every single SyncEvent, confirmed as a real, severe
/// slowdown once thousands of conflicts accumulate in one session, since
/// the overwhelming majority of events (a plain successful upload, a
/// "looked conflicting but matched" resolution) never actually change
/// what that rebuild would produce in the first place.
/// Inserts thousand-separator commas into a plain integer for display --
/// e.g. 84203 -> "84,203". No external crate for something this small
/// and self-contained; std has no built-in locale-aware number
/// formatting, and this app has no other need for one.
fn format_with_thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut result = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Appends exactly one new, already-formatted line to the log
/// TextView's own buffer directly, in place -- called from apply_
/// status_event, right where that same line gets pushed to state.
/// status_log itself, never from update_status_ui or any other,
/// broader refresh. That placement is what makes this genuinely
/// append-only rather than needing its own "did anything actually
/// change" check: this function only ever runs when a real, new line
/// exists, since that's the only time its own caller runs at all.
///
/// Prepended at the start of the buffer, not appended at the end --
/// matches StatusLog::recent's own newest-first ordering, which this
/// app's log display has always used; switching to newest-last just to
/// make "append" literal would be a real, visible behavior change
/// nobody asked for. GtkTextBuffer's own insert() is efficient at any
/// position, not just the end (see its own B-tree-backed implementation
/// -- gtk4's real docs confirm line_count() alone is cached specifically
/// because it's expected to be called often), so prepending here costs
/// nothing extra over appending would have.
///
/// was_empty is decided by the caller, before its own push() -- this
/// function has no way to tell "the buffer already holds one real line"
/// from "the buffer still holds the never-cleared 'Nothing logged yet.'
/// placeholder" apart from being told directly, and guessing from the
/// buffer's own text content would mean comparing against that exact
/// placeholder string, which is fragile in a way an explicit bool isn't.
fn append_status_log_line(state: &Rc<AppState>, formatted: &str, was_empty: bool) {
    let Some(status_log_label) = state.window_widgets.borrow().as_ref().map(|w| w.status_log_label.clone()) else {
        return; // no window open right now -- nothing to append to
    };
    let buffer = status_log_label.buffer();
    if was_empty {
        // Replaces the "Nothing logged yet." placeholder outright --
        // the one and only case elsewhere in this function that's a
        // full replace rather than a true prepend, since there's
        // nothing meaningful to prepend *to* yet.
        buffer.set_text(formatted);
        return;
    }
    let mut start = buffer.start_iter();
    // Trailing \n, not leading -- this inserts before whatever was
    // already the first line, so the separator has to land between
    // this new line and that old first line, not before this new line
    // itself (which would just be a blank first line in the display).
    buffer.insert(&mut start, &format!("{formatted}\n"));

    let max_lines = status::STATUS_LOG_CAPACITY as i32;
    if buffer.line_count() > max_lines {
        // Oldest line(s) trimmed from the *end* of the buffer, not the
        // start -- the reverse of where a normal append-only log would
        // trim from, because of the same newest-first ordering this
        // whole function preserves: with the newest content at the
        // start, the oldest is always what's sitting at the end. At
        // most one line over on any single call (this function only
        // ever adds one line at a time), so in practice this only ever
        // deletes the single, exact line that just pushed the count
        // over -- but the loop handles more regardless, rather than
        // assuming that invariant silently holds forever.
        while buffer.line_count() > max_lines {
            let mut trim_start = buffer.iter_at_line(max_lines).unwrap_or_else(|| buffer.end_iter());
            let mut end = buffer.end_iter();
            buffer.delete(&mut trim_start, &mut end);
        }
    }
}

/// Sets the log TextView's own initial content once, right after the
/// window itself is first built -- build_status_tab has no access to
/// state at all (it takes no parameters), so this is the earliest point
/// state.status_log's own, possibly-already-nonempty contents (sync
/// activity can genuinely happen before a window is ever opened, on a
/// hidden/background launch) can actually be reflected. The one
/// legitimate full-buffer replace in this whole file outside Clear
/// Log's own handler -- every other update after this point is
/// append_status_log_line's own, single-line prepend instead.
fn populate_status_log_initial(state: &Rc<AppState>, status_log_label: &gtk4::TextView) {
    let log = state.status_log.borrow();
    let mut lines = log.recent().peekable();
    if lines.peek().is_none() {
        status_log_label.buffer().set_text("Nothing logged yet.");
    } else {
        let text = lines.cloned().collect::<Vec<_>>().join("\n");
        status_log_label.buffer().set_text(&text);
    }
}

fn update_status_ui(state: &Rc<AppState>, refresh_issues: bool) {
    // Tray icon update -- BEFORE the early return below, deliberately.
    // The tray (when enabled) needs to reflect status even with no
    // window open at all (launched with --hidden, say) -- that pairing
    // is the whole point of the two features working together. Cheap
    // no-op when the tray isn't running: tray_update_tx is only Some
    // once spawn_tray_if_enabled actually spawns it.
    if let Some(tx) = state.tray_update_tx.borrow().as_ref() {
        let no_account_or_connections = state.config.borrow().active_account_id.is_none()
            || !state.active_account_ready.get()
            || active_connections(state).is_empty();
        let needs_attention = !state.pending_issues.borrow().is_empty();
        let paused = state.paused_for_metered.get() || state.paused_for_power_save.get() || state.paused_by_user.get();
        let new_status = tray::select_status(
            no_account_or_connections,
            needs_attention,
            paused,
            state.sync_state.get(),
        );
        let _ = tx.try_send(tray::TrayUpdate::Icon(new_status));
        // Same conditions as the Status tab's own Sync Now button below
        // -- see RippleTray::sync_now_enabled's own doc comment for why
        // this has to be sent as its own value rather than derived from
        // new_status on the tray's own side.
        let sync_now_enabled = !no_account_or_connections && !state.paused_by_user.get() && state.sync_state.get() == status::SyncState::Idle;
        let _ = tx.try_send(tray::TrayUpdate::SyncNowEnabled(sync_now_enabled));
        // Same reasoning, same fix as sync_now_enabled just above -- see
        // TrayUpdate::Paused's own doc comment for the real, confirmed
        // bug this closes: `paused` above already went into new_status's
        // own selection, but select_status's own priority order means
        // `new_status` can show Attention even while genuinely paused,
        // which the tray's own Pause/Resume menu label needs to know
        // regardless of what icon is currently showing.
        let _ = tx.try_send(tray::TrayUpdate::Paused(paused));
    }

    let Some(widgets) = state.window_widgets.borrow().as_ref()
        .map(|w| (w.status_row.clone(), w.active_transfers_row.clone(), w.conflicts_row.clone(), w.conflicts_icon.clone(), w.export_log_button.clone(), w.clear_log_button.clone(), w.sync_now_button.clone(), w.pause_button.clone(), w.pause_banner.clone())) else {
        return; // no window open right now -- nothing further to refresh
    };
    let (status_row, active_transfers_row, conflicts_row, conflicts_icon, export_log_button, clear_log_button, sync_now_button, pause_button, pause_banner) = widgets;

    if state.config.borrow().active_account_id.is_none() {
        status_row.set_title("No account selected");
        status_row.set_subtitle("Add one from the Connections tab.");
    } else if !state.active_account_ready.get() {
        status_row.set_title("Account not ready");
        status_row.set_subtitle("See Connections tab for details.");
    } else if active_connections(state).is_empty() {
        status_row.set_title("No connections configured");
        status_row.set_subtitle("Add one from the Connections tab.");
    } else if state.paused_by_user.get() && state.paused_reason_restored_account.get() {
        // Checked before the plain paused_by_user branch just below --
        // see paused_reason_restored_account's own doc comment on
        // AppState for why this is the more specific of the two, not a
        // genuinely separate pause.
        status_row.set_title("Paused");
        status_row.set_subtitle("Newly restored account");
    } else if state.paused_by_user.get() {
        status_row.set_title("Paused");
        status_row.set_subtitle("Paused by you");
    } else if state.paused_for_metered.get() {
        status_row.set_title("Paused");
        status_row.set_subtitle("Metered connection detected");
    } else if state.paused_for_power_save.get() {
        status_row.set_title("Paused");
        status_row.set_subtitle("Power Save Mode is active");
    } else {
        status_row.set_title(state.sync_state.get().label());
        let any_push_auth_failed = active_connections(state).iter()
            .any(|c| state.push_auth_failed.borrow().get(&c.account_id).copied().unwrap_or(false));
        let subtitle = if any_push_auth_failed {
            // Deliberately independent of sync_state -- a notify_push
            // auth failure doesn't touch the main WebDAV sync path at all
            // (it's a separate, optional side channel), so the ordinary
            // sync round can be working perfectly fine while this is
            // still worth surfacing on its own. Specifically distinguishes
            // a rejected credential (the app password was revoked, most
            // likely) from an ordinary, self-resolving network blip --
            // the latter looks identical to Disconnected otherwise, but
            // only one of the two is something the user can actually do
            // anything about.
            "notify_push: credential rejected -- try re-authenticating this account in GNOME Settings -> Online Accounts".to_string()
        } else if matches!(state.sync_state.get(), status::SyncState::Scanning | status::SyncState::Syncing) {
            // "Last checked" isn't meaningful mid-round (it refers to the
            // *previous* round's own completion), so this replaces it
            // specifically while there's still real, visible work
            // underway -- either the main crawl itself, or, once that's
            // caught up, the background verification pass still queued
            // behind it (see trigger_sync_round's own PendingVerification
            // check for why sync_state can still be Scanning/Syncing even
            // after the crawl itself has technically finished).
            if state.discovered_count.get() != state.processed_count.get() {
                // Main crawl still actively discovering/processing new
                // items -- see discovered_count/processed_count's own doc
                // comments in sync.rs for why these are cheap to read
                // here on every ordinary update_status_ui call rather
                // than needing their own, separate update mechanism.
                format!(
                    "{} discovered; {} processed",
                    format_with_thousands(state.discovered_count.get()),
                    format_with_thousands(state.processed_count.get()),
                )
            } else {
                // Main crawl's own per-item work has caught up -- if
                // there's still a live PendingVerification queue behind
                // it, that's what's actually happening now. See
                // journal::count_in_status' own doc comment for why a
                // live query here (rather than a separately-tracked
                // counter) stays both cheap and always accurate, even
                // across multiple, potentially-overlapping rounds.
                let verifying: i64 = active_connections(state).iter()
                    .filter_map(|c| journal::count_in_status(&state.journal, &c.id, journal::SyncStatus::PendingVerification).ok())
                    .sum();
                if verifying > 0 {
                    format!("{} processing", format_with_thousands(verifying as u64))
                } else {
                    state.last_checked.borrow().as_ref()
                        .map(|t| format!("Last checked {t}"))
                        .unwrap_or_default()
                }
            }
        } else {
            state.last_checked.borrow().as_ref()
                .map(|t| format!("Last checked {t}"))
                .unwrap_or_default()
        };
        status_row.set_subtitle(&subtitle);
    }

    // Same condition, same specific-before-generic ordering, as the
    // status_row subtitle branch just above.
    pause_banner.set_revealed(state.paused_by_user.get() && state.paused_reason_restored_account.get());

    if state.paused_by_user.get() {
        pause_button.set_icon_name("media-playback-start-symbolic");
        pause_button.set_tooltip_text(Some("Resume syncing"));
    } else {
        pause_button.set_icon_name("media-playback-pause-symbolic");
        pause_button.set_tooltip_text(Some("Pause syncing"));
    }

    // Enabled whenever the status above would read "Idle" OR would read
    // "Paused" for an automatic policy reason (metered/low battery) --
    // NOT when paused_by_user specifically. See trigger_sync_round's own
    // doc comment on its force parameter for the full reasoning: an
    // automatic policy pause is worth being able to override for one
    // deliberate reason (get today's files before Wi-Fi, say); an
    // explicit "stop everything" the user just clicked is not something
    // this button should quietly talk them out of.
    let account_and_connections_ready = state.config.borrow().active_account_id.is_some() && !active_connections(state).is_empty();
    let not_manually_paused = !state.paused_by_user.get();
    let is_idle = state.sync_state.get() == status::SyncState::Idle;
    sync_now_button.set_sensitive(account_and_connections_ready && not_manually_paused && is_idle);

    // TextView content itself is no longer touched here at all --
    // append_status_log_line (called from apply_status_event, right
    // where a new line is genuinely pushed) is the only thing that ever
    // updates it now, a real, single-line prepend rather than this
    // function's own former full-buffer rebuild-and-replace on every
    // call regardless of cause. Button sensitivity alone stays here --
    // cheap (an empty-check on an in-memory VecDeque), and this is
    // still the right, general-purpose place for it, unlike the log
    // content itself.
    let log_is_empty = state.status_log.borrow().recent().next().is_none();
    export_log_button.set_sensitive(!log_is_empty);
    clear_log_button.set_sensitive(!log_is_empty);

    // Unconditional, unlike refresh_conflicts_ui just below -- this is
    // cheap (reading an in-memory map), not a real journal query, so it
    // doesn't need the same refresh_issues gate that exists specifically
    // to bound how often that more expensive rebuild runs.
    refresh_active_transfers_ui(state, &active_transfers_row);

    if refresh_issues {
        refresh_conflicts_ui(state, &conflicts_row, &conflicts_icon);
    }
}

/// Refreshes state.pending_issues from the journal's sync_items rows
/// flagged status=Conflict or permission_error, scoped to the
/// currently-active account, and updates the Sync Issues row to match --
/// hidden entirely at zero (no persistent "None detected" reminder that
/// something's imperfect when nothing actually is), "N need attention"
/// otherwise. The full detail (local/remote info for a conflict,
/// Retry/Dismiss for a permission error) lives in
/// show_sync_issues_dialog, opened by activating this row.
fn refresh_conflicts_ui(state: &Rc<AppState>, conflicts_row: &libadwaita::ActionRow, conflicts_icon: &gtk4::Image) {
    let config = state.config.borrow();
    // Scoped to the active account, same as everything else in the UI --
    // by account_id, not active_connections()'s enabled filter, so an
    // issue on a connection you've merely disabled (but not switched
    // away from) still counts here.
    let Some(active_id) = config.active_account_id.clone() else {
        *state.pending_issues.borrow_mut() = Vec::new();
        conflicts_row.set_subtitle("None");
        // Dimmed, not full opacity -- nothing to resolve with no account
        // selected, same as the zero-issues case below.
        conflicts_icon.set_opacity(0.3);
        return;
    };

    let mut built: Vec<SyncIssue> = Vec::new();
    for connection in config.connections.iter().filter(|c| c.account_id == active_id) {
        let conflicts = journal::items_in_status(&state.journal, &connection.id, journal::SyncStatus::Conflict)
            .unwrap_or_default();
        built.extend(conflicts.into_iter().map(|item| SyncIssue::Conflict(connection.clone(), item)));
        let permission_errors = journal::items_with_permission_error(&state.journal, &connection.id)
            .unwrap_or_default();
        built.extend(permission_errors.into_iter().map(|item| SyncIssue::PermissionError(connection.clone(), item)));
    }
    built.sort_by(|a, b| b.item().updated_at.cmp(&a.item().updated_at)); // most recent first

    let count = built.len();
    // Same Rc<RefCell<>> the resolver dialog holds its own clone of --
    // see pending_issues' own doc comment on AppState for why this is
    // a full replace rather than something the dialog owns a snapshot of.
    *state.pending_issues.borrow_mut() = built;

    if count > 0 {
        conflicts_row.set_subtitle(&format!("{count} need attention"));
        conflicts_icon.set_opacity(1.0);
    } else {
        conflicts_row.set_subtitle("None");
        // Dimmed, not full opacity -- the row itself now stays visible
        // at zero (see AppState's own doc comment on pending_issues, and
        // the earlier decision to keep this row shown rather than
        // hiding it entirely), so the icon needs its own way to show
        // "nothing to resolve right now" instead of looking identical
        // to the "issues need attention" state.
        conflicts_icon.set_opacity(0.3);
    }
}

/// Rebuilds the Active Transfers row's own child list from
/// state.active_transfers, called from update_status_ui the same way
/// refresh_conflicts_ui is. No journal query needed here at all --
/// unlike Sync Issues, this is purely live, in-memory state populated
/// directly by SyncEvent::TransferStarted/TransferEnded (see
/// apply_status_event), not something persisted or derived from the
/// journal.
fn refresh_active_transfers_ui(state: &Rc<AppState>, active_transfers_row: &libadwaita::ExpanderRow) {
    // Sorted by path for a stable display order -- a HashMap's own
    // iteration order is unstable, which would otherwise mean rows
    // visibly reordering themselves on every refresh for no reason a
    // user could make sense of.
    let mut transfers: Vec<(String, String, status::TransferDirection, Option<(i64, i64)>)> = state.active_transfers.borrow().values()
        .map(|t| (t.path.clone(), t.connection.clone(), t.direction, t.progress))
        .collect();
    transfers.sort_by(|a, b| a.0.cmp(&b.0));

    for row in state.active_transfer_rows.borrow_mut().drain(..) {
        active_transfers_row.remove(&row);
    }

    let count = transfers.len();
    if count > 0 {
        active_transfers_row.set_subtitle(&format!("{count}"));
    } else {
        active_transfers_row.set_subtitle("None");
    }

    let mut rows = state.active_transfer_rows.borrow_mut();
    for (path, connection, direction, progress) in transfers {
        // Percent, not a raw "X/Y bytes" fallback -- see
        // ActiveTransfer::progress's own doc comment: this is only ever
        // Some at all once bytes_total is confirmed > 0, so a percentage
        // is always computable whenever there's anything to show here in
        // the first place.
        let subtitle = match progress {
            Some((so_far, total)) => {
                let percent = ((so_far.max(0) as f64 / total as f64) * 100.0).min(100.0) as u32;
                // At 100%, the byte-transfer itself is genuinely done --
                // but the row doesn't clear yet, since there's still the
                // move/rename, an ETag query, and a journal write left
                // before TransferEnded actually fires (see
                // TransferGuard's own doc comment in sync.rs). A frozen
                // "100%" read as stuck, not as "still working, just a
                // different phase" -- this says so directly instead.
                if percent >= 100 {
                    format!("{} (Finalizing\u{2026})", direction.label())
                } else {
                    format!("{} ({percent}%)", direction.label())
                }
            }
            None => direction.label().to_string(),
        };
        let row = libadwaita::ActionRow::builder()
            .title(format!("{connection}: {path}"))
            .subtitle(subtitle)
            // Both path and connection name are arbitrary filesystem/
            // user-chosen text, not markup -- same fix, and the same
            // real, reported bug, as folder_picker.rs's own row titles:
            // an unescaped "&" (or "<"/">") in either would otherwise
            // break Pango's markup parser, since ActionRow interprets
            // both title and subtitle as markup by default.
            .use_markup(false)
            .build();
        active_transfers_row.add_row(&row);
        rows.push(row);
    }
}

fn format_bytes_short(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} bytes")
    }
}

/// The four resolutions, matching the four buttons in the dialog below --
/// One deferred action -- queued specifically because either a broader
/// round was already in flight, or the connection was paused and the
/// user didn't check "Apply immediately, even while paused." Drained
/// only at the end of a round that actually runs to completion (see
/// trigger_sync_round's own drain logic) -- for a pause specifically,
/// that only happens once the connection is actually unpaused, since a
/// paused connection has no round running at all until then.
enum QueuedSyncAction {
    RetryPermissionError { connection_id: String, relative_path: String, is_directory: bool },
    ResolveConflict { connection_id: String, relative_path: String, action: ConflictAction },
}

/// One entry in the Sync Issues carousel -- either a genuine conflict
/// (both sides changed, ambiguous which should win, four resolution
/// choices) or a permission error (not ambiguous at all -- the
/// operation simply can't happen right now, just Retry/Dismiss).
/// Deliberately one shared list/carousel rather than two separate ones:
/// from a user's own perspective, both are just "things Ripples wants
/// me to look at," and reusing the same paging shell (position label,
/// Back/Next, the pause-aware checkbox) avoids building a second one
/// for what's structurally the same "step through a list of items, one
/// at a time" interaction.
#[derive(Debug, Clone)]
enum SyncIssue {
    Conflict(config::SyncConnection, journal::SyncItem),
    PermissionError(config::SyncConnection, journal::SyncItem),
}

impl SyncIssue {
    fn connection(&self) -> &config::SyncConnection {
        match self {
            SyncIssue::Conflict(c, _) | SyncIssue::PermissionError(c, _) => c,
        }
    }
    fn item(&self) -> &journal::SyncItem {
        match self {
            SyncIssue::Conflict(_, i) | SyncIssue::PermissionError(_, i) => i,
        }
    }
    /// Identity for list-membership comparisons (removing just-acted-on
    /// items from the shared list) -- connection + path is already
    /// unique per sync_items row, same key journal::mark_synced itself
    /// upserts on.
    fn key(&self) -> (String, String) {
        (self.connection().id.clone(), self.item().relative_path.clone())
    }
}

/// Per-item state within a resolver dialog's own snapshot -- see
/// ResolverItem's own doc comment for why a full snapshot with mutable,
/// per-item resolution state replaced the earlier "shared list, remove
/// on resolve" design entirely.
#[derive(Debug, Clone, PartialEq)]
enum IssueResolution {
    Unresolved,
    /// A resolution attempt is currently in flight for this item -- set
    /// the moment its own button is clicked, before the actual async
    /// work even starts, specifically so the UI can advance immediately
    /// rather than waiting on the result (see this dialog's own doc
    /// comment on why "optimistic" means the advance itself happens
    /// before the outcome is known, not just before the item gets
    /// tagged). Treated the same as Succeeded/Failed by advance_to_
    /// next_unresolved's own "skip anything not Unresolved" scan -- an
    /// item already being worked on shouldn't be offered again.
    Pending,
    /// Distinct from Pending -- this item's own action was queued
    /// because the connection is paused, not because it's actively
    /// running right now. The distinction matters for the summary
    /// page's own icon logic (spinner only for genuine, in-flight work;
    /// a paused connection with nothing actually running shouldn't claim
    /// one is) and for correctness: neither apply_conflict_resolution
    /// nor retry_single_item ever re-check pause state mid-execution, so
    /// the pause/no-pause decision has to be made synchronously, before
    /// anything spawns, by the button handler itself -- see this
    /// dialog's own doc comment on why that closes the race window a
    /// check made inside a spawned task wouldn't. Re-checked on every
    /// carousel page change (see check_paused_items) and flipped to
    /// Pending the moment the connection actually unpauses while this
    /// dialog is still open; queued because a round was merely already
    /// in flight (state.syncing, not an actual pause) stays Pending
    /// instead, since that's a genuinely transient wait, not something
    /// that could be stuck indefinitely the way a real pause can.
    Paused,
    Succeeded,
    Failed(String),
}

/// One page's worth of state in the resolver dialog's own snapshot --
/// the issue itself (fixed, from whatever state.pending_issues held at
/// the moment the dialog opened) paired with this dialog instance's own,
/// independent record of what happened to it. Deliberately NOT the same
/// Rc<RefCell<Vec<SyncIssue>>> the dialog used to share directly with
/// state.pending_issues: that sharing was confirmed as a real,
/// structural conflict with keeping resolved items visible at all --
/// refresh_conflicts_ui does a full *replace* of state.pending_issues
/// (not an incremental update) on every call, which happens constantly
/// from background sync activity while this dialog might be open, and a
/// fresh journal query naturally excludes anything just resolved (it's
/// no longer in Conflict/PermissionError status), so it would silently
/// erase this dialog's own view of what it had just done. A true,
/// independent snapshot -- cloned once, at open time, then never touched
/// by anything outside this dialog again -- is what makes "resolved
/// items stay visible, tagged with what happened" possible at all; see
/// show_sync_issues_dialog's own doc comment for the full reconciliation
/// story once the dialog actually closes.
#[derive(Debug, Clone)]
struct ResolverItem {
    issue: SyncIssue,
    resolution: IssueResolution,
}

/// Keep Local/Remote push whichever side wins as-is; Rename Local/Remote
/// keep both by moving one aside first. No Ignore: closing the dialog or
/// moving to the next conflict already does the same nothing an Ignore
/// button would, so it isn't a fifth option, just the absence of picking
/// one of these four.
#[derive(Debug, Clone, Copy)]
enum ConflictAction {
    KeepLocal,
    KeepRemote,
    KeepBothRenameLocal,
    KeepBothRenameRemote,
}

impl ConflictAction {
    fn label(self) -> &'static str {
        match self {
            ConflictAction::KeepLocal => "Keep Local",
            ConflictAction::KeepRemote => "Keep Remote",
            ConflictAction::KeepBothRenameLocal => "Rename Local",
            ConflictAction::KeepBothRenameRemote => "Rename Remote",
        }
    }
}

/// Builds a fresh SyncTarget for `connection` and dispatches to the
/// matching sync::resolve_* function. Returns whether it actually
/// succeeded -- best-effort: a mount/credential/transfer failure here
/// just means the item stays flagged as a conflict (nothing removed from
/// the pending list), same as any other transient sync failure
/// elsewhere in this app. Actually logged now (not just eprintln!'d) --
/// see status::SyncEvent::ConflictResolved/ConflictResolutionFailed --
/// which matters more than it used to now that the button handler clears
/// the UI immediately and resolves in the background: the log is the
/// only place a failure here is still visible at all.
async fn apply_conflict_resolution(
    state: &Rc<AppState>,
    watcher: &Rc<GoaWatcher>,
    connection: &config::SyncConnection,
    item: &journal::SyncItem,
    action: ConflictAction,
    // None for every direct, click-triggered call from the resolver
    // dialog itself (Keep/Rename) -- unchanged behavior, the shared,
    // app-wide content_semaphore as always. Some only ever comes from
    // the queued-action drain loop's own per-action decision -- see
    // AppState::resolver_privileged_semaphore's own doc comment for why
    // a real override, not just an extra permit into the same shared
    // pool, is what that specific case actually needs.
    content_semaphore_override: Option<&Rc<sync::Semaphore>>,
) -> Result<(), String> {
    if is_debug_enabled() {
        eprintln!("[{}] [debug] resolution starting: {} / {} -- {action:?}", now_hms(), connection.name, item.relative_path);
    }
    let mut target = match build_sync_target_for_connection(state, watcher, connection).await {
        Ok(t) => t,
        Err(e) => {
            if is_debug_enabled() {
                eprintln!("[{}] [debug] resolution aborted: {} / {} -- couldn't build sync target: {e}", now_hms(), connection.name, item.relative_path);
            }
            let _ = state.status_events_tx.try_send(status::SyncEvent::ConflictResolutionFailed {
                connection: connection.name.clone(), path: item.relative_path.clone(), error: e.to_string(),
            });
            return Err(e);
        }
    };
    if let Some(privileged) = content_semaphore_override {
        target.content_semaphore = privileged.clone();
    }
    let result = match action {
        ConflictAction::KeepLocal => sync::resolve_keep_local(&state.journal, &target, &item.relative_path, &state.status_events_tx).await,
        ConflictAction::KeepRemote => sync::resolve_keep_remote(&state.journal, &target, &item.relative_path, &state.status_events_tx).await,
        ConflictAction::KeepBothRenameLocal => sync::resolve_keep_both_rename_local(&state.journal, &target, &item.relative_path, &state.status_events_tx).await,
        ConflictAction::KeepBothRenameRemote => sync::resolve_keep_both_rename_remote(&state.journal, &target, &item.relative_path, &state.status_events_tx).await,
    };
    match result {
        Ok(()) => {
            if is_debug_enabled() {
                eprintln!("[{}] [debug] resolution succeeded: {} / {} -- {action:?}", now_hms(), connection.name, item.relative_path);
            }
            let _ = state.status_events_tx.try_send(status::SyncEvent::ConflictResolved {
                connection: connection.name.clone(), path: item.relative_path.clone(), resolution: action.label(),
            });
            Ok(())
        }
        Err(e) => {
            let error_string = e.to_string();
            if is_debug_enabled() {
                eprintln!("[{}] [debug] resolution failed: {} / {} -- {action:?} -- {error_string}", now_hms(), connection.name, item.relative_path);
            }
            let _ = state.status_events_tx.try_send(status::SyncEvent::ConflictResolutionFailed {
                connection: connection.name.clone(), path: item.relative_path.clone(), error: error_string.clone(),
            });
            Err(error_string)
        }
    }
}

/// Local half of the info display -- synchronous, since it's just a
/// stat call, no network involved. Deliberately live (not the frozen
/// mark_conflict_pending snapshot) so what the user sees matches what a
/// resolution action would actually operate on right now.
fn format_local_file_info(local_path: &Path) -> String {
    match std::fs::metadata(local_path) {
        Ok(meta) => {
            let size = format_bytes_short(meta.len() as i64);
            let modified = meta.modified().ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .and_then(|d| glib::DateTime::from_unix_local(d.as_secs() as i64).ok())
                .and_then(|d| d.format("%Y-%m-%d %H:%M").ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown time".to_string());
            format!("{size}\nmodified {modified}")
        }
        Err(_) => "Not present locally".to_string(),
    }
}

/// Remote half of the info display -- needs a live mount, so this is
/// async and can fail (credentials, network, account removed) in ways
/// the local side can't. Mirror of format_local_file_info otherwise.
async fn format_remote_file_info(
    state: &Rc<AppState>,
    watcher: &Rc<GoaWatcher>,
    connection: &config::SyncConnection,
    relative_path: &str,
) -> String {
    let target = match build_sync_target_for_connection(state, watcher, connection).await {
        Ok(t) => t,
        Err(e) => return format!("Unavailable - {e}"),
    };
    match sync::remote_stat(&target, relative_path).await {
        Ok(info) => {
            let size = format_bytes_short(info.size);
            let modified = info.mtime
                .and_then(|t| glib::DateTime::from_unix_utc(t).ok())
                .and_then(|dt| dt.to_local().ok())
                .and_then(|dt| dt.format("%Y-%m-%d %H:%M").ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown time".to_string());
            format!("{size}\nmodified {modified}")
        }
        Err(sync::SyncError::WebDav(webdav::WebDavError::UnexpectedStatus { status: 404, .. }, _)) => "Not present remotely".to_string(),
        Err(e) => format!("Unavailable - {e}"),
    }
}

/// Snapshot-based, carousel-driven resolver -- a full redesign from the
/// earlier "shared list, remove on resolve, single view rebuilt in
/// place" version, by explicit request. Key differences:
///   - `items` is cloned ONCE, at open time, into this dialog's own,
///     independent Vec -- never the shared state.pending_issues Rc
///     directly. See ResolverItem's own doc comment for exactly why
///     that sharing was a real, structural problem for keeping resolved
///     items visible at all.
///   - Resolving an item does NOT remove it. It's tagged Succeeded or
///     Failed(error) in place, and the carousel optimistically advances
///     to the next Unresolved page -- Back/Next (or swiping) can still
///     reach an already-acted-on page at any time, showing whatever its
///     own tag says (a green success banner with disabled buttons, or a
///     red error, never both).
///   - An AdwCarousel, not a single set of shared, rebuilt-in-place
///     widgets -- one full page per item, built once, upfront.
///   - On close, this dialog's own snapshot is simply discarded and
///     refresh_conflicts_ui is called once: a fresh journal query
///     naturally reconstructs state.pending_issues correctly with zero
///     extra bookkeeping here -- a resolution that failed (or was never
///     attempted) never touched the journal in the first place, so it's
///     still flagged there and reappears on its own; a genuine success
///     already updated the journal directly (via apply_conflict_
///     resolution/journal::mark_synced), so it naturally no longer
///     matches the query; anything newly discovered while this dialog
///     sat open is naturally included too, all for free from the same
///     query this app already runs elsewhere.
fn show_sync_issues_dialog(
    state: Rc<AppState>,
    watcher: Rc<GoaWatcher>,
    issues: Vec<SyncIssue>,
    start_index: usize,
) {
    if issues.is_empty() {
        return;
    }
    let parent_window = state.window_widgets.borrow().as_ref().map(|w| w.window.clone());
    let items: Rc<RefCell<Vec<ResolverItem>>> = Rc::new(RefCell::new(
        issues.into_iter().map(|issue| ResolverItem { issue, resolution: IssueResolution::Unresolved }).collect()
    ));
    // Shared across every page of this one open dialog instance --
    // defaults unchecked each time the dialog itself is opened (a fresh
    // Cell, not something persisted anywhere), but deliberately carries
    // forward between pages within this same instance rather than
    // resetting per-page: re-checking the same box on every single page
    // while stepping through many items on a metered connection would
    // be exactly the repetitive annoyance this is meant to avoid.
    let apply_immediately = Rc::new(Cell::new(false));

    let carousel = libadwaita::Carousel::builder().vexpand(true).build();

    let prev_button = gtk4::Button::builder().label("Back").build();
    let next_button = gtk4::Button::builder().label("Next").build();
    let close_button = gtk4::Button::builder().label("Close").build();
    // Icon-only, distinct from the three text buttons around it -- a
    // secondary, occasional action (checking for anything new without
    // fully closing out), not something that should visually compete
    // with Close for attention in this same row.
    let refresh_button = gtk4::Button::builder().icon_name("view-refresh-symbolic").tooltip_text("Check for new issues").build();
    let position_label = gtk4::Label::builder().css_classes(["dim-label"]).build();

    let bottom_box = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).spacing(6).build();
    bottom_box.append(&position_label);
    bottom_box.append(&gtk4::Box::builder().hexpand(true).build());
    bottom_box.append(&refresh_button);
    bottom_box.append(&close_button);
    bottom_box.set_margin_start(24);
    bottom_box.set_margin_end(24);
    bottom_box.set_margin_bottom(24);
    bottom_box.set_margin_top(6);

    let content_box = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).build();
    content_box.append(&carousel);
    content_box.append(&bottom_box);

    let toolbar_view = libadwaita::ToolbarView::builder().content(&content_box).build();
    let header_bar = libadwaita::HeaderBar::builder()
        .show_start_title_buttons(false)
        .show_end_title_buttons(false)
        .build();
    header_bar.pack_start(&prev_button);
    header_bar.pack_end(&next_button);
    toolbar_view.add_top_bar(&header_bar);

    let mut dialog_builder = libadwaita::Window::builder()
        .title("Sync Issues")
        .default_width(480)
        .default_height(640)
        .modal(true)
        .content(&toolbar_view);
    if let Some(parent) = parent_window.as_ref() {
        dialog_builder = dialog_builder.transient_for(parent);
    }
    let dialog = dialog_builder.build();
    dialog.set_widget_name("ripples-themed-window");

    let escape_controller = gtk4::EventControllerKey::new();
    escape_controller.connect_key_pressed(clone!(
        #[strong] dialog,
        move |_, key, _, _| {
            if key == gtk4::gdk::Key::Escape {
                dialog.close();
                glib::Propagation::Stop
            } else {
                glib::Propagation::Proceed
            }
        }
    ));
    dialog.add_controller(escape_controller);

    // Single breakpoint for the whole dialog, shared across every page's
    // own columns_box -- add_setter is called once per page below, all
    // accumulating onto this same instance, since AdwBreakpoint itself
    // is dialog-wide, not per-page. Same 360sp threshold, same reasoning
    // (a Box's own "orientation" property is exactly what add_setter can
    // flip at runtime, unlike a Grid's fixed row/column attachments),
    // preserved verbatim from the single-page version this replaced.
    let dialog_breakpoint = libadwaita::Breakpoint::new(libadwaita::BreakpointCondition::new_length(
        libadwaita::BreakpointConditionLengthType::MaxWidth,
        360.0,
        libadwaita::LengthUnit::Sp,
    ));

    // Per-page state the lazy remote-info fetch (trigger_remote_fetch,
    // further down) needs to reach back into later -- position-indexed,
    // matching `items`' own indices 1:1 (both built by the same loop, in
    // the same order, and neither is ever reordered afterward).
    // success_banner/resolution_error_label/action_buttons deliberately
    // aren't stored here -- each button's own click handler already
    // captures its own, direct reference to exactly the widgets it needs
    // to update, so nothing else in this dialog ever needs to look them
    // up by index afterward; the Refresh button (see refresh_button's
    // own doc comment further down) reads the underlying `items` data
    // directly instead of inspecting individual page widgets at all.
    struct IssuePageWidgets {
        remote_info_label: Option<gtk4::Label>,
        remote_info_loaded: Cell<bool>,
    }
    let mut page_widgets: Vec<IssuePageWidgets> = Vec::new();

    // Advances the carousel to the next Unresolved page after this one,
    // if any -- shared by every action handler below rather than each
    // reimplementing the same "scan forward, skip anything already
    // tagged" search. Deliberately does NOT wrap back to the start: this
    // is meant to feel like working through a list front-to-back, not a
    // loop that could land back on an already-handled item without the
    // person choosing to go there themselves via Back or a swipe.
    // Lands on the summary page (the carousel's own last page, one past
    // every item's own index -- see the summary page's own construction
    // further down) whenever nothing Unresolved remains after
    // from_index. By request: gives the last resolution in a session
    // somewhere natural to send the user, rather than the earlier
    // version's own "do nothing, stay on whatever page was just acted
    // on" -- which left no signal at all that there was nothing further
    // to do.
    let advance_to_next_unresolved = Rc::new(clone!(
        #[strong] items,
        #[strong] carousel,
        move |from_index: usize| {
            let items_ref = items.borrow();
            let summary_index = items_ref.len() as u32;
            for i in (from_index + 1)..items_ref.len() {
                if items_ref[i].resolution == IssueResolution::Unresolved {
                    drop(items_ref);
                    let page = carousel.nth_page(i as u32);
                    carousel.scroll_to(&page, true);
                    return;
                }
            }
            drop(items_ref);
            let page = carousel.nth_page(summary_index);
            carousel.scroll_to(&page, true);
        }
    ));

    // Summary page -- the carousel's own last page, one past every real
    // item's own index (see advance_to_next_unresolved's own doc
    // comment for where that index comes from). By request: a natural
    // landing spot once nothing Unresolved remains, a live tally so the
    // user doesn't need to page back and forth to see where things
    // stand, and a place to actually explain what Refresh does, which
    // stays on every other page too (a "refresh right now, mid-review"
    // need is real and shouldn't require paging all the way here first)
    // -- this is a second, explained copy for whoever lands here
    // naturally, not a replacement.
    let summary_spinner = gtk4::Spinner::builder().width_request(48).height_request(48).visible(false).build();
    // Default set here, at construction, not left blank until the first
    // page-changed fires -- refresh_summary only ever runs once this
    // page is actually the one showing, which could trail slightly
    // behind the carousel's own scroll animation settling. checkbox-
    // checked (the "nothing left to do" reading) is the safest default
    // of the three regardless -- it's what a dialog with nothing yet
    // acted on genuinely is.
    let summary_icon = gtk4::Image::builder().pixel_size(48).icon_name("checkbox-checked-symbolic").build();
    let summary_pending_label = gtk4::Label::builder().xalign(0.0).build();
    let summary_paused_label = gtk4::Label::builder().xalign(0.0).build();
    let summary_skipped_label = gtk4::Label::builder().xalign(0.0).build();
    let summary_resolved_label = gtk4::Label::builder().xalign(0.0).build();
    let summary_failed_label = gtk4::Label::builder().xalign(0.0).build();
    // Unified, not two separate notes -- covers both Pending and Paused
    // together, since they need genuinely different explanations (one's
    // actively running right now, the other is just queued) and a
    // reader with both nonzero at once shouldn't have to reconcile two
    // separate messages that might otherwise have shown side by side.
    // Text itself is set entirely by refresh_summary below; hidden by
    // default here since the common "nothing in progress" case has
    // nothing to explain at all.
    let summary_safe_to_close_note = gtk4::Label::builder()
        .wrap(true).xalign(0.0).css_classes(["dim-label"]).visible(false)
        .build();
    let summary_refresh_note = gtk4::Label::builder()
        .label("Use the Refresh button below to clear resolved issues from this deck and add those newly identified.")
        .wrap(true).xalign(0.0).css_classes(["dim-label"])
        .build();

    let summary_counts_box = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).spacing(6).build();
    summary_counts_box.append(&summary_pending_label);
    summary_counts_box.append(&summary_paused_label);
    summary_counts_box.append(&summary_skipped_label);
    summary_counts_box.append(&summary_resolved_label);
    summary_counts_box.append(&summary_failed_label);

    let summary_box = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .spacing(18)
        .halign(gtk4::Align::Center)
        .margin_top(24).margin_bottom(12).margin_start(24).margin_end(24)
        .build();
    summary_box.append(&gtk4::Label::builder().label("Summary").css_classes(["title-1"]).build());
    summary_box.append(&summary_spinner);
    summary_box.append(&summary_icon);
    summary_box.append(&summary_counts_box);
    summary_box.append(&summary_safe_to_close_note);
    summary_box.append(&summary_refresh_note);

    let summary_scroller = gtk4::ScrolledWindow::builder()
        .child(&summary_box)
        .hscrollbar_policy(gtk4::PolicyType::Never)
        .vscrollbar_policy(gtk4::PolicyType::Automatic)
        .vexpand(true)
        .build();

    // Recomputes every count and the icon/spinner together from `items`
    // -- called only when the summary page is actually the one visible
    // (see connect_page_changed further down), not on every resolution
    // anywhere else in this dialog; nothing here is visible otherwise,
    // so there's nothing to gain from keeping it live in the background.
    let refresh_summary: Rc<dyn Fn()> = Rc::new(clone!(
        #[strong] items,
        #[strong] summary_spinner,
        #[strong] summary_icon,
        #[strong] summary_pending_label,
        #[strong] summary_paused_label,
        #[strong] summary_skipped_label,
        #[strong] summary_resolved_label,
        #[strong] summary_failed_label,
        #[strong] summary_safe_to_close_note,
        move || {
            let items_ref = items.borrow();
            let total = items_ref.len();
            let (mut pending, mut paused, mut resolved, mut failed) = (0usize, 0usize, 0usize, 0usize);
            for resolver_item in items_ref.iter() {
                match &resolver_item.resolution {
                    IssueResolution::Pending => pending += 1,
                    IssueResolution::Paused => paused += 1,
                    IssueResolution::Succeeded => resolved += 1,
                    IssueResolution::Failed(_) => failed += 1,
                    IssueResolution::Unresolved => {}
                }
            }
            drop(items_ref);
            // Skipped isn't its own tracked state at all -- by request,
            // just whatever's left once every other, real category is
            // accounted for: an item that's been viewed and passed over
            // is indistinguishable, at this point, from one never
            // visited at all, and both still need attention either way.
            let skipped = total.saturating_sub(resolved).saturating_sub(failed).saturating_sub(pending).saturating_sub(paused);

            summary_pending_label.set_label(&format!("Pending: {pending}"));
            summary_paused_label.set_label(&format!("Paused: {paused}"));
            summary_skipped_label.set_label(&format!("Skipped: {skipped}"));
            summary_resolved_label.set_label(&format!("Resolved: {resolved}"));
            summary_failed_label.set_label(&format!("Failed: {failed}"));

            // By request -- reassures that closing this dialog is safe
            // whenever there's real, still-in-progress work behind the
            // scenes, since neither case actually depends on this dialog
            // staying open at all: a Pending item's own spawned task
            // keeps running to completion regardless (see retry_
            // resolution's own doc comment), and a Paused item is
            // already sitting in state.pending_sync_actions, safely
            // queued until the connection actually resumes, dialog open
            // or not. Wording adapts to whichever of the two is actually
            // nonzero, rather than a single, generic sentence that would
            // have to describe both mechanisms every time regardless of
            // whether both are actually happening right now.
            let safe_to_close_text = match (pending > 0, paused > 0) {
                (true, true) => Some("It's safe to close this window -- pending actions will continue being performed, and paused actions are queued for processing once the connection resumes.".to_string()),
                (true, false) => Some("It's safe to close this window -- pending actions will continue being performed.".to_string()),
                (false, true) => Some("It's safe to close this window -- paused actions are queued for processing once the connection resumes.".to_string()),
                (false, false) => None,
            };
            summary_safe_to_close_note.set_visible(safe_to_close_text.is_some());
            if let Some(text) = safe_to_close_text {
                summary_safe_to_close_note.set_label(&text);
            }

            // Priority order matters: Pending, genuinely in-flight work
            // (see IssueResolution::Pending's own doc comment on exactly
            // what "in flight" means here) always wins over Paused, even
            // when both are nonzero at once -- a pause that started
            // after something was already running never stops that one,
            // specific item (neither apply_conflict_resolution nor
            // retry_single_item re-check pause mid-execution), so a
            // spinner is still the accurate read: something really is
            // happening right now, regardless of the connection's own
            // current pause state.
            if pending > 0 {
                summary_spinner.set_visible(true);
                summary_spinner.set_spinning(true);
                summary_icon.set_visible(false);
            } else {
                summary_spinner.set_visible(false);
                summary_spinner.set_spinning(false);
                summary_icon.set_visible(true);
                if paused > 0 {
                    summary_icon.set_icon_name(Some("media-playback-pause-symbolic"));
                } else {
                    summary_icon.set_icon_name(Some("checkbox-checked-symbolic"));
                }
            }
        }
    ));

    // Flips any Paused item back to Pending the moment the connection
    // actually unpauses while this dialog is still open -- checked
    // alongside the summary refresh above (same trigger: landing on
    // this page), not on a separate timer of its own, since nothing
    // needs this to be caught faster than the next time someone's
    // actually looking at the summary. A newly-Pending item here has no
    // spawned task of its own behind it (nothing to attempt right now,
    // it was only ever queued) -- it stays Pending, tracked, until
    // whatever round eventually drains pending_sync_actions actually
    // runs it, exactly the same as the moment it was first queued.
    let check_paused_items: Rc<dyn Fn()> = Rc::new(clone!(
        #[strong] state,
        #[strong] items,
        move || {
            let paused = state.paused_by_user.get() || state.paused_for_metered.get() || state.paused_for_power_save.get();
            if paused {
                return;
            }
            for resolver_item in items.borrow_mut().iter_mut() {
                if resolver_item.resolution == IssueResolution::Paused {
                    resolver_item.resolution = IssueResolution::Pending;
                }
            }
        }
    ));

    for (idx, resolver_item) in items.borrow().iter().enumerate() {
        let issue = resolver_item.issue.clone();
        let connection = issue.connection().clone();
        let item = issue.item().clone();

        let path_label = gtk4::Label::builder().css_classes(["heading"]).wrap(true).wrap_mode(gtk4::pango::WrapMode::WordChar).xalign(0.0).label(&item.relative_path).build();
        let connection_label = gtk4::Label::builder().css_classes(["dim-label"]).wrap(true).wrap_mode(gtk4::pango::WrapMode::WordChar).xalign(0.0).label(&connection.name).build();

        // Hidden until a resolution actually succeeds or fails -- see
        // this whole page's own doc comment on IssuePageWidgets for how
        // these two, plus action_buttons' own disabled state, together
        // represent this item's current IssueResolution.
        let success_banner = gtk4::Label::builder()
            .label("Resolution succeeded!")
            .css_classes(["success"])
            .wrap(true).wrap_mode(gtk4::pango::WrapMode::WordChar).xalign(0.0)
            .visible(false)
            .build();
        let resolution_error_label = gtk4::Label::builder()
            .css_classes(["error"])
            .wrap(true).wrap_mode(gtk4::pango::WrapMode::WordChar).xalign(0.0)
            .visible(false)
            .build();

        let page_scrollable = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(12)
            .margin_top(24).margin_bottom(12).margin_start(24).margin_end(24)
            .build();
        page_scrollable.append(&path_label);
        page_scrollable.append(&connection_label);
        page_scrollable.append(&success_banner);
        page_scrollable.append(&resolution_error_label);

        let (action_buttons, remote_info_label) = match &issue {
            SyncIssue::Conflict(..) => {
                dialog.set_title(Some("Resolve Conflicts"));
                let local_info_label = gtk4::Label::builder().wrap(true).wrap_mode(gtk4::pango::WrapMode::WordChar).xalign(0.0).valign(gtk4::Align::Start).height_request(72).build();
                let remote_info_label = gtk4::Label::builder().wrap(true).wrap_mode(gtk4::pango::WrapMode::WordChar).xalign(0.0).valign(gtk4::Align::Start).height_request(72).label("Loading\u{2026}").build();
                local_info_label.set_label(&format_local_file_info(&connection.local_path.join(&item.relative_path)));

                let local_column = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).spacing(18).hexpand(true).build();
                local_column.append(&gtk4::Label::builder().label("Local").css_classes(["heading"]).xalign(0.0).build());
                local_column.append(&local_info_label);
                let remote_column = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).spacing(18).hexpand(true).build();
                remote_column.append(&gtk4::Label::builder().label("Remote").css_classes(["heading"]).xalign(0.0).build());
                remote_column.append(&remote_info_label);

                let columns_box = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).spacing(48).homogeneous(true).build();
                columns_box.append(&local_column);
                columns_box.append(&remote_column);
                dialog_breakpoint.add_setter(&columns_box, "orientation", Some(&gtk4::Orientation::Vertical.to_value()));
                dialog_breakpoint.add_setter(&columns_box, "spacing", Some(&12i32.to_value()));
                dialog_breakpoint.add_setter(&columns_box, "homogeneous", Some(&false.to_value()));
                page_scrollable.append(&columns_box);

                let keep_local_button = gtk4::Button::builder().label("Keep Local").hexpand(true).build();
                let keep_remote_button = gtk4::Button::builder().label("Keep Remote").hexpand(true).build();
                let rename_local_button = gtk4::Button::builder().label("Rename Local").hexpand(true).build();
                let rename_remote_button = gtk4::Button::builder().label("Rename Remote").hexpand(true).build();
                local_column.append(&keep_local_button);
                local_column.append(&rename_local_button);
                remote_column.append(&keep_remote_button);
                remote_column.append(&rename_remote_button);

                let buttons = vec![keep_local_button.clone(), keep_remote_button.clone(), rename_local_button.clone(), rename_remote_button.clone()];
                for (button, action) in [
                    (&keep_local_button, ConflictAction::KeepLocal),
                    (&keep_remote_button, ConflictAction::KeepRemote),
                    (&rename_local_button, ConflictAction::KeepBothRenameLocal),
                    (&rename_remote_button, ConflictAction::KeepBothRenameRemote),
                ] {
                    button.connect_clicked(clone!(
                        #[strong] state,
                        #[strong] watcher,
                        #[strong] items,
                        #[strong] connection,
                        #[strong] item,
                        #[strong] success_banner,
                        #[strong] resolution_error_label,
                        #[strong] advance_to_next_unresolved,
                        #[strong] apply_immediately,
                        #[strong] buttons,
                        #[strong] refresh_summary,
                        move |_| {
                            // Paused (either kind) and the user didn't
                            // check "apply immediately anyway" -- queued
                            // rather than run now, tagged Paused (not
                            // Succeeded -- see that variant's own doc
                            // comment on IssueResolution for why an
                            // earlier version of this got that wrong).
                            // Whether the queued resolution itself
                            // eventually succeeds happens later, entirely
                            // outside this dialog's own awareness, once
                            // sync actually resumes -- check_paused_items
                            // is what notices the unpause itself and
                            // flips this back to Pending, if it happens
                            // while this dialog is still open.
                            let paused = state.paused_by_user.get() || state.paused_for_metered.get() || state.paused_for_power_save.get();
                            if paused && !apply_immediately.get() {
                                state.pending_sync_actions.borrow_mut().push(QueuedSyncAction::ResolveConflict {
                                    connection_id: connection.id.clone(),
                                    relative_path: item.relative_path.clone(),
                                    action,
                                });
                                for b in &buttons { b.set_sensitive(false); }
                                items.borrow_mut()[idx].resolution = IssueResolution::Paused;
                                refresh_summary();
                                advance_to_next_unresolved(idx);
                                return;
                            }

                            for b in &buttons { b.set_sensitive(false); }
                            resolution_error_label.set_visible(false);
                            // Tagged Pending, then advances immediately
                            // -- deliberately not waiting for apply_
                            // conflict_resolution's own result first.
                            // "Optimistically moves to the next issue"
                            // means the advance itself happens before
                            // the outcome is known, not just before the
                            // item gets its final tag; the spawned task
                            // below still runs to completion regardless
                            // of whether the dialog has already moved on
                            // from this page, updating this exact page's
                            // own widgets whenever the real result comes
                            // back -- harmless, and correctly visible,
                            // whether the user is still looking at it or
                            // returns to it later.
                            items.borrow_mut()[idx].resolution = IssueResolution::Pending;
                            refresh_summary();
                            advance_to_next_unresolved(idx);
                            glib::MainContext::default().spawn_local(clone!(
                                #[strong] state,
                                #[strong] watcher,
                                #[strong] items,
                                #[strong] connection,
                                #[strong] item,
                                #[strong] success_banner,
                                #[strong] resolution_error_label,
                                #[strong] idx,
                                #[strong] action,
                                #[strong] refresh_summary,
                                #[strong] buttons,
                                async move {
                                    // Incremented before retry_resolution
                                    // even starts, not after -- needs to
                                    // already be nonzero by the time the
                                    // real remote write inside it begins,
                                    // so a notify_push echo of that exact
                                    // write can't slip through in the gap.
                                    // Only these two, specific actions --
                                    // see pending_remote_writes' own doc
                                    // comment for why KeepRemote/
                                    // KeepBothRenameLocal don't count
                                    // here at all (both are read-only
                                    // from remote's own perspective).
                                    let writes_remote = matches!(action, ConflictAction::KeepLocal | ConflictAction::KeepBothRenameRemote);
                                    if writes_remote {
                                        begin_remote_write(&state, &watcher);
                                    }
                                    let result = retry_resolution(|| apply_conflict_resolution(&state, &watcher, &connection, &item, action, None)).await;
                                    update_status_ui(&state, true);
                                    match result {
                                        Ok(()) => {
                                            items.borrow_mut()[idx].resolution = IssueResolution::Succeeded;
                                            success_banner.set_visible(true);
                                        }
                                        Err(error) => {
                                            items.borrow_mut()[idx].resolution = IssueResolution::Failed(error.clone());
                                            resolution_error_label.set_label(&error);
                                            resolution_error_label.set_visible(true);
                                            // Re-enabled here, not left
                                            // disabled forever -- by
                                            // request: all three attempts
                                            // retry_resolution already
                                            // made are exhausted by the
                                            // time this arm runs at all,
                                            // but that's not a reason to
                                            // block a further, manual
                                            // retry later (the underlying
                                            // problem may have since
                                            // cleared -- a permission fix,
                                            // a network recovering), which
                                            // disabled buttons on a page
                                            // the user might return to
                                            // would otherwise make
                                            // impossible from here.
                                            for b in &buttons { b.set_sensitive(true); }
                                        }
                                    }
                                    if writes_remote {
                                        end_remote_write(&state, &watcher);
                                    }
                                    refresh_summary();
                                }
                            ));
                        }
                    ));
                }
                (buttons, Some(remote_info_label))
            }
            SyncIssue::PermissionError(..) => {
                dialog.set_title(Some("Sync Issues"));
                let permission_error_label = gtk4::Label::builder().wrap(true).xalign(0.0).label(
                    "Ripples couldn't write to this location -- it looks like a permissions problem, on either the local or remote side. It won't be retried automatically until you retry it here, unpause, or restart."
                ).build();
                page_scrollable.append(&permission_error_label);

                // By request -- previously there was no way to see *why*
                // a given path was actually flagged, only the bare fact
                // that it was. None for a row from before this field
                // existed, or the rare case a message genuinely wasn't
                // captured -- hidden entirely rather than showing an
                // empty or placeholder line in that case.
                if let Some(message) = item.permission_error_message.as_deref().filter(|m| !m.is_empty()) {
                    let permission_error_detail_label = gtk4::Label::builder()
                        .wrap(true).wrap_mode(gtk4::pango::WrapMode::WordChar).xalign(0.0).css_classes(["dim-label", "caption"])
                        .label(message)
                        .build();
                    page_scrollable.append(&permission_error_detail_label);
                }

                let retry_button = gtk4::Button::builder().label("Retry").hexpand(true).build();
                let retry_now_button = gtk4::Button::builder().label("Retry Now").hexpand(true).css_classes(["suggested-action"]).build();
                let dismiss_button = gtk4::Button::builder().label("Dismiss").hexpand(true).build();
                let permission_button_row = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).spacing(12).build();
                permission_button_row.append(&retry_button);
                permission_button_row.append(&retry_now_button);
                permission_button_row.append(&dismiss_button);
                page_scrollable.append(&permission_button_row);

                let buttons = vec![retry_button.clone(), retry_now_button.clone(), dismiss_button.clone()];
                // Retry/Retry Now now await attempt_retry_now's own real
                // outcome (see that function's own doc comment for the
                // real, reported bug an earlier, fire-and-forget version
                // of this had) and tag Succeeded/Failed/Paused/Pending
                // accordingly -- see IssueResolution's own variants for
                // what each one means here specifically. Dismiss, just
                // below, is the one genuine exception left: it was never
                // an attempt at anything in the first place, so
                // Succeeded immediately, on click, is the only reading
                // that makes sense for it.
                for (button, force) in [(&retry_button, false), (&retry_now_button, true)] {
                    button.connect_clicked(clone!(
                        #[strong] state,
                        #[strong] items,
                        #[strong] connection,
                        #[strong] item,
                        #[strong] success_banner,
                        #[strong] resolution_error_label,
                        #[strong] advance_to_next_unresolved,
                        #[strong] buttons,
                        #[strong] refresh_summary,
                        move |_| {
                            for b in &buttons { b.set_sensitive(false); }
                            resolution_error_label.set_visible(false);
                            // Checked synchronously, here, before
                            // anything spawns -- see retry_defer_
                            // reason's own doc comment for why this
                            // specific ordering is what closes the race
                            // window an equivalent check made from
                            // inside a spawned task wouldn't.
                            if let Some(defer) = retry_defer_reason(&state, force) {
                                state.pending_sync_actions.borrow_mut().push(QueuedSyncAction::RetryPermissionError {
                                    connection_id: connection.id.clone(),
                                    relative_path: item.relative_path.clone(),
                                    is_directory: item.is_directory,
                                });
                                items.borrow_mut()[idx].resolution = match defer {
                                    RetryDefer::Paused => IssueResolution::Paused,
                                    RetryDefer::Busy => IssueResolution::Pending,
                                };
                                refresh_summary();
                                advance_to_next_unresolved(idx);
                                return;
                            }
                            // Pending, then advance immediately -- same
                            // "optimistic means before the outcome is
                            // known, not just before the tag is set"
                            // reasoning as the Conflict actions above,
                            // now applied here too. Confirmed as a real,
                            // reported bug without this: tagging
                            // Succeeded on click, before the real attempt
                            // had even started, meant the banner was
                            // never actually tied to whether the retry
                            // worked -- a repeat permission failure
                            // re-flagged the item in the journal
                            // immediately, but this dialog had already
                            // told the user it succeeded and moved on.
                            items.borrow_mut()[idx].resolution = IssueResolution::Pending;
                            refresh_summary();
                            advance_to_next_unresolved(idx);
                            glib::MainContext::default().spawn_local(clone!(
                                #[strong] state,
                                #[strong] items,
                                #[strong] connection,
                                #[strong] item,
                                #[strong] success_banner,
                                #[strong] resolution_error_label,
                                #[strong] idx,
                                #[strong] refresh_summary,
                                #[strong] buttons,
                                async move {
                                    let outcome = retry_resolution(|| attempt_retry_now(&state, connection.id.clone(), item.relative_path.clone(), item.is_directory)).await;
                                    match outcome {
                                        Ok(()) => {
                                            items.borrow_mut()[idx].resolution = IssueResolution::Succeeded;
                                            success_banner.set_visible(true);
                                        }
                                        Err(error) => {
                                            items.borrow_mut()[idx].resolution = IssueResolution::Failed(error.clone());
                                            resolution_error_label.set_label(&error);
                                            resolution_error_label.set_visible(true);
                                            // Same reasoning as the
                                            // Conflict actions' own
                                            // identical fix -- re-enabled
                                            // rather than left disabled
                                            // forever, so a later, manual
                                            // retry from this same page
                                            // stays possible.
                                            for b in &buttons { b.set_sensitive(true); }
                                        }
                                    }
                                    refresh_summary();
                                }
                            ));
                        }
                    ));
                }
                dismiss_button.connect_clicked(clone!(
                    #[strong] items,
                    #[strong] success_banner,
                    #[strong] advance_to_next_unresolved,
                    #[strong] buttons,
                    #[strong] refresh_summary,
                    move |_| {
                        for b in &buttons { b.set_sensitive(false); }
                        items.borrow_mut()[idx].resolution = IssueResolution::Succeeded;
                        success_banner.set_visible(true);
                        refresh_summary();
                        advance_to_next_unresolved(idx);
                    }
                ));
                (buttons, None)
            }
        };

        // Pause notice + "apply immediately anyway" -- only ever shown
        // for a Conflict page (the same restriction the single-page
        // version this replaced already had; permission-error retries
        // aren't gated by pause the same way at all), and only while the
        // connection is actually paused right now.
        if matches!(issue, SyncIssue::Conflict(..)) {
            let paused = state.paused_by_user.get() || state.paused_for_metered.get() || state.paused_for_power_save.get();
            if paused {
                let pause_notice_label = gtk4::Label::builder()
                    .label("Sync is currently paused. Changes will only apply once sync resumes.")
                    .wrap(true).xalign(0.0)
                    .css_classes(["dim-label"])
                    .build();
                let apply_immediately_check_label = gtk4::Label::builder().label("Apply immediately anyway").xalign(0.0).hexpand(true).build();
                let apply_immediately_check = gtk4::CheckButton::builder().active(apply_immediately.get()).build();
                apply_immediately_check.connect_toggled(clone!(#[strong] apply_immediately, move |cb| apply_immediately.set(cb.is_active())));
                let apply_immediately_row = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).build();
                apply_immediately_row.append(&apply_immediately_check_label);
                apply_immediately_row.append(&apply_immediately_check);
                page_scrollable.append(&pause_notice_label);
                page_scrollable.append(&apply_immediately_row);
            }
        }

        let page_scroller = gtk4::ScrolledWindow::builder()
            .child(&page_scrollable)
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .build();
        carousel.append(&page_scroller);

        page_widgets.push(IssuePageWidgets {
            remote_info_label,
            remote_info_loaded: Cell::new(false),
        });
        // Each button's own click handler already captured its own
        // clone of these three during the match arm above -- these
        // original bindings have nothing further to do.
        drop((success_banner, resolution_error_label, action_buttons));

        let _ = idx; // used throughout the closures above via capture, not directly here
    }
    // Rc, not a plain Vec, from this point on -- nothing after this line
    // ever needs to add/remove a page's own widget set again (that only
    // ever happens during the loop just above), just read individual
    // pages' own fields and mutate their interior-mutable
    // remote_info_loaded Cell -- but the closures that do that (the
    // page-changed handler just below, most concretely) need to hold
    // their own, independent reference to this same Vec, which a plain
    // Vec captured by a single, specific closure couldn't provide.
    let page_widgets = Rc::new(page_widgets);

    carousel.append(&summary_scroller);

    dialog.add_breakpoint(dialog_breakpoint);

    // Shared by both the page-changed handler just below and the
    // initial-page trigger at the very end of this function -- looks up
    // page `idx`'s own remote_info_label/remote_info_loaded (if this is
    // even a Conflict page; PermissionError pages have neither, hence
    // the Option), fires the fetch once and only once per page.
    let trigger_remote_fetch: Rc<dyn Fn(usize)> = Rc::new(clone!(
        #[strong] state,
        #[strong] watcher,
        #[strong] items,
        #[strong] page_widgets,
        move |idx: usize| {
            let Some(widgets) = page_widgets.get(idx) else { return };
            let Some(remote_info_label) = &widgets.remote_info_label else { return };
            if widgets.remote_info_loaded.get() {
                return;
            }
            widgets.remote_info_loaded.set(true);
            let resolver_item = items.borrow()[idx].clone();
            let connection = resolver_item.issue.connection().clone();
            let item = resolver_item.issue.item().clone();
            glib::MainContext::default().spawn_local(clone!(
                #[strong] state,
                #[strong] watcher,
                #[strong] remote_info_label,
                async move {
                    let info = format_remote_file_info(&state, &watcher, &connection, &item.relative_path).await;
                    remote_info_label.set_label(&info);
                }
            ));
        }
    ));

    // Lazily fetches remote info for a Conflict page the first time it's
    // actually shown, rather than eagerly firing off every page's own
    // request at once when the dialog first opens -- with many
    // conflicts queued, that would mean dozens of simultaneous WebDAV
    // requests for pages nobody may ever even scroll to.
    carousel.connect_page_changed(clone!(
        #[strong] items,
        #[strong] position_label,
        #[strong] prev_button,
        #[strong] next_button,
        #[strong] trigger_remote_fetch,
        #[strong] check_paused_items,
        #[strong] refresh_summary,
        move |_, position| {
            let idx = position as usize;
            let len = items.borrow().len();
            // len itself, not len - 1 -- the carousel holds one page
            // past every real item's own index now (the summary page,
            // built right after this same items list further up), so
            // idx == len is a real, valid page, not an out-of-bounds one.
            if idx == len {
                position_label.set_label("");
                check_paused_items();
                refresh_summary();
            } else {
                position_label.set_label(&format!("{} of {len}", idx + 1));
                trigger_remote_fetch(idx);
            }
            prev_button.set_sensitive(idx > 0);
            next_button.set_sensitive(idx < len);
        }
    ));

    prev_button.connect_clicked(clone!(
        #[strong] carousel,
        move |_| {
            let pos = carousel.position() as u32;
            if pos > 0 {
                let page = carousel.nth_page(pos - 1);
                carousel.scroll_to(&page, true);
            }
        }
    ));
    next_button.connect_clicked(clone!(
        #[strong] carousel,
        move |_| {
            let pos = carousel.position() as u32;
            if pos + 1 < carousel.n_pages() {
                let page = carousel.nth_page(pos + 1);
                carousel.scroll_to(&page, true);
            }
        }
    ));
    // See this dialog's own top doc comment for why this is as simple as
    // "discard the snapshot, refresh once from the journal" -- no manual
    // reconciliation of Unresolved/Failed items needed at all.
    close_button.connect_clicked(clone!(
        #[strong] state,
        #[strong] dialog,
        move |_| {
            dialog.close();
            if let Some((row, icon)) = state.window_widgets.borrow().as_ref().map(|w| (w.conflicts_row.clone(), w.conflicts_icon.clone())) {
                refresh_conflicts_ui(&state, &row, &icon);
            }
        }
    ));
    // "Refresh," without fully closing out -- by request, for working
    // through a large queue in one sitting rather than needing to
    // reopen this dialog from the Sync Issues row every time. Simpler
    // implementation than surgically inserting/removing individual
    // carousel pages in place: closes this dialog and opens a fresh one
    // instead, with a merged snapshot -- this instance's own current
    // Unresolved/Failed items (Succeeded ones dropped, since they're
    // done), plus whatever a fresh refresh_conflicts_ui journal query
    // additionally turns up that wasn't already here (deduped by
    // SyncIssue::key()). Same underlying data either way; the
    // difference is only that the dialog itself briefly closes and
    // reopens rather than visibly rebuilding its own pages in place.
    refresh_button.connect_clicked(clone!(
        #[strong] state,
        #[strong] watcher,
        #[strong] items,
        #[strong] dialog,
        move |_| {
            if let Some((row, icon)) = state.window_widgets.borrow().as_ref().map(|w| (w.conflicts_row.clone(), w.conflicts_icon.clone())) {
                refresh_conflicts_ui(&state, &row, &icon);
            }
            let mut merged: Vec<SyncIssue> = items.borrow().iter()
                .filter(|ri| ri.resolution != IssueResolution::Succeeded)
                .map(|ri| ri.issue.clone())
                .collect();
            let existing_keys: std::collections::HashSet<(String, String)> = merged.iter().map(|i| i.key()).collect();
            for issue in state.pending_issues.borrow().iter() {
                if !existing_keys.contains(&issue.key()) {
                    merged.push(issue.clone());
                }
            }
            dialog.close();
            if !merged.is_empty() {
                show_sync_issues_dialog(state.clone(), watcher.clone(), merged, 0);
            }
        }
    ));
    dialog.connect_close_request(clone!(
        #[strong] state,
        move |_| {
            if let Some((row, icon)) = state.window_widgets.borrow().as_ref().map(|w| (w.conflicts_row.clone(), w.conflicts_icon.clone())) {
                refresh_conflicts_ui(&state, &row, &icon);
            }
            glib::Propagation::Proceed
        }
    ));

    let len = items.borrow().len();
    position_label.set_label(&format!("{} of {len}", start_index + 1));
    prev_button.set_sensitive(start_index > 0);
    // start_index < len, not start_index + 1 < len -- same reasoning as
    // connect_page_changed's own version of this just above: the
    // carousel now holds one page past every real item's own index (the
    // summary page), so there's always a valid "next" as long as
    // start_index hasn't already reached len itself.
    next_button.set_sensitive(start_index < len);
    let page = carousel.nth_page(start_index as u32);
    carousel.scroll_to(&page, false);

    // Lazy remote-info fetch, triggered by the very first page shown
    // too, not just later navigation -- connect_page_changed above only
    // fires on an actual change, which the dialog's own initial paint
    // isn't.
    trigger_remote_fetch(start_index);

    dialog.present();
}

/// Combines GOA accounts (watcher.accounts(), rebuilt live from real GOA
/// D-Bus state every call) with direct accounts (state.config's own
/// persisted direct_accounts, since nothing else tracks those) into one
/// list -- the single place both kinds get merged, so callers elsewhere
/// don't each need to remember to check both sources themselves.
fn all_accounts(state: &Rc<AppState>, watcher: &GoaWatcher) -> Vec<AccountSource> {
    let mut accounts: Vec<AccountSource> = watcher.accounts().into_iter().map(AccountSource::Goa).collect();
    accounts.extend(state.config.borrow().direct_accounts.iter().cloned().map(AccountSource::Direct));
    accounts
}

/// Removes a direct (non-GOA) account -- its own credential and its
/// entry in direct_accounts, deactivating it if it was active, but
/// deliberately *not* its own connections. That's a real change from
/// this function's own earlier behavior (which deleted them outright,
/// on the theory that a randomly-generated account_id could never
/// recur) -- account_id is now deterministic, derived from (server,
/// login_name) rather than a random UUID (see DirectNextcloudAccount::
/// account_id's own doc comment), specifically so a later re-login to
/// the same account naturally reconnects to the same, preserved
/// connections, the same as switching a GOA account away and back
/// already does for free (see active_account_id's own doc comment in
/// config.rs). Removing the credential and the account entry itself is
/// still real and immediate -- this isn't a soft "hide," the account
/// genuinely stops being usable until someone logs into it again -- but
/// what comes back on that later login is a real reconnection, not a
/// fresh start.
async fn remove_direct_account(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>, account_id: &str) {
    // Captured before this account's own entry is gone from config
    // below -- server/login_name are needed for the revocation call
    // just after, and won't be recoverable from config once retain()
    // runs.
    let account_info = state.config.borrow().direct_accounts.iter()
        .find(|a| a.account_id == account_id)
        .map(|a| (a.server.clone(), a.login_name.clone()));
    // Best-effort, not propagated as a user-facing error -- see revoke_
    // app_password's own doc comment for the full reasoning (confirmed
    // directly against Nextcloud's own official guidance for exactly
    // this: destroy the apptoken when an account is removed, but still
    // proceed with the removal regardless of whether that call
    // succeeds). Fetches the app password fresh here, before delete_
    // app_password (below) removes it from the keyring -- revoking it
    // needs the credential itself, not just knowing it once existed.
    if let Some((server, login_name)) = account_info {
        if let Ok(Some(app_password)) = nextcloud_login::get_app_password(account_id).await {
            let _ = nextcloud_login::revoke_app_password(&server, &login_name, app_password.as_str()).await;
        }
    }
    state.config.borrow_mut().direct_accounts.retain(|a| a.account_id != account_id);
    if state.config.borrow().active_account_id.as_deref() == Some(account_id) {
        state.config.borrow_mut().active_account_id = None;
        clear_restored_account_pause_if_stale(state);
    }
    // cached_server_themes is a per-account map now, not a single,
    // account-agnostic field the way it used to be -- see its own doc
    // comment in config.rs for the real, reported staleness bug that
    // fixed. Nothing needs clearing on an ordinary account switch
    // anymore (a different account is just a different, independently-
    // correct map entry), but this account's own entry genuinely is
    // gone for good here, not just switched away from -- removed
    // alongside the account itself so a repeated add/remove cycle
    // doesn't quietly accumulate dead entries in the persisted config
    // forever.
    state.config.borrow_mut().cached_server_themes.remove(account_id);
    // Best-effort, not propagated as a user-facing error -- the account
    // is already gone from this app's own world regardless of whether
    // this particular cleanup step succeeds; a credential left behind in
    // the keyring under an account_id nothing references anymore is an
    // orphan, not a live security exposure the user needs to act on
    // (net.dunevent.Ripples's own keyring entries are already invisible
    // to every other application by construction, not just this one).
    let _ = nextcloud_login::delete_app_password(account_id).await;
    save_config(state);
    refresh_account_ui(state, watcher);
}

/// Disconnects whichever account is currently active, regardless of its
/// own type -- by request, the same, uniform action and confirmation
/// wording either way, so the person choosing this doesn't need to know
/// or care which kind of account they had. A direct account is
/// genuinely removed from Ripples (see remove_direct_account's own doc
/// comment for what that does and doesn't touch); a GOA account is only
/// ever deselected -- Ripples never owns a GOA account at all, so
/// "disconnect" here can only correctly mean "stop using it in
/// Ripples," never "delete the GOA account itself." remove_direct_
/// account already calls save_config/refresh_account_ui internally for
/// its own branch; the GOA branch does the equivalent itself, since it
/// doesn't go through that function at all.
async fn disconnect_active_account(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>) {
    let Some(account_id) = state.config.borrow().active_account_id.clone() else { return };
    let is_direct = state.config.borrow().direct_accounts.iter().any(|a| a.account_id == account_id);
    if is_direct {
        remove_direct_account(state, watcher, &account_id).await;
    } else {
        state.config.borrow_mut().active_account_id = None;
        clear_restored_account_pause_if_stale(state);
        save_config(state);
        refresh_account_ui(state, watcher);
    }
    spawn_immediate_sync_round(state, watcher);
}

fn refresh_account_ui(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>) {
    let Some(widgets) = state.window_widgets.borrow().as_ref()
        .map(|w| (w.window.clone(), w.account_section.clone(), w.connections_list.clone(), w.theming_row.clone())) else {
        if is_debug_enabled() {
            eprintln!("[{}] [debug] refresh_account_ui: no window open, nothing to refresh", now_hms());
        }
        return; // no window open right now -- nothing to refresh
    };
    let (window, account_section, connections_list, theming_row) = widgets;

    while let Some(row) = account_section.row_at_index(0) {
        account_section.remove(&row);
    }
    while let Some(row) = connections_list.row_at_index(0) {
        connections_list.remove(&row);
    }

    let every_account = all_accounts(state, watcher);
    let active_account_id = state.config.borrow().active_account_id.clone();
    // Matched against every_account, not just the is_ready()-filtered
    // subset -- an active account that currently isn't ready (Files
    // disabled in Online Accounts, most concretely) still needs to be
    // found here and shown, with its own explanation, rather than
    // silently treated the same as "nothing configured at all." See
    // AccountSource::is_ready's own doc comment.
    let active_account: Option<AccountSource> = active_account_id.as_deref()
        .and_then(|id| every_account.iter().find(|a| a.account_id() == id))
        .cloned();
    if is_debug_enabled() {
        eprintln!(
            "[{}] [debug] refresh_account_ui: {} accounts known, active={:?} (ready={:?})",
            now_hms(), every_account.len(), active_account.as_ref().map(|a| a.presentation_identity()), active_account.as_ref().map(|a| a.is_ready()),
        );
    }

    state.active_account_ready.set(active_account.as_ref().map_or(true, |a| a.is_ready()));
    // Kept in sync with whatever this function itself just observed --
    // real, confirmed timing gap without this: build_or_present_window's
    // own early apply_server_theme call runs before GoaWatcher::new has
    // even completed (state.watcher is still None at that point on a
    // fresh launch), and nothing later ever re-applied the theme once
    // run_background's own, later refresh_account_ui call actually
    // confirmed the account. This function is the one place both the
    // theme and the account UI itself are already guaranteed to agree
    // on the same active_account_id, rather than each call site that
    // happens to touch account state needing its own explicit
    // apply_server_theme call to stay in sync.
    apply_server_theme(state);
    // Same reasoning, same fix, for the Settings toggle's own enabled
    // state -- it had the identical staleness problem, checked once at
    // Settings-page build time using whatever was known at that moment.
    // cached_server_themes' own entry for the active account, not
    // etag_capable -- see this same toggle's own construction-time doc
    // comment for why that flag answers a different question (ETag-tier
    // eligibility) than "does this account's server actually have real
    // theme data."
    theming_row.set_sensitive(
        active_account_id.as_deref().is_some_and(|id| state.config.borrow().cached_server_themes.contains_key(id))
    );

    match &active_account {
        Some(account) => {
            // Only "GOA: " gets a prefix, not a "Nextcloud: "
            // counterpart -- deliberate, by request: a direct account is
            // the "normal" case now, GOA is the one worth flagging as
            // the special case that needs an external system (Online
            // Accounts) to manage it.
            let source_prefix = match account {
                AccountSource::Goa(_) => "GOA: ",
                AccountSource::Direct(_) => "",
            };
            let row = libadwaita::ActionRow::builder()
                .title(glib::markup_escape_text(&format!("{source_prefix}{}", account.presentation_identity())).as_str())
                .subtitle(if !account.is_ready() {
                    "Enable Files access in Online Accounts"
                } else if account.attention_needed() {
                    // A lesser, still-functional problem than !is_ready()
                    // above -- GOA's own "this account needs re-
                    // authentication" flag, distinct from Files being
                    // off entirely. Only reachable for a Goa variant in
                    // practice (attention_needed() is unconditionally
                    // false for Direct, see its own doc comment), but
                    // checked via the method rather than assumed, same
                    // reasoning as the source_prefix match just above.
                    "Needs attention in Online Accounts"
                } else {
                    ""
                })
                .build();
            let disconnect_button = gtk4::Button::builder()
                .icon_name("user-trash-symbolic")
                .valign(gtk4::Align::Center)
                .css_classes(["flat"])
                .tooltip_text("Disconnect Account")
                .build();
            disconnect_button.connect_clicked(clone!(
                #[strong] state,
                #[strong] watcher,
                #[strong] account,
                #[strong] disconnect_button,
                move |_| {
                    // "Disconnect," not "Edit" -- by request, confirmed
                    // as literally accurate: there is no such thing as
                    // editing an account in Ripples. This button used to
                    // open show_account_dialog, which, once an account
                    // already existed, could only ever remove a direct
                    // one or switch to a different GOA one -- neither of
                    // which is editing the current account at all. See
                    // disconnect_active_account's own doc comment for
                    // why the wording and action are uniform across both
                    // account types, and show_account_dialog's own,
                    // rewritten doc comment for the fuller picture: that
                    // dialog is now reachable only when no account is
                    // configured at all, and this button is the only
                    // other place account_section's own "Add Account"
                    // row (further below) could otherwise be reached
                    // from -- removing this in favor of Disconnect is
                    // what makes that invariant hold structurally,
                    // without needing a guard inside that dialog itself.
                    //
                    // Escaped the same way this same identity already is
                    // for this row's own title just above -- AdwAlertDialog's
                    // own heading interprets Pango markup too.
                    let dialog = libadwaita::AlertDialog::new(
                        Some(&format!("Disconnect \u{201c}{}\u{201d}?", glib::markup_escape_text(&account.presentation_identity()))),
                        Some("This will stop syncing the account and allow you to add a different one."),
                    );
                    dialog.add_responses(&[("cancel", "Cancel"), ("disconnect", "Disconnect")]);
                    dialog.set_response_appearance("disconnect", libadwaita::ResponseAppearance::Destructive);
                    dialog.set_close_response("cancel");
                    dialog.set_default_response(Some("cancel"));
                    glib::MainContext::default().spawn_local(clone!(
                        #[strong] state,
                        #[strong] watcher,
                        #[strong] disconnect_button,
                        async move {
                            let response = dialog.choose_future(Some(&disconnect_button)).await;
                            if response == "disconnect" {
                                disconnect_active_account(&state, &watcher).await;
                            }
                        }
                    ));
                }
            ));
            action_row_add_suffix(&row, &disconnect_button);
            account_section.append(&row);

            if !account.is_ready() {
                // Nothing else to show -- an account this app can't
                // currently act on has no folders it could meaningfully
                // list here either (build_sync_target_for_connection
                // would just fail "Account no longer available" for
                // every one of them anyway). Its own connections are
                // still safely preserved in config.json regardless (see
                // remove_direct_account's own doc comment on why that
                // matters) -- just not shown here until the account is
                // actually usable again.
                update_status_ui(state, true);
                return;
            }

            let connections_for_account: Vec<config::SyncConnection> = state.config.borrow()
                .connections.iter().filter(|c| c.account_id == account.account_id()).cloned().collect();

            for connection in &connections_for_account {
                let remote_display = display_remote_subpath(&connection.remote_subpath);
                // Matches this connection's own, real direction now,
                // rather than a single, hardcoded \u{2194} regardless of
                // it -- unicode arrows (not ASCII "<-"/"->"/"<->") to
                // stay visually consistent with the glyph this subtitle
                // already used before direction existed at all.
                let arrow = match connection.direction {
                    config::SyncDirection::Bidirectional => '\u{2194}', // ↔
                    config::SyncDirection::UploadOnly => '\u{2192}',    // →
                    config::SyncDirection::DownloadOnly => '\u{2190}',  // ←
                };
                let subtitle = format!("{} {arrow} {}", display_path_with_tilde(&connection.local_path), remote_display);

                let row = libadwaita::ActionRow::builder()
                    .title(glib::markup_escape_text(&connection.name).as_str())
                    .subtitle(glib::markup_escape_text(&subtitle).as_str())
                    .build();

                // Wording is enabled/disabled; paused/resumed is the global
                // pause (paused_by_user, Session tab, tray menu).
                let enabled_switch = gtk4::Switch::builder()
                    .active(connection.enabled)
                    .valign(gtk4::Align::Center)
                    .build();
                enabled_switch.connect_active_notify(clone!(
                    #[strong] state,
                    #[strong] watcher,
                    #[strong] connection,
                    move |switch| {
                        let now_enabled = switch.is_active();
                        {
                            let mut config = state.config.borrow_mut();
                            if let Some(c) = config.connections.iter_mut().find(|c| c.id == connection.id) {
                                c.enabled = now_enabled;
                            }
                        }
                        save_config(&state);
                        if !now_enabled {
                            // Cancels only this connection's in-flight sync.
                            state.active_sync_cancels.borrow_mut().remove(&connection.id);

                            // forget_item per issue, not forget_connection: synced
                            // files keep their journal entries for rename detection.
                            let mut issues = state.pending_issues.borrow_mut();
                            let (to_forget, remaining): (Vec<_>, Vec<_>) = issues
                                .drain(..)
                                .partition(|issue| issue.connection().id == connection.id);
                            *issues = remaining;
                            drop(issues);
                            for issue in &to_forget {
                                if let Err(e) = journal::forget_item(&state.journal, &connection.id, &issue.item().relative_path) {
                                    eprintln!("couldn't forget issue for disabled connection {}: {e}", connection.id);
                                }
                            }
                        } else {
                            // Bypasses try_etag_skip so every file is rescanned.
                            state.force_full_check.borrow_mut().insert(connection.id.clone());
                        }
                        refresh_account_ui(&state, &watcher);
                        spawn_immediate_sync_round(&state, &watcher);
                    }
                ));
                action_row_add_prefix(&row, &enabled_switch);

                let edit_button = gtk4::Button::builder()
                    .icon_name("document-edit-symbolic")
                    .valign(gtk4::Align::Center)
                    .css_classes(["flat"])
                    .tooltip_text("Edit excluded folders")
                    .build();
                edit_button.connect_clicked(clone!(
                    #[strong] state,
                    #[strong] watcher,
                    #[strong] account,
                    #[strong] connection,
                    move |_| show_folder_dialog(state.clone(), watcher.clone(), account.clone(), Some(connection.clone()))
                ));
                action_row_add_suffix(&row, &edit_button);

                let remove_button = gtk4::Button::builder()
                    .icon_name("user-trash-symbolic")
                    .valign(gtk4::Align::Center)
                    .css_classes(["flat"])
                    .tooltip_text("Remove this Sync Connection")
                    .build();
                remove_button.connect_clicked(clone!(
                    #[strong] state,
                    #[strong] watcher,
                    #[strong] connection,
                    #[strong] remove_button,
                    move |_| {
                        // Same confirmation pattern the Direct-account
                        // Remove button already established (see its own
                        // doc comment) -- by request, extended here too,
                        // since this is a more, not less, destructive
                        // action than that one: account removal leaves
                        // every connection's own sync history intact,
                        // but this purges it outright (see journal::
                        // forget_connection's own call below), and the
                        // trash icon sits directly next to this same
                        // row's edit button, one accidental click away.
                        //
                        // "Remove," not "Delete," by request -- this
                        // doesn't touch a single actual file on either
                        // side, only this connection's own settings and
                        // Ripples' own sync bookkeeping for it, and
                        // "Delete" risks suggesting otherwise (by report,
                        // unlike the real Nextcloud desktop client,
                        // which is said to actually delete local files
                        // in some configurations when a sync pair is
                        // removed -- not independently confirmed here,
                        // just the stated reason for choosing this
                        // wording deliberately).
                        //
                        // Escaped the same way this same connection.name
                        // already is for this row's own title just above
                        // -- AdwAlertDialog's own heading interprets
                        // Pango markup too, so a free-text, user-chosen
                        // name containing a literal "&" (or "<"/">")
                        // would otherwise break it the same way an
                        // unescaped ActionRow title would.
                        let dialog = libadwaita::AlertDialog::new(
                            Some(&format!("Remove \u{201c}{}\u{201d}?", glib::markup_escape_text(&connection.name))),
                            Some("This will remove the Sync Connection and its settings from Ripples. The contents will no longer be synced."),
                        );
                        dialog.add_responses(&[("cancel", "Cancel"), ("remove", "Remove")]);
                        dialog.set_response_appearance("remove", libadwaita::ResponseAppearance::Destructive);
                        dialog.set_close_response("cancel");
                        dialog.set_default_response(Some("cancel"));
                        glib::MainContext::default().spawn_local(clone!(
                            #[strong] state,
                            #[strong] watcher,
                            #[strong] connection,
                            #[strong] remove_button,
                            async move {
                                let response = dialog.choose_future(Some(&remove_button)).await;
                                if response != "remove" {
                                    return;
                                }
                                state.config.borrow_mut().connections.retain(|c| c.id != connection.id);
                                save_config(&state);
                                // If run_sync_round is mid-flight for this connection
                                // right now, this is what actually stops it -- closing
                                // the sender wakes up the recv() run_sync_round is
                                // racing sync_target against, which drops sync_target
                                // and cancels whatever GIO operation was in progress.
                                // See active_sync_cancels' own doc comment on AppState.
                                // A plain remove() (rather than an explicit .close())
                                // is enough: async_channel drops the Receiver's other
                                // half implicitly once every Sender is gone, and a
                                // dropped channel wakes a pending recv() with an Err
                                // just the same as an explicit close() would.
                                state.active_sync_cancels.borrow_mut().remove(&connection.id);
                                // Explicit user action, not a GOA-driven event -- an
                                // account merely going quiet on the bus (daemon
                                // restart, transient D-Bus hiccup) never reaches this
                                // code path at all, so there's no risk of a
                                // communication error masquerading as "removed" here.
                                if let Err(e) = journal::forget_connection(&state.journal, &connection.id) {
                                    eprintln!("couldn't purge journal for removed connection {}: {e}", connection.id);
                                }
                                refresh_account_ui(&state, &watcher);
                                spawn_immediate_sync_round(&state, &watcher);
                            }
                        ));
                    }
                ));
                action_row_add_suffix(&row, &remove_button);
                connections_list.append(&row);
            }

            let add_row = libadwaita::ActionRow::builder()
                .title("Add folder to sync")
                .activatable(true)
                .build();
            action_row_add_suffix(&add_row, &gtk4::Image::from_icon_name("list-add-symbolic"));
            add_row.connect_activated(clone!(
                #[strong] state,
                #[strong] watcher,
                #[strong] account,
                move |_| show_folder_dialog(state.clone(), watcher.clone(), account.clone(), None)
            ));
            connections_list.append(&add_row);
        }
        None => {
            let add_account_row = libadwaita::ActionRow::builder()
                .title("Add Account")
                .activatable(true)
                .build();
            action_row_add_suffix(&add_account_row, &gtk4::Image::from_icon_name("list-add-symbolic"));
            add_account_row.connect_activated(clone!(
                #[strong] state,
                #[strong] watcher,
                #[strong] window,
                move |_| show_account_dialog(state.clone(), watcher.clone(), window.clone())
            ));
            account_section.append(&add_account_row);
        }
    }

    update_status_ui(state, true);
}

/// The "Add Account" dialog -- reached only from the Connections tab's
/// own "Add Account" row (see refresh_account_ui's own None branch),
/// which itself only ever shows when no account is currently
/// configured. By request: this dialog is now genuinely, structurally
/// only ever reachable in that one state, not "the same dialog,
/// rendered differently" across three possible states the way it used
/// to be -- the old "edit pencil" entry point (reachable once an
/// account already existed) is gone, replaced by disconnect_active_
/// account's own trash icon on the Connections tab, which never opens
/// this dialog at all. AdwDialog, not the libadwaita::Window +
/// modal(true) pattern every other dialog in this file still uses -- a
/// deliberate, confirmed upgrade specifically for this one: AdwDialog
/// presents as a bottom sheet on a small/mobile screen and a centered
/// floating window on a larger one, automatically, with no extra work
/// needed to make that adaptive.
///
/// Two pages, switched via dialog_pages (a Stack, not two separate
/// dialogs) -- "main" (the Nextcloud login form, plus a compact
/// "Alternative Method" row) and "goa" (the GNOME Online Accounts list,
/// reached only by choosing that row). By request: this replaces the
/// old design's always-visible, always-present Online Accounts
/// section entirely, since that section being visible (even disabled,
/// with no explanation for why) whenever the OTHER account type was
/// active was the actual, reported source of confusion here -- not
/// something a purely visual change (tabs, in particular) would
/// actually have fixed, since the two sections' own mutual exclusivity
/// was the real problem, not how they were laid out.
///
/// This dialog closes itself the moment either path actually succeeds
/// -- a direct login completing, or a GOA account being selected -- by
/// request, since an account existing is exactly the one condition
/// under which this dialog should never be reachable at all. There is
/// deliberately no way to switch account type from within a single,
/// still-open instance of this dialog once one is already configured;
/// the only way back in is disconnecting the current one first (see
/// disconnect_active_account).
fn show_account_dialog(state: Rc<AppState>, watcher: Rc<GoaWatcher>, parent_window: libadwaita::ApplicationWindow) {
    // -- Nextcloud login section -- EntryRow (a gtk4::Editable, same as
    // gtk4::Entry -- .text()/.connect_changed() below work identically
    // either way), wrapped in its own boxed-list ListBox rather than
    // used bare, so it reads as a real, rounded-rectangle "set and
    // changeable" field, matching the same visual language every other
    // list in this app already uses -- by request.
    let server_entry = libadwaita::EntryRow::builder()
        .title("Server Address")
        .build();
    let server_entry_list = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    server_entry_list.append(&server_entry);
    let login_error_label = gtk4::Label::builder()
        .css_classes(["error"])
        .wrap(true).xalign(0.0)
        .visible(false)
        .build();
    let login_button = gtk4::Button::builder()
        .label("Log In")
        .css_classes(["suggested-action"])
        .halign(gtk4::Align::End)
        .sensitive(false)
        .build();
    let login_input_box = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).spacing(6).build();
    login_input_box.append(&server_entry_list);
    login_input_box.append(&login_button);
    login_input_box.append(&login_error_label);

    // -- Nextcloud login section: "waiting" sub-state --
    let spinner = gtk4::Spinner::builder().spinning(true).halign(gtk4::Align::Center).build();
    let waiting_label = gtk4::Label::builder()
        .label("Continue in your browser to finish logging in.")
        .wrap(true).xalign(0.5).justify(gtk4::Justification::Center)
        .build();
    let reopen_browser_button = gtk4::Button::builder()
        .label("Open Browser Again")
        .halign(gtk4::Align::Center)
        .build();
    let login_waiting_box = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).spacing(6).build();
    login_waiting_box.append(&spinner);
    login_waiting_box.append(&waiting_label);
    login_waiting_box.append(&reopen_browser_button);

    let login_stack = gtk4::Stack::new();
    // vhomogeneous(false) -- same, real, reported bug and fix as
    // dialog_pages' own identical call further below: "input" and
    // "waiting" are meaningfully different heights, and GtkStack
    // defaults to sizing itself for the tallest of all its named
    // children regardless of which is actually visible.
    login_stack.set_vhomogeneous(false);
    login_stack.add_named(&login_input_box, Some("input"));
    login_stack.add_named(&login_waiting_box, Some("waiting"));

    let nextcloud_group = libadwaita::PreferencesGroup::builder()
        .title("Nextcloud Account")
        .description("Log in directly with your server.")
        .build();
    preferences_group_add(&nextcloud_group, &login_stack);

    // -- "Alternative Method" row -- by request, replaces the old
    // design's always-visible, always-present Online Accounts section
    // (an expander that stayed on screen, disabled, with no explanation,
    // whenever a direct account was the active one instead -- exactly
    // the reported source of confusion this whole redesign exists to
    // fix). A single, compact row instead: choosing it navigates to
    // dialog_pages' own "goa" page below, rather than expanding in
    // place.
    let alternative_method_row = libadwaita::ActionRow::builder()
        .title("Alternative Method")
        .activatable(true)
        .build();
    action_row_add_suffix(&alternative_method_row, &gtk4::Image::from_icon_name("go-next-symbolic"));
    let alternative_method_list = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    alternative_method_list.append(&alternative_method_row);

    let main_page = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical).spacing(18)
        .margin_top(18).margin_bottom(18).margin_start(18).margin_end(18)
        .build();
    main_page.append(&nextcloud_group);
    main_page.append(&alternative_method_list);

    // -- "goa" page -- GNOME Online Accounts, reached only via
    // alternative_method_row above. online_accounts_box holds the flat
    // list of GOA account rows directly now, not wrapped in an
    // ExpanderRow the way the old, always-visible design needed --
    // there's nothing to collapse, since this is already its own,
    // dedicated page rather than a section sharing space with anything
    // else. Rebuilt fresh on every render() call below (same "rebuild
    // from scratch rather than incrementally diff" approach
    // refresh_account_ui's own account_section already uses).
    let online_accounts_box = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    let online_accounts_group = libadwaita::PreferencesGroup::builder()
        .title("GNOME Online Accounts")
        .description("Select a compatible account. Files access must be configured.")
        .build();
    preferences_group_add(&online_accounts_group, &online_accounts_box);

    let goa_manage_button = gtk4::Button::builder().label("Manage Online Accounts").build();
    goa_manage_button.connect_clicked(|_| open_online_accounts_panel());

    let goa_page = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical).spacing(18)
        .margin_top(18).margin_bottom(18).margin_start(18).margin_end(18)
        .build();
    goa_page.append(&online_accounts_group);
    goa_page.append(&goa_manage_button);

    // Two pages, not two dialogs -- by request. vhomogeneous(false),
    // same, real, reported bug and fix as login_stack's own identical
    // call further above: main_page and goa_page are meaningfully
    // different heights (a GOA account list can run to several rows),
    // and GtkStack defaults to sizing itself for the tallest of all its
    // named children regardless of which is actually visible.
    let dialog_pages = gtk4::Stack::new();
    dialog_pages.set_vhomogeneous(false);
    dialog_pages.add_named(&main_page, Some("main"));
    dialog_pages.add_named(&goa_page, Some("goa"));
    alternative_method_row.connect_activated(clone!(
        #[strong] dialog_pages,
        move |_| dialog_pages.set_visible_child_name("goa")
    ));

    // "Cancel," not "Done" -- by request, more accurate now: this
    // dialog's only purpose is adding an account, so closing without
    // having done that is genuinely canceling that flow, not just "I'm
    // done looking at this" the way it read when this same button could
    // also be reached after an action had already, immediately taken
    // effect (the old design's "configured" states). Present in the
    // header bar, shared across both pages, not per-page -- header_
    // back_button already covers "go back to try the other method," a
    // distinct action from "leave this dialog entirely," so having both
    // available on the goa page isn't redundant.
    let cancel_button = gtk4::Button::builder().label("Cancel").build();

    // By request -- moved here from a plain button at the bottom of the
    // goa page, to the standard GNOME header-bar-back-button position
    // instead (top left). Icon-only (go-previous-symbolic), not a text
    // "Back" label -- a design choice made here, not specified in the
    // request: this is the idiomatic convention for exactly this
    // position (see, for comparison, cancel_button just above, which
    // keeps its own text label since it isn't in this same, recognized
    // "navigate back" slot).
    let header_back_button = gtk4::Button::builder()
        .icon_name("go-previous-symbolic")
        .tooltip_text("Back")
        .visible(false)
        .build();
    header_back_button.connect_clicked(clone!(
        #[strong] dialog_pages,
        move |_| dialog_pages.set_visible_child_name("main")
    ));
    // Single source of truth for header_back_button's own visibility,
    // rather than setting it manually at every place dialog_pages'
    // own visible child could change (alternative_method_row's click,
    // this button's own click, and any future one) -- deriving it
    // directly from whatever the current page actually is means there's
    // no way for the two to drift out of sync with each other.
    dialog_pages.connect_notify_local(Some("visible-child-name"), clone!(
        #[strong] header_back_button,
        move |stack, _| header_back_button.set_visible(stack.visible_child_name().as_deref() == Some("goa"))
    ));

    let header_bar = libadwaita::HeaderBar::builder()
        .title_widget(&libadwaita::WindowTitle::new("Add Account", ""))
        .show_start_title_buttons(false)
        .show_end_title_buttons(false)
        .build();
    header_bar.pack_start(&header_back_button);
    header_bar.pack_end(&cancel_button);

    let toolbar_view = libadwaita::ToolbarView::builder().content(&dialog_pages).build();
    toolbar_view.add_top_bar(&header_bar);

    let dialog = libadwaita::Dialog::builder()
        .title("Add Account")
        .content_width(420)
        .follows_content_size(true)
        .child(&toolbar_view)
        .build();

    // Set true the moment the dialog is closing for any reason -- same
    // purpose, same reasoning, as the earlier single-purpose version of
    // this dialog had: checked by poll_until_complete's own cancelled
    // closure at the start of every polling iteration, so a login still
    // in flight when this dialog goes away stops promptly.
    let cancelled = Rc::new(Cell::new(false));
    // Holds the in-flight login's own poll credentials across the gap
    // between starting the flow and reopen_browser_button needing them
    // later -- None until start() actually succeeds.
    let current_flow: Rc<RefCell<Option<nextcloud_login::LoginFlowStart>>> = Rc::new(RefCell::new(None));

    // Rc<RefCell<Option<Box<dyn Fn()>>>>, not a plain closure bound
    // directly to a `let` -- several separate closures built later in
    // this same function (the login flow's own error and completion
    // paths in particular) still need to call render, before it's fully
    // constructed itself; this "tie the knot" indirection is the
    // standard way around that -- the cell starts empty, gets filled in
    // once render's own body is fully built, and every place that needs
    // to trigger it holds a clone of the cell itself, not the closure
    // directly. render's own body no longer needs to call itself the
    // way the old design's GOA row handler once did -- this dialog
    // closes outright on a successful selection now, rather than
    // re-rendering in place -- but the same indirection is still the
    // simplest way to let those other, later closures reach it.
    let render: Rc<RefCell<Option<Box<dyn Fn()>>>> = Rc::new(RefCell::new(None));
    *render.borrow_mut() = Some(Box::new(clone!(
        #[strong] state,
        #[strong] watcher,
        #[strong] server_entry,
        #[strong] login_button,
        #[strong] online_accounts_box,
        #[strong] dialog,
        move || {
            // Always enabled, unconditionally -- unlike the old design,
            // there's no longer a mutual-exclusivity case to disable for
            // at all: this dialog is only ever open when nothing is
            // configured yet (see this function's own doc comment), so
            // there's never a GOA account already active that direct
            // login would need to defer to, or vice versa.
            server_entry.set_sensitive(true);
            login_button.set_sensitive(!server_entry.text().trim().is_empty());

            while let Some(row) = online_accounts_box.row_at_index(0) {
                online_accounts_box.remove(&row);
            }
            for account in &watcher.accounts() {
                let row = libadwaita::ActionRow::builder()
                    .title(glib::markup_escape_text(&account.presentation_identity).as_str())
                    .activatable(true)
                    .build();
                action_row_add_suffix(&row, &gtk4::Image::from_icon_name("go-next-symbolic"));
                let account_id = account.account_id.clone();
                row.connect_activated(clone!(
                    #[strong] state,
                    #[strong] watcher,
                    #[strong] account_id,
                    #[strong] dialog,
                    move |_| {
                        // No toggle-on-reclick here the way the old
                        // design needed -- by request, every row shown
                        // on this page is, by definition, not yet
                        // selected (nothing can be active while this
                        // dialog is even open at all), so there's no
                        // "already selected, clicking again deselects"
                        // case left to handle; this is a plain, one-way
                        // selection.
                        state.config.borrow_mut().active_account_id = Some(account_id.clone());
                        save_config(&state);
                        refresh_account_ui(&state, &watcher);
                        let state = state.clone();
                        let watcher = watcher.clone();
                        let account_id = account_id.clone();
                        glib::MainContext::default().spawn_local(async move {
                            // force: false -- respects a pause (metered/
                            // power-save/manual) exactly like any other
                            // capabilities check would; selecting an
                            // account isn't an explicit "do this anyway"
                            // override the way Sync Now is.
                            if let Some(account) = all_accounts(&state, &watcher).into_iter().find(|a| a.account_id() == account_id) {
                                check_account_capabilities(&state, &watcher, &account, false).await;
                            }
                            // Checked (and, if needed, paused) here,
                            // AFTER the capabilities check just above but
                            // BEFORE spawn_immediate_sync_round just
                            // below -- see pause_for_restored_
                            // connections_if_needed's own doc comment for
                            // why that specific ordering is the one that
                            // actually closes the race it's meant to
                            // close.
                            pause_for_restored_connections_if_needed(&state, &account_id);
                            spawn_immediate_sync_round(&state, &watcher);
                        });
                        // Closes immediately -- by request, see this
                        // function's own doc comment: an account existing
                        // is exactly the one condition under which this
                        // dialog should never be reachable at all, so
                        // there's nothing left to re-render here the way
                        // the old, in-place-transforming design needed.
                        // Also sets cancelled (via dialog's own connect_
                        // closed handler further below), which is
                        // harmless here -- nothing is currently polling a
                        // login flow that would ever check it.
                        dialog.close();
                    }
                ));
                online_accounts_box.append(&row);
            }
        }
    )));

    server_entry.connect_changed(clone!(
        #[strong] login_button,
        move |entry| login_button.set_sensitive(!entry.text().trim().is_empty())
    ));

    let show_error = clone!(
        #[strong] login_stack,
        #[strong] login_error_label,
        #[strong] login_button,
        #[strong] server_entry,
        move |message: String| {
            login_error_label.set_label(&message);
            login_error_label.set_visible(true);
            login_stack.set_visible_child_name("input");
            login_button.set_sensitive(!server_entry.text().trim().is_empty());
        }
    );

    reopen_browser_button.connect_clicked(clone!(
        #[strong] current_flow,
        #[strong] parent_window,
        move |_| {
            let Some(flow) = current_flow.borrow().clone() else { return };
            glib::MainContext::default().spawn_local(clone!(
                #[strong] parent_window,
                async move {
                    let _ = gtk4::UriLauncher::new(&flow.login_url).launch_future(Some(&parent_window)).await;
                }
            ));
        }
    ));

    let commit = clone!(
        #[strong] state,
        #[strong] watcher,
        #[strong] server_entry,
        #[strong] login_error_label,
        #[strong] login_stack,
        #[strong] current_flow,
        #[strong] cancelled,
        #[strong] parent_window,
        #[strong] show_error,
        #[strong] dialog,
        move || {
            let server_url = server_entry.text().trim().to_string();
            if server_url.is_empty() {
                return; // Enter can fire activate regardless of login_button's sensitivity -- same guard show_add_pattern_dialog's own commit uses
            }
            login_error_label.set_visible(false);
            login_stack.set_visible_child_name("waiting");

            glib::MainContext::default().spawn_local(clone!(
                #[strong] state,
                #[strong] watcher,
                #[strong] server_url,
                #[strong] current_flow,
                #[strong] cancelled,
                #[strong] parent_window,
                #[strong] show_error,
                #[strong] dialog,
                async move {
                    let start_result = nextcloud_login::start(&server_url).await;
                    // Checked immediately after this await, before even
                    // looking at start_result -- same reasoning, same
                    // fix, as the identical check after poll_until_
                    // complete just below: the dialog may already be
                    // gone by the time this network call returns,
                    // whether it succeeded or failed, and neither branch
                    // below should touch any of its widgets in that
                    // case.
                    if cancelled.get() {
                        return;
                    }
                    let flow = match start_result {
                        Ok(flow) => flow,
                        Err(e) => {
                            show_error(e.to_string());
                            return;
                        }
                    };
                    *current_flow.borrow_mut() = Some(flow.clone());
                    // Best-effort -- a failed launch isn't fatal on its
                    // own, since reopen_browser_button above offers the
                    // same action again; not worth its own dedicated
                    // error path when polling below already has to
                    // handle "the user never actually got to the
                    // browser" as an ordinary timeout regardless.
                    let _ = gtk4::UriLauncher::new(&flow.login_url).launch_future(Some(&parent_window)).await;

                    let result = nextcloud_login::poll_until_complete(&flow, &|| cancelled.get()).await;
                    // Checked again here, not just relying on the error
                    // paths below to no-op harmlessly -- cancelled being
                    // true means the dialog itself may already be gone,
                    // so nothing past this point (which all assumes the
                    // dialog and its widgets are still alive) should run
                    // at all.
                    if cancelled.get() {
                        return;
                    }
                    let result = match result {
                        Ok(result) => result,
                        Err(e) => {
                            show_error(e.to_string());
                            return;
                        }
                    };

                    let account_id = nextcloud_login::direct_account_id(&result.server, &result.login_name);
                    let store_result = nextcloud_login::store_app_password(&account_id, &result.app_password).await;
                    // Same fix, same reasoning, as the identical check
                    // right after nextcloud_login::start's own await
                    // further up -- checked immediately, before looking
                    // at store_result, so neither branch below touches a
                    // widget the dialog may have already dropped, and so
                    // an account that genuinely finished logging in isn't
                    // saved and switched to after the person had already
                    // closed this out.
                    if cancelled.get() {
                        return;
                    }
                    if let Err(e) = store_result {
                        show_error(format!("Logged in, but couldn't save the app password: {e}"));
                        return;
                    }
                    let account = nextcloud_login::DirectNextcloudAccount {
                        account_id: account_id.clone(),
                        server: result.server,
                        login_name: result.login_name,
                    };
                    // Retain-then-push, not a plain push -- account_id is
                    // now deterministic (same server+login_name always
                    // produces the same id, see direct_account_id's own
                    // doc comment), so logging back into an account
                    // that's already present here would otherwise leave
                    // a stale duplicate entry behind rather than
                    // correctly updating it in place.
                    {
                        let mut config = state.config.borrow_mut();
                        config.direct_accounts.retain(|a| a.account_id != account_id);
                        config.direct_accounts.push(account);
                        // Sets this as the active account directly, not
                        // left for some other, separate "select" step --
                        // there isn't one anymore. This dialog is now the
                        // only place an account ever becomes active at
                        // all, unlike the old multi-account radio-row
                        // list it replaced, where adding an account and
                        // choosing to use it were two genuinely separate
                        // actions.
                        //
                        // .clone() here, not a move -- account_id is
                        // needed again just below, for the direct
                        // capabilities check.
                        config.active_account_id = Some(account_id.clone());
                    }
                    clear_restored_account_pause_if_stale(&state);
                    save_config(&state);
                    refresh_account_ui(&state, &watcher);
                    // By request -- same fix, same reasoning, as the GOA
                    // account-selection row's own identical addition
                    // (see its own doc comment): a freshly-added direct
                    // account has zero connections configured yet, the
                    // normal state right after adding one, so without
                    // this, check_account_capabilities would never run
                    // for it at all until a connection eventually gets
                    // set up. Awaited directly, not spawned separately --
                    // this whole closure is already async.
                    if let Some(account) = all_accounts(&state, &watcher).into_iter().find(|a| a.account_id() == account_id) {
                        // force: false -- same reasoning as the GOA
                        // account-selection row's own identical
                        // argument. Just having logged in isn't an
                        // explicit "do this anyway" override either.
                        check_account_capabilities(&state, &watcher, &account, false).await;
                    }
                    // Checked (and, if needed, paused) here, AFTER the
                    // capabilities check just above but BEFORE spawn_
                    // immediate_sync_round just below -- see pause_for_
                    // restored_connections_if_needed's own doc comment
                    // for why that specific ordering is the one that
                    // actually closes the race it's meant to close.
                    pause_for_restored_connections_if_needed(&state, &account_id);
                    spawn_immediate_sync_round(&state, &watcher);
                    // Closes immediately -- by request, same reasoning as
                    // the GOA row's own identical close() call: see this
                    // function's own doc comment.
                    dialog.close();
                }
            ));
        }
    );
    login_button.connect_clicked(clone!(#[strong] commit, move |_| commit()));
    // connect_entry_activated, not connect_activate -- AdwEntryRow has
    // its own, dedicated "entry-activated" signal (added in libadwaita
    // 1.2), distinct from gtk4::Entry's own "activate" that a plain
    // gtk4::Entry would have used here instead.
    server_entry.connect_entry_activated(move |_| commit());

    // See cancel_button's own construction, above, for why "Cancel,"
    // not "Done," is the accurate label now.
    cancel_button.connect_clicked(clone!(
        #[strong] cancelled,
        #[strong] dialog,
        move |_| {
            cancelled.set(true);
            dialog.close();
        }
    ));
    // Esc and the dialog's own close/swipe-down gesture both need the
    // same cancellation, not just the Cancel button above -- otherwise a
    // login still polling in the background after the dialog visually
    // closes either of those other two ways would keep running
    // unnoticed for up to 20 minutes. connect_closed, not connect_
    // close_attempt -- that second signal only ever fires when can_close
    // has been explicitly set to false first (never done here, since
    // there's no reason to actually prevent closing, only to react once
    // it happens); connect_closed fires for a genuinely closed dialog
    // regardless of which of the three ways it closed.
    dialog.connect_closed(clone!(
        #[strong] cancelled,
        move |_| cancelled.set(true)
    ));

    (render.borrow().as_ref().expect("render always Some once dialog construction finishes"))();
    dialog.present(Some(&parent_window));
    server_entry.grab_focus();
}

/// Fires an out-of-schedule sync round without making the caller (a
/// button click handler, not an async context) wait for it. Used
/// anywhere a config change means "the set of things to sync just
/// changed" -- switching accounts, adding or removing a folder -- so the
/// remote poll interval being set to something long (see Settings tab)
/// doesn't also mean waiting that long to see a just-added folder start
/// syncing. trigger_sync_round's own busy guard makes this safe to call
/// even if a round happens to already be running.
fn spawn_immediate_sync_round(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>) {
    glib::MainContext::default().spawn_local(clone!(
        #[strong] state,
        #[strong] watcher,
        async move { trigger_sync_round(&state, &watcher, false, false).await; }
    ));
}

/// Add/Edit Folder dialog -- two pages. Page 1 (Add mode only) picks the
/// local and remote sync roots, each through our own in-app folder
/// browser (folder_picker::show_single_folder_picker) rather than the
/// system file-chooser portal -- see folder_picker.rs's own top-of-file
/// comment for why: Phosh's own portal backend can't select GVfs-mounted
/// remote locations at all, a confirmed, still-open upstream
/// xdg-desktop-portal limitation, not a bug specific to this app or even
/// to Phosh's own implementation. Page 2 shows the merged local+remote
/// folder tree with checkboxes -- unchecking a folder excludes it,
/// regardless of which side it currently exists on (see
/// config::SyncConnection::excluded_subpaths's own doc comment).
///
/// Edit mode skips Page 1 entirely, opening directly to Page 2 -- roots
/// are permanent once a connection exists (every journal row is keyed
/// against the exact path, so changing it after the fact would silently
/// orphan or misattribute existing sync state), so there's nothing on
/// Page 1 Edit mode could ever change anyway.
fn show_folder_dialog(state: Rc<AppState>, watcher: Rc<GoaWatcher>, account: AccountSource, editing: Option<config::SyncConnection>) {
    let parent_window = state.window_widgets.borrow().as_ref().map(|w| w.window.clone());

    let chosen_local_path: Rc<RefCell<Option<PathBuf>>> = Rc::new(RefCell::new(editing.as_ref().map(|c| c.local_path.clone())));
    // Distinct from an empty string being the legitimate value for "the
    // whole account root" -- None specifically means "hasn't been
    // chosen yet at all," same reasoning the old dialog's
    // remote_path_chosen flag existed for: without this distinction, a
    // local-only pick could silently sync an entire account root,
    // exactly the kind of one-click disaster this app otherwise goes out
    // of its way to avoid.
    let chosen_remote_subpath: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(editing.as_ref().map(|c| c.remote_subpath.clone())));
    let excluded_subpaths: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(
        editing.as_ref().map(|c| c.excluded_subpaths.clone()).unwrap_or_default()
    ));

    // -- Page 1 widgets (Add mode only -- Edit mode never builds these) --
    let local_path_label = gtk4::Label::builder()
        .label(chosen_local_path.borrow().as_ref().map(|p| display_path_with_tilde(p)).unwrap_or_else(|| "No folder chosen yet".to_string()))
        .css_classes(["dim-label"]).wrap(true).xalign(0.0)
        .build();
    let choose_local_button = gtk4::Button::builder().label("Choose Local Folder\u{2026}").build();

    let remote_path_label = gtk4::Label::builder()
        .label(chosen_remote_subpath.borrow().as_ref().map(|s| display_remote_subpath(s)).unwrap_or_else(|| "No folder chosen yet".to_string()))
        .css_classes(["dim-label"]).wrap(true).xalign(0.0)
        .build();
    let choose_remote_button = gtk4::Button::builder().label("Choose Remote Folder\u{2026}").build();

    // Hidden until an overlap is actually found -- see
    // find_overlapping_local_connection's own doc comment for why this
    // is a hard block (kept the Next button insensitive, not just a
    // warning the user could click past) rather than a softer warning.
    let overlap_error_label = gtk4::Label::builder()
        .css_classes(["error"])
        .wrap(true).xalign(0.0)
        .visible(false)
        .build();

    let next_button = gtk4::Button::builder()
        .label("Next")
        .css_classes(["suggested-action"])
        .sensitive(chosen_local_path.borrow().is_some() && chosen_remote_subpath.borrow().is_some())
        .build();
    let page1_cancel_button = gtk4::Button::builder().label("Cancel").build();

    let page1_fields = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical).spacing(12)
        .margin_top(24).margin_bottom(24).margin_start(24).margin_end(24)
        .vexpand(true)
        .build();
    page1_fields.append(&gtk4::Label::builder().label("Local folder").xalign(0.0).css_classes(["heading"]).build());
    page1_fields.append(&choose_local_button);
    page1_fields.append(&local_path_label);
    page1_fields.append(&gtk4::Label::builder().label("Remote folder").xalign(0.0).css_classes(["heading"]).build());
    page1_fields.append(&choose_remote_button);
    page1_fields.append(&remote_path_label);

    // Sync direction -- locked at creation, same as local_path/
    // remote_subpath just above: this page is never shown again once a
    // connection exists at all (Edit mode jumps straight to page2, see
    // this dialog's own page_stack wiring further down), so there's no
    // "initialize from editing" concern here the way page2's own
    // excluded_subpaths tree has -- editing is always None whenever
    // this page is even being built. Plain grouped CheckButtons in
    // radio mode, not an AdwComboRow -- this app has no existing
    // ComboRow usage to match, and three, always-visible options
    // (rather than one collapsed into a dropdown) means the consequence
    // of each choice (see each row's own subtitle) is visible up front,
    // not hidden behind an extra click to open the list first.
    page1_fields.append(&gtk4::Label::builder().label("Sync direction").xalign(0.0).css_classes(["heading"]).build());
    let direction_bidirectional_check = gtk4::CheckButton::builder().label("Bidirectional").active(true).build();
    let direction_upload_check = gtk4::CheckButton::builder().label("Upload only").group(&direction_bidirectional_check).build();
    let direction_download_check = gtk4::CheckButton::builder().label("Download only").group(&direction_bidirectional_check).build();
    let direction_subtitle = gtk4::Label::builder()
        .label("Changes sync in both directions. Conflicting edits ask which to keep.")
        .css_classes(["dim-label"]).wrap(true).xalign(0.0)
        .build();
    for (check, text) in [
        (&direction_bidirectional_check, "Changes sync in both directions. Conflicting edits ask which to keep."),
        (&direction_upload_check, "Only local changes are sent. Remote-only changes and deletions are overwritten by the local copy."),
        (&direction_download_check, "Only remote changes are received. Local-only changes and deletions are overwritten by the remote copy."),
    ] {
        check.connect_toggled(clone!(
            #[strong] direction_subtitle,
            move |c| {
                if c.is_active() {
                    direction_subtitle.set_label(text);
                }
            }
        ));
    }
    page1_fields.append(&direction_bidirectional_check);
    page1_fields.append(&direction_upload_check);
    page1_fields.append(&direction_download_check);
    page1_fields.append(&direction_subtitle);
    // Shown only for the one-way directions: the source side is
    // authoritative, so items present only on the other side are deleted.
    let direction_warning = gtk4::Label::builder()
        .css_classes(["error"])
        .wrap(true).xalign(0.0)
        .visible(false)
        .build();
    for (check, warning) in [
        (&direction_bidirectional_check, ""),
        (&direction_upload_check, "Remote files not present locally will be deleted!"),
        (&direction_download_check, "Local files not present on the remote will be deleted!"),
    ] {
        check.connect_toggled(clone!(
            #[strong] direction_warning,
            move |c| {
                if c.is_active() {
                    direction_warning.set_label(warning);
                    direction_warning.set_visible(!warning.is_empty());
                }
            }
        ));
    }
    page1_fields.append(&direction_warning);

    page1_fields.append(&overlap_error_label);

    // Shared by both on_selected callbacks below, called every time
    // either side changes -- re-checks whatever's currently known
    // (possibly just one side so far) against every other existing
    // connection, and keeps overlap_error_label/next_button in sync
    // with the result. A local overlap can be checked the moment
    // local_path alone is chosen (it's global, independent of which
    // account this new connection is even for); a remote overlap
    // needs remote_subpath too, but never needs to wait on the local
    // side, since account is already fixed for this whole dialog.
    let revalidate = clone!(
        #[strong] state,
        #[strong] account,
        #[strong] chosen_local_path,
        #[strong] chosen_remote_subpath,
        #[strong] overlap_error_label,
        #[strong] next_button,
        move || {
            let connections = &state.config.borrow().connections;
            let local = chosen_local_path.borrow().clone();
            let remote = chosen_remote_subpath.borrow().clone();

            let overlap = local.as_deref()
                .and_then(|p| find_overlapping_local_connection(connections, account.account_id(), p))
                .or_else(|| remote.as_deref().and_then(|s| find_overlapping_remote_connection(connections, account.account_id(), s)));

            match overlap {
                Some(existing) => {
                    overlap_error_label.set_label(&format!(
                        "This overlaps with the existing \u{201c}{}\u{201d} connection -- one folder can't be synced by two connections at once.",
                        existing.name,
                    ));
                    overlap_error_label.set_visible(true);
                    next_button.set_sensitive(false);
                }
                None => {
                    overlap_error_label.set_visible(false);
                    next_button.set_sensitive(local.is_some() && remote.is_some());
                }
            }
        }
    );

    let page1_button_row = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).spacing(12).halign(gtk4::Align::End)
        .margin_top(12).margin_bottom(12).margin_start(12).margin_end(12)
        .build();
    page1_button_row.append(&page1_cancel_button);
    page1_button_row.append(&next_button);

    let page1 = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).build();
    page1.append(&page1_fields);
    page1.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
    page1.append(&page1_button_row);

    // -- Page 2: the merged local+remote folder tree --
    let page2_cancel_button = gtk4::Button::builder().label("Cancel").build();
    let save_button = gtk4::Button::builder().label("Save").css_classes(["suggested-action"]).build();
    let tree_list = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    let tree_scroller = gtk4::ScrolledWindow::builder()
        .child(&tree_list)
        .vexpand(true)
        .hscrollbar_policy(gtk4::PolicyType::Never)
        .build();
    let page2_content = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .spacing(6)
        .margin_top(24).margin_bottom(12).margin_start(24).margin_end(24)
        .build();
    // Explicit heading + description -- the tree alone didn't say what
    // it was for, and looked like a second, redundant "add a folder"
    // step rather than what it actually is: refining what within the
    // folder just chosen (or, in Edit mode, already chosen) actually
    // gets synced.
    page2_content.append(&gtk4::Label::builder().label("Choose What to Sync").xalign(0.0).css_classes(["heading"]).build());
    page2_content.append(&gtk4::Label::builder()
        .label("Uncheck any folders you don't want included. Everything is included by default.")
        .xalign(0.0).wrap(true).css_classes(["dim-label"])
        .build());
    page2_content.append(&tree_scroller);
    let page2_button_row = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).spacing(12).halign(gtk4::Align::End)
        .margin_top(12).margin_bottom(12).margin_start(12).margin_end(12)
        .build();
    page2_button_row.append(&page2_cancel_button);
    page2_button_row.append(&save_button);
    let page2 = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).build();
    page2.append(&page2_content);
    page2.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
    page2.append(&page2_button_row);

    let page_stack = gtk4::Stack::builder()
        .transition_type(gtk4::StackTransitionType::SlideLeftRight)
        .vexpand(true)
        .build();
    page_stack.add_named(&page1, Some("page1"));
    page_stack.add_named(&page2, Some("page2"));
    page_stack.set_visible_child_name(if editing.is_some() { "page2" } else { "page1" });

    // Resolves both roots and populates the tree -- shared between Edit
    // mode (called immediately, roots already known) and Add mode
    // (called once, after Next -- Page 1's remote picker only stored the
    // chosen subpath string, not a live location, so the remote side has
    // to be re-resolved here). No mount involved on either call --
    // remote_browse_location is a plain GOA credential fetch, same
    // reasoning as sync_target's own setup in sync.rs.
    let load_tree = clone!(
        #[strong] watcher,
        #[strong] account,
        #[strong] tree_list,
        #[strong] excluded_subpaths,
        move |local_path: PathBuf, remote_subpath: String, direction: config::SyncDirection| {
            let watcher = watcher.clone();
            let account = account.clone();
            let tree_list = tree_list.clone();
            let excluded_subpaths = excluded_subpaths.clone();
            glib::MainContext::default().spawn_local(async move {
                // Neither side is polled at all when this connection's
                // own direction says it could never matter -- by
                // request: a DownloadOnly connection never acts on a
                // local-only change, so there's nothing this picker's
                // own exclusion choices could do with a local listing
                // that a plain, always-there local_root wouldn't already
                // cover for real sync purposes; same reasoning, mirrored,
                // for UploadOnly and the remote side. populate_merged_
                // tree already tolerates either side being None on its
                // own (that's what already lets a bare local folder, with
                // no remote chosen yet, render at all before Page 1's own
                // Next button is even pressed) -- this is just the same,
                // existing tolerance applied deliberately instead of only
                // incidentally.
                let local_root = if direction == config::SyncDirection::DownloadOnly {
                    None
                } else {
                    Some(folder_picker::BrowseLocation::Local(local_path))
                };
                let remote_root = if direction == config::SyncDirection::UploadOnly {
                    None
                } else {
                    remote_browse_location(&watcher, &account, &remote_subpath).await
                };
                let global_patterns = config::effective_exclude_patterns();
                folder_picker::populate_merged_tree(
                    &tree_list, local_root, remote_root, global_patterns, excluded_subpaths,
                );
            });
        }
    );

    if let Some(existing) = editing.as_ref() {
        load_tree(existing.local_path.clone(), existing.remote_subpath.clone(), existing.direction);
    }

    // No buttons in the header bar at all -- Cancel/Next and Cancel/Save
    // both live at the bottom of their own page instead, per spec.
    let header_bar = libadwaita::HeaderBar::builder()
        .show_start_title_buttons(false)
        .show_end_title_buttons(false)
        .build();

    let toolbar_view = libadwaita::ToolbarView::builder().content(&page_stack).build();
    toolbar_view.add_top_bar(&header_bar);

    let mut dialog_builder = libadwaita::Window::builder()
        .title(if editing.is_some() { "Edit Folder" } else { "Add Folder to Sync" })
        .default_width(420)
        .default_height(560)
        .modal(true)
        .content(&toolbar_view);
    if let Some(parent) = parent_window.as_ref() {
        dialog_builder = dialog_builder.transient_for(parent);
    }
    let dialog = dialog_builder.build();
    // Same widget name as the main window -- see the Add Pattern
    // dialog's own comment on this same pattern for the full reasoning.
    dialog.set_widget_name("ripples-themed-window");

    // Esc-to-close: same rationale as elsewhere in this app -- Cancel is
    // always visible, this is convention polish, not a fix for getting
    // stuck.
    let escape_controller = gtk4::EventControllerKey::new();
    escape_controller.connect_key_pressed(clone!(
        #[strong] dialog,
        move |_, key, _, _| {
            if key == gtk4::gdk::Key::Escape {
                dialog.close();
                glib::Propagation::Stop
            } else {
                glib::Propagation::Proceed
            }
        }
    ));
    dialog.add_controller(escape_controller);

    page1_cancel_button.connect_clicked(clone!(#[strong] dialog, move |_| dialog.close()));
    page2_cancel_button.connect_clicked(clone!(#[strong] dialog, move |_| dialog.close()));

    choose_local_button.connect_clicked(clone!(
        #[strong] dialog,
        #[strong] chosen_local_path,
        #[strong] local_path_label,
        #[strong] revalidate,
        move |_| {
            let filesystem_root = folder_picker::BrowseLocation::Local(PathBuf::from("/"));
            // Only the Normal components -- skips the root separator
            // itself (Component::RootDir), which isn't a navigable
            // segment, just the boundary already represented by root
            // itself.
            let home_segments: Vec<String> = glib::home_dir()
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                    _ => None,
                })
                .collect();
            folder_picker::show_single_folder_picker(
                &dialog,
                filesystem_root,
                "Filesystem".to_string(),
                home_segments,
                clone!(
                    #[strong] chosen_local_path,
                    #[strong] local_path_label,
                    #[strong] revalidate,
                    move |location, display_path| {
                        if let folder_picker::BrowseLocation::Local(path) = location {
                            *chosen_local_path.borrow_mut() = Some(path);
                            local_path_label.set_label(&display_path);
                            revalidate();
                        }
                    }
                ),
            );
        }
    ));

    choose_remote_button.connect_clicked(clone!(
        #[strong] dialog,
        #[strong] watcher,
        #[strong] account,
        #[strong] remote_path_label,
        #[strong] chosen_remote_subpath,
        #[strong] revalidate,
        #[strong] choose_remote_button,
        move |_| {
            let dialog = dialog.clone();
            let watcher = watcher.clone();
            let account = account.clone();
            let remote_path_label = remote_path_label.clone();
            let chosen_remote_subpath = chosen_remote_subpath.clone();
            let revalidate = revalidate.clone();
            // By request -- confirmed as a real, reachable gap: this
            // whole handler already has a real async gap (remote_
            // browse_location's own network round-trip) before the
            // picker even opens, with nothing stopping a second click
            // from landing in that same gap and racing to open a second,
            // stacked picker sub-dialog. Re-enabled below at every path
            // that doesn't end in a picker actually opening (the
            // connection failing outright), and once the picker itself
            // closes (via its own returned window handle's closed
            // signal) -- whether that close came from a real folder
            // selection or a plain cancel, since either one is this
            // button's own job finishing either way.
            choose_remote_button.set_sensitive(false);
            let choose_remote_button = choose_remote_button.clone();
            glib::MainContext::default().spawn_local(async move {
                remote_path_label.set_label("Connecting\u{2026}");
                match remote_browse_location(&watcher, &account, "").await {
                    Some(remote_root) => {
                        remote_path_label.set_label(
                            &chosen_remote_subpath.borrow().as_ref()
                                .map(|s| display_remote_subpath(s))
                                .unwrap_or_else(|| "No folder chosen yet".to_string())
                        );
                        let picker_window = folder_picker::show_single_folder_picker(
                            &dialog,
                            remote_root.clone(),
                            account.presentation_identity(),
                            Vec::new(),
                            clone!(
                                #[strong] remote_root,
                                #[strong] chosen_remote_subpath,
                                #[strong] remote_path_label,
                                #[strong] revalidate,
                                move |location, _display_path| {
                                    // Stored value is the raw relative path
                                    // (BrowseLocation::relative_to -- same
                                    // reliable descendance check used
                                    // elsewhere in this app, rather than
                                    // comparing path/URI strings), not the
                                    // picker's own "Account Name / seg / seg"
                                    // display string.
                                    if let Some(subpath) = location.relative_to(&remote_root) {
                                        remote_path_label.set_label(&display_remote_subpath(&subpath));
                                        *chosen_remote_subpath.borrow_mut() = Some(subpath);
                                        revalidate();
                                    }
                                }
                            ),
                        );
                        picker_window.connect_close_request(clone!(
                            #[strong] choose_remote_button,
                            move |_| {
                                choose_remote_button.set_sensitive(true);
                                glib::Propagation::Proceed
                            }
                        ));
                    }
                    None => {
                        remote_path_label.set_label("Couldn't connect to this account's remote storage");
                        choose_remote_button.set_sensitive(true);
                    }
                }
            });
        }
    ));

    next_button.connect_clicked(clone!(
        #[strong] page_stack,
        #[strong] chosen_local_path,
        #[strong] chosen_remote_subpath,
        #[strong] load_tree,
        #[strong] dialog,
        #[strong] direction_upload_check,
        #[strong] direction_download_check,
        move |_| {
            page_stack.set_visible_child_name("page2");
            // Signals a genuinely different step, not a repeat of Page
            // 1 -- see Page 2's own heading/description for the same
            // reasoning.
            dialog.set_title(Some("Choose What to Sync"));
            // Both are guaranteed Some here -- next_button is only ever
            // sensitive once both pickers have actually been used (see
            // its own sensitivity wiring in both on_selected callbacks
            // above).
            if let (Some(local), Some(remote)) = (chosen_local_path.borrow().clone(), chosen_remote_subpath.borrow().clone()) {
                // Read directly off the checkboxes here too, same
                // reasoning as the save handler's own identical read --
                // this is the only other place this dialog needs to know
                // the currently-selected direction before it's actually
                // saved.
                let direction = if direction_upload_check.is_active() {
                    config::SyncDirection::UploadOnly
                } else if direction_download_check.is_active() {
                    config::SyncDirection::DownloadOnly
                } else {
                    config::SyncDirection::Bidirectional
                };
                load_tree(local, remote, direction);
            }
        }
    ));

    save_button.connect_clicked(clone!(
        #[strong] dialog,
        #[strong] state,
        #[strong] watcher,
        #[strong] account,
        #[strong] chosen_local_path,
        #[strong] chosen_remote_subpath,
        #[strong] excluded_subpaths,
        #[strong] editing,
        #[strong] direction_upload_check,
        #[strong] direction_download_check,
        move |_| {
            let excluded_subpaths = excluded_subpaths.borrow().clone();
            // Read directly off the checkboxes' own final state at save
            // time, not tracked separately -- radio-group membership
            // already guarantees exactly one of the three is active, so
            // there's nothing to gain from a parallel Cell duplicating
            // what the widgets themselves already, reliably know.
            let direction = if direction_upload_check.is_active() {
                config::SyncDirection::UploadOnly
            } else if direction_download_check.is_active() {
                config::SyncDirection::DownloadOnly
            } else {
                config::SyncDirection::Bidirectional
            };

            if let Some(existing) = editing.as_ref() {
                // Roots and direction are both locked (Page 1 -- the
                // only page either of them are ever set on -- is never
                // shown at all in Edit mode), so there's nothing to
                // write back for local_path/remote_subpath/name/
                // account_id/enabled/direction here -- only
                // excluded_subpaths, page2's own field, can actually
                // have changed. `direction` computed just above still
                // isn't used in this branch at all: direction_upload_
                // check/direction_download_check are never shown or
                // toggled in Edit mode, so reading them here would
                // silently reset an existing connection's own direction
                // back to Bidirectional on every edit -- confirmed as a
                // real bug caught before ever shipping, not a
                // hypothetical.
                // Looked up by id rather than trusting the captured
                // snapshot is still current -- it could have been
                // removed while this dialog was open; not found ->
                // silently do nothing, same "don't resurrect what's
                // gone" policy used elsewhere rather than erroring.
                //
                // No manual conflict-clearing loop here anymore -- by
                // request, removed rather than fixed in place. A path
                // that just became excluded needs its existing conflict
                // cleared, or it would sit in the Conflicts list forever
                // (the walker filters excluded paths out of enumeration
                // entirely, before it ever reaches the code that would
                // normally reconsider or resolve one), but this dialog's
                // own manual version of that check was never ancestor-
                // aware (see exclude::matches_pattern's own doc comment
                // -- it matches an item's own full path or name, not
                // whether that path falls under an excluded directory
                // higher up), so excluding a whole folder here rather
                // than the specific conflicting file itself would leave
                // the conflict behind uncleared. save_config below
                // already, unconditionally calls reevaluate_pending_
                // issues -- which does use the correct, ancestor-aware
                // path_or_ancestor_excluded, and is already the app's
                // own single, deliberately centralized place for exactly
                // this check (see its own doc comment) rather than
                // duplicated across every path that could act on a
                // pending item, this one included.
                if let Some(conn) = state.config.borrow_mut().connections.iter_mut().find(|c| c.id == existing.id) {
                    conn.excluded_subpaths = excluded_subpaths.clone();
                }
            } else {
                let Some(local_path) = chosen_local_path.borrow().clone() else { return };
                let Some(remote_subpath) = chosen_remote_subpath.borrow().clone() else { return };
                let name = local_path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "Sync Folder".to_string());

                state.config.borrow_mut().connections.push(config::SyncConnection {
                    id: glib::uuid_string_random().to_string(),
                    account_id: account.account_id().to_string(),
                    name,
                    local_path,
                    remote_subpath,
                    enabled: true,
                    direction,
                    excluded_subpaths,
                });
            }

            save_config(&state);
            refresh_account_ui(&state, &watcher);
            spawn_immediate_sync_round(&state, &watcher);
            dialog.close();
        }
    ));

    dialog.present();
}


/// See the design discussion this came from. notify_push alone -> x8 (a
/// live, persistent connection is a much stronger signal than a cheap
/// poll, so the periodic backstop's own role shrinks the most). ETag
/// alone -> x4 (still means the *first* real change waits a full,
/// unwidened interval to be caught, so a more moderate factor). Both ->
/// x32, multiplicative, per that same discussion. Applied to
/// remote_poll_interval_secs() inside run_sync_round's own per-
/// connection scheduling below -- each connection can genuinely have a
/// different multiplier, since different connections can have
/// different capabilities (one account on a Nextcloud server with
/// notify_push installed, another on a generic WebDAV server with
/// neither).
fn poll_interval_multiplier(notify_push_connected: bool, etag_capable: bool) -> u64 {
    match (notify_push_connected, etag_capable) {
        (true, true) => 32,
        (true, false) => 8,
        (false, true) => 4,
        (false, false) => 1,
    }
}

/// Derives this connection's own remote root as a plain https:// (or
/// http://) URL, suitable for a direct HTTP request -- needed for the
/// ETag tier's own PROPFIND. For a GOA account, neither account.
/// server_url (the bare server root, no path at all) nor account.
/// files_uri (this account's own WebDAV root, but as a davs://... GVfs
/// URI, and not including this specific connection's own remote_subpath)
/// is quite what's needed on its own; the Goa branch combines the right
/// parts of both, best-effort, same defensive posture as goa.rs's own
/// reconstruct_server_url: returns None if files_uri's own shape doesn't
/// match what's expected, rather than guessing at a URL that might be
/// wrong. For a direct account, this is simpler -- webdav_base_url()
/// already gives the plain https:// WebDAV root directly, no URI
/// parsing needed at all, just the same remote_subpath join every other
/// caller already uses (webdav::build_url).
fn connection_remote_root_url(account: &AccountSource, connection: &config::SyncConnection) -> Option<String> {
    match account {
        AccountSource::Goa(a) => {
            let files_uri = a.files_uri.as_ref()?;
            let (scheme, rest) = files_uri.split_once("://")?;
            let new_scheme = match scheme {
                "davs" => "https",
                "dav" => "http",
                _ => return None,
            };
            let authority_end = rest.find('/').unwrap_or(rest.len());
            let (authority, base_path) = rest.split_at(authority_end);
            let authority = authority.rsplit_once('@').map(|(_, host)| host).unwrap_or(authority);
            let base_path = base_path.trim_end_matches('/');
            let subpath = connection.remote_subpath.trim_start_matches('/');
            if subpath.is_empty() {
                Some(format!("{new_scheme}://{authority}{base_path}"))
            } else {
                Some(format!("{new_scheme}://{authority}{base_path}/{subpath}"))
            }
        }
        AccountSource::Direct(a) => {
            let base = a.webdav_base_url();
            if connection.remote_subpath.is_empty() {
                Some(base)
            } else {
                Some(webdav::build_url(&base, &connection.remote_subpath))
            }
        }
    }
}

/// Tries the ETag tier's cheap precheck for one connection -- true means
/// confirmed unchanged (the caller should skip the full crawl entirely
/// this round); false means either a real change was found, or the
/// check itself couldn't be completed for any reason (no stored
/// baseline yet, couldn't derive the remote root URL, couldn't retrieve
/// a password, the PROPFIND itself failed) -- every one of those falls
/// through to false, meaning "do the real crawl," never defaulting to
/// true. See push.rs's own top-level doc comment for why that asymmetry
/// is deliberate.
async fn try_etag_skip(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>, journal: &rusqlite::Connection, account: &AccountSource, connection: &config::SyncConnection) -> bool {
    let Ok(Some(stored_etag)) = journal::get_root_etag(journal, &connection.id) else {
        return false; // no baseline yet -- first round for this connection always needs a real crawl
    };
    let Some(remote_root_url) = connection_remote_root_url(account, connection) else {
        return false;
    };
    let Some((_, username, password)) = account.credentials(watcher).await else {
        return false;
    };
    let password_str = password.as_str().to_string();
    // Pool 2 (confirm/detect) -- see this session's own conversation for
    // why this belongs here despite living in a different module and
    // using push.rs's own client rather than webdav.rs's: it's exactly
    // the same shape of work as sync.rs's own Pool 2 calls (a single
    // stat-like check confirming whether something changed), just
    // structurally separate for reasons that predate this pool
    // hierarchy entirely. Acquired here, on the main thread, before
    // run_blocking hands off to a separate OS thread -- Rc/async_channel
    // aren't Send, so the permit itself couldn't be acquired from inside
    // that spawned thread even if this were restructured to try.
    let _permit = state.confirm_pool.acquire(&state.content_semaphore, "Pool 2 (confirm/detect)").await;
    push::run_blocking(move || push::root_etag_unchanged(&remote_root_url, &username, &password_str, &stored_etag)).await
}

/// If `account_id` already has connections configured, pauses Ripples
/// and informs the user, rather than letting those connections start
/// syncing unreviewed. By request: a real, deliberate safety net for an
/// account old enough, or changed enough since it was last active, that
/// picking back up exactly where it left off could be badly wrong.
///
/// The only way `account_id` can already have connections at the exact
/// moment it's becoming active is if this same account (account_id
/// itself is deterministic -- server+login_name for a direct account,
/// the real GOA id for a GOA one) was active before, had connections
/// configured, and was later removed (direct) or deselected (GOA) --
/// removing an account or deselecting it never touches config.
/// connections itself (see remove_direct_account's own doc comment), so
/// those connections simply sit there, unowned by any currently-active
/// account, until this same account_id becomes active again. A
/// genuinely brand-new account can't already have connections under an
/// id nothing has ever used before, so no separate "was this actually
/// removed before" tracking is needed -- the presence of a match here
/// already means exactly that. Does nothing at all if there are none:
/// a genuinely new account has nothing to review, so there's nothing to
/// pause for.
///
/// Must be called BEFORE spawn_immediate_sync_round at either of this
/// function's own two call sites, not after -- spawn_immediate_sync_
/// round itself is fire-and-forget, not awaited, so calling this
/// afterward would leave a real window where a sync round could already
/// be running before the pause this function sets ever took effect.
/// Reuses paused_by_user itself, not a genuinely separate gate -- see
/// paused_reason_restored_account's own doc comment on AppState for why.
fn pause_for_restored_connections_if_needed(state: &Rc<AppState>, account_id: &str) {
    let has_connections = state.config.borrow().connections.iter().any(|c| c.account_id == account_id);
    if !has_connections {
        return;
    }
    state.paused_by_user.set(true);
    state.paused_reason_restored_account.set(true);
    log_and_persist(state, format!("[{}] Paused newly restored account", now_hms()));
    // Same reasoning as toggle_pause's own identical call -- a round
    // already mid-flight (for whichever account was active before this
    // one) shouldn't keep running to completion just because this
    // specific pause happened to arrive through a different code path
    // than the pause button itself.
    state.active_sync_cancels.borrow_mut().clear();
    update_status_ui(state, true);
}

/// Clears the restored-account pause (see paused_reason_restored_
/// account's own doc comment on AppState) whenever the active account
/// itself changes, for any reason -- removed, deselected, switched to a
/// different one, or a new one activated. Confirmed as a real, reported
/// bug without this: this pause exists specifically to let someone
/// review ONE account's own restored connections before they sync: if
/// that account stops being the active one, the banner and subtitle
/// explaining why Ripples is paused kept showing anyway, and the pause
/// itself kept blocking everything, even though whatever it was raised
/// to protect against no longer has anything to do with what's active
/// now.
///
/// Does nothing at all if paused_reason_restored_account isn't
/// currently the reason -- an ordinary manual pause (or no pause at
/// all) is left completely untouched. This clears exactly the one,
/// specific pause reason it's about, never anything else that happens
/// to also be true at the same moment.
///
/// Called from every site that reassigns active_account_id (GOA
/// selection, a freshly-added or newly-activated direct account,
/// remove_direct_account) -- not just the removal case the bug report
/// specifically named, since switching to a different account without
/// removing the original one leaves this exact same problem: the
/// pause's own reason no longer applies to whatever's active, regardless
/// of whether the old account was removed outright or simply left
/// behind, still configured, no longer selected.
fn clear_restored_account_pause_if_stale(state: &Rc<AppState>) {
    if state.paused_reason_restored_account.get() {
        state.paused_by_user.set(false);
        state.paused_reason_restored_account.set(false);
        log_and_persist(state, format!("[{}] Sync resumed -- the restored account this was paused for is no longer active", now_hms()));
        update_status_ui(state, true);
    }
}

/// Checks a single account's own ETag/notify_push/theming capabilities
/// against its server, throttled to once per account per session for
/// the ETag/notify_push parts (a server's support for either doesn't
/// change mid-session) with the theme specifically re-fetched every
/// call after the first (an admin's actual theme configuration
/// genuinely can change at any time, and there's no push-based way to
/// learn about that -- confirmed via direct research that theming
/// changes aren't among the event categories notify_push ever
/// advertises or emits: this server's own capabilities response lists
/// exactly "files", "activities", "notifications" and nothing else).
///
/// Deliberately account-level, not tied to iterating this account's own
/// connections -- by request, extracted out of run_sync_round's own
/// per-connection loop (which still calls this, once per connection,
/// same as before) specifically so it can ALSO be called directly the
/// moment an account becomes active, before it necessarily has any
/// connections configured at all. Confirmed as a real, reported gap
/// without that: capabilities_checked (and therefore ETag capability,
/// notify_push, AND theming, all three -- not theme alone, since none
/// of the three have any real reason to wait on a connection existing
/// either) previously only ever got checked as a side effect of a sync
/// round actually reaching this account's own connections -- an
/// account with zero connections yet (a fresh install right after
/// adding one, or a freshly-added direct account switched to from a
/// different, previously-active one) would never trigger this at all,
/// no matter how many sync rounds ran, since the per-connection loop
/// this used to live inside would never execute even once.
///
/// Safe to call from multiple places at once (this function's own new
/// call sites, and run_sync_round's own per-connection loop, could
/// plausibly reach the same account around the same moment) for the
/// same reason the single, original call site already was:
/// capabilities_checked itself is marked synchronously, before this
/// function's own first await point -- since glib's main context is
/// single-threaded, nothing else can interleave and see "not yet
/// checked" in between, so at most one full discovery (and therefore at
/// most one notify_push listener spawn) can ever be in flight for a
/// given account at a time, regardless of how many call sites might
/// invoke this around the same moment.
async fn check_account_capabilities(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>, account: &AccountSource, force: bool) {
    // Same three flags, same force-bypass, as trigger_sync_round's own
    // identical check -- by request, confirmed as a real, genuinely
    // introduced gap without this: this function makes a real network
    // request (push::discover_capabilities), and its two new, direct
    // call sites (account selection, a freshly-added account) bypass
    // trigger_sync_round entirely, so neither one inherited its pause
    // gate the way this function's original, single call site (from
    // within run_sync_round, itself only ever reached after trigger_
    // sync_round's own gate already passed) always implicitly did.
    // Threaded through from both of those new call sites (force: false,
    // since neither is an explicit "Sync Now"-style override) and from
    // run_sync_round's own call (force: whatever trigger_sync_round
    // itself was given) -- not a fresh check against the pause flags
    // alone, since those flags themselves stay true even when force
    // already decided to bypass them (see trigger_sync_round's own
    // paused_for_metered handling for why), which would otherwise
    // silently break capabilities discovery during a legitimate "Sync
    // Now while paused."
    if !force && (state.paused_by_user.get() || state.paused_for_metered.get() || state.paused_for_power_save.get()) {
        return;
    }
    let account_id = account.account_id().to_string();
    if !state.capabilities_checked.borrow().contains(&account_id) {
        // credentials() folds "password retrieval failed" and "GOA
        // account not ready"/"direct account credential fetch failed"
        // into the same single None case -- preserves the original's
        // own deliberate behavior exactly: if this fails for any
        // reason, capabilities_checked is left unmarked below, so it's
        // simply retried next time this is called rather than treated
        // as a real, terminal failure.
        if let Some((_, username, password)) = account.credentials(watcher).await {
            state.capabilities_checked.borrow_mut().insert(account_id.clone());
            let server_url = account.server_url().expect("checked Some above");
            let password_str = password.as_str().to_string();
            // Cloned here, before username is moved into the
            // run_blocking closure below -- needed again further down
            // for the push listener thread.
            let username_for_listener = username.clone();
            let capabilities = push::run_blocking(move || push::discover_capabilities(&server_url, &username, &password_str)).await;
            if is_debug_enabled() {
                eprintln!(
                    "[{}] [debug] capabilities check ({}): is_nextcloud_or_owncloud={} theme={:?}",
                    now_hms(), account.presentation_identity(), capabilities.is_nextcloud_or_owncloud, capabilities.theme,
                );
            }

            state.etag_capable.borrow_mut().insert(account_id.clone(), capabilities.is_nextcloud_or_owncloud);
            let _ = state.status_events_tx.try_send(status::SyncEvent::EtagCapability {
                connection: account.presentation_identity(),
                available: capabilities.is_nextcloud_or_owncloud,
            });

            // Cached regardless of apply_server_theming's own current
            // value -- toggling the setting back on later should be
            // able to use whatever was last actually fetched, not need
            // a fresh network round-trip just because it was off at the
            // moment this particular check happened to run.
            if let Some(theme) = &capabilities.theme {
                state.config.borrow_mut().cached_server_themes.insert(account_id.clone(), theme.clone());
                save_config(state);
            }
            apply_server_theme(state);
            // Direct set_sensitive here rather than calling
            // refresh_account_ui again -- this can run mid-sync-round,
            // not a place that should also be rebuilding the account
            // list UI as a side effect. capabilities.theme.is_some(),
            // not is_nextcloud_or_owncloud -- see this toggle's own
            // construction-time doc comment for why that flag alone
            // isn't the right signal (true for ownCloud too, which has
            // no real theming data at all).
            if let Some(theming_row) = state.window_widgets.borrow().as_ref().map(|w| w.theming_row.clone()) {
                let is_active_account = state.config.borrow().active_account_id.as_deref() == Some(account_id.as_str());
                theming_row.set_sensitive(is_active_account && capabilities.theme.is_some());
            }

            match (capabilities.websocket_url, capabilities.pre_auth_url) {
                (Some(websocket_url), Some(pre_auth_url)) => {
                    let senders = (
                        state.push_password_requests_tx.borrow().clone(),
                        state.push_events_tx.borrow().clone(),
                    );
                    if let (Some(password_requests_tx), Some(push_events_tx)) = senders {
                        let account_id = account_id.clone();
                        std::thread::spawn(move || {
                            push::run_listener(websocket_url, pre_auth_url, account_id, username_for_listener, password_requests_tx, push_events_tx);
                        });
                    }
                    // Connected/Disconnected -- and their own
                    // NotifyPushCapability logging -- come from the
                    // listener thread itself once it actually
                    // establishes (or fails to establish) a connection;
                    // see the push-event handler task in run_background.
                }
                _ => {
                    // notify_push genuinely isn't installed on this
                    // server -- known for certain, right now, so logged
                    // right here rather than left to a listener that
                    // will never even be spawned to report it.
                    let _ = state.status_events_tx.try_send(status::SyncEvent::NotifyPushCapability {
                        connection: account.presentation_identity(),
                        available: false,
                    });
                }
            }
        }
        // If password retrieval itself failed, capabilities_checked is
        // deliberately left unmarked -- this account's own discovery is
        // simply retried next time this is called, rather than being
        // permanently stuck reporting "nothing available" because of
        // what was actually just a transient credential-retrieval
        // problem, not a real answer about the server's own
        // capabilities.
    } else if let Some((_, username, password)) = account.credentials(watcher).await {
        // Re-fetches just for a fresh theme, every call after the first
        // -- by request, piggybacked on this same already-throttled
        // touchpoint rather than adding a separate, dedicated throttle
        // of its own on top of it. ETag-capability detection above
        // doesn't need repeating (a server's support for ETags doesn't
        // change mid-session), but the admin's actual theme
        // configuration genuinely can change at any time -- and there's
        // no push-based way to learn about that (see this function's
        // own doc comment above for the confirmed research on that).
        let server_url = account.server_url().expect("checked Some above");
        let password_str = password.as_str().to_string();
        let capabilities = push::run_blocking(move || push::discover_capabilities(&server_url, &username, &password_str)).await;
        if is_debug_enabled() {
            eprintln!("[{}] [debug] theme re-fetch ({}): server returned theme={:?}", now_hms(), account.presentation_identity(), capabilities.theme);
        }
        if let Some(theme) = capabilities.theme {
            state.config.borrow_mut().cached_server_themes.insert(account_id.clone(), theme);
            save_config(state);
        }
        apply_server_theme(state);
    }
}

/// `scoped`: when true (only the watch-debounce path -- see
/// trigger_sync_round's own doc comment), each connection's own entry in
/// state.dirty_dirs is drained and handed to its SyncTarget as
/// dirty_scope, restricting that connection's walk to the
/// reported-dirty directories (plus their ancestors, plus any genuinely
/// new content -- see sync_child_directory's own recursion gate for the
/// exact rule). A connection with nothing currently in dirty_dirs is
/// skipped entirely in a scoped round -- nothing indicated it needs
/// checking right now, and its own normal periodic schedule still
/// applies via the unscoped path. When false, dirty_scope always stays
/// None (an ordinary full recursive crawl) and any accumulated
/// dirty_dirs entries for a connection this round actually visits are
/// cleared -- a full crawl already covers everything a scoped rescan
/// would have, so there's nothing left worth keeping queued.
async fn run_sync_round(state: &Rc<AppState>, watcher: &Rc<GoaWatcher>, scoped: bool, force: bool) -> bool {
    let connections = active_connections(state);
    let accounts = all_accounts(state, watcher);
    // Whether any connection actually proceeded past its own
    // tier-multiplier gate this round -- see trigger_sync_round's own
    // use of this return value for why: "Last checked" should reflect a
    // real check having happened, not just this function having been
    // called and finding every connection's own turn hadn't come up yet.
    // Confirmed as a real, reported problem otherwise: the label looked
    // identical whether a genuine check occurred or every connection was
    // silently skipped, with no way for a user to tell which had
    // actually happened.
    let mut any_check_happened = false;

    // Queried once here, not inside the loop below -- on-battery state
    // doesn't vary per connection, only the tier multiplier does. The
    // only remaining caller of on_battery_power at all now --
    // trigger_sync_round's own pause check used to also query this
    // directly (hence the historical "second, independent query"
    // framing this comment once had), but that's gone now: it checks
    // gio::PowerProfileMonitor for Power Save Mode instead, which has
    // nothing to do with on-battery status specifically.
    let on_battery = on_battery_power(state).await.unwrap_or(false);

    for connection in connections {
        // connections above is a snapshot taken once at the top of this
        // round -- a connection removed, or its own account deactivated,
        // after that snapshot but before its own turn in this loop would
        // otherwise still get a fresh active_sync_cancels entry and run
        // untouched, since the cancel race below only protects a
        // connection *already* mid-sync, not one whose turn hasn't come
        // up yet. Re-checking against active_connections here (not just
        // "does this connection still exist in config at all") closes
        // that gap cheaply, before any network I/O for this connection
        // has started -- the same class of gap found and fixed
        // elsewhere this session (see the background verification
        // task's and the queued-action drain's own doc comments): a
        // connection still existing in config doesn't mean its account
        // is still the active one.
        if !active_connections(state).iter().any(|c| c.id == connection.id) {
            continue;
        }

        let Some(account) = accounts.iter().find(|a| a.account_id() == connection.account_id) else {
            continue; // account no longer available (removed from GOA, or a direct account deleted) -- skip, don't error
        };
        // Same two-condition check the original GOA-only version had --
        // is_ready() alone (files_uri-equivalent) isn't quite the same
        // thing refresh_account_ui's own filter checks, so this
        // deliberately doesn't just call is_ready() and stop there;
        // server_url() is checked separately to preserve that exact
        // distinction rather than risk quietly loosening it.
        if !account.is_ready() || account.server_url().is_none() {
            continue; // Files interface hasn't resolved yet -- try again next round
        }

        // Removed (not just peeked) up front -- but restored, merged with
        // anything that's arrived since, at every point below that
        // decides to `continue` without this round actually covering it.
        // See restore_dirty_dirs' own doc comment for the full reasoning
        // and the real, reported bug this exists to fix: a periodic
        // round that drained this and then got short-circuited (the
        // tier-multiplier gate, or try_etag_skip) was silently
        // destroying the only evidence that a local change was still
        // waiting -- confirmed as exactly the mechanism behind a real
        // report (a newly-extracted archive, and a moved directory, both
        // silently never syncing until an unrelated rename happened to
        // win the race against an unlucky periodic round instead).
        let dirty_dirs_absolute: HashSet<PathBuf> = state.dirty_dirs.borrow_mut().remove(&connection.id).unwrap_or_default();
        let had_dirty_dirs = !dirty_dirs_absolute.is_empty();
        // Converted to the relative_path convention sync.rs's own
        // SyncTarget expects (root-relative, no leading '/', '/'-
        // separated) via strip_prefix against this connection's own
        // local_root; a path that somehow doesn't start with local_root
        // (shouldn't happen -- watch.rs only ever watches directories
        // under it) is dropped rather than guessed at.
        let dirty_relative_dirs: HashSet<String> = dirty_dirs_absolute.iter()
            .filter_map(|absolute| absolute.strip_prefix(&connection.local_path).ok()
                .map(|rel| rel.to_string_lossy().replace('\\', "/")))
            .collect();

        if scoped && !had_dirty_dirs {
            // Nothing reported dirty for this connection specifically --
            // it isn't the one whose watch just fired, and a scoped
            // round has no reason to touch it. Its own ordinary
            // periodic schedule (the unscoped path) still applies
            // regardless. Nothing to restore either -- dirty_dirs_absolute
            // is already empty in this branch.
            continue;
        }
        let dirty_scope = if scoped { Some(sync::expand_dirty_scope(dirty_relative_dirs.clone())) } else { None };

        // Extracted into check_account_capabilities -- see its own doc
        // comment for the full reasoning (throttling, the theme's own
        // per-call re-fetch, and why this is now also called directly
        // elsewhere, not just from within this loop).
        check_account_capabilities(state, watcher, account, force).await;

        // notify_push having told us something under this account
        // genuinely changed overrides the tier-multiplier scheduling
        // below entirely -- see force_full_check's own doc comment for
        // the bug this exists to fix. Skips the ETag precheck too, not
        // just the elapsed-time gate: Sync Now is a direct, explicit
        // user request for a real, thorough check right now, not a
        // question "did the remote root's own ETag change" could
        // meaningfully answer instead.
        let forced = state.force_full_check.borrow_mut().remove(&connection.id);
        // A genuinely different case from forced above, despite the
        // similar name -- see notify_push_pending's own doc comment on
        // AppState for the full reasoning. Bypasses only the scheduling
        // gate just below, not try_etag_skip itself.
        let pending = state.notify_push_pending.borrow_mut().remove(&connection.id);

        // Tier-multiplier scheduling: this connection's own turn hasn't
        // necessarily come up yet on this particular firing of the
        // shared, global periodic timer -- see poll_interval_multiplier's
        // own doc comment for why a connection with notify_push
        // connected and/or the ETag tier available can go substantially
        // longer between real checks than remote_poll_interval_minutes'
        // own base value alone would imply.
        let notify_push_connected = state.notify_push_status.borrow().get(&connection.account_id).copied().unwrap_or(NotifyPushStatus::Disconnected).is_connected();
        let etag_capable = state.etag_capable.borrow().get(&connection.account_id).copied().unwrap_or(false);
        let multiplier = poll_interval_multiplier(notify_push_connected, etag_capable);
        // 1.5x on top of the tier multiplier above, not a replacement
        // for it -- a connection already checking rarely (notify_push
        // connected, ETag-capable) goes rarer still on battery, rather
        // than this resetting back to some separate, battery-specific
        // schedule of its own.
        let battery_multiplier = if on_battery { 1.5 } else { 1.0 };
        let effective_interval_secs = (state.config.borrow().remote_poll_interval_secs() as f64 * multiplier as f64 * battery_multiplier) as u64;

        // A scoped round only ever reaches this point for a connection
        // with real, known-dirty directories waiting (see the
        // dirty_dirs_absolute check above, which `continue`s otherwise).
        // An UNSCOPED (periodic) round can also observe pending dirt here
        // -- this connection's own watch-debounce timer just hasn't
        // fired yet -- and needs the exact same bypass: a full crawl is
        // about to run regardless, so there's no reason to let the
        // ETag precheck below skip it and leave that pending local
        // change to be discovered "later" by a scoped round that, by
        // then, would otherwise have nothing left to find (see
        // restore_dirty_dirs' own doc comment for why that's not
        // actually a risk here either way).
        //
        // state.first_round_this_session.get() belongs in this same
        // bypass -- real, reported bug without it: try_etag_skip's own
        // stored_etag lookup is a *connection's* baseline, persisted
        // across restarts, not a *session's* -- so on a fresh launch,
        // if the remote genuinely hasn't changed since the app was last
        // closed, the ETag tier confirms "unchanged" and this connection
        // gets skipped on literally the first round it's ever checked
        // this session, before a single real crawl has run. The ETag
        // check can only prove the remote side is unchanged; it has no
        // way to know about local edits made while Ripples wasn't
        // running at all to watch for them (the local FileMonitor only
        // observes changes while the process is alive) -- skipping the
        // very first crawl of a session is exactly how those get missed
        // permanently, surfacing as the app going straight to "Idle" on
        // startup without ever having actually looked. Every later round
        // this same session is unaffected -- had_dirty_dirs is what
        // correctly covers "changed while running" from here on, the
        // same as it always did.
        // skip_tier_gate: bypasses only the elapsed-time scheduling gate
        // just below, guaranteeing this connection is actually looked at
        // this round rather than potentially skipped outright -- forced
        // and pending both belong here, since either one firing at all
        // is real information this connection's own schedule wouldn't
        // otherwise have, whether or not it also warrants skipping
        // try_etag_skip (see bypass_etag_check just below for that
        // separate distinction).
        let skip_tier_gate = forced || pending || had_dirty_dirs || state.first_round_this_session.get();
        if !skip_tier_gate {
            if let Some(last_check) = state.last_full_check.borrow().get(&connection.id) {
                if last_check.elapsed().as_secs() < effective_interval_secs {
                    // Not covering this connection this round after all --
                    // restore whatever was removed above (merged with
                    // anything a racing FileMonitor event has already
                    // added back since -- see restore_dirty_dirs' own doc
                    // comment) so it survives for a later round instead of
                    // being silently lost.
                    restore_dirty_dirs(&state, &connection.id, dirty_dirs_absolute);
                    continue; // this connection's own effective interval hasn't elapsed yet
                }
            }
        }
        // Reaching this point means this connection is actually being
        // checked this round -- at minimum the ETag precheck just below,
        // a real network round-trip, not just this function having been
        // invoked and finding nothing due yet.
        any_check_happened = true;

        // bypass_etag_check: unlike skip_tier_gate above, this one
        // deliberately does NOT include `pending` -- an unresolved
        // notify_push event is exactly the question try_etag_skip itself
        // is positioned to answer (did this connection's own remote root
        // actually change), not a reason to skip asking it. forced
        // (Sync Now) stays here, along with had_dirty_dirs and
        // first_round_this_session, all for the same underlying reason:
        // real information the remote-only ETag check has no way to
        // account for on its own -- an explicit user request for a
        // thorough check, a pending local change, or "this session has
        // never actually crawled yet, so a remote-unchanged verdict
        // alone can't rule out a local edit made while the app wasn't
        // running to watch for it."
        let bypass_etag_check = forced || had_dirty_dirs || state.first_round_this_session.get();

        // ETag tier: a cheap precheck before ever committing to the full
        // crawl below -- see try_etag_skip's own doc comment for why
        // every failure of the check itself falls through to a real
        // crawl rather than assuming nothing changed.
        if !bypass_etag_check && etag_capable && try_etag_skip(state, watcher, &state.journal, account, &connection).await {
            state.last_full_check.borrow_mut().insert(connection.id.clone(), boot_time::Instant::now());
            // Same restoration as the tier-gate exit just above -- this
            // connection isn't actually getting a real crawl this round
            // either, so anything removed from dirty_dirs at the top of
            // this loop needs to survive for whatever round does end up
            // covering it.
            restore_dirty_dirs(&state, &connection.id, dirty_dirs_absolute);
            continue;
        }

        // Resolved eagerly, here, for either account type -- by request,
        // confirmed as a real, reported bug without this: sync_target
        // itself used to do this resolution on its own, but only ever
        // via watcher.account_info (a GOA-only lookup -- see goa.rs's
        // own GoaWatcher::account_info, whose backing map is populated
        // exclusively from GOA's own D-Bus signals, with no path for a
        // direct account to ever appear in it), so every direct
        // account's own connection failed this unconditionally, on its
        // very first sync attempt, with "Mount failed - account not
        // found." Never caught before now because every real, prior
        // sync connection had only ever been created under a GOA
        // account. Matches build_sync_target_for_connection's own,
        // already-correct pattern exactly -- see that function's own
        // doc comment for the full reasoning on AccountSource::
        // credentials() covering both account types with one call.
        let Some((base_url, username, password)) = account.credentials(watcher).await else {
            let _ = state.status_events_tx.try_send(status::SyncEvent::MountFailed {
                connection: connection.name.clone(),
                error: "account not found".to_string(),
            });
            restore_dirty_dirs(&state, &connection.id, dirty_dirs_absolute);
            continue;
        };
        let remote_base_url = if connection.remote_subpath.is_empty() {
            base_url
        } else {
            webdav::build_url(&base_url, &connection.remote_subpath)
        };
        let chunking_base_url = if etag_capable {
            account.server_url()
                .map(|server_url| format!("{}/remote.php/dav/uploads/{}", server_url.trim_end_matches('/'), username))
        } else {
            None
        };

        let target = sync::SyncTarget {
            connection_id: connection.id.clone(), // unique per connection -- multiple folders per account no longer collide in the journal
            connection_name: connection.name.clone(),
            local_root: connection.local_path.clone(),
            direction: connection.direction,
            remote_base_url: RefCell::new(Some(remote_base_url)),
            chunking_base_url: RefCell::new(chunking_base_url),
            username: RefCell::new(Some(username)),
            password: RefCell::new(Some(password)),

            settings_version: state.settings_version.clone(),
            // u64::MAX as the initial "cached generation" -- the real
            // counter only ever counts up from 0, so this can never
            // naturally match, guaranteeing current_settings' very
            // first call always does a real fetch rather than trusting
            // an empty placeholder.
            cached_settings: RefCell::new((u64::MAX, sync::CachedSettings::default())),
            renamed_away: Rc::new(RefCell::new(HashSet::new())),
            discovered_count: state.discovered_count.clone(),
            processed_count: state.processed_count.clone(),
            cleanup_temp_files: state.first_round_this_session.get(),
            // The app-wide one, shared (not a fresh Semaphore of its
            // own) -- see AppState's own doc comment on content_semaphore
            // for why.
            content_semaphore: state.content_semaphore.clone(),
            discovery_pool: state.discovery_pool.clone(),
            confirm_pool: state.confirm_pool.clone(),
            dirty_scope,
            cleanup_retry_cooldown: state.cleanup_retry_cooldown.clone(),
            stability_streak: state.stability_streak.clone(),
            remote_stability_streak: state.remote_stability_streak.clone(),
            deletion_pending: state.deletion_pending.clone(),
            webdav_failure_streak: state.webdav_failure_streak.clone(),
            move_locks: state.move_locks.clone(),
            had_freshness_deferral: state.had_freshness_deferral_this_round.clone(),
            dirty_dirs: state.dirty_dirs.clone(),
        };

        // Registered right before starting, deregistered right after --
        // see active_sync_cancels' own doc comment for why this needs to
        // be a real select() rather than a flag sync_target polls
        // between items. bounded(1) is plenty: this only ever needs to
        // carry a single "stop" signal, never a queue of them.
        let (cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);
        state.active_sync_cancels.borrow_mut().insert(connection.id.clone(), cancel_tx);

        let sync_future = std::pin::pin!(sync::sync_target(&state.journal, &target, &state.status_events_tx));
        let cancel_future = std::pin::pin!(cancel_rx.recv());
        match futures_util::future::select(sync_future, cancel_future).await {
            futures_util::future::Either::Left(_) => {
                // Ran to completion normally -- this connection's own
                // effective interval is satisfied fresh starting now,
                // and if the ETag tier is available, a fresh baseline is
                // worth capturing: the crawl that just finished may
                // itself have changed the server's own root ETag, so
                // re-fetching now (rather than reusing whatever was
                // fetched before the crawl, which is already stale)
                // gets the actual, current value.
                //
                // Only when this wasn't a scoped round -- a targeted
                // rescan of a handful of reported-dirty directories is
                // not the same thing as "this connection was fully
                // checked," and letting it satisfy the tier-multiplier
                // gate above would push the next genuine full crawl
                // (this connection's own safety net against whatever the
                // local watch itself might have missed) further out
                // than the setting actually promises.
                if !scoped {
                    state.last_full_check.borrow_mut().insert(connection.id.clone(), boot_time::Instant::now());
                }
                if etag_capable {
                    if let (Some(remote_root_url), Some((_, username, password))) = (
                        connection_remote_root_url(account, &connection),
                        account.credentials(watcher).await,
                    ) {
                        let password_str = password.as_str().to_string();
                        let connection_id = connection.id.clone();
                        if let Some(etag) = push::run_blocking(move || push::fetch_root_etag(&remote_root_url, &username, &password_str)).await {
                            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                            if let Err(e) = journal::set_root_etag(&state.journal, &connection_id, &etag, now) {
                                eprintln!("sync[{connection_id}]: couldn't store fresh ETag baseline: {e}");
                            }
                        }
                    }
                }
            }
            futures_util::future::Either::Right(_) => {
                // Dropping sync_future here (it's still owned by this match
                // arm, going out of scope) is what actually cancels
                // whatever GIO operation was mid-flight -- see
                // active_sync_cancels' doc comment for why that's not just
                // wishful thinking.
                eprintln!("sync[{}]: cancelled -- connection was removed mid-round", connection.id);
            }
        }
        state.active_sync_cancels.borrow_mut().remove(&connection.id);
    }
    any_check_happened
}

/// Builds a folder_picker::BrowseLocation::Remote for `account`'s own
/// WebDAV root, joined with `remote_subpath` if given -- the folder
/// picker's own equivalent of build_sync_target_for_connection just
/// below, minus everything that function needs beyond just a location
/// to browse from. None on any failure (account not ready, credential
/// fetch failed) -- every caller already treats a missing remote root
/// as "nothing to browse on that side yet," the same tolerance
/// ensure_mounted's own .ok() used to get for free.
async fn remote_browse_location(watcher: &GoaWatcher, account: &AccountSource, remote_subpath: &str) -> Option<folder_picker::BrowseLocation> {
    let (base_url, username, password) = account.credentials(watcher).await?;
    let url = if remote_subpath.is_empty() { base_url } else { webdav::build_url(&base_url, remote_subpath) };
    Some(folder_picker::BrowseLocation::Remote { url, username, password })
}

/// Builds a working SyncTarget for one connection on demand -- the same
/// account lookup / mount dance run_sync_round runs per connection
/// inside its loop, factored out so a single-item action (conflict
/// resolution, a live remote info lookup) doesn't have to run an entire
/// sync round just to reach one file. Returns a plain String error
/// rather than a typed one since the only consumers are dialogs that
/// just need a line to show the user -- account missing, Files
/// interface not ready yet, or mount failed.
async fn build_sync_target_for_connection(
    state: &Rc<AppState>,
    watcher: &Rc<GoaWatcher>,
    connection: &config::SyncConnection,
) -> Result<sync::SyncTarget, String> {
    let accounts = all_accounts(state, watcher);
    let account = accounts.iter().find(|a| a.account_id() == connection.account_id).cloned()
        .ok_or_else(|| {
            if is_debug_enabled() {
                eprintln!("[{}] [debug] build_sync_target_for_connection {}: account {} no longer known ({} total)", now_hms(), connection.name, connection.account_id, accounts.len());
            }
            "Account no longer available".to_string()
        })?;

    // Populated eagerly here, unlike the main round's own construction
    // (which leaves these None, since sync_target's own setup always
    // runs immediately after and does this itself) -- this function's
    // own callers (single-item retry, conflict resolution, the file-info
    // display) call straight into sync.rs's own functions without ever
    // going through sync_target at all, so nothing else would ever
    // populate these otherwise. Direct mirror of sync_target's own
    // setup logic -- see that function for the full reasoning on why a
    // plain credential fetch replaced the old ensure_mounted call here.
    // AccountSource::credentials() covers both a GOA account's own
    // files_uri-not-ready-yet case and a direct account's own
    // credential-fetch failure with the same single error path -- this
    // function no longer needs to distinguish why it failed, since both
    // cases are already "nothing to sync yet" as far as this caller's
    // own retry logic is concerned.
    let (base_url, username, password) = account.credentials(watcher).await
        .ok_or_else(|| {
            if is_debug_enabled() {
                eprintln!("[{}] [debug] build_sync_target_for_connection {}: account {} credentials() returned None", now_hms(), connection.name, account.account_id());
            }
            "Account's remote location isn't ready yet -- try again in a moment".to_string()
        })?;
    let remote_base_url = if connection.remote_subpath.is_empty() {
        base_url
    } else {
        webdav::build_url(&base_url, &connection.remote_subpath)
    };
    // Same reasoning as remote_base_url just above -- see
    // SyncTarget::chunking_base_url's own doc comment for why this is
    // built from the bare server root, not remote_base_url itself, and
    // why it's gated behind this account's own confirmed
    // Nextcloud/ownCloud status rather than just server_url being
    // present.
    let etag_capable = state.etag_capable.borrow().get(account.account_id()).copied().unwrap_or(false);
    let chunking_base_url = if etag_capable {
        account.server_url()
            .map(|server_url| format!("{}/remote.php/dav/uploads/{}", server_url.trim_end_matches('/'), username))
    } else {
        None
    };

    Ok(sync::SyncTarget {
        connection_id: connection.id.clone(),
        connection_name: connection.name.clone(),
        local_root: connection.local_path.clone(),
        direction: connection.direction,
        remote_base_url: RefCell::new(Some(remote_base_url)),
        chunking_base_url: RefCell::new(chunking_base_url),
        username: RefCell::new(Some(username)),
        password: RefCell::new(Some(password)),
        settings_version: state.settings_version.clone(),
        cached_settings: RefCell::new((u64::MAX, sync::CachedSettings::default())),
        renamed_away: Rc::new(RefCell::new(HashSet::new())),
        // Same as cleanup_temp_files below -- this path's own callers
        // never invoke sync_directory, so these are effectively moot
        // here too, set only for consistency with the real round's own
        // construction above.
        discovered_count: state.discovered_count.clone(),
        processed_count: state.processed_count.clone(),
        // This path's own callers (single-item retry, conflict
        // resolution) never invoke sync_directory at all, so this value
        // is effectively moot here -- set for consistency with the real
        // round's own construction above rather than leaving a field
        // that looks meaningfully different for no real reason.
        cleanup_temp_files: state.first_round_this_session.get(),
        // The app-wide one, shared -- same reasoning as the main round's
        // own construction (see AppState's own doc comment on
        // content_semaphore). A manual retry or conflict resolution is
        // still real I/O against the same mount, and should draw from
        // the same, real, shared pool as everything else currently
        // in flight, not a separate, disconnected budget of its own that
        // doesn't coordinate with anything -- which is what a dedicated,
        // isolated semaphore here used to mean before content_semaphore
        // became app-wide.
        content_semaphore: state.content_semaphore.clone(),
        discovery_pool: state.discovery_pool.clone(),
        confirm_pool: state.confirm_pool.clone(),
        // This path's own callers never invoke sync_directory (see
        // cleanup_temp_files' own comment just above for the same
        // reasoning) -- dirty_scope only ever matters to sync_directory's
        // recursion decision, so None here is simply moot, not a
        // meaningful choice.
        dirty_scope: None,
        cleanup_retry_cooldown: state.cleanup_retry_cooldown.clone(),
        stability_streak: state.stability_streak.clone(),
        remote_stability_streak: state.remote_stability_streak.clone(),
        deletion_pending: state.deletion_pending.clone(),
        webdav_failure_streak: state.webdav_failure_streak.clone(),
        move_locks: state.move_locks.clone(),
        // Genuinely shared here too, not moot the way dirty_scope is --
        // this path's callers (retry_single_item, in particular) do call
        // sync_child_file directly, which is exactly where this gets
        // set. A manual retry landing on a just-modified file should
        // still get the same short follow-up recheck as an ordinary
        // round's own deferral would.
        had_freshness_deferral: state.had_freshness_deferral_this_round.clone(),
        dirty_dirs: state.dirty_dirs.clone(),
    })
}

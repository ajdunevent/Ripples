// src/config.rs
//
// Filename-*pattern* exclusions are the global, cross-cutting complement
// to exclude.rs's per-directory .no-sync files -- one flat list, checked
// everywhere, the same way a global gitignore complements a per-
// directory .gitignore.
//
// Built-in defaults (DEFAULT_EXCLUDE_PATTERNS) live in this binary, not
// in any file at all -- same for every install, nothing to store.
// User-added patterns live in their own, separate file
// (custom-exclude-patterns.json, see CustomExcludePatterns below),
// deliberately NOT inside config.json/AppConfig -- confirmed as a real,
// wanted distinction: config.json is full of machine-specific state
// (account IDs, connection paths, this machine's own remote-poll
// interval) that wouldn't make sense to carry to another machine, while
// a set of filename patterns to always skip is exactly the kind of
// thing someone would want to share/sync across their own several
// machines independently of all that. effective_exclude_patterns is
// what actually combines the two at read time; see exclude.rs's own doc
// comment for the (deliberately small) glob syntax actually supported.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

/// Mirrors Nextcloud Desktop's own default sync-exclude.lst, minus their
/// `]`-prefixed auto-delete entries -- silently deleting a user's data
/// because it matched a default pattern is exactly the kind of surprise
/// this app avoids elsewhere (see move_deleted_to_trash's default). Every
/// entry here is skip-only. Directory-only entries end in `/` (see
/// exclude::matches_pattern); everything else matches files and
/// directories both.
/// (pattern, auto_removable) pairs. auto_removable defaults to false for anything
/// that could plausibly be real content someone cares about (lock
/// files, in-progress downloads, node_modules/.git) -- true only for
/// pure OS/filesystem litter that's never meaningful on its own
/// (Finder/Explorer metadata, trash folders, thumbnail caches): safe to
/// clean up if it's the only thing left standing between a folder and
/// its own removal actually propagating.
pub const DEFAULT_EXCLUDE_PATTERNS: &[(&str, bool)] = &[
    ("*.~lock*#", false),
    ("*.temp", false),
    ("*.swp", false),
    ("*.part", false),
    ("*.crdownload", false),
    ("~$*", false),
    (".~*", false),
    ("*.~*", true),
    ("Icon\r*", true),
    ("Thumbs.db", true),
    ("photothumb.db", true),
    ("desktop.ini", false),
    (".DS_Store", true),
    (".ds_store", true),
    (".TemporaryItems", true),
    (".Trashes", true),
    (".DocumentRevisions-V100", true),
    (".Trash-*", true),
    ("node_modules/", false),
    (".git/", false),
    ("*~", false),
    (".~lock.*", false),
    ("~*.tmp", false),
    ("*.textClipping", false),
    ("._*", false),
    ("System Volume Information", false),
    (".*.sw?", false),
    (".*.*sw?", false),
    (".fseventd", false),
    (".apdisk", false),
    (".Spotlight-V100", false),
    (".directory", false),
    ("*.filepart", false),
    ("*.kate-swp", false),
    ("*.gnucash.tmp-*", false),
    (".synkron.*", false),
    (".sync.ffs_db", false),
    (".symform", false),
    (".symform-store", false),
    (".fuse_hidden*", false),
    ("*.unison", false),
    (".nfs*", false),
    (".stfolder", false),
    (".stignore", false),
    (".stversions", false),
    ("My Saved Places.", false),
    ("*.sb-*", false),
];

/// This is entirely separate from the upload-chunking threshold
/// (CHUNKED_UPLOAD_THRESHOLD_BYTES in sync.rs, 50 MiB) -- that one
/// decides whether a given upload goes through Nextcloud's own chunking
/// API at all; this one decides whether a file syncs in the first place,
/// full stop. A file over this limit is skipped entirely, not synced via
/// some slower fallback path. Downloads have no chunked/resumable
/// counterpart yet (uploads do -- see sync.rs's own top-of-file comment),
/// so a very large download that's interrupted restarts from scratch,
/// and both directions risk hitting server-side request timeouts the
/// larger a single file gets. 500 MB is a starting point, not a measured
/// number -- raise it if you need to, just with that tradeoff in mind.
pub const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 500 * 1024 * 1024;

/// How often the remote side is polled, in whole minutes. There's no
/// push notification for WebDAV changes, so this is a plain timer --
/// unlike local changes, which are caught near-instantly via a
/// filesystem watch (see watch.rs) and don't depend on this value at
/// all. 5 minutes is a reasonable default for someone on Wi-Fi/AC most
/// of the time; raise it a lot (20+) if you're mostly on mobile data and
/// battery -- the local watch means you're not giving up responsiveness
/// for your *own* edits by doing so, only for picking up *other*
/// devices' remote changes promptly.
pub const DEFAULT_REMOTE_POLL_INTERVAL_MINUTES: u32 = 5;
pub const MIN_REMOTE_POLL_INTERVAL_MINUTES: u32 = 1;
pub const MAX_REMOTE_POLL_INTERVAL_MINUTES: u32 = 240;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncDirection {
    /// Both sides can change, both sides can win -- the only direction
    /// that ever produces a real ConflictXXX action from journal::
    /// decide_action, since it's the only one where either side's own
    /// change could plausibly be the one to keep. Kept as this enum's
    /// own #[default] specifically so an existing connection loaded from
    /// a config predating this field (see SyncConnection::direction's
    /// own #[serde(default)] just below) gets exactly the behavior it
    /// already had, not a silent, retroactive behavior change no one
    /// asked for.
    #[default]
    Bidirectional,
    /// Local is the only side that can ever really change something --
    /// see journal::decide_action's own direction-aware match arms for
    /// the full, by-request mapping this enables: a remote-side change
    /// (including a remote deletion) never wins against local, and
    /// never even reaches the resolver as a conflict, since local always
    /// wins outright instead.
    UploadOnly,
    /// The mirror of UploadOnly -- remote is the only side that can ever
    /// really change something, and always wins.
    DownloadOnly,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SyncConnection {
    /// Unique per connection -- NOT the same as account_id. Multiple
    /// connections can share an account_id (multiple folders synced from
    /// one account), and each needs its own identity in the sync journal
    /// or their sync state would collide and corrupt each other's.
    pub id: String,
    /// Which GOA account (GoaAccountInfo::account_id) this connection
    /// belongs to. A connection with no matching available account is
    /// simply not synced -- not an error state, just inactive.
    pub account_id: String,
    pub name: String,
    /// User-chosen, never auto-generated. No default sync location is
    /// assumed for anyone.
    pub local_path: PathBuf,
    /// User-chosen remote folder. Empty string means the account's whole
    /// Files root -- but that's an explicit choice the user made, not a
    /// default we picked for them.
    pub remote_subpath: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Bidirectional for every connection created before this field
    /// existed at all -- see SyncDirection's own #[default] variant for
    /// why that specific choice, not one of the other two, is what
    /// #[serde(default)] falls back to here.
    #[serde(default)]
    pub direction: SyncDirection,
    /// Root-anchored patterns (leading "/", see exclude::validate_pattern),
    /// picked via the merged local+remote folder tree in the Add/Edit
    /// Folder dialog -- never typed freehand, so there's no UI path for
    /// an unanchored or malformed entry to land here.
    ///
    /// A single, unified list -- NOT split by which side a folder was
    /// unchecked on. This used to be two separate fields
    /// (excluded_local_subpaths/excluded_remote_subpaths), kept apart
    /// purely so the old two-page Add/Edit dialog could show each list
    /// back in the page it came from; the fields themselves were always
    /// functionally identical to sync.rs's matching engine (see
    /// exclude::is_excluded_anywhere -- both just got chained into one
    /// pattern list when a SyncTarget was built, since a root-relative
    /// path is a root-relative path regardless of which side picked it).
    /// The new merged-tree picker shows local and remote folders as one
    /// combined view with no concept of "which side" a checkbox belongs
    /// to at all, so that old split no longer has a UI reason to exist
    /// either -- unchecking a folder means "never sync anything at this
    /// relative path," regardless of which side it currently exists on
    /// or might exist on later.
    #[serde(default)]
    pub excluded_subpaths: Vec<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppConfig {
    /// Deliberately single-account: only one account (GOA or direct) can
    /// be active for syncing at a time. Folder configuration for other
    /// accounts is kept (not deleted) if you switch away and back, but
    /// only this account's folders sync or show their detail in the UI.
    /// This ID alone doesn't say which kind of account it refers to --
    /// see AccountSource in main.rs, which checks both watcher.accounts()
    /// (GOA) and direct_accounts below (direct) to resolve it.
    #[serde(default)]
    pub active_account_id: Option<String>,
    /// Direct (non-GOA) Nextcloud accounts this app manages entirely
    /// itself -- unlike a GOA account, there's no D-Bus service watching
    /// for these to appear/disappear, so unlike GoaWatcher's own
    /// watcher.accounts() (rebuilt live from real GOA state every time),
    /// this app is the only source of truth for which direct accounts
    /// exist at all, and has to persist that list itself or it would
    /// simply vanish on every restart -- even though the underlying app
    /// password each one refers to is still safely sitting in the
    /// keyring regardless (nextcloud_login::DirectNextcloudAccount's own
    /// fields never include a secret; see that module's own credential
    /// storage functions for where the actual password lives).
    #[serde(default)]
    pub direct_accounts: Vec<crate::nextcloud_login::DirectNextcloudAccount>,
    #[serde(default = "default_max_file_size")]
    pub max_file_size_bytes: u64,
    /// Whether a locally-propagated deletion (remote file removed,
    /// therefore removing the local copy) goes to the local Trash
    /// (recoverable) or is deleted outright. Defaults to the safer,
    /// recoverable choice.
    #[serde(default = "default_true")]
    pub move_deleted_to_trash: bool,
    /// Whether a round is skipped entirely (no mount, no network touch
    /// at all -- see trigger_sync_round) while gio::NetworkMonitor
    /// reports the current connection as metered. Defaults to true, in
    /// keeping with this app's low-impact/safety-first stance
    /// elsewhere (move_deleted_to_trash above, the conflicted-copy
    /// policy, etc): a network's metered flag can occasionally be
    /// wrong, and a false-positive pause is a minor, easily-corrected
    /// annoyance (the Status row and log both say exactly why nothing's
    /// happening -- see "Sync paused -- metered connection detected"),
    /// while the alternative -- defaulting to on and quietly burning
    /// through someone's mobile data allotment before they happen to
    /// find this toggle -- is a real cost with no equivalent easy
    /// undo.
    #[serde(default = "default_true")]
    pub pause_on_metered: bool,
    /// Pause syncing entirely while the system reports Power Save Mode
    /// active (net.hadess.PowerProfiles' own ActiveProfile ==
    /// "power-saver" on the system bus, see main.rs::power_save_mode for
    /// the actual D-Bus query and its own doc comment for why this --
    /// not a battery-percentage threshold, what this setting used to be
    /// -- is the right signal). It reacts to a deliberate system-level
    /// power posture (which a battery percentage is only ever an
    /// imperfect, delayed proxy for), covers the case where a desktop is
    /// on AC power but the user has explicitly chosen to conserve power
    /// anyway, and needs no threshold to configure at all, since the
    /// desktop's own power-saver decision is exactly the line this app
    /// wants to respect, not a second opinion on top of it. Same
    /// reasoning as pause_on_metered for defaulting to something active
    /// rather than off: a real, if occasionally premature, pause with a
    /// clear, visible reason (the Status row and log both say why) is a
    /// far smaller cost than quietly continuing background sync work
    /// while the desktop itself has already decided to conserve power.
    #[serde(default = "default_true")]
    pub pause_on_power_save_mode: bool,
    /// Caps every HTTP request this app makes (webdav.rs's own
    /// throttle(), applied uniformly to metadata calls and transfer
    /// payloads alike, not just uploads/downloads) to this many
    /// kilobytes/sec. 0 is a legitimate opt-out -- not a special case
    /// needing a separate enabled/disabled toggle, and not something
    /// requiring an absurdly large number typed in to mean "no limit."
    #[serde(default)]
    pub bandwidth_limit_kbps: u32,
    /// Whether an XDG autostart .desktop entry is (re-)written to
    /// `$XDG_CONFIG_HOME/autostart/` on every launch -- see
    /// main.rs::ensure_autostart. Off by default: an app silently adding
    /// itself to someone's login session isn't something to opt into by
    /// default, however convenient it'd be for someone who wants it.
    #[serde(default)]
    pub launch_on_startup: bool,
    /// Whether a Nextcloud/ownCloud account's own server-side "Theming"
    /// colors (Settings > Theming, admin-configured) get applied to this
    /// app's own headerbar/window/accent colors -- see
    /// main.rs::apply_server_theme. On by default, unlike
    /// launch_on_startup just above -- this doesn't add any background
    /// behavior the way autostart does, it's a purely visual choice that
    /// takes effect only while the app is already open and only reads
    /// data the app already fetches for other reasons (the same
    /// capabilities response notify_push detection uses). Hidden
    /// entirely in Settings (not just disabled) when the active account
    /// isn't confirmed Nextcloud/ownCloud-capable -- a generic WebDAV
    /// server has no theming endpoint to read from at all, so offering
    /// a toggle that could never do anything would be misleading.
    #[serde(default = "default_true")]
    pub apply_server_theming: bool,
    /// The most recently fetched server theme for each account that's
    /// ever had one confirmed, keyed by account_id -- persisted so a
    /// fresh launch can apply the *currently active* account's own
    /// theme immediately (see main.rs's own startup sequence: config is
    /// fully loaded, active_account_id included, before the window is
    /// ever constructed) rather than the headerbar visibly flashing
    /// default colors for the brief window before a live capabilities
    /// check can complete. Refreshed opportunistically alongside the
    /// existing capabilities check (on account selection, and once per
    /// app session) -- there's no push notification for a theming
    /// change (confirmed directly against notify_push's own documented
    /// message types: notify_file/notify_file_id/notify_activity/
    /// notify_notification, nothing about capabilities or theming), so
    /// any one entry can only ever be as fresh as the last time that
    /// happened to run for that account, not the live, current value.
    ///
    /// Per-account, not a single, shared Option<ServerTheme> the way
    /// this used to be -- confirmed as a real, reported bug without
    /// this: a single, account-agnostic field meant switching from an
    /// account with server theming to one without (or to one whose own
    /// theme simply hadn't been checked yet this session) left the
    /// *previous* account's own colors applied indefinitely, since
    /// nothing else ever cleared it and a server without theming would
    /// never overwrite it either. Keying by account_id closes this at
    /// the source: switching accounts is just reading a different map
    /// entry, correct by construction, with nothing to remember to
    /// clear anywhere a switch happens to occur.
    #[serde(default)]
    pub cached_server_themes: HashMap<String, ServerTheme>,
    /// Whether the ksni-based system tray icon spawns at all -- see
    /// tray.rs. On by default -- a deliberate exception to this app's
    /// usual "nothing opts a user into background behavior silently"
    /// stance (see launch_on_startup, which stays off by default): a
    /// tray icon is common enough across desktop sync tools generally
    /// that most people expect one, even though it's not the strictly
    /// GNOME-native default this project otherwise targets. Read at
    /// startup for the initial state, but the Settings toggle also
    /// spawns/tears down the tray LIVE
    /// (main.rs::spawn_tray_if_enabled/shutdown_tray) -- unlike
    /// launch_on_startup, which genuinely can't take effect until a
    /// future login, this setting describes THIS session's current
    /// behavior, so requiring a restart to see it wasn't defensible and
    /// was confirmed as a real bug once someone actually hit it in
    /// practice.
    #[serde(default = "default_true")]
    pub tray_enabled: bool,
    /// Which of the two pre-rendered tray icon sets to use -- see
    /// tray.rs's own doc comment for why there are two at all (no
    /// reliable way to query the actual panel background color, so this
    /// is a manual choice rather than automatic detection). Defaults to
    /// light icons (false): correct for the overwhelming majority of
    /// real setups (GNOME and Phosh both ship with a dark panel by
    /// default), so someone who's never touched this setting gets the
    /// right icon without doing anything -- only someone who's
    /// deliberately installed a shell theme that lightens the panel ever
    /// needs to flip this.
    #[serde(default)]
    pub tray_icon_dark: bool,
    /// Minutes, not seconds -- nobody configuring this cares about
    /// second-level precision, and minutes keep the settings UI's spin
    /// button from needing three extra digits for anything above a
    /// couple minutes.
    #[serde(default = "default_remote_poll_interval_minutes")]
    pub remote_poll_interval_minutes: u32,
    #[serde(default)]
    pub connections: Vec<SyncConnection>,
}

/// One global filename-pattern exclusion, plus whether it's allowed to
/// actually delete a matching file/directory when doing so is the only
/// way to let a folder-removal propagate correctly (see sync.rs's
/// folder-cleanup logic) -- Nextcloud Desktop's own `]`-prefix idea,
/// adopted properly this time (see this file's own top-of-file comment
/// for why an earlier pass here rejected it, and why that rejection
/// doesn't actually apply to this version: the earlier concern was
/// specifically about DEFAULT patterns silently enabling deletion no
/// one asked for. This version's defaults DO include some `auto_removable:
/// true` entries (see DEFAULT_EXCLUDE_PATTERNS above) -- but that's a
/// deliberate, documented choice about pure OS/filesystem litter that's
/// never meaningful content on its own, not a silent surprise the way
/// an opaque flat-file convention would be. `#[serde(default)]` on the
/// field itself still means `false` for anything not otherwise
/// specified -- a user adding their own pattern via the Add Pattern
/// dialog starts unchecked, and stays that way unless they explicitly
/// opt in).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcludePattern {
    pub pattern: String,
    #[serde(default)]
    pub auto_removable: bool,
}

/// The four colors this app actually applies from a Nextcloud/ownCloud
/// account's server-side Theming settings -- see
/// main.rs::apply_server_theme for how each one gets used. All plain
/// hex strings (e.g. "#343966"), matching exactly the format the
/// capabilities API itself returns them in, so no parsing is needed
/// between fetching and caching.
///
/// No body/sidebar-style color is fetched or cached here at all -- there's
/// no separate field for it anywhere in the capabilities response
/// (confirmed directly against a real server's own raw response, not
/// assumed), only `background`/`background-text` (the headerbar-
/// equivalent pair) and `color`/`color-text` (the accent pair). The
/// body/sidebar color is computed from `background` at apply time
/// instead (see apply_server_theme in main.rs), not cached here --
/// confirmed directly against Nextcloud's own real theming source
/// (DefaultTheme.php/DarkTheme.php) that the actual formula depends on
/// which appearance (light/dark) is currently active: light mode mixes
/// 80% toward white, dark mode mixes 85% toward #171717, both exact
/// matches (not approximations) against real measured server output.
/// Since that can change live while the app is running, it can't be a
/// single cached value the way background/primary can.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerTheme {
    pub background: String,
    pub background_text: String,
    pub primary: String,
    pub primary_text: String,
}

fn default_max_file_size() -> u64 {
    DEFAULT_MAX_FILE_SIZE_BYTES
}

fn default_remote_poll_interval_minutes() -> u32 {
    DEFAULT_REMOTE_POLL_INTERVAL_MINUTES
}

pub fn default_exclude_patterns() -> Vec<ExcludePattern> {
    DEFAULT_EXCLUDE_PATTERNS.iter().map(|(s, auto_removable)| ExcludePattern { pattern: s.to_string(), auto_removable: *auto_removable }).collect()
}

/// The separate, shareable file's own contents -- user-added patterns,
/// plus which built-in defaults (by pattern string) the user has
/// explicitly suppressed. The default's own string/auto_removable pair
/// stays hardcoded in DEFAULT_EXCLUDE_PATTERNS regardless -- what's
/// tracked here is only the *decision* to exclude it from the effective
/// list, the same way a user's own addition is a decision to include
/// one. Removing a default doesn't erase it from the binary, it just
/// means effective_exclude_patterns skips it when assembling the
/// combined list.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CustomExcludePatterns {
    #[serde(default)]
    pub added: Vec<ExcludePattern>,
    #[serde(default)]
    pub removed_defaults: Vec<String>,
}

/// Path for the new, separate custom-patterns file -- same directory as
/// config.json (glib::user_config_dir()/ripples/), just its own file
/// within it, so the two remain independently readable/shareable
/// without needing to touch each other's own format at all.
fn custom_exclude_patterns_path() -> PathBuf {
    let mut path = glib::user_config_dir();
    path.push("ripples");
    path.push("custom-exclude-patterns.json");
    path
}

/// Missing/unreadable falls back to CustomExcludePatterns::default()
/// (both lists empty) -- a genuinely ordinary state, same reasoning as
/// AppConfig::load's own identical handling of a missing config.json.
///
/// A file that exists but fails to parse is preserved (renamed aside)
/// rather than silently discarded, same fix, same reasoning, as
/// AppConfig::load -- see that function's own doc comment for the full,
/// confirmed risk this closes. Smaller blast radius here specifically
/// (losing pattern customizations, not sync connections themselves),
/// but the underlying risk -- an interrupted, non-atomic write leaving
/// a corrupted file that the very next save silently overwrites for
/// good -- is identical.
pub fn load_custom_exclude_patterns() -> CustomExcludePatterns {
    let path = custom_exclude_patterns_path();
    let Ok(contents) = std::fs::read_to_string(&path) else {
        return CustomExcludePatterns::default();
    };
    match serde_json::from_str(&contents) {
        Ok(custom) => custom,
        Err(e) => {
            let mut backup_path = path.as_os_str().to_owned();
            backup_path.push(".corrupted");
            let backup_path = PathBuf::from(backup_path);
            match std::fs::rename(&path, &backup_path) {
                Ok(()) => eprintln!("custom-exclude-patterns.json failed to parse ({e}) -- preserved as {} and starting fresh", backup_path.display()),
                Err(rename_err) => eprintln!("custom-exclude-patterns.json failed to parse ({e}), and couldn't be preserved either ({rename_err}) -- starting fresh"),
            }
            CustomExcludePatterns::default()
        }
    }
}

pub fn save_custom_exclude_patterns(custom: &CustomExcludePatterns) -> std::io::Result<()> {
    let path = custom_exclude_patterns_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(custom)
        .expect("CustomExcludePatterns serialization cannot fail -- no non-serializable fields");
    write_atomically(&path, &json)
}

/// What sync.rs/exclude.rs/main.rs's own pattern-matching code actually
/// reads -- built-in defaults (minus any the user has explicitly
/// removed) plus whatever the user has added, combined fresh each time
/// rather than cached, so a change to either source is picked up
/// immediately without needing its own separate invalidation step.
pub fn effective_exclude_patterns() -> Vec<ExcludePattern> {
    let custom = load_custom_exclude_patterns();
    let mut patterns: Vec<ExcludePattern> = default_exclude_patterns()
        .into_iter()
        .filter(|d| !custom.removed_defaults.iter().any(|r| r == &d.pattern))
        .collect();
    patterns.extend(custom.added);
    patterns
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            active_account_id: None,
            direct_accounts: Vec::new(),
            max_file_size_bytes: DEFAULT_MAX_FILE_SIZE_BYTES,
            move_deleted_to_trash: true,
            pause_on_metered: true,
            pause_on_power_save_mode: true,
            bandwidth_limit_kbps: 0,
            launch_on_startup: false,
            apply_server_theming: true,
            cached_server_themes: HashMap::new(),
            tray_enabled: true,
            tray_icon_dark: false,
            remote_poll_interval_minutes: DEFAULT_REMOTE_POLL_INTERVAL_MINUTES,
            connections: Vec::new(),
        }
    }
}

impl AppConfig {
    pub fn remote_poll_interval_secs(&self) -> u32 {
        self.remote_poll_interval_minutes
            .clamp(MIN_REMOTE_POLL_INTERVAL_MINUTES, MAX_REMOTE_POLL_INTERVAL_MINUTES)
            * 60
    }

    fn config_path() -> PathBuf {
        let mut path = glib::user_config_dir();
        path.push("ripples");
        path.push("config.json");
        path
    }

    /// Missing/unreadable falls back to Self::default() -- a genuinely
    /// ordinary, expected state (a fresh install, or a permissions
    /// issue that isn't this function's job to diagnose), not something
    /// worth treating any differently than it already was.
    ///
    /// A file that exists but fails to PARSE is handled differently,
    /// deliberately: it's renamed aside (preserved) before falling back
    /// to defaults, rather than silently discarded the same way a
    /// missing file is. Confirmed as a real, significant risk without
    /// this, not a hypothetical: save() below used to write directly to
    /// this same path with a plain std::fs::write, which isn't atomic
    /// -- an interrupted write (a crash, a forced kill, a power loss;
    /// this app's own primary target is a mobile device via FuriOS/
    /// Phosh, where an unexpected shutdown is meaningfully more likely
    /// than on a desktop) could leave a truncated, unparseable
    /// config.json behind. Without this distinction, the very next
    /// launch would hit this same load() path, silently fall back to
    /// Self::default() with no warning at all, and the very next save()
    /// after that would overwrite the corrupted file with a fresh,
    /// empty configuration -- permanently destroying every configured
    /// sync connection with nothing to recover from and no indication
    /// anything had gone wrong. Preserving the file first means a
    /// corrupted-but-mostly-intact config.json (a single truncated byte
    /// near the end, say) stays recoverable by hand even if this app
    /// itself can't make sense of it.
    pub fn load() -> Self {
        let path = Self::config_path();
        let Ok(contents) = std::fs::read_to_string(&path) else {
            return Self::default();
        };
        match serde_json::from_str(&contents) {
            Ok(config) => config,
            Err(e) => {
                let mut backup_path = path.as_os_str().to_owned();
                backup_path.push(".corrupted");
                let backup_path = PathBuf::from(backup_path);
                match std::fs::rename(&path, &backup_path) {
                    Ok(()) => eprintln!("config.json failed to parse ({e}) -- preserved as {} and starting fresh", backup_path.display()),
                    Err(rename_err) => eprintln!("config.json failed to parse ({e}), and couldn't be preserved either ({rename_err}) -- starting fresh"),
                }
                Self::default()
            }
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)
            .expect("AppConfig serialization cannot fail -- no non-serializable fields");
        write_atomically(&path, &json)
    }
}

/// Writes `contents` to `path` atomically -- to a fresh, sibling temp
/// file first, then renamed into place over the real target, rather
/// than a plain std::fs::write directly to it. A direct write is NOT
/// atomic: if the process is interrupted partway through (a crash, a
/// forced kill, a power loss), the result can be a truncated,
/// unparseable file sitting at the real path -- see AppConfig::load's
/// own doc comment for the full, confirmed consequence this has for
/// config.json specifically. rename() within the same directory (and
/// therefore the same filesystem) is atomic on every platform this app
/// targets: the target path always shows either the complete old
/// contents or the complete new ones, never something in between,
/// regardless of what interrupts it. Shared by both AppConfig::save and
/// save_custom_exclude_patterns below -- the same risk applies to
/// either file equally, just with a smaller blast radius for the
/// latter (losing pattern customizations, not sync connections
/// themselves).
fn write_atomically(path: &Path, contents: &str) -> std::io::Result<()> {
    let mut temp_path = path.as_os_str().to_owned();
    temp_path.push(".tmp");
    let temp_path = PathBuf::from(temp_path);
    std::fs::write(&temp_path, contents)?;
    std::fs::rename(&temp_path, path)
}

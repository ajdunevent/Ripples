// src/folder_picker.rs
//
// Shared machinery for browsing a directory hierarchy, local or remote.
// BrowseLocation (below) is what makes one module of walk logic serve
// both Page 1's single-folder choosers and Page 2's merged local+remote
// tree, rather than writing it twice -- it used to be gio::File doing
// that job (a local path and a GOA-mounted remote root exposed the
// identical gio::File enumeration API), but that stopped being true the
// moment the sync engine itself dropped GVfs for a direct WebDAV client
// (see webdav.rs's own top-level doc comment for the full reasoning).
// BrowseLocation is the same idea rebuilt to span two genuinely
// different backends -- a local path via std::fs, a remote one via
// webdav::list -- rather than one daemon-mediated common type.
//
// This replaces relying on the system file-chooser portal entirely for
// folder selection -- see TODO.md's "Custom folder picker" entry for
// why: Phosh's own portal backend doesn't support selecting GVfs-mounted
// remote locations at all (confirmed via direct testing, and matches a
// long-standing, still-open upstream xdg-desktop-portal limitation, not
// a Phosh-specific bug -- see the same TODO entry for the sourcing).
// Browsing the remote location directly, in-app, sidesteps that problem
// completely rather than working around it -- and, now, does so without
// needing gvfsd-dav or a mount of any kind either.

use gtk4::prelude::*;
// A full glob, not a named list of just the traits this file happens to
// use today -- confirmed directly against libadwaita's own real
// prelude.rs that it re-exports crate::auto::traits::* (every
// auto-generated Ext trait, including ActionRowExt/ExpanderRowExt/
// AlertDialogExt/AdwDialogExt) plus an explicit AlertDialogExtManual
// (for choose_future). A named-list import here previously missed
// AlertDialogExt/AlertDialogExtManual/AdwDialogExt entirely -- a real,
// reported compile failure once this file actually used AlertDialog --
// which a glob import can't leave a gap in the same way, since it
// always covers everything the prelude itself provides, not just
// whatever this file's author remembered to name explicitly at the
// time.
use libadwaita::prelude::*;
use glib::clone;
use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::collections::HashSet;
use zeroize::Zeroizing;

use crate::webdav;

/// HH:MM:SS, local time -- exact same format as main.rs's own now_hms
/// (duplicated here rather than shared, for the same reasoning sync.rs's
/// and goa.rs's own copies already give: that one's private to main.rs
/// and this module has no other reason to depend on it).
fn now_hms() -> String {
    glib::DateTime::now_local().ok()
        .and_then(|d| d.format("%H:%M:%S").ok())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Runs a blocking closure on a fresh, one-off OS thread and returns its
/// result asynchronously -- same shape (and same reasoning) as push.rs's
/// and sync.rs's own identically-named, deliberately separate copies of
/// this: a handful of occasional, one-shot blocking calls (a directory
/// listing, here) doesn't justify a shared crate-wide utility, and each
/// module already has its own for exactly this reason.
async fn run_blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = async_channel::bounded(1);
    std::thread::spawn(move || {
        let result = f();
        let _ = tx.send_blocking(result);
    });
    rx.recv().await.expect("the spawned thread always sends exactly one result before finishing")
}

/// A browsable location, local or remote -- the direct replacement for
/// gio::File as the thing every function in this module navigates and
/// enumerates, now that the two sides are genuinely different backends
/// rather than one daemon-mediated common type (see this module's own
/// top comment). Remote carries its own credentials, fetched once when
/// a picker is first opened and cloned along into every child location
/// derived from it via `child()` below -- the same "fetch once, reuse
/// for the whole session" discipline sync.rs's own SyncTarget already
/// follows, just without anywhere central like a SyncTarget to cache
/// them in for a browsing session that's just these few functions.
#[derive(Clone)]
pub enum BrowseLocation {
    Local(PathBuf),
    Remote {
        url: String,
        username: String,
        password: Zeroizing<String>,
    },
}

impl BrowseLocation {
    /// Navigates one level down -- the direct replacement for
    /// gio::File::child(name).
    fn child(&self, name: &str) -> BrowseLocation {
        match self {
            BrowseLocation::Local(path) => BrowseLocation::Local(path.join(name)),
            BrowseLocation::Remote { url, username, password } => BrowseLocation::Remote {
                url: webdav::build_url(url, name),
                username: username.clone(),
                password: password.clone(),
            },
        }
    }

    /// Navigates to an arbitrary depth at once -- the direct replacement
    /// for gio::File::resolve_relative_path(segments.join("/")), used by
    /// the Up button and the initial-segments starting position.
    fn resolve(&self, segments: &[String]) -> BrowseLocation {
        let mut current = self.clone();
        for segment in segments {
            current = current.child(segment);
        }
        current
    }

    /// Self's own path relative to `ancestor`, as plain (not
    /// percent-encoded) segments joined by '/' -- the direct replacement
    /// for gio::File::relative_path(&self), used once self is confirmed
    /// a descendant of ancestor by the successful strip; None otherwise
    /// (not a descendant at all, or the two are different variants
    /// entirely -- comparing a local path against a remote account
    /// root, say), matching relative_path's own "None means not a
    /// descendant" contract exactly.
    pub fn relative_to(&self, ancestor: &BrowseLocation) -> Option<String> {
        match (self, ancestor) {
            (BrowseLocation::Local(path), BrowseLocation::Local(root)) => {
                path.strip_prefix(root).ok().map(|p| p.to_string_lossy().into_owned())
            }
            (BrowseLocation::Remote { url, .. }, BrowseLocation::Remote { url: root_url, .. }) => {
                let root_trimmed = root_url.trim_end_matches('/');
                url.strip_prefix(root_trimmed)
                    .map(|rest| rest.trim_start_matches('/'))
                    .map(webdav::percent_decode)
            }
            _ => None,
        }
    }

    /// Creates this location as a new, empty directory -- the direct
    /// replacement for gio::File::make_directory_future, used by the
    /// New Folder button in show_single_folder_picker. Synchronous, the
    /// same as every other BrowseLocation-adjacent I/O in this module
    /// -- callers run this through run_blocking, matching how every
    /// other blocking call in this file is already handled, rather than
    /// this doing that internally.
    fn create(&self) -> Result<(), String> {
        match self {
            BrowseLocation::Local(path) => std::fs::create_dir(path).map_err(|e| e.to_string()),
            BrowseLocation::Remote { url, username, password } => {
                webdav::mkcol(url, username, password).map_err(|e| e.to_string())
            }
        }
    }
}

/// One subdirectory found while listing a single side (local or
/// remote) -- not yet merged with the other side, not yet annotated
/// with exclusion state. `location` is already resolved (root.child(name)),
/// ready to enumerate further or to hand off as a chosen root.
pub struct SubdirEntry {
    pub name: String,
    pub location: BrowseLocation,
}

/// Local-only directory names one level under `path` -- directories
/// only (files skipped entirely, every picker in this app is folder-
/// only start to finish), symlinked directories excluded. DirEntry's
/// own file_type() doesn't traverse symlinks (confirmed via std's own
/// documented behavior, not assumed) -- a symlink pointing at a
/// directory reports as a symlink here, not a directory, matching
/// watch.rs's existing "symlinked directories aren't traversed/synced
/// at all" rule elsewhere in this app: the picker showing one as
/// selectable/navigable would be a real, misleading inconsistency.
/// Genuinely blocking (std::fs::read_dir, no async anywhere in here) --
/// always called via run_blocking, never directly from async code.
fn list_local_subdir_names(path: &Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                out.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    out
}

/// Lists just the subdirectories of `root`, one level deep. Sorted
/// case-insensitively for a stable, predictable order regardless of
/// what the backend hands back (WebDAV enumeration order isn't
/// guaranteed the way a local filesystem's directory-entry order at
/// least tends to be consistent). Never fails outright -- a listing
/// error on either backend just reads as "no subdirectories found"
/// (matching every call site's own pre-existing `.unwrap_or_default()`
/// tolerance), since a picker showing an empty folder is a far better
/// failure mode than one that silently stops navigating at all.
pub async fn list_subdirectories(root: &BrowseLocation) -> Vec<SubdirEntry> {
    let names: Vec<String> = match root {
        BrowseLocation::Local(path) => {
            let path = path.clone();
            run_blocking(move || list_local_subdir_names(&path)).await
        }
        BrowseLocation::Remote { url, username, password } => {
            let (url, username, password) = (url.clone(), username.clone(), password.to_string());
            match run_blocking(move || webdav::list(&url, &username, &password)).await {
                Ok(entries) => entries.into_iter().filter(|(_, e)| e.is_dir).map(|(name, _)| name).collect(),
                Err(_) => Vec::new(),
            }
        }
    };
    let mut out: Vec<SubdirEntry> = names.into_iter().map(|name| {
        let location = root.child(&name);
        SubdirEntry { name, location }
    }).collect();
    out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    out
}

/// Like list_subdirectories, but for the remote side specifically, and
/// additionally reports whether a .no-sync file exists in this same
/// directory -- see list_merged_children's own use for why. A plain
/// webdav::list already returns every entry in the directory, files
/// included; list_subdirectories itself just happens to discard
/// everything but directories before ever returning, which is exactly
/// what makes checking for .no-sync's own presence there impossible
/// without paying for a second, separate listing call just to look for
/// one specific filename. This does the identical listing once and
/// keeps both pieces of information from it, rather than duplicating
/// the network round-trip.
async fn list_remote_subdirectories_and_no_sync(root: &BrowseLocation) -> (Vec<SubdirEntry>, bool) {
    let BrowseLocation::Remote { url, username, password } = root else {
        return (Vec::new(), false);
    };
    let (url, username, password) = (url.clone(), username.clone(), password.to_string());
    let entries = match run_blocking(move || webdav::list(&url, &username, &password)).await {
        Ok(entries) => entries,
        Err(_) => return (Vec::new(), false),
    };
    let has_no_sync = entries.contains_key(crate::exclude::NO_SYNC_FILENAME);
    let mut out: Vec<SubdirEntry> = entries.into_iter()
        .filter(|(_, e)| e.is_dir)
        .map(|(name, _)| {
            let location = root.child(&name);
            SubdirEntry { name, location }
        })
        .collect();
    out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    (out, has_no_sync)
}

/// One row in the merged local+remote tree (Page 2). Unlike
/// SubdirEntry above, this already reflects both sides (a folder that
/// exists on only one side is still one row, not two) and already
/// carries its own exclusion state, computed once here rather than
/// re-derived by every caller that wants to render a row. Clone is
/// needed here -- build_tree_node's own closures (the expand handler,
/// the checkbox handler) each need their own owned copy via clone!'s
/// #[strong], and every field is itself cheap to clone (GObjects and
/// plain owned data, no deep structures).
#[derive(Clone)]
pub struct MergedEntry {
    pub name: String,
    pub relative_path: String, // root-anchored, e.g. "/Photos/2024" -- same shape as exclude::validate_pattern expects
    pub local_location: Option<BrowseLocation>,
    pub remote_location: Option<BrowseLocation>,
    /// Some(reason) means this folder is excluded by something OTHER
    /// than this connection's own checkbox state -- a global pattern or
    /// a local .no-sync entry. Rows in this state are shown disabled,
    /// forced unchecked, and not expandable (its own contents are never
    /// enumerated at all, matching watch.rs's existing "an excluded
    /// directory saves both the enumeration work and, there, an inotify
    /// watch slot -- here, a WebDAV round-trip and a wasted tree node").
    /// None means this row's checked state is purely up to
    /// excluded_subpaths, editable normally.
    pub global_exclusion: Option<crate::exclude::ExclusionReason>,
}

/// Lists the union of subdirectories at `relative_path` under both
/// `local_root` and `remote_root` -- either may be None (this
/// connection's root on that side hasn't been chosen yet, or, for a
/// mid-tree call, that side simply has nothing at this path). A missing
/// side is treated as "empty," not an error: half a sync tree not
/// existing yet on one side is the normal, expected state of things
/// this app already treats as ordinary everywhere else, not something
/// this listing should fail over.
///
/// Reads .no-sync from BOTH sides, unioned together -- matching
/// exclude.rs's own top-level doc comment and sync.rs's own identical
/// handling exactly (see sync_directory's own .no-sync read). Confirmed
/// as a real, reported gap without this: a remote-only .no-sync entry
/// -- exactly the case exclude.rs's own doc comment calls out as
/// mattering most, since it applies from round one without needing the
/// tree built locally first -- would show as a normal, selectable
/// folder in this exact picker, someone would set up a new connection
/// using it, and the folder would then turn out already-excluded the
/// moment syncing actually started, with nothing in this dialog ever
/// having shown that coming.
pub async fn list_merged_children(
    local_root: Option<&BrowseLocation>,
    remote_root: Option<&BrowseLocation>,
    relative_path: &str,
    global_patterns: &[crate::config::ExcludePattern],
) -> Vec<MergedEntry> {
    let local_children = match local_root {
        Some(root) => list_subdirectories(root).await,
        None => Vec::new(),
    };
    let (remote_children, remote_has_no_sync) = match remote_root {
        Some(root) => list_remote_subdirectories_and_no_sync(root).await,
        None => (Vec::new(), false),
    };

    let mut excluded_local: HashSet<String> = local_root
        .and_then(|root| match root {
            BrowseLocation::Local(path) => Some(crate::exclude::load(path)),
            BrowseLocation::Remote { .. } => None,
        })
        .unwrap_or_default();
    // Only actually fetched when the listing just above already showed
    // one exists here -- a directory without one (the common case) never
    // pays for a speculative fetch, the same "check first, fetch only if
    // actually present" discipline sync.rs's own identical read follows.
    // A read failure (network hiccup, permissions) just means the
    // remote list doesn't apply this round, same forgiving default
    // exclude::load already uses for a missing/unreadable local file.
    if remote_has_no_sync {
        if let Some(BrowseLocation::Remote { url, username, password }) = remote_root {
            let no_sync_url = webdav::build_url(url, crate::exclude::NO_SYNC_FILENAME);
            let (no_sync_url, username, password) = (no_sync_url, username.clone(), password.to_string());
            if let Ok(bytes) = run_blocking(move || webdav::get_to_memory(&no_sync_url, &username, &password)).await {
                excluded_local.extend(crate::exclude::parse(&String::from_utf8_lossy(&bytes)));
            }
        }
    }

    // Union by name -- a name present on both sides is one row, not two.
    // Plain HashMap, not BTreeMap: the final sort_by below is what
    // actually determines display order (case-insensitive), so there's
    // no reason to pay for an ordered map whose own ordering never gets
    // used.
    let mut by_name: std::collections::HashMap<String, (Option<BrowseLocation>, Option<BrowseLocation>)> = std::collections::HashMap::new();
    for entry in local_children {
        by_name.entry(entry.name).or_default().0 = Some(entry.location);
    }
    for entry in remote_children {
        by_name.entry(entry.name).or_default().1 = Some(entry.location);
    }

    let mut out: Vec<MergedEntry> = by_name
        .into_iter()
        .map(|(name, (local_location, remote_location))| {
            let child_relative_path = format!("{}/{}", relative_path.trim_end_matches('/'), name);
            // Empty connection_patterns here on purpose -- this checks
            // ONLY the global mechanisms (a .no-sync entry, a
            // Settings-level pattern), never this connection's own
            // excluded_subpaths. That list IS what the checkbox itself
            // represents; running it back through exclusion_reason here
            // would be circular, checking a row's own in-progress edit
            // state against itself instead of against something outside
            // the picker's control.
            let global_exclusion = crate::exclude::exclusion_reason(
                &excluded_local, global_patterns, &[], &child_relative_path, &name, true,
            );
            MergedEntry { name, relative_path: child_relative_path, local_location, remote_location, global_exclusion }
        })
        .collect();

    out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));

    out
}

/// Shared, cloneable bundle of widgets + navigation state for one
/// open picker popover. Cloning this is cheap -- every GTK widget field
/// is a GObject (reference-counted already), and the two Rc<RefCell<>>
/// fields are themselves cheap to clone by design -- so passing this
/// around by value to closures and to refresh_folder_browser's own
/// recursive calls is the natural way to share it, not a needless
/// allocation.
#[derive(Clone)]
struct FolderBrowserWidgets {
    listbox: gtk4::ListBox,
    path_label: gtk4::Label,
    up_button: gtk4::Button,
    select_button: gtk4::Button,
    spinner: gtk4::Spinner,
    /// Navigation state: `current_location` is always root joined with
    /// `current_segments` -- kept as two separate fields rather than
    /// re-deriving current_location from segments on every use, since it's
    /// the actual location a click handler needs to act on immediately,
    /// while current_segments exists purely for the breadcrumb label and
    /// the "pop one level" logic behind the Up button.
    current_location: Rc<RefCell<BrowseLocation>>,
    current_segments: Rc<RefCell<Vec<String>>>,
    root: BrowseLocation,
    root_label: String,
    /// Bumped by refresh_folder_browser every time it's called, before
    /// spawning its own listing task -- that task's own, later append
    /// only actually runs if this still matches the generation it
    /// captured at spawn time. Confirmed as a real, reported bug
    /// without this: refresh_folder_browser clears the listbox
    /// synchronously up front, but the actual listing itself is async,
    /// with nothing stopping an in-flight, now-stale task from a
    /// PREVIOUS call from still appending its own (wrong) results after
    /// a LATER navigation's own call already cleared and repopulated
    /// the same listbox with what's actually current -- most likely to
    /// actually surface on the remote side, where a directory that
    /// happens to be slow to list (a large folder, a slow connection)
    /// can easily finish after a quicker navigation to a different,
    /// smaller directory the user backed into instead.
    generation: Rc<Cell<u64>>,
}

/// Local and remote genuinely need different treatment here, not the
/// same breadcrumb style for both -- confirmed as a real, reported
/// problem with the single, shared "root_label / seg / seg" format this
/// used to always produce: for a local path, that meant literally
/// "Filesystem / home / aj / Documents" on screen, a constructed
/// breadcrumb standing in for what's actually just a real, ordinary
/// absolute path the whole time. The local side has that real path
/// sitting right there in current_location, so showing it directly (via
/// main.rs's own display_path_with_tilde -- a plain "~/Documents" when
/// under the home directory, matching the exact same convention already
/// used for a connection's own subtitle elsewhere in this app, or the
/// full absolute path otherwise) is both simpler and far more legible
/// than reconstructing an approximation of it from root_label and
/// segments. The remote side keeps the breadcrumb style -- there's no
/// absolute path to fall back to there (root_label is the account's own
/// display name, not a filesystem root), so a constructed "MyAccount /
/// Documents / Work" trail is the right, and only sensible, way to show
/// where browsing currently is.
fn display_path(current_location: &BrowseLocation, root_label: &str, segments: &[String]) -> String {
    if let BrowseLocation::Local(path) = current_location {
        return crate::display_path_with_tilde(path);
    }
    // Plain "/" throughout, no padding spaces around it -- confirmed as
    // a real, deliberate request: the remote side reads as a genuine
    // path even though it's constructed rather than a real filesystem
    // one, so it should look like one (the same reasoning display_path_
    // with_tilde's own doc comment gives for the local side's trailing
    // slash), not like a breadcrumb trail with its own separate visual
    // convention.
    if segments.is_empty() {
        format!("{root_label}/")
    } else {
        format!("{root_label}/{}/", segments.join("/"))
    }
}

/// Clears and repopulates the listbox for whatever `widgets` currently
/// points at, and updates the breadcrumb/Up-button/Select-button state
/// to match. Takes `widgets` by value (see the struct's own doc comment
/// on why that's cheap) specifically so this can call itself again --
/// each row's own click handler recurses back into this same function
/// with current_location/current_segments already updated to that child,
/// rather than this needing some separate "navigate" function that
/// duplicates the refresh logic.
fn refresh_folder_browser(widgets: FolderBrowserWidgets) {
    while let Some(row) = widgets.listbox.first_child() {
        widgets.listbox.remove(&row);
    }

    let segments = widgets.current_segments.borrow().clone();
    widgets.path_label.set_label(&display_path(&widgets.current_location.borrow(), &widgets.root_label, &segments));
    widgets.up_button.set_sensitive(!segments.is_empty());
    let current_name = segments.last().cloned().unwrap_or_else(|| widgets.root_label.clone());
    widgets.select_button.set_label(&format!("Select \u{201c}{current_name}\u{201d}"));

    widgets.spinner.set_visible(true);
    widgets.spinner.start();

    // Captured before the listing itself starts -- see generation's own
    // doc comment on FolderBrowserWidgets for the real, confirmed race
    // this closes. Bumped here, synchronously, so it's already
    // incremented by the time ANY later call to this same function
    // (from a click landing while this one's own listing is still in
    // flight) could possibly run.
    widgets.generation.set(widgets.generation.get() + 1);
    let my_generation = widgets.generation.get();

    let current_location = widgets.current_location.borrow().clone();
    glib::MainContext::default().spawn_local(clone!(
        #[strong] widgets,
        async move {
            let children = list_subdirectories(&current_location).await;
            // Discarded, not applied, if a later call already bumped
            // this past what was captured above -- this task's own
            // results describe a directory that isn't necessarily even
            // the one currently showing anymore, and whichever later
            // call is actually current already has (or will have) its
            // own, correct results in flight.
            if widgets.generation.get() != my_generation {
                return;
            }
            widgets.spinner.stop();
            widgets.spinner.set_visible(false);

            if children.is_empty() {
                let empty_row = libadwaita::ActionRow::builder()
                    .title("No subfolders here")
                    .sensitive(false)
                    .build();
                widgets.listbox.append(&empty_row);
                return;
            }

            for entry in children {
                let child_name = entry.name.clone();
                let child_location = entry.location.clone();
                let row = libadwaita::ActionRow::builder()
                    .title(&entry.name)
                    // Folder names are plain filesystem text, never
                    // intended as markup -- an unescaped "&" (or "<"/">")
                    // in a real folder name (e.g. an author name like
                    // "Grahame-Smith, Seth & Jane Austen") otherwise
                    // breaks Pango's markup parser entirely, since
                    // ActionRow's own title/subtitle interpret markup by
                    // default. Confirmed as a real, reported case, not a
                    // hypothetical one.
                    .use_markup(false)
                    .activatable(true)
                    .build();
                // icon_name() on the builder is deprecated since
                // libadwaita 1.3 (confirmed via the real cargo check
                // output, not assumed) -- add_prefix with an explicit
                // Image is the replacement, same pattern already used
                // for the suffix arrow below.
                row.add_prefix(&gtk4::Image::from_icon_name("folder-symbolic"));
                row.add_suffix(&gtk4::Image::from_icon_name("go-next-symbolic"));
                row.connect_activated(clone!(
                    #[strong] widgets,
                    #[strong] child_name,
                    #[strong] child_location,
                    move |_| {
                        widgets.current_segments.borrow_mut().push(child_name.clone());
                        *widgets.current_location.borrow_mut() = child_location.clone();
                        refresh_folder_browser(widgets.clone());
                    }
                ));
                widgets.listbox.append(&row);
            }
        }
    ));
}

/// This is Page 1's local/remote root chooser -- see this module's own
/// top-of-file comment for why it exists instead of the system file-
/// chooser portal (gtk4::FileDialog): Phosh's own portal backend can't
/// select GVfs-mounted remote locations at all, and this sidesteps that
/// by never going through the portal in the first place, for either
/// side -- not just the remote one that's actually broken, since one
/// consistent interface for both was an explicit design goal, not just
/// a side effect of fixing the remote case.
///
/// Shows a window, transient for `parent_window`, letting the user
/// browse anywhere reachable from `root` and select any folder along the
/// way (including root itself, via Select with no navigation at all).
/// `root_label` is what's shown for the top level -- e.g. "Filesystem"
/// for the local side, or the account's own display name for the
/// remote side.
///
/// `root` is the true navigable boundary -- Up never goes above it, so
/// it needs to be wherever browsing should be allowed to reach, not just
/// a convenient starting point. `initial_segments` is that separate,
/// convenient starting point: where the browser actually opens, relative
/// to root. Confirmed as a real, reported bug: the local side used to
/// pass $HOME as root itself, which meant Up was disabled the moment the
/// picker opened and any directory outside the user's own home (an
/// external drive under /mnt, say) was completely unreachable. Root is
/// now the filesystem root for that side, with initial_segments set to
/// $HOME's own path components -- opens in the same familiar place, but
/// Up can now walk all the way back to /. The remote side passes an
/// empty Vec here, unchanged: the account's own mount root already is
/// the correct upper bound for that side, nothing above it to reach.
///
/// A full Window, not a Popover -- an earlier version used a Popover
/// (genuinely simpler for a quick, anchored picker), but a Popover has
/// to fit within the screen space around its own anchor widget, and
/// that constraint is exactly what caused a real bug: a long, deep path
/// could grow the popover's own content past what actually fits on a
/// small phone screen, and it would fail to display at all rather than
/// just look tall. A Window has no such constraint -- it's its own
/// independent surface, sized however makes sense regardless of where
/// it was triggered from -- and matches the main Add/Edit Folder
/// dialog's own established pattern (HeaderBar with no buttons in it,
/// Cancel + the primary action at the bottom) rather than mixing two
/// different container conventions in the same overall dialog flow.
/// Needs an explicit Cancel button now, unlike the popover -- there's
/// no "tap outside to dismiss" equivalent for a modal window.
/// Returns the picker's own window handle -- callers that need to react
/// to the picker closing (not just a folder being selected via
/// on_selected) can hook connect_close_request on it themselves, same
/// signal every other libadwaita::Window-typed dialog in this app
/// already uses (this is a plain Window, not an AdwDialog -- that's the
/// one with its own, different connect_closed instead). Confirmed as a
/// real, needed case, not speculative: on_selected alone never fires at
/// all if the person opens this picker and cancels out without choosing
/// anything, which any caller wanting to re-enable something it
/// disabled before opening this picker (a "Choose Remote Folder..."
/// button, for instance) genuinely needs to know about too.
pub fn show_single_folder_picker(
    parent_window: &impl IsA<gtk4::Window>,
    root: BrowseLocation,
    root_label: String,
    initial_segments: Vec<String>,
    on_selected: impl Fn(BrowseLocation, String) + 'static,
) -> libadwaita::Window {
    let path_label = gtk4::Label::builder()
        .css_classes(["heading"])
        .xalign(0.0)
        .wrap(true)
        .hexpand(true)
        .valign(gtk4::Align::Start)
        .build();
    // Still capped even though the Window itself can no longer
    // disappear the way the Popover could -- a very deep path taking up
    // most of the screen would still leave little room for the actual
    // folder list, which is the more important thing to see.
    // propagate_natural_height means a short path still only takes its
    // own small, natural height; this only engages once a path is long
    // enough to actually need it.
    let path_scroller = gtk4::ScrolledWindow::builder()
        .child(&path_label)
        .hscrollbar_policy(gtk4::PolicyType::Never)
        .max_content_height(80)
        .propagate_natural_height(true)
        .hexpand(true)
        .build();
    let up_button = gtk4::Button::builder().icon_name("go-up-symbolic").valign(gtk4::Align::Start).build();
    let new_folder_button = gtk4::Button::builder().icon_name("folder-new-symbolic").valign(gtk4::Align::Start).tooltip_text("New Folder").build();
    let spinner = gtk4::Spinner::builder().visible(false).valign(gtk4::Align::Start).build();

    let header_row = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).spacing(6).build();
    header_row.append(&up_button);
    header_row.append(&new_folder_button);
    header_row.append(&path_scroller);
    header_row.append(&spinner);

    let listbox = gtk4::ListBox::builder()
        .selection_mode(gtk4::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();
    let scroller = gtk4::ScrolledWindow::builder()
        .child(&listbox)
        .vexpand(true)
        .hscrollbar_policy(gtk4::PolicyType::Never)
        .build();

    let select_button = gtk4::Button::builder()
        .css_classes(["suggested-action"])
        .build();
    let cancel_button = gtk4::Button::builder().label("Cancel").build();

    let content = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .spacing(8)
        .margin_top(12).margin_bottom(12).margin_start(12).margin_end(12)
        .build();
    content.append(&header_row);
    content.append(&scroller);

    let button_row = gtk4::Box::builder().orientation(gtk4::Orientation::Horizontal).spacing(12).halign(gtk4::Align::End)
        .margin_top(12).margin_bottom(12).margin_start(12).margin_end(12)
        .build();
    button_row.append(&cancel_button);
    button_row.append(&select_button);

    let outer = gtk4::Box::builder().orientation(gtk4::Orientation::Vertical).build();
    outer.append(&content);
    outer.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
    outer.append(&button_row);

    let header_bar = libadwaita::HeaderBar::builder()
        .show_start_title_buttons(false)
        .show_end_title_buttons(false)
        .build();
    let toolbar_view = libadwaita::ToolbarView::builder().content(&outer).build();
    toolbar_view.add_top_bar(&header_bar);

    let picker_window = libadwaita::Window::builder()
        .title("Choose Folder")
        .default_width(420)
        .default_height(560)
        .modal(true)
        .transient_for(parent_window)
        .content(&toolbar_view)
        .build();
    // Same widget name as the main window and the other dialogs in
    // main.rs -- see the Add Pattern dialog's own comment there for the
    // full reasoning. Covers both the local and remote folder pickers
    // in one place, since both go through this same shared function.
    picker_window.set_widget_name("ripples-themed-window");

    let starting_location = if initial_segments.is_empty() {
        root.clone()
    } else {
        root.resolve(&initial_segments)
    };
    let widgets = FolderBrowserWidgets {
        listbox,
        path_label,
        up_button: up_button.clone(),
        select_button: select_button.clone(),
        spinner,
        current_location: Rc::new(RefCell::new(starting_location)),
        current_segments: Rc::new(RefCell::new(initial_segments)),
        root,
        root_label,
        generation: Rc::new(Cell::new(0)),
    };

    // Wired once, here -- NOT inside refresh_folder_browser, since that
    // function runs on every navigation and re-wiring these on every
    // refresh would stack up duplicate handlers rather than replacing
    // them. Both close over the same shared `widgets` bundle, so they
    // always act on whatever's current regardless of how many times
    // refresh has run since setup.
    up_button.connect_clicked(clone!(
        #[strong] widgets,
        move |_| {
            widgets.current_segments.borrow_mut().pop();
            let new_location = widgets.root.resolve(&widgets.current_segments.borrow());
            *widgets.current_location.borrow_mut() = new_location;
            refresh_folder_browser(widgets.clone());
        }
    ));
    new_folder_button.connect_clicked(clone!(
        #[strong] widgets,
        #[strong] picker_window,
        move |_| {
            let entry = gtk4::Entry::builder().placeholder_text("Folder name").activates_default(true).build();
            let dialog = libadwaita::AlertDialog::builder()
                .heading("New Folder")
                .extra_child(&entry)
                .build();
            dialog.add_response("cancel", "Cancel");
            dialog.add_response("create", "Create");
            dialog.set_response_appearance("create", libadwaita::ResponseAppearance::Suggested);
            dialog.set_default_response(Some("create"));
            dialog.set_close_response("cancel");

            glib::MainContext::default().spawn_local(clone!(
                #[strong] widgets,
                #[strong] picker_window,
                #[strong] entry,
                async move {
                    let response = dialog.choose_future(Some(&picker_window)).await;
                    let name = entry.text();
                    if response != "create" || name.trim().is_empty() {
                        return;
                    }
                    let new_location = widgets.current_location.borrow().child(name.trim());
                    let location_owned = new_location.clone();
                    let result = run_blocking(move || location_owned.create()).await;
                    match result {
                        Ok(()) => refresh_folder_browser(widgets.clone()),
                        Err(e) => {
                            let error_dialog = libadwaita::AlertDialog::builder()
                                .heading("Couldn't Create Folder")
                                .body(e)
                                .build();
                            error_dialog.add_response("ok", "OK");
                            error_dialog.present(Some(&picker_window));
                        }
                    }
                }
            ));
        }
    ));
    select_button.connect_clicked(clone!(
        #[strong] widgets,
        #[strong] picker_window,
        move |_| {
            let display = display_path(&widgets.current_location.borrow(), &widgets.root_label, &widgets.current_segments.borrow());
            on_selected(widgets.current_location.borrow().clone(), display);
            picker_window.close();
        }
    ));
    cancel_button.connect_clicked(clone!(
        #[strong] picker_window,
        move |_| picker_window.close()
    ));

    refresh_folder_browser(widgets);
    picker_window.present();
    picker_window
}

/// Minimal RAII guard for TreeContext::check_limiter -- hands its token
/// back to the channel unconditionally on Drop, covering early return
/// and task cancellation alike, the same guarantee sync::SemaphoreGuard
/// already relies on. Deliberately not reusing that type directly: it's
/// specifically tuned for the main sync engine's own fixed permit count,
/// not parameterized for a different one, and pulling in a cross-module
/// dependency for this small a piece of logic isn't worth it.
struct CheckPermit(async_channel::Sender<()>);

impl Drop for CheckPermit {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

/// How many of build_tree_node's own "does this have children" checks
/// can run at once, across the whole tree -- see TreeContext's own
/// check_limiter field for the full reasoning. Small and fixed,
/// deliberately not derived from anything (no server-connection budget
/// to match, no memory/CPU calculation to make) -- this just needs to
/// be "enough to feel responsive, not so many that expanding a large
/// folder floods the event loop with simultaneous background work,"
/// and a small constant satisfies that without needing to be clever
/// about it.
const TREE_CHECK_CONCURRENCY: usize = 4;

/// Shared, cloneable context threaded through every node in the merged
/// tree -- one instance per open Page 2, passed to every recursive
/// build_tree_node call. Cloning is cheap (see FolderBrowserWidgets's
/// own doc comment for the same reasoning -- GObjects and Rc<RefCell<>>
/// fields, nothing here is an expensive deep copy).
#[derive(Clone)]
struct TreeContext {
    global_patterns: Vec<crate::config::ExcludePattern>,
    /// Caps how many of build_tree_node's own "does this row have
    /// children" background checks can genuinely be in flight at once,
    /// across the whole tree -- a real, confirmed fix for a real, live
    /// UI freeze: each row already defers building its own children
    /// until actually expanded, but the check that decides whether to
    /// even show a chevron at all was never deferred or throttled.
    /// Expanding one parent with, say, a hundred subfolders meant a
    /// hundred simultaneous list_merged_children calls firing at once
    /// (local disk reads and remote PROPFINDs alike), which is what
    /// bogged the whole dialog down -- not literally every row
    /// expanding, but enough concurrent background work in flight at
    /// once (a hundred spinners, a hundred pending async tasks) to make
    /// the UI thread feel exactly like it had. A plain bounded channel,
    /// pre-filled with TREE_CHECK_CONCURRENCY tokens, not sync::
    /// Semaphore -- that type is specifically tuned (and documented) for
    /// the main sync engine's own server-connection budget, a completely
    /// unrelated concern from how many local/remote listing calls this
    /// dialog's own tree should run concurrently while just browsing.
    /// One shared pair per open Page 2 (constructed once in
    /// populate_merged_tree, cloned into every node via this same
    /// TreeContext), not per-row -- the whole point is one, real,
    /// tree-wide ceiling, the same reasoning sync::Semaphore's own doc
    /// comment gives for why it has to be shared rather than
    /// reconstructed at each recursive level.
    check_limiter: (async_channel::Sender<()>, async_channel::Receiver<()>),
    /// The connection's own in-progress edit -- shared and mutated live
    /// as checkboxes are toggled, read back by the caller when Save is
    /// clicked. Same Rc<RefCell<Vec<String>>> the dialog itself owns,
    /// not a copy.
    excluded_subpaths: Rc<RefCell<Vec<String>>>,
}

/// Builds one row for `entry`, given its own direct children already
/// known (`own_children` -- see this module's own top-level doc comment
/// update below for why these arrive pre-fetched rather than being
/// fetched lazily on expand). Returns a plain gtk4::Widget --
/// ExpanderRow and ActionRow don't share a more specific common base
/// this code can use directly, and both get added to their parent the
/// same way regardless (ExpanderRow::add_row wants impl IsA<Widget>).
///
/// No chevron at all -- a plain ActionRow, not an ExpanderRow -- for
/// either a globally-excluded folder (its own contents are never
/// enumerated at all, matching watch.rs's own reasoning for not even
/// watching such directories in the first place) OR a confirmed dead
/// end (own_children is empty, already known by the time this is
/// called, not discovered later by trying to expand it). This is what
/// actually resolves "still loading vs genuinely empty": both states
/// used to render identically (an expandable row that, when clicked,
/// either populates after a delay or populates with nothing) until the
/// user waited long enough to tell them apart. Knowing own_children
/// before the row is even built removes the ambiguity at its source
/// instead of trying to make waiting more legible.
///
/// Pixels of left margin added per tree depth level -- applied to each
/// row's own checkbox prefix, since that's the leftmost element already
/// present on every row regardless of type, rather than introducing a
/// separate spacer widget just for this.
const INDENT_PER_DEPTH: i32 = 20;

/// One row in the merged local+remote exclusion tree -- either a plain
/// ActionRow (globally excluded, so there's genuinely nothing further
/// to explore inside it), or an ExpanderRow whose own expand affordance
/// starts disabled and gets enabled the moment a background check
/// confirms it actually has something inside.
///
/// Deliberate design, not the simpler "always assume it might have
/// children" version this briefly was -- real, reported feedback on
/// that: a connection being browsed specifically to decide what to
/// exclude needs to see up front which folders are even worth
/// clicking into, not discover "actually empty" one click at a time.
/// But the *original* version (fully eager: every visible node's own
/// children fetched before rendering anything at all) had a real,
/// confirmed performance cost too -- quadratic in the width of
/// whatever directory was being browsed, since it couldn't show a
/// single row until every sibling's own children had *also* been
/// fetched.
///
/// This is the middle ground: every row renders immediately (a single,
/// plain level-1 listing, not one multiplied by sibling count), each
/// with its own small spinner suffix, while a separate, per-row
/// background task independently checks whether that specific entry
/// has real children. Nothing waits on a sibling's own check --
/// whichever rows resolve first update first. Once a row's own check
/// resolves: empty means the spinner just clears (`enable_expansion`
/// stays false, exactly as it already was, so it never grows a chevron
/// at all); non-empty means the spinner clears *and* the chevron
/// becomes real (`set_enable_expansion(true)`). The fetched children
/// themselves are kept, not discarded -- if the row is later actually
/// expanded, connect_expanded_notify reuses them directly rather than
/// re-fetching the same listing a second time.
fn build_tree_node(entry: MergedEntry, ctx: TreeContext, depth: u32) -> gtk4::Widget {
    let checkbox = gtk4::CheckButton::new();
    checkbox.set_margin_start(depth as i32 * INDENT_PER_DEPTH);
    let is_globally_excluded = entry.global_exclusion.is_some();
    let already_excluded_by_choice = ctx.excluded_subpaths.borrow().iter().any(|p| p == &entry.relative_path);
    checkbox.set_active(!is_globally_excluded && !already_excluded_by_choice);
    checkbox.set_sensitive(!is_globally_excluded);

    if is_globally_excluded {
        let row = libadwaita::ActionRow::builder()
            .title(&entry.name)
            // Same reasoning as the other ActionRow/ExpanderRow titles
            // in this file -- see the first one's own comment.
            .use_markup(false)
            .build();
        row.add_prefix(&checkbox);
        return row.upcast();
    }

    let row = libadwaita::ExpanderRow::builder()
        .title(&entry.name)
        .use_markup(false)
        // Unknown yet whether this actually has anything inside --
        // starts non-expandable (no chevron shown at all), flipped to
        // true below only once the background check below confirms
        // real children exist. Confirmed directly against libadwaita's
        // own real source that this property exists specifically for
        // "don't yet know, decide later" cases like this one.
        .enable_expansion(false)
        .build();
    row.add_prefix(&checkbox);

    let spinner = gtk4::Spinner::builder().spinning(true).build();
    row.add_suffix(&spinner);

    // Direct children this node has actually built so far (empty until
    // first expansion, given lazy loading) -- tracked explicitly rather
    // than queried back out of the widget tree, since ExpanderRow has no
    // "give me my own child rows" accessor. Only direct children are
    // tracked; GTK's own widget-sensitivity propagation already makes
    // everything nested inside an insensitive widget read as insensitive
    // too, so disabling one level here is sufficient -- it isn't a
    // shortcut that misses deeper descendants.
    let child_widgets: Rc<RefCell<Vec<gtk4::Widget>>> = Rc::new(RefCell::new(Vec::new()));
    let loaded = Rc::new(Cell::new(false));
    // Guards connect_expanded_notify's own handler against a real,
    // confirmed libadwaita quirk -- see its own use, right where
    // set_enable_expansion(true) is called below, for the full
    // reasoning (confirmed directly against libadwaita's own C source).
    // Set true for the exact duration of that one call, false the rest
    // of the time -- including while the row is later genuinely,
    // manually expanded by an actual click, which is exactly when this
    // must NOT suppress the handler.
    let suppress_spurious_expand = Rc::new(Cell::new(false));
    // Populated by the background check below the moment it confirms
    // real children exist -- connect_expanded_notify consumes this
    // directly instead of re-fetching, so the one, real network
    // round-trip this row ever needs happens exactly once, regardless
    // of whether the user actually expands it or not.
    let prefetched_children: Rc<RefCell<Option<Vec<MergedEntry>>>> = Rc::new(RefCell::new(None));
    // Set true only when the background check below actually confirms
    // real children exist -- distinct from `loaded` (whether this row's
    // own child widgets have been built yet), and needed for the
    // uncheck/re-check handling further down: re-checking a parent
    // should only ever restore its own expandability if it genuinely
    // has something to expand into, never unconditionally.
    let has_real_children = Rc::new(Cell::new(false));

    glib::MainContext::default().spawn_local(clone!(
        #[strong] row,
        #[strong] spinner,
        #[strong] checkbox,
        #[strong] ctx,
        #[strong] entry,
        #[strong] prefetched_children,
        #[strong] has_real_children,
        #[strong] suppress_spurious_expand,
        async move {
            // Waits its turn if TREE_CHECK_CONCURRENCY checks are
            // already running elsewhere in the tree -- see
            // TreeContext::check_limiter's own doc comment for why this
            // exists at all. Held via a small RAII guard, not a plain
            // acquire-then-manually-release pair -- this task can be
            // dropped mid-flight (the dialog closing while a check is
            // still running is the obvious case), and a manual release
            // placed after the awaited call would simply never run in
            // that case, permanently shrinking the pool by one permit
            // every time it happened. The guard's own Drop impl hands
            // the token back unconditionally, the same guarantee
            // sync::SemaphoreGuard already relies on for the exact same
            // reason.
            let _permit = ctx.check_limiter.1.recv().await.ok().map(|()| CheckPermit(ctx.check_limiter.0.clone()));
            let children = list_merged_children(
                entry.local_location.as_ref(), entry.remote_location.as_ref(),
                &entry.relative_path, &ctx.global_patterns,
            ).await;
            row.remove(&spinner);
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] folder picker: {} (depth {depth}) has {} children -> enable_expansion={}", now_hms(), entry.relative_path, children.len(), !children.is_empty());
            }
            if !children.is_empty() {
                *prefetched_children.borrow_mut() = Some(children);
                has_real_children.set(true);
                // Gated on the checkbox's own current state -- this
                // background check can resolve well after row
                // construction, by which point the row may already have
                // started (or since become) unchecked, either from
                // already_excluded_by_choice at construction time or a
                // genuine uncheck while this was still in flight. Either
                // way, has_real_children above is still set unconditionally
                // (a later re-check needs to know this row genuinely has
                // something to expand into), but the actual
                // enable_expansion(true) call itself -- the one with the
                // real, guarded side effect -- only happens if this row
                // is currently meant to be expandable at all.
                if checkbox.is_active() {
                    // suppress_spurious_expand guards connect_expanded_notify's
                    // own handler for the specific duration of this call --
                    // see that flag's own doc comment for why this exists at
                    // all: confirmed directly against libadwaita's own real
                    // source (adw_expander_row_set_enable_expansion, adw-
                    // expander-row.c) that set_enable_expansion(TRUE)
                    // unconditionally calls set_expanded(TRUE) internally as
                    // part of its own implementation, firing a completely
                    // genuine notify::expanded synchronously, before this
                    // call even returns -- not a bug in this app's own code,
                    // just a real, surprising, and not clearly documented
                    // side effect of the widget itself. Without this guard,
                    // that one call alone was enough to trigger this row's
                    // own "build my children" logic immediately, and since
                    // build_tree_node recurses into the exact same check for
                    // every child it builds, the same thing happened all the
                    // way down -- confirmed via real [debug] output showing
                    // hundreds of genuine notify::expanded firings across an
                    // entire real tree within about a minute, none of them
                    // from an actual click.
                    suppress_spurious_expand.set(true);
                    row.set_enable_expansion(true);
                    // Explicit reset, not just the guard above -- the guard
                    // only stops this app's own handler from reacting to the
                    // spurious signal; it does nothing to the row's own,
                    // real internal expanded state, which set_enable_expansion
                    // genuinely did just set to true as a side effect (see
                    // this whole block's own doc comment). Left unreset, the
                    // row would sit in a genuinely inconsistent state --
                    // internally expanded, but with no children ever added
                    // (since the guard correctly stopped that from
                    // happening) -- so the very next real click would
                    // toggle it to *collapsed* instead of expanding it,
                    // reading as "nothing happens" the way it was reported.
                    // Confirmed as the right fix by looking at libadwaita's
                    // own constructor, which does exactly this same pairing
                    // for exactly this same reason.
                    row.set_expanded(false);
                    suppress_spurious_expand.set(false);
                }
            }
            // Empty: enable_expansion deliberately left false -- the
            // row already looks and behaves like a plain, non-
            // expandable entry, nothing further to do.
        }
    ));

    row.connect_expanded_notify(clone!(
        #[strong] row,
        #[strong] loaded,
        #[strong] child_widgets,
        #[strong] ctx,
        #[strong] entry,
        #[strong] prefetched_children,
        #[strong] suppress_spurious_expand,
        move |r| {
            // Checked first, before is_expanded()/loaded -- see this
            // flag's own doc comment. A real, confirmed libadwaita quirk
            // (adw_expander_row_set_enable_expansion unconditionally
            // calls set_expanded(TRUE) internally, firing this exact
            // signal), not a genuine click, whenever this is true.
            if suppress_spurious_expand.get() {
                return;
            }
            if !r.is_expanded() || loaded.get() {
                return;
            }
            loaded.set(true);
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] folder picker: {} (depth {depth}) EXPANDED (user-triggered notify::expanded)", now_hms(), entry.relative_path);
            }

            if let Some(children) = prefetched_children.borrow_mut().take() {
                if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] folder picker: {} (depth {depth}) building {} prefetched children", now_hms(), entry.relative_path, children.len());
                }
                for child in children {
                    let child_widget = build_tree_node(child, ctx.clone(), depth + 1);
                    row.add_row(&child_widget);
                    child_widgets.borrow_mut().push(child_widget);
                }
                return;
            }

            // Shouldn't normally happen -- enable_expansion only ever
            // becomes true once prefetched_children is already Some --
            // but falls back to a direct fetch rather than leaving an
            // expanded row with nothing in it if it somehow does.
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] folder picker: {} (depth {depth}) expanded with no prefetched children -- unexpected fallback fetch", now_hms(), entry.relative_path);
            }
            glib::MainContext::default().spawn_local(clone!(
                #[strong] row,
                #[strong] child_widgets,
                #[strong] ctx,
                #[strong] entry,
                async move {
                    let children = list_merged_children(
                        entry.local_location.as_ref(), entry.remote_location.as_ref(),
                        &entry.relative_path, &ctx.global_patterns,
                    ).await;
                    for child in children {
                        let child_widget = build_tree_node(child, ctx.clone(), depth + 1);
                        row.add_row(&child_widget);
                        child_widgets.borrow_mut().push(child_widget);
                    }
                }
            ));
        }
    ));

    checkbox.connect_toggled(clone!(
        #[strong] ctx,
        #[strong] entry,
        #[strong] child_widgets,
        #[strong] row,
        #[strong] has_real_children,
        #[strong] suppress_spurious_expand,
        move |cb| {
            if cb.is_active() {
                ctx.excluded_subpaths.borrow_mut().retain(|p| p != &entry.relative_path);
                for child in child_widgets.borrow().iter() {
                    child.set_sensitive(true);
                }
                // Restores this row's own expandability, undoing the
                // disable below -- but only if it actually has real
                // children to expand into; a row that never had any
                // stays exactly as inert as it always was.
                // set_enable_expansion(true) is guarded the same way the
                // background check's own, earlier call already is (see
                // its own doc comment on the confirmed libadwaita side
                // effect) -- re-checking a parent row is a genuine click,
                // but not on the *expander* itself, so the resulting
                // notify::expanded here would be exactly as spurious as
                // the one that call already has to guard against.
                if has_real_children.get() {
                    suppress_spurious_expand.set(true);
                    row.set_enable_expansion(true);
                    row.set_expanded(false);
                    suppress_spurious_expand.set(false);
                }
            } else {
                if !ctx.excluded_subpaths.borrow().iter().any(|p| p == &entry.relative_path) {
                    ctx.excluded_subpaths.borrow_mut().push(entry.relative_path.clone());
                }
                // Children's own excluded_subpaths entries (if any, from
                // an earlier, separate, deliberate uncheck of their own)
                // are deliberately left untouched here -- only visually
                // disabled. Re-checking this row later restores each
                // child's own checkbox state by re-reading
                // excluded_subpaths fresh, which still holds whatever it
                // held before, rather than this parent having
                // overwritten it.
                for child in child_widgets.borrow().iter() {
                    child.set_sensitive(false);
                }
                // Collapses and disables further expansion -- the actual
                // fix, not just cosmetic tidiness: child_widgets above is
                // only ever populated once this row has genuinely been
                // expanded at least once (lazy loading), so a row
                // unchecked before that ever happened has nothing in
                // child_widgets to disable at all, and disabling it here
                // does nothing to protect grandchildren that get lazily
                // built *later*, after this uncheck, if expansion were
                // still allowed. Blocking expansion itself while
                // unchecked closes that gap entirely, rather than trying
                // to patch every future lazy-load to remember to check
                // an ancestor's own state. set_enable_expansion(false),
                // unlike the (true) case elsewhere in this file, has no
                // documented spurious-signal side effect of its own to
                // guard against; explicit set_expanded(false) alongside
                // it regardless, rather than relying on an undocumented
                // collapse-on-disable behavior to do that implicitly.
                row.set_expanded(false);
                row.set_enable_expansion(false);
            }
        }
    ));

    row.upcast()
}

/// Populates `container` with the top-level merged tree for a chosen
/// local_root/remote_root pair -- the actual entry point Page 2 calls
/// once both roots are known (Add mode, after Next) or already known
/// (Edit mode, immediately). `container` should be empty when this is
/// called; existing children aren't cleared first, since the two real
/// callers (initial Page 2 load in each mode) only ever call this once
/// per dialog, not on every re-entry to the page.
pub fn populate_merged_tree(
    container: &gtk4::ListBox,
    local_root: Option<BrowseLocation>,
    remote_root: Option<BrowseLocation>,
    global_patterns: Vec<crate::config::ExcludePattern>,
    excluded_subpaths: Rc<RefCell<Vec<String>>>,
) {
    let (limiter_tx, limiter_rx) = async_channel::bounded::<()>(TREE_CHECK_CONCURRENCY);
    for _ in 0..TREE_CHECK_CONCURRENCY {
        limiter_tx.try_send(()).expect("just-created bounded channel sized to exactly TREE_CHECK_CONCURRENCY");
    }
    let ctx = TreeContext { global_patterns, excluded_subpaths, check_limiter: (limiter_tx, limiter_rx) };
    glib::MainContext::default().spawn_local(clone!(
        #[strong] container,
        async move {
            // A single, plain level-1 listing -- the same shape every
            // later expansion already uses. Each resulting row runs its
            // own separate, independent "does it have children"
            // background check (see build_tree_node's own doc comment);
            // this call itself doesn't do anything eager beyond the one
            // level asked for.
            let children = list_merged_children(
                local_root.as_ref(), remote_root.as_ref(), "", &ctx.global_patterns,
            ).await;
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] folder picker: top level has {} entries, building rows now", now_hms(), children.len());
            }
            if children.is_empty() {
                let empty_row = libadwaita::ActionRow::builder()
                    .title("No subfolders here")
                    .sensitive(false)
                    .build();
                container.append(&empty_row);
                return;
            }
            for entry in children {
                let widget = build_tree_node(entry, ctx.clone(), 0);
                container.append(&widget);
            }
        }
    ));
}

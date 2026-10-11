// src/watch.rs
//
// Local-side change detection via GIO's FileMonitor (inotify under the
// hood on Linux) -- same MainContext as everything else, no new
// dependency. This answers "did anything change locally" so the sync
// loop doesn't have to find out by crawling on a fixed timer; it does
// NOT replace crawling. A watch is a trigger, not a source of truth: the
// actual sync round still walks the tree and compares against the
// journal, the same way it always has. This just decides *when* to do
// that early, instead of waiting for the next scheduled round.
//
// inotify doesn't recurse, so this holds one FileMonitor per directory
// and rebuilds that set after each sync round (cheap -- a stat-only walk
// over a tree that, by definition, was just fully crawled a moment ago).
// A directory excluded via .no-sync is skipped entirely: not walked,
// not watched, saving both the enumeration work and an inotify watch
// slot for anything under it.
//
// Deliberately NOT built incrementally off sync.rs's own directory
// creation calls -- rebuilding wholesale after each round is a few
// dozen lines instead of threading watch add/remove calls through every
// directory-create and directory-delete path in sync.rs, and the cost
// difference is negligible for the tree sizes this app targets.
//
// Event-driven, not polled: a FileMonitor callback used to just flip a
// bare `Rc<Cell<bool>>`, and a separate loop in main.rs woke up every 2
// seconds to check it -- meaning the app woke the CPU on a fixed
// schedule forever, even across long stretches with nothing to watch
// for at all. Every watch_one callback now calls back into main.rs's own
// `on_change` directly, by connection_id and the specific directory that
// fired -- main.rs is what actually arms/resets a one-shot debounce
// timer and accumulates *which* directories were reported dirty (see
// AppState::dirty_dirs), rather than this module tracking any of that
// itself. watch.rs's own job stays narrow: notice something changed,
// and say exactly where.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use gio::prelude::*;

use crate::exclude;

/// One FileMonitor per watched directory, grouped by connection so an
/// entire connection's watches can be dropped in one go (folder removed,
/// account switched away, or connection disabled -- all the same "stop
/// watching this" operation).
#[derive(Default)]
pub struct WatchManager {
    watches: RefCell<HashMap<String, HashMap<PathBuf, gio::FileMonitor>>>,
}

impl WatchManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reconciles watches against `wanted` (connection_id, local_root,
    /// connection-specific patterns triples -- already filtered by the
    /// caller to enabled connections under the active account, and
    /// carrying that connection's own excluded_local_subpaths +
    /// excluded_remote_subpaths chained together, same as sync.rs's own
    /// SyncTarget::connection_patterns). Anything not in `wanted` is
    /// dropped entirely, which is also how "no connections active"
    /// naturally ends up watching nothing: pass an empty slice and every
    /// watch goes away, no separate on/off switch needed.
    ///
    /// `global_patterns` is the same config::effective_exclude_patterns
    /// list sync.rs filters against -- a directory matching one of
    /// these, OR one of its own connection's patterns, gets skipped here
    /// the same as a .no-sync-excluded one, so an exclusion pattern
    /// (global or connection-specific) saves an inotify watch slot, not
    /// just a sync pass.
    ///
    /// `on_change` is called with (connection_id, the exact directory
    /// that fired) every time any watched directory reports a change --
    /// see this module's own top-level doc comment for why that's a
    /// direct callback now rather than a flag main.rs polls. `Rc<dyn Fn>`
    /// rather than a generic type parameter: this needs to be cloned
    /// into a 'static FileMonitor callback closure per directory, so it
    /// has to be an owned, reference-counted handle, not a borrow tied to
    /// this call's own stack frame.
    pub fn sync_watches(&self, wanted: &[(String, PathBuf, Vec<String>)], global_patterns: &[crate::config::ExcludePattern], on_change: &Rc<dyn Fn(&str, &Path)>) {
        let mut watches = self.watches.borrow_mut();

        // Drop connections no longer wanted at all.
        let wanted_ids: std::collections::HashSet<&str> =
            wanted.iter().map(|(id, _, _)| id.as_str()).collect();
        watches.retain(|id, _| wanted_ids.contains(id.as_str()));

        for (connection_id, local_root, connection_patterns) in wanted {
            let current = watches.entry(connection_id.clone()).or_default();
            let desired_dirs = walk_dirs(local_root, global_patterns, connection_patterns);
            let desired_set: std::collections::HashSet<&PathBuf> = desired_dirs.iter().collect();

            // Drop watches on directories that no longer exist / are now excluded.
            current.retain(|path, _| desired_set.contains(path));

            // Add watches on newly discovered directories.
            for dir in &desired_dirs {
                if current.contains_key(dir) {
                    continue;
                }
                if let Some(monitor) = watch_one(dir, connection_id.clone(), on_change.clone()) {
                    current.insert(dir.clone(), monitor);
                }
            }
        }
    }
}

/// Root + every subdirectory under it, skipping anything a .no-sync file
/// in its parent excludes (or that matches a global or connection-
/// specific pattern) and not following symlinks (matches sync.rs's
/// existing "no symlink handling" limitation -- a symlinked directory
/// isn't traversed here either).
fn walk_dirs(root: &Path, global_patterns: &[crate::config::ExcludePattern], connection_patterns: &[String]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_dirs_into(root, root, global_patterns, connection_patterns, &mut out);
    out
}

fn walk_dirs_into(root: &Path, dir: &Path, global_patterns: &[crate::config::ExcludePattern], connection_patterns: &[String], out: &mut Vec<PathBuf>) {
    out.push(dir.to_path_buf());

    let Ok(read) = std::fs::read_dir(dir) else { return }; // not created yet, or unreadable -- fine, nothing to watch
    let excluded = exclude::load(dir);

    for entry in read.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Ok(file_type) = entry.file_type() else { continue };
        let entry_path = entry.path();
        // Same "no leading /, / separated" shape sync.rs's own
        // relative_path values have -- root-anchored patterns compare
        // against this directly.
        let relative_path = entry_path.strip_prefix(root).unwrap_or(&entry_path).to_string_lossy().into_owned();
        if exclude::exclusion_reason(&excluded, global_patterns, connection_patterns, &relative_path, &name, file_type.is_dir()).is_some() {
            continue;
        }
        if file_type.is_dir() {
            walk_dirs_into(root, &entry_path, global_patterns, connection_patterns, out);
        }
    }
}

/// Sets up one FileMonitor on `dir`. Failures (directory vanished
/// between the walk and here, permission issue) just mean that one
/// directory goes unwatched -- not fatal, the periodic sync round still
/// covers it.
fn watch_one(dir: &Path, connection_id: String, on_change: Rc<dyn Fn(&str, &Path)>) -> Option<gio::FileMonitor> {
    let file = gio::File::for_path(dir);
    let monitor = file
        .monitor_directory(gio::FileMonitorFlags::NONE, gio::Cancellable::NONE)
        .map_err(|e| eprintln!("watch: couldn't monitor {}: {e}", dir.display()))
        .ok()?;

    // GFileMonitor already coalesces bursts internally; main.rs's own
    // on_change additionally debounces on top (see AppState's own doc
    // comment on watch_debounce_timer), so there's no need to be
    // selective about which event variant fired here -- any of them
    // means "go re-crawl this directory eventually."
    let dir_owned = dir.to_path_buf();
    monitor.connect_changed(move |_monitor, _file, _other_file, _event| {
        on_change(&connection_id, &dir_owned);
    });
    monitor.set_rate_limit(2000);

    Some(monitor)
}

//! Persistent, on-disk logging -- a genuine, permanent record of what
//! this app did, surviving both app restarts and the in-app StatusLog's
//! own, bounded ring-buffer cap (see status.rs). One file per session
//! (or per mid-session rotation -- see should_rotate's own doc comment),
//! written with buffered, infrequent, event-driven flushes rather than
//! either a continuous per-line write (real, unnecessary disk/battery
//! cost on a phone for something this frequent) or a fixed, always-
//! running periodic timer (a wakeup every interval even when nothing
//! happened at all, which is exactly the kind of needless wakeup this
//! app has already gone out of its way to avoid elsewhere -- see
//! Semaphore's own doc comment on this same reasoning). Deliberately no
//! in-app UI for browsing past sessions' own files -- these are meant to
//! be found and read the same way any other log file is, outside the
//! app, not re-implemented as a second, in-app file browser.

use std::path::{Path, PathBuf};

/// How many past session files to keep at startup -- see
/// prune_old_sessions. Chosen as a simple, predictable count rather than
/// an age-based policy (e.g. "delete anything older than 30 days")
/// specifically because a count is trivial to reason about regardless of
/// how often the app actually gets launched -- an age-based policy would
/// quietly keep either far more or far fewer files than intended
/// depending on usage pattern, without ever making that obvious.
const MAX_SESSION_FILES: usize = 30;

/// Mid-session rotation threshold -- see should_rotate. 20MB is well
/// past what even a very active, long-running session accumulates in
/// normal operation (the in-app StatusLog's own 20000-line cap is
/// itself only a few MB at typical line lengths), so this exists purely
/// as a backstop against the pathological case this feature was
/// explicitly built to survive -- a session that's still running weeks
/// or months later -- rather than something expected to fire in
/// ordinary use.
const ROTATE_AT_BYTES: u64 = 20 * 1024 * 1024;

/// ~/.local/share/ripples/logs -- a subdirectory alongside, not inside,
/// the existing journal.sqlite3's own "ripples" directory (see
/// journal_db_path in main.rs), so log files are trivially distinguished
/// from the actual sync-state database by anyone browsing that folder
/// directly.
pub fn log_directory() -> PathBuf {
    let mut path = glib::user_data_dir();
    path.push("ripples");
    path.push("logs");
    std::fs::create_dir_all(&path).ok();
    path
}

/// Filesystem-safe, lexically-sortable timestamp -- deliberately the
/// same underlying glib::DateTime::now_local already used elsewhere
/// (see now_hms in main.rs), just a different format string suited to a
/// filename rather than an in-log timestamp. Sorting by filename alone
/// (no need to stat/parse anything) is what makes prune_old_sessions'
/// own "keep the most recent N" logic simple.
fn session_timestamp() -> String {
    glib::DateTime::now_local().ok()
        .and_then(|d| d.format("%Y-%m-%d_%H-%M-%S").ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown-time".to_string())
}

/// A fresh session log file's own path -- called once at startup, and
/// again by should_rotate whenever a session's current file crosses
/// ROTATE_AT_BYTES. Deliberately just a new, ordinary-looking filename
/// with the current timestamp, not something suffixed "-part2" or
/// otherwise marked as a continuation -- with no in-app "browse past
/// sessions" feature reading these back, there's no code anywhere that
/// actually needs to know two files both belong to what a human would
/// call "the same session"; a person reading the directory listing
/// directly can already see they're chronologically adjacent from the
/// timestamps alone.
pub fn new_session_path() -> PathBuf {
    log_directory().join(format!("ripples-{}.log", session_timestamp()))
}

/// Deletes all but the most recent MAX_SESSION_FILES log files -- called
/// once at startup, before this session's own new_session_path is even
/// created, so the file this session is about to write never risks
/// being the one pruned. Best-effort throughout: a directory read or
/// individual delete failing here isn't worth surfacing to the user or
/// aborting startup over -- worst case, one extra file survives a bit
/// longer than intended, not a real problem worth interrupting anything
/// else for.
pub fn prune_old_sessions() {
    let dir = log_directory();
    let Ok(entries) = std::fs::read_dir(&dir) else { return };

    let mut files: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
        .collect();
    // Lexical sort on the full path is equivalent to sorting by
    // timestamp here, since session_timestamp's own format
    // (YYYY-MM-DD_HH-MM-SS) sorts identically both ways -- no need to
    // parse anything back out of the filename.
    files.sort();

    if files.len() <= MAX_SESSION_FILES {
        return;
    }
    for old in &files[..files.len() - MAX_SESSION_FILES] {
        let _ = std::fs::remove_file(old);
    }
}

/// Appends already-formatted lines (each pushed as its own line) to
/// path, creating it if this is the first write of a new session or a
/// just-rotated one. Best-effort by design, same reasoning as
/// prune_old_sessions -- a disk-log write failing shouldn't ever be
/// treated as seriously as, say, a journal write failing, since nothing
/// about the app's actual sync correctness depends on this file existing
/// at all; it's a diagnostic convenience layered on top; the in-app
/// StatusLog remains the source of truth for anything the running app
/// itself needs to reason about.
pub fn append_lines(path: &Path, lines: &[String]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    for line in lines {
        writeln!(file, "{line}")?;
    }
    Ok(())
}

/// Whether path has crossed ROTATE_AT_BYTES and the caller should start
/// a fresh file (via new_session_path) for anything written from here
/// on. Checked only on an actual flush, not on any separate timer of its
/// own -- there's no reason to check this on any cadence tighter than
/// "right after more data was just written," since that's the only
/// moment the file's own size could have changed at all.
pub fn should_rotate(path: &Path) -> bool {
    std::fs::metadata(path).map(|m| m.len() >= ROTATE_AT_BYTES).unwrap_or(false)
}

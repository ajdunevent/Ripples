// src/exclude.rs
//
// Per-directory ".no-sync" files, modeled on GNOME/Nautilus's own
// ".hidden" convention: a plain list of names, one per line, that this
// directory doesn't want synced. Not a glob/pattern engine on purpose --
// exact child names only, same as .hidden. Cross-cutting patterns
// (*.tmp, node_modules, etc, wanted anywhere) belong in a separate global
// list in AppConfig, not duplicated into every directory's .no-sync --
// this file is deliberately narrow, the same way .hidden is.
//
// Read from BOTH sides, unioned together: a .no-sync sitting in the
// local directory and one sitting in the corresponding remote directory
// both contribute names to the same exclusion set for that directory
// (see sync.rs's sync_directory, which is the only thing that actually
// fetches the remote copy -- this module stays filesystem/network
// agnostic and just knows how to parse the text once it has it, via
// `parse`; `load` is a thin std::fs-backed wrapper around that for the
// local side). This matters most for a brand-new connection on a large,
// deep remote tree: a *local* .no-sync can't exclude anything below the
// root until that subtree has already been created locally by a first
// sync pass, but a *remote* one applies from round one, on every device
// syncing that account, without needing the tree built locally first.
// It is never synced itself, on either side -- see NO_SYNC_FILENAME
// callers, which always drop it from the working set before anything
// else sees it.
//
// Deliberately conservative: excluding a name here does NOT retroactively
// delete or forget anything already synced under that name. It only
// stops that name from being *looked at* on future rounds. Removing
// already-synced content is a separate, explicit action -- silently
// deleting a user's data because they added a line to a text file is
// exactly the kind of surprise this app tries hard to avoid elsewhere
// (see move_deleted_to_trash's default, the conflicted-copy policy,
// etc). One consequence worth knowing: a directory that was already
// synced locally before a *remote* .no-sync started excluding it keeps
// existing on disk (correctly, per the policy above) and so also keeps
// its `watch.rs` inotify watch -- watch.rs only ever sees the local
// side, so it has no way to know a remote-only exclusion applies to an
// already-present local directory. Harmless: an edit inside it still
// wakes a sync round, which then correctly skips it, same as any other
// excluded name -- just a watch slot that isn't strictly needed anymore
// rather than a correctness problem.

use std::collections::HashSet;
use std::path::Path;

pub const NO_SYNC_FILENAME: &str = ".no-sync";

/// Prefix for this app's own temp-file-first transfer naming convention
/// (`.ripples-partial-<random>`, see sync.rs) -- hardcoded and checked
/// unconditionally, same as NO_SYNC_FILENAME, and NOT seeded into
/// config::effective_exclude_patterns's own combined list. That list is
/// user-editable via
/// Settings; this pattern must never appear there or be removable -- an
/// in-progress temp file being mistaken for a real one to sync isn't a
/// preference, it's a correctness requirement. The random suffix only
/// needs to avoid two concurrent transfers colliding on the same name,
/// nothing more -- see sync.rs's temp-naming code for why it doesn't
/// need to be recoverable or meaningful.
pub const TEMP_FILE_PREFIX: &str = ".ripples-partial-";

/// Parses .no-sync's own format (one name per line, blank lines and `#`
/// comments ignored) from already-read text -- shared by `load` (local,
/// via std::fs) and sync.rs's remote read (via GIO's load_contents_future),
/// so the format is defined in exactly one place regardless of which side
/// it came from.
pub fn parse(contents: &str) -> HashSet<String> {
    let mut names = HashSet::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        names.insert(line.to_string());
    }
    names
}

/// Reads `dir`'s local .no-sync file, if any, and returns the set of
/// child names it lists. Missing file, unreadable file, or an empty file
/// all just mean "nothing excluded here, from this side" -- not an
/// error, since most directories won't have one. The remote side's copy
/// (if any) is a separate read entirely -- see sync.rs -- and gets
/// unioned in with this by the caller, not handled here.
pub fn load(dir: &Path) -> HashSet<String> {
    std::fs::read_to_string(dir.join(NO_SYNC_FILENAME))
        .map(|contents| parse(&contents))
        .unwrap_or_default()
}

/// Which of the three exclusion mechanisms actually matched -- lets the
/// log say something more useful than a generic "excluded" (see
/// status::SyncEvent::Excluded), pointing at exactly which of three
/// different places to go check: a stray .no-sync file in that specific
/// directory, the global Ignored Files list in Settings, or this
/// connection's own excluded-path list in its Edit dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionReason {
    NoSyncFile,
    TempFile,
    GlobalPattern,
    ConnectionPath,
}

/// Same job the old is_excluded_anywhere did, but reports WHICH
/// mechanism matched instead of just whether one did. `global_patterns`
/// (config::effective_exclude_patterns's own combined list) and
/// `connection_patterns` (a connection's own excluded_subpaths) have to be passed as genuinely separate slices
/// here, not one pre-merged list -- once merged there'd be no way to
/// tell which list a given match actually came from. Checked in this
/// order (.no-sync first, then global, then connection-specific) since
/// that's roughly "most likely to be what a user forgot about" to
/// "least" for troubleshooting purposes, though the order has no effect
/// on the exclusion decision itself -- a match on any one of the three
/// excludes the item regardless of what order they were checked in.
pub fn exclusion_reason(
    excluded: &HashSet<String>,
    global_patterns: &[crate::config::ExcludePattern],
    connection_patterns: &[String],
    relative_path: &str,
    name: &str,
    is_directory: bool,
) -> Option<ExclusionReason> {
    if name.starts_with(TEMP_FILE_PREFIX) {
        return Some(ExclusionReason::TempFile);
    }
    if name == NO_SYNC_FILENAME || excluded.contains(name) {
        return Some(ExclusionReason::NoSyncFile);
    }
    if global_patterns.iter().any(|p| matches_pattern(&p.pattern, relative_path, name, is_directory)) {
        return Some(ExclusionReason::GlobalPattern);
    }
    if connection_patterns.iter().any(|p| matches_pattern(p, relative_path, name, is_directory)) {
        return Some(ExclusionReason::ConnectionPath);
    }
    None
}

/// For the folder-cleanup logic in sync.rs: was this item excluded
/// specifically via a global filename pattern, and if so, is that
/// pattern marked auto_removable? Returns `None` if no global pattern matched
/// at all -- deliberately NOT checked against .no-sync entries or a
/// connection's own excluded subpaths, since the auto_removable flag is
/// scoped to global patterns only (see config::ExcludePattern's own doc
/// comment): a subpath exclusion is a deliberate, specific "don't touch
/// this whole folder" decision, a meaningfully bigger and more
/// intentional choice than a generic filename pattern, and doesn't get
/// the same easy override. If multiple patterns match the same item,
/// the first match wins -- same "first match, no further searching"
/// behavior exclusion_reason itself already has.
pub fn global_pattern_auto_removable(
    global_patterns: &[crate::config::ExcludePattern],
    relative_path: &str,
    name: &str,
    is_directory: bool,
) -> Option<bool> {
    global_patterns.iter()
        .find(|p| matches_pattern(&p.pattern, relative_path, name, is_directory))
        .map(|p| p.auto_removable)
}

/// Matches either one path component (basename-only, same scope
/// .no-sync already has) or, for a pattern starting with `/`, the whole
/// path from the sync root -- e.g. "/stuff/secrets/plotting" excludes
/// exactly that path without needing the tree built locally first to
/// place a .no-sync file inside it, and without scattering .no-sync
/// files through a deep remote-heavy tree at all. Root-anchored patterns
/// reuse the exact same glob engine as basename ones, which has one
/// real consequence worth knowing rather than hitting by surprise: `*`
/// matches any run of characters INCLUDING `/`, so "/stuff/*" also
/// matches "stuff/secrets/plotting/deep/file" -- there's no `**`
/// distinction. Deliberate: a second path-aware glob dialect is more
/// syntax to get wrong for a feature nothing here has asked for yet.
///
/// Only `*` (any run of characters, including none) and `?` (exactly
/// one character) are supported -- no character classes, no brace
/// expansion -- since nothing in Nextcloud Desktop's own default list
/// needs more than that, and every extra bit of syntax is one more
/// thing to get wrong.
///
/// A trailing `/` on the pattern restricts it to directories only --
/// "temp/" matches a directory named exactly "temp" but not a file of
/// that name; "temp" (no slash) matches either. Mirrors Nextcloud's own
/// sync-exclude.lst syntax for that one piece; their `]` auto-delete
/// prefix is deliberately NOT mirrored (see config.rs's
/// DEFAULT_EXCLUDE_PATTERNS doc comment for why).
///
/// The whole name (or whole path, for a root-anchored pattern) must
/// match the whole pattern, same as .gitignore or shell globbing -- no
/// implicit substring search.
pub fn matches_pattern(pattern: &str, relative_path: &str, name: &str, is_directory: bool) -> bool {
    let (pattern, directory_only) = match pattern.strip_suffix('/') {
        Some(stripped) => (stripped, true),
        None => (pattern, false),
    };
    if directory_only && !is_directory {
        return false;
    }
    match pattern.strip_prefix('/') {
        Some(anchored) => glob_match(anchored.as_bytes(), relative_path.as_bytes()),
        None => glob_match(pattern.as_bytes(), name.as_bytes()),
    }
}

/// Sanity-checks pattern *shape* before it's ever added to the list.
/// There's no real "invalid syntax" to catch here -- matches_pattern's
/// grammar (`*`, `?`, optional leading/trailing `/`) is permissive
/// enough that any string parses as *something* -- so this instead
/// catches the specific mistakes that silently produce a pattern which
/// can never match anything, which is a much easier trap to fall into
/// than it sounds. Doesn't know about the existing pattern list --
/// checking for an exact duplicate is main.rs's job, since that needs
/// live config state this module deliberately stays free of.
///
/// This accepts a leading `/` (root-anchored) -- appropriate for
/// per-connection patterns (SyncConnection::excluded_subpaths), where
/// "the sync root" is unambiguous. See validate_global_pattern below for
/// the global list, where it isn't.
pub fn validate_pattern(text: &str) -> Result<(), String> {
    let (body, _directory_only) = match text.strip_suffix('/') {
        Some(stripped) => (stripped, true),
        None => (text, false),
    };
    let anchored = body.starts_with('/');
    let core = if anchored { &body[1..] } else { body };

    if core.is_empty() {
        return Err("Matches nothing -- there's nothing left after the leading/trailing \"/\".".to_string());
    }
    // A name (not a full path) can never actually contain "/" -- one
    // filesystem component's name is bounded by its separators, by
    // definition. So a pattern with a "/" in it that ISN'T anchored is
    // being matched against `name` (see matches_pattern), which is
    // guaranteed to never contain the "/" the pattern is looking for --
    // a plausible, easy mistake (forgetting the leading "/" while typing
    // a path), not a deliberately weird pattern, so worth catching
    // rather than silently accepting a pattern that can never do anything.
    if !anchored && core.contains('/') {
        return Err(
            "Contains \"/\" but doesn't start with one, so as a plain name it can never \
             match anything -- start it with \"/\" to match a full path from the sync root."
                .to_string(),
        );
    }
    Ok(())
}

/// Global-list variant of validate_pattern: additionally rejects a
/// leading `/` outright, rather than accepting it. Root-anchoring means
/// "relative to the sync root" -- fine when there's exactly one sync
/// root a pattern could possibly mean (a single connection), meaningless
/// when applied globally across every connection at once, each with its
/// own different root. A global "/secrets/plotting" would silently mean
/// something different (or nothing at all) depending on which
/// connection's round happens to be running -- confusing regardless of
/// whether it happens to match by coincidence. Per-connection anchored
/// patterns (SyncConnection::excluded_subpaths) don't go through this
/// function at all -- they're built by the app from an actual picked
/// path (see the folder-exclusion pickers), never typed freehand, so
/// there's no UI path for a user to type a leading "/" into the global
/// list in the first place; this is the backstop, not the only guard.
pub fn validate_global_pattern(text: &str) -> Result<(), String> {
    if text.starts_with('/') {
        return Err("Patterns starting with \"/\" aren't supported here.".to_string());
    }
    validate_pattern(text)
}


/// to read in one sitting on purpose, fine at filename lengths.
fn glob_match(pattern: &[u8], name: &[u8]) -> bool {
    match (pattern.first(), name.first()) {
        (None, None) => true,
        (Some(b'*'), _) => {
            glob_match(&pattern[1..], name) || (!name.is_empty() && glob_match(pattern, &name[1..]))
        }
        (Some(b'?'), Some(_)) => glob_match(&pattern[1..], &name[1..]),
        (Some(&p), Some(&n)) if p == n => glob_match(&pattern[1..], &name[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod pattern_tests {
    use super::*;

    #[test]
    fn whole_name_only() {
        assert!(matches_pattern("temp", "temp", "temp", true));
        assert!(!matches_pattern("temp", "first-temp", "first-temp", true));
        assert!(matches_pattern("*temp", "first-temp", "first-temp", true));
    }

    #[test]
    fn trailing_slash_is_directory_only() {
        assert!(matches_pattern("temp/", "temp", "temp", true));
        assert!(!matches_pattern("temp/", "temp", "temp", false));
        assert!(matches_pattern("temp", "temp", "temp", false));
    }

    #[test]
    fn star_and_question_mark() {
        assert!(matches_pattern("*.tmp", "foo.tmp", "foo.tmp", false));
        assert!(!matches_pattern("*.tmp", "foo.tmpx", "foo.tmpx", false));
        assert!(matches_pattern(".*.sw?", ".file.swp", ".file.swp", false));
    }

    #[test]
    fn root_anchored_path() {
        // AJ's motivating case: exclude a specific deep path without
        // needing it built locally first to drop a .no-sync file inside it.
        assert!(matches_pattern("/stuff/secrets/plotting", "stuff/secrets/plotting", "plotting", true));
        // Same basename elsewhere on the tree is untouched.
        assert!(!matches_pattern("/stuff/secrets/plotting", "stuff/sharable/plotting", "plotting", true));
        // A basename-only pattern still only sees the basename, never the full path.
        assert!(!matches_pattern("plotting", "elsewhere/plotting-but-not-quite", "plotting-but-not-quite", true));
    }

    #[test]
    fn root_anchored_directory_only() {
        assert!(matches_pattern("/stuff/secrets/", "stuff/secrets", "secrets", true));
        assert!(!matches_pattern("/stuff/secrets/", "stuff/secrets", "secrets", false));
    }

    #[test]
    fn validate_accepts_ordinary_patterns() {
        assert!(validate_pattern("*.tmp").is_ok());
        assert!(validate_pattern("node_modules/").is_ok());
        assert!(validate_pattern("/stuff/secrets/plotting").is_ok());
        assert!(validate_pattern("/stuff/secrets/").is_ok());
    }

    #[test]
    fn validate_global_rejects_root_anchored() {
        // The whole reason this variant exists: a leading "/" means
        // "relative to *a* sync root," which is ambiguous the moment
        // there's more than one connection with a different root.
        assert!(validate_global_pattern("/stuff/secrets/plotting").is_err());
        assert!(validate_global_pattern("/stuff/secrets/").is_err());
        // Ordinary patterns are unaffected -- same acceptance as the
        // per-connection variant for anything that isn't root-anchored.
        assert!(validate_global_pattern("*.tmp").is_ok());
        assert!(validate_global_pattern("node_modules/").is_ok());
    }

    #[test]
    fn validate_rejects_unanchored_path_looking_pattern() {
        // The easy-to-make mistake this exists to catch: typing a path
        // but forgetting the leading "/" that makes it root-anchored --
        // as written, this could never match anything.
        assert!(validate_pattern("stuff/secrets/plotting").is_err());
        assert!(validate_pattern("stuff/secrets/").is_err());
    }

    #[test]
    fn validate_rejects_empty_after_anchors_stripped() {
        assert!(validate_pattern("/").is_err());
        assert!(validate_pattern("//").is_err());
    }
}

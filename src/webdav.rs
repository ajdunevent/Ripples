//! Direct WebDAV client, replacing `gio`/`gvfsd-dav` as the remote side
//! of sync entirely. Nothing in this module touches GVfs, GNOME's
//! virtual filesystem layer, or any mount at all -- every operation here
//! is a single, plain HTTP request against the account's own WebDAV
//! endpoint, using credentials pulled once from GOA
//! (`GoaWatcher::get_password_for_http_auth`) and passed in directly.
//!
//! Exists because gvfsd-dav turned out to be the source of nearly every
//! GVfs-flavored bug found across this project's testing so far: a
//! mount that silently wedges at 0% CPU with no error at all; a
//! client-side cache reporting a directory listing accurately while the
//! mount's own DELETE handling still, repeatedly, disagreed with that
//! same listing; "Backend currently unmounting" collateral failures on
//! completely unrelated concurrent operations sharing the same mount.
//! All of that is a single daemon's own internal state, invisible to
//! and uncontrollable by this app -- a plain HTTP request has none of
//! it: every operation here is exactly the request it looks like, with
//! this app owning its own retry, timeout, and concurrency behavior
//! directly instead of trusting an intermediary to get those right.
//!
//! Built on `reqwest`'s `blocking` client, not `ureq` -- ureq was the
//! original choice here, and this module was fully written and working
//! against it, but real-world testing turned up a hard dealbreaker
//! found directly in `ureq-proto`'s own source (a dependency of ureq
//! that encodes the request line): it hardcodes a fixed allowlist of
//! methods per HTTP version (GET/HEAD/POST, plus PUT/DELETE/CONNECT/
//! OPTIONS/TRACE/PATCH for 1.1) and rejects anything outside it
//! outright -- confirmed still true as of ureq-proto 0.6.0, its current
//! latest release at the time, not something fixed in a newer version.
//! PROPFIND/MKCOL/MOVE, the WebDAV extension methods this whole client
//! exists to send, simply could not be sent through ureq at all, no
//! matter how the request was built on this app's own side. hyper (what
//! reqwest is built on) was checked the same way before committing to
//! this rewrite -- it only ever special-cases GET/HEAD/CONNECT for
//! their own RFC-mandated response-body quirks, never rejects a method
//! outright.
//!
//! Deliberately plain, synchronous functions throughout, same as
//! before -- reqwest's `blocking` client manages its own internal async
//! runtime per call entirely on its own, so nothing here needs `async`
//! or any awareness of that at all. Same convention as elsewhere in
//! this app: the *caller* wraps a call in `run_blocking` (a fresh,
//! one-off OS thread) rather than this module doing that internally,
//! keeping this a plain client library decoupled from any particular
//! calling convention.
//!
//! Scope of this module as it stands: the plain WebDAV verbs
//! (PROPFIND/GET/PUT/DELETE/MOVE/MKCOL), server-agnostic, nothing
//! Nextcloud-specific. Nextcloud's own additions on top of this --
//! `oc:fileid` as a stable, rename-surviving identity, and the chunked
//! upload API for real resumability on large transfers -- are a
//! deliberate follow-up, not yet included here.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// App-wide bandwidth cap, in bytes/sec -- None (the default) means
/// unlimited. Shared across every request this module makes, not
/// per-request or per-thread: the whole point of a cap is bounding the
/// *aggregate* traffic this app produces, not letting each of several
/// concurrent transfers independently consume the full configured
/// rate. Set via set_bandwidth_limit, read by throttle() below -- a
/// plain Mutex<Option<u64>>, not something requiring the heavier
/// token-bucket state just below, since this value only ever changes
/// when the user edits Settings, not on every read.
static BANDWIDTH_LIMIT_BYTES_PER_SEC: Mutex<Option<u64>> = Mutex::new(None);

/// Token-bucket state for the limiter above -- separate from the limit
/// value itself since this needs `Instant::now()` to initialize
/// (`available`/`last_refill`), which isn't callable in a const
/// context the way a plain `Mutex::new(None)` is. Lazily initialized on
/// first actual use via OnceLock instead, rather than requiring a
/// newer Rust than this project otherwise needs just for a `LazyLock`.
struct BandwidthLimiterState {
    /// How many bytes' worth of "credit" are available to spend right
    /// now without sleeping, replenished continuously based on real
    /// elapsed time since the last check.
    available: f64,
    last_refill: std::time::Instant,
}
fn limiter_state() -> &'static Mutex<BandwidthLimiterState> {
    static STATE: OnceLock<Mutex<BandwidthLimiterState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(BandwidthLimiterState { available: 0.0, last_refill: std::time::Instant::now() }))
}

/// Sets (or clears, via None) the app-wide bandwidth cap -- called from
/// main.rs whenever the Settings value changes, and once at startup
/// with whatever was loaded from config. Doesn't touch the token-bucket
/// state itself: a change mid-transfer just means the very next
/// throttle() call starts refilling at the new rate from that point on,
/// not a hard reset of whatever credit happened to already be banked.
pub fn set_bandwidth_limit(bytes_per_sec: Option<u64>) {
    *BANDWIDTH_LIMIT_BYTES_PER_SEC.lock().unwrap() = bytes_per_sec;
}

/// Blocks the calling thread until `n` more bytes are "allowed" under
/// the configured cap -- called with however many bytes were *just*
/// read or written, not before the fact, so a transfer already well
/// under the cap never has to wait at all; it only ever catches up on
/// credit already spent past what's been earned since the last check.
/// Applied uniformly to every response body and upload body this
/// module handles (see each function's own call to this), not just
/// transfer payloads -- a deliberate, comprehensive cap rather than one
/// that only covers the common case.
///
/// A capped burst allowance (up to 2 seconds' worth of saved-up credit)
/// means a brief lull before a request doesn't force the next one into
/// perfectly robotic, tick-by-tick pacing, while still keeping the
/// sustained average genuinely bounded at the configured rate rather
/// than letting unlimited banked credit erase the cap's own point over
/// a long enough idle stretch.
fn throttle(n: usize) {
    let Some(limit) = *BANDWIDTH_LIMIT_BYTES_PER_SEC.lock().unwrap() else {
        return; // no cap configured -- the common case stays a single, cheap lock+compare
    };
    if limit == 0 {
        // A configured cap of literally zero would mean "never send
        // anything, ever," which isn't a sensible reading of a
        // bandwidth *limit* -- treated the same as unlimited rather
        // than deadlocking every transfer forever.
        return;
    }
    let mut state = limiter_state().lock().unwrap();
    let now = std::time::Instant::now();
    let elapsed = now.duration_since(state.last_refill).as_secs_f64();
    state.available = (state.available + elapsed * limit as f64).min(limit as f64 * 2.0);
    state.last_refill = now;
    state.available -= n as f64;
    if state.available < 0.0 {
        let deficit = -state.available;
        let sleep_secs = deficit / limit as f64;
        state.available = 0.0;
        drop(state); // released before sleeping, so this thread's own wait doesn't also block every other thread's throttle() call
        std::thread::sleep(std::time::Duration::from_secs_f64(sleep_secs));
    }
}

/// Wraps any Read, calling throttle() after each chunk actually read.
struct ThrottledReader<R> {
    inner: R,
}
impl<R: Read> Read for ThrottledReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            throttle(n);
        }
        Ok(n)
    }
}

/// Reads a whole response body as UTF-8 text, throttled the same as
/// every other read in this module -- the throttled equivalent of
/// reqwest's own convenience `Response::text()`, which reads the whole
/// body internally with no hook to apply a cap to.
fn read_throttled_to_string(response: reqwest::blocking::Response) -> Result<String, WebDavError> {
    let mut out = String::new();
    ThrottledReader { inner: response }.read_to_string(&mut out)?;
    Ok(out)
}

/// Same as read_throttled_to_string, for a body that isn't necessarily
/// valid UTF-8 -- the throttled equivalent of reqwest's own `.bytes()`.
fn read_throttled_to_vec(response: reqwest::blocking::Response) -> Result<Vec<u8>, WebDavError> {
    let mut out = Vec::new();
    ThrottledReader { inner: response }.read_to_end(&mut out)?;
    Ok(out)
}

/// Every way a single WebDAV operation in this module can fail.
/// Deliberately not folded into sync.rs's own SyncError yet -- see
/// this module's own top comment on scope; wiring these together is
/// part of the later call-site migration, not this foundational pass.
#[derive(Debug)]
pub enum WebDavError {
    /// Anything reqwest itself failed on -- connection refused, TLS
    /// failure, one of the timeouts below actually firing, a malformed
    /// header value caught before anything was sent, and so on. A
    /// single variant covers all of these: unlike ureq (which split
    /// "request never got built" from "request failed once sent" into
    /// two separate error types from two different crates), reqwest's
    /// RequestBuilder methods are infallible to chain and only ever
    /// surface a problem -- of any kind -- from the final `.send()`
    /// call, confirmed directly against its real source rather than
    /// assumed from familiarity with a different HTTP crate's API
    /// shape.
    Transport(reqwest::Error),
    /// Reading or writing the local side of a transfer (the file being
    /// uploaded from, or downloaded to).
    Io(std::io::Error),
    /// A response body that claimed to be the XML this module itself
    /// asked for, but couldn't actually be parsed as such.
    Xml(quick_xml::Error),
    /// The server responded, successfully, with a status this
    /// operation didn't expect -- carries the path involved and the
    /// status code, not just a bare number, since a 404 while
    /// downloading and a 404 while deleting call for different
    /// handling upstream.
    UnexpectedStatus { path: String, status: u16 },
}

impl std::fmt::Display for WebDavError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // reqwest::Error's own Display is deliberately terse -- just
            // "error sending request for url (...)" with no indication
            // of *why* (timed out, connection reset, DNS failure, TLS
            // handshake failure) -- confirmed as a real, reported gap:
            // logs from real transient failures showed literally nothing
            // beyond that one generic line, no way to tell one cause
            // from another. The actual reason lives in the source chain
            // (std::error::Error::source()), which reqwest's own Error
            // does populate -- walking it and appending each layer is
            // what actually makes a failure log line diagnosable.
            WebDavError::Transport(e) => {
                write!(f, "WebDAV transport error: {e}")?;
                let mut source = std::error::Error::source(e);
                while let Some(s) = source {
                    write!(f, ": {s}")?;
                    source = s.source();
                }
                Ok(())
            }
            WebDavError::Io(e) => write!(f, "local I/O error: {e}"),
            WebDavError::Xml(e) => write!(f, "malformed XML response: {e}"),
            WebDavError::UnexpectedStatus { path, status } => write!(f, "unexpected HTTP {status} for {path}"),
        }
    }
}
impl From<reqwest::Error> for WebDavError { fn from(e: reqwest::Error) -> Self { WebDavError::Transport(e) } }
impl From<std::io::Error> for WebDavError { fn from(e: std::io::Error) -> Self { WebDavError::Io(e) } }
impl From<quick_xml::Error> for WebDavError { fn from(e: quick_xml::Error) -> Self { WebDavError::Xml(e) } }

/// Retries once on a transient transport failure OR an unexpected HTTP
/// status -- by request, extended from Transport-only: a genuine 401/403
/// doesn't necessarily mean a real, persistent permissions problem (a
/// session/app-password token expiring mid-request, or a reverse proxy/
/// WAF enforcing its own size or timeout limit ahead of the actual
/// server, can both produce a perfectly real, accurate status code for a
/// reason that has nothing to do with the path's own permissions), and
/// this app's own is_permission_error classification takes that status
/// at face value once the retry itself is exhausted -- confirmed
/// directly against a real session's own logs: 86 "error sending
/// request" failures against 674 successes on one connection over about
/// 11 minutes, every single one self-recovering by the next scheduled
/// round anyway -- exactly the shape a plain "try again right now"
/// resolves far faster than waiting on; there's no reason to assume an
/// UnexpectedStatus can't be the exact same shape of transient blip.
/// Deliberately still only ONE retry -- a genuinely, persistently
/// forbidden/down resource still surfaces as a failure promptly, not a
/// loop, just with one extra, cheap attempt first. Not used for upload/
/// download -- retrying a large, partway-failed transfer wholesale is a
/// different, more expensive tradeoff than retrying a cheap metadata
/// call, and this app's own next-round retry already covers that case
/// reasonably.
fn retry_once<T>(f: impl Fn() -> Result<T, WebDavError>) -> Result<T, WebDavError> {
    match f() {
        Err(WebDavError::Transport(_)) => f(),
        Err(WebDavError::UnexpectedStatus { .. }) => f(),
        other => other,
    }
}

/// One entry from a directory listing.
#[derive(Debug, Clone)]
pub struct RemoteEntry {
    pub is_dir: bool,
    pub etag: Option<String>,
    pub size: i64,
    pub mtime: Option<i64>,
    /// oc:fileid -- a server-assigned, rename-surviving identity,
    /// requested in every PROPFIND this module sends alongside the
    /// standard DAV: properties. Genuinely optional: confirmed directly
    /// (not assumed) that oc: is ownCloud's own namespace, predating
    /// Nextcloud's fork, and both projects' current developer docs
    /// still document oc:fileid -- but a plain WebDAV server with
    /// neither simply never returns it, and per WebDAV's own semantics
    /// for an unrequested-but-unsupported property, that shows up here
    /// as this field just staying None, not as any kind of error or
    /// missing-property failure. Every caller already treats None as
    /// "can't corroborate this way," the same tolerance already used
    /// for a missing etag or mtime -- this app works identically
    /// without it, just without the extra corroboration/rename-
    /// detection benefit this field enables where a server does report
    /// it.
    pub fileid: Option<String>,
}

/// For every metadata-only call (PROPFIND, MKCOL, DELETE, MOVE) -- a
/// single overall request timeout is safe and desirable here, since
/// none of these ever move a meaningful amount of body data.
///
/// Built once and cached, not reconstructed on every call -- confirmed
/// directly against reqwest's own documentation that this matters, not
/// just a style preference: "The Client holds a connection pool
/// internally, so it is advised that you create one and reuse it."
/// Rebuilding a fresh Client per call was a real, confirmed cost here,
/// not a theoretical one -- every metadata-only call this app makes
/// (PROPFIND for every directory listing, MKCOL, DELETE, MOVE) was
/// paying a fresh TCP+TLS handshake instead of reusing an existing,
/// warm connection to the same server, for a sync round that can easily
/// involve hundreds of these in a row against one host. Infallible, not
/// `Result`-returning like this used to be -- a `.build()` failure here
/// is a fixed, environment-level problem (TLS backend or DNS resolver
/// config), not something that varies per call or that this app could
/// meaningfully recover from differently the second time; matches the
/// same precedent push.rs's own client() already established for the
/// identical situation.
fn metadata_client() -> reqwest::blocking::Client {
    static CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("a client with just timeouts configured should never fail to build")
    }).clone()
}

/// For upload/download specifically -- connect_timeout only, no overall
/// `.timeout()` at all. reqwest's own `.timeout()` bounds the *entire*
/// request including body transfer, not a per-chunk or no-progress
/// window (confirmed directly against its docs/source, the same
/// distinction ureq's own timeout_send_body/timeout_recv_body already
/// required getting right) -- setting one short enough to catch a
/// genuine hang would also abort a large, slow, but perfectly healthy
/// multi-gigabyte transfer partway through. Stall detection for a
/// transfer actually in progress belongs at the application layer
/// (checking for forward progress within the read/write loop), not as
/// a single timer over a duration that can legitimately be arbitrarily
/// long.
///
/// Cached the same way, same reasoning, as metadata_client just above --
/// a separate OnceLock, not the same one, since these are two distinctly
/// configured clients (this one has no overall .timeout() at all), not
/// interchangeable instances of the same configuration.
fn transfer_client() -> reqwest::blocking::Client {
    static CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .build()
            .expect("a client with just a connect timeout configured should never fail to build")
    }).clone()
}

/// Converts GOA's own `davs://user@host:port/path` Files:Uri shape
/// (confirmed via GoaAccountInfo's own doc comment, e.g.
/// "davs://aj@cloud.dunevent.net:443/remote.php/webdav/") into a plain
/// `https://` URL. gvfsd-dav needed the `davs://` scheme, and the
/// leading `user@` userinfo, to know how to mount the account; neither
/// belongs in an ordinary request URI for a client that isn't mounting
/// anything -- the scheme just becomes https (davs -> https, dav ->
/// http, the same mapping GVfs itself uses internally), and the
/// userinfo is redundant with the Authorization every request in this
/// module already sends via `.basic_auth()`.
pub fn davs_to_https(files_uri: &str) -> Option<String> {
    let (scheme, rest) = if let Some(rest) = files_uri.strip_prefix("davs://") {
        ("https", rest)
    } else if let Some(rest) = files_uri.strip_prefix("dav://") {
        ("http", rest)
    } else {
        return None;
    };
    let rest = match rest.find('@') {
        Some(idx) => &rest[idx + 1..],
        None => rest,
    };
    if rest.is_empty() {
        return None;
    }
    Some(format!("{scheme}://{rest}"))
}

/// Pulls the username back out of GOA's own `davs://user@host:port/path`
/// Files:Uri -- the userinfo portion davs_to_https above deliberately
/// strips before building a request URL, since it doesn't belong in an
/// ordinary request URI, but the actual username itself is still needed
/// for the Authorization header every request sends. Sourced from here
/// rather than GoaAccountInfo's own `identity` field: files_uri is the
/// one GVfs itself was confirmed to authenticate correctly against (see
/// GoaAccountInfo's own doc comment), so it's the more trustworthy
/// source for what the server actually expects as a username, rather
/// than assuming `identity`'s own display-oriented format matches.
pub fn extract_username(files_uri: &str) -> Option<String> {
    let rest = files_uri.strip_prefix("davs://").or_else(|| files_uri.strip_prefix("dav://"))?;
    let at = rest.find('@')?;
    let username = &rest[..at];
    if username.is_empty() { None } else { Some(username.to_string()) }
}

/// Percent-encodes a single path *segment* -- everything between two
/// '/' characters, never the '/' itself. Deliberately hand-rolled
/// rather than pulling in the `percent-encoding` (or `url`) crate for
/// this alone: the scope is genuinely narrow (encode on the way out
/// building a request URI, decode on the way in reading a `<d:href>`
/// back from the server) and small enough to keep self-contained and
/// directly readable, rather than pulling in general-purpose URL
/// parsing this app has no other need for.
fn percent_encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The inverse of percent_encode_segment, applied to a whole path (not
/// segment-by-segment, since '%'-escapes don't interact with '/' at
/// all -- decoding the full string in one pass is both simpler and
/// correct). Invalid UTF-8 after decoding falls back to the
/// lossily-converted result rather than failing the whole listing over
/// one oddly-named entry -- matches list_local's own established
/// tolerance elsewhere in this app (to_string_lossy) for exactly this
/// class of "don't let one bad name break everything else" situation.
pub fn percent_decode(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(value) = u8::from_str_radix(hex, 16) {
                    out.push(value);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Joins `base` (an account's own WebDAV root, from davs_to_https, or
/// any URL already built by a previous call to this same function) with
/// a relative path, percent-encoding each segment individually so
/// filenames containing spaces, unicode, or reserved URI characters
/// (all confirmed present in this app's own real test tree -- "FLX1s
/// Helpers", "Amazon Store Card.txt") end up correctly encoded rather
/// than producing a malformed request URI. Public: this is how sync.rs
/// itself is expected to build every URL it passes into this module's
/// other functions, not just something used internally here -- the
/// direct replacement for the `.child(name)` chaining gio::File calls
/// used to do.
pub fn build_url(base: &str, relative_path: &str) -> String {
    let base = base.trim_end_matches('/');
    if relative_path.is_empty() {
        return base.to_string();
    }
    let encoded: Vec<String> = relative_path.split('/').map(percent_encode_segment).collect();
    format!("{base}/{}", encoded.join("/"))
}

/// Strips the last path segment off a full URL -- e.g.
/// "https://host/remote.php/webdav/Downloads/files/main.rs" ->
/// "https://host/remote.php/webdav/Downloads/files". Used to build a
/// sibling temp-file URL in the same directory as a real upload target,
/// the direct replacement for gio::File::parent() -- returns None only
/// if there's genuinely no '/' left to strip past the scheme (a
/// malformed or bare-root URL), the same "can't sensibly answer this"
/// case parent() itself returns None for.
pub fn parent_url(url: &str) -> Option<String> {
    let scheme_end = url.find("://")? + 3;
    let last_slash = url[scheme_end..].rfind('/')? + scheme_end;
    if last_slash <= scheme_end {
        return None;
    }
    Some(url[..last_slash].to_string())
}

/// Parses an HTTP-date (RFC 7231's own required format for
/// `Last-Modified`/WebDAV's `getlastmodified` -- the fixed-length,
/// fixed-field IMF-fixdate shown in RFC 7231 §7.1.1.1, e.g. "Sat, 03
/// Aug 2026 22:40:31 GMT") into a Unix timestamp. Hand-rolled rather
/// than pulling in a date-parsing crate for one fixed, well-known
/// format this app's own server always sends in exactly this shape (a
/// requirement of the HTTP spec itself, not a Nextcloud-specific
/// convention) -- always UTC (the literal "GMT" suffix is part of the
/// fixed format, not something that varies), so no timezone-offset
/// handling is needed beyond that. Returns None on anything that
/// doesn't match this exact shape rather than guessing -- a mtime this
/// module can't confidently parse should read as "unknown," the same
/// as sync.rs's own has_attribute guards elsewhere in this app treat a
/// genuinely absent value, not as a wrong one.
fn parse_http_date(s: &str) -> Option<i64> {
    // "Sat, 03 Aug 2026 22:40:31 GMT"
    let s = s.trim();
    let comma = s.find(", ")?;
    let rest = &s[comma + 2..];
    let mut parts = rest.split_whitespace();
    let day: i64 = parts.next()?.parse().ok()?;
    let month = match parts.next()? {
        "Jan" => 1, "Feb" => 2, "Mar" => 3, "Apr" => 4, "May" => 5, "Jun" => 6,
        "Jul" => 7, "Aug" => 8, "Sep" => 9, "Oct" => 10, "Nov" => 11, "Dec" => 12,
        _ => return None,
    };
    let year: i64 = parts.next()?.parse().ok()?;
    let time = parts.next()?;
    let tz = parts.next()?;
    if tz != "GMT" {
        return None;
    }
    let mut time_parts = time.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next()?.parse().ok()?;

    // Days since the Unix epoch via a standard civil-calendar algorithm
    // (Howard Hinnant's days_from_civil, the well-known closed-form
    // conversion -- no calendar library needed for this, and it's
    // correct for the full proleptic Gregorian range this app could
    // ever actually see).
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as i64; // [0, 399]
    let mp = (month + 9) % 12; // [0, 11], Mar=0 .. Feb=11
    let doy = (153 * mp + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days_since_epoch = era * 146097 + doe - 719468;

    Some(days_since_epoch * 86400 + hour * 3600 + minute * 60 + second)
}

/// Depth-1 PROPFIND -- immediate children only, matching
/// `gio::File::enumerate_children_future`'s own semantics (not the
/// whole subtree). `dir_url` is this directory's own full URL (already
/// built via build_url by the caller, since the caller is the one that
/// knows the relative path being listed).
pub fn list(dir_url: &str, username: &str, password: &str) -> Result<HashMap<String, RemoteEntry>, WebDavError> {
    let body = r#"<?xml version="1.0"?><d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:getetag/><d:getcontentlength/><d:getlastmodified/><d:resourcetype/><oc:fileid/></d:prop></d:propfind>"#;
    let method = reqwest::Method::from_bytes(b"PROPFIND").expect("PROPFIND is a valid HTTP method token");
    let response = retry_once(|| {
        Ok(metadata_client()
            .request(method.clone(), dir_url)
            .basic_auth(username, Some(password))
            .header("Depth", "1")
            .header("Content-Type", "application/xml")
            .body(body)
            .send()?)
    })?;
    let status = response.status().as_u16();
    if status != 207 {
        return Err(WebDavError::UnexpectedStatus { path: dir_url.to_string(), status });
    }
    let xml = read_throttled_to_string(response)?;
    parse_propfind_listing(&xml, dir_url)
}

/// Splits a PROPFIND multistatus response into a per-child map, keyed
/// by decoded, URL-relative name (not the full href) -- the same key
/// shape sync.rs's own list_local/list_remote already use. Matched by
/// local_name() throughout, never a hardcoded namespace-prefix string
/// (confirmed via quick-xml's own source that this correctly ignores
/// whichever prefix a given server happens to use for the DAV:
/// namespace, rather than assuming Nextcloud's own "d:" convention is
/// universal).
///
/// The first `<d:response>` in a Depth:1 listing is the directory
/// itself, self-describing -- explicitly excluded by comparing its own
/// decoded href against dir_url's own path, the same way
/// gio::File::enumerate_children_future never includes the directory
/// being listed among its own results.
fn parse_propfind_listing(xml: &str, dir_url: &str) -> Result<HashMap<String, RemoteEntry>, WebDavError> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let self_path = url_path(dir_url).trim_end_matches('/').to_string();

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut out = HashMap::new();
    let mut href: Option<String> = None;
    let mut etag: Option<String> = None;
    let mut size: Option<i64> = None;
    let mut mtime: Option<i64> = None;
    let mut fileid: Option<String> = None;
    let mut is_dir = false;
    let mut in_resourcetype = false;
    let mut current_tag: Option<Vec<u8>> = None;

    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(e) | Event::Empty(e) => {
                let name = e.local_name().as_ref().to_vec();
                match name.as_slice() {
                    b"response" => {
                        href = None; etag = None; size = None; mtime = None; fileid = None; is_dir = false;
                    }
                    b"resourcetype" => in_resourcetype = true,
                    b"collection" if in_resourcetype => is_dir = true,
                    _ => {}
                }
                current_tag = Some(name);
            }
            Event::Text(t) => {
                if let Some(tag) = &current_tag {
                    let text = t.unescape()?.into_owned();
                    match tag.as_slice() {
                        b"href" => href = Some(text),
                        b"getetag" => etag = Some(text.trim_matches('"').to_string()),
                        b"getcontentlength" => size = text.parse().ok(),
                        b"getlastmodified" => mtime = parse_http_date(&text),
                        b"fileid" => fileid = Some(text),
                        _ => {}
                    }
                }
            }
            Event::End(e) => {
                let name = e.local_name().as_ref().to_vec();
                if name == b"resourcetype" {
                    in_resourcetype = false;
                }
                if name == b"response" {
                    if let Some(href_val) = &href {
                        let decoded = percent_decode(href_val);
                        let path = decoded.trim_end_matches('/').to_string();
                        if path != self_path {
                            if let Some(child_name) = path.rsplit('/').next() {
                                if !child_name.is_empty() {
                                    out.insert(child_name.to_string(), RemoteEntry {
                                        is_dir,
                                        etag: etag.clone(),
                                        size: size.unwrap_or(0),
                                        mtime,
                                        fileid: fileid.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
                current_tag = None;
            }
            _ => {}
        }
    }
    Ok(out)
}

/// The path component of a full URL -- e.g.
/// "https://host/remote.php/webdav/Downloads" -> "/remote.php/webdav/Downloads".
/// Used only to compare against a decoded `<d:href>` from the same
/// server, both un-percent-decoded at this point on the URL side (the
/// href is decoded separately in parse_propfind_listing) -- this is
/// intentionally the minimal amount of URL handling needed for that
/// one comparison, not a general-purpose URL parser.
fn url_path(url: &str) -> String {
    match url.find("://").and_then(|scheme_end| url[scheme_end + 3..].find('/')) {
        Some(host_end) => percent_decode(&url[url.find("://").unwrap() + 3 + host_end..]),
        None => String::new(),
    }
}

/// Depth-0 PROPFIND on a single item (file or directory) -- the direct
/// replacement for gio::File::query_info_future, used anywhere this app
/// needs one item's own current metadata (most commonly its ETag right
/// after a MOVE, which -- unlike PUT -- doesn't return one in its own
/// response headers). Reuses the exact same request body and property
/// set as list()'s own Depth:1 listing; the only real difference is
/// Depth:0 (this resource itself, not its children) and that the
/// single response block describes the resource being asked about
/// directly, not something to exclude the way list()'s own
/// self-referencing first entry is.
pub fn stat(url: &str, username: &str, password: &str) -> Result<RemoteEntry, WebDavError> {
    let body = r#"<?xml version="1.0"?><d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:getetag/><d:getcontentlength/><d:getlastmodified/><d:resourcetype/><oc:fileid/></d:prop></d:propfind>"#;
    let method = reqwest::Method::from_bytes(b"PROPFIND").expect("PROPFIND is a valid HTTP method token");
    let response = retry_once(|| {
        Ok(metadata_client()
            .request(method.clone(), url)
            .basic_auth(username, Some(password))
            .header("Depth", "0")
            .header("Content-Type", "application/xml")
            .body(body)
            .send()?)
    })?;
    let status = response.status().as_u16();
    if status != 207 {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    }
    let xml = read_throttled_to_string(response)?;
    parse_propfind_self(&xml, url)
}

/// Parses a Depth:0 PROPFIND's own single-entry multistatus -- same
/// event loop shape as parse_propfind_listing, just without that
/// function's own self-exclusion logic, since here the one entry
/// present *is* what's being asked about, not something to skip.
fn parse_propfind_self(xml: &str, url: &str) -> Result<RemoteEntry, WebDavError> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut etag: Option<String> = None;
    let mut size: Option<i64> = None;
    let mut mtime: Option<i64> = None;
    let mut fileid: Option<String> = None;
    let mut is_dir = false;
    let mut in_resourcetype = false;
    let mut current_tag: Option<Vec<u8>> = None;
    let mut saw_response = false;

    loop {
        match reader.read_event()? {
            Event::Eof => break,
            Event::Start(e) | Event::Empty(e) => {
                let name = e.local_name().as_ref().to_vec();
                match name.as_slice() {
                    b"response" => saw_response = true,
                    b"resourcetype" => in_resourcetype = true,
                    b"collection" if in_resourcetype => is_dir = true,
                    _ => {}
                }
                current_tag = Some(name);
            }
            Event::Text(t) => {
                if let Some(tag) = &current_tag {
                    let text = t.unescape()?.into_owned();
                    match tag.as_slice() {
                        b"getetag" => etag = Some(text.trim_matches('"').to_string()),
                        b"getcontentlength" => size = text.parse().ok(),
                        b"getlastmodified" => mtime = parse_http_date(&text),
                        b"fileid" => fileid = Some(text),
                        _ => {}
                    }
                }
            }
            Event::End(e) => {
                let name = e.local_name().as_ref().to_vec();
                if name == b"resourcetype" {
                    in_resourcetype = false;
                }
                current_tag = None;
            }
            _ => {}
        }
    }
    if !saw_response {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status: 207 });
    }
    Ok(RemoteEntry { is_dir, etag, size: size.unwrap_or(0), mtime, fileid })
}

/// MKCOL -- creates a single collection (directory). Not recursive:
/// same contract as gio::File::make_directory_future, which this
/// replaces -- the caller is responsible for creating ancestors first
/// if needed, the same discipline sync.rs's own directory-creation
/// code already follows today.
pub fn mkcol(url: &str, username: &str, password: &str) -> Result<(), WebDavError> {
    let method = reqwest::Method::from_bytes(b"MKCOL").expect("MKCOL is a valid HTTP method token");
    let response = retry_once(|| {
        Ok(metadata_client()
            .request(method.clone(), url)
            .basic_auth(username, Some(password))
            .send()?)
    })?;
    let status = response.status().as_u16();
    if status != 201 {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    }
    Ok(())
}

/// DELETE -- a single item, file or collection. For a non-empty
/// collection this is recursive by the protocol's own design (Depth:
/// infinity is the only depth a DELETE on a collection can even
/// request) -- but see remote_delete_recursive in sync.rs (already
/// written, ahead of this module, once the gvfsd-dav-specific
/// recursive-DELETE inconsistency was confirmed) for why the migrated
/// call site won't actually rely on that: it already deletes children
/// individually and calls this function once per item, never on a
/// non-empty collection.
pub fn delete(url: &str, username: &str, password: &str) -> Result<(), WebDavError> {
    let response = retry_once(|| {
        Ok(metadata_client()
            .delete(url)
            .basic_auth(username, Some(password))
            .send()?)
    })?;
    let status = response.status().as_u16();
    // 404 treated as success -- already-gone is the outcome this
    // caller wanted, not a failure, the same tolerance list_remote's
    // own NotFound handling already applies on the read side.
    if status != 204 && status != 200 && status != 404 {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    }
    Ok(())
}

/// MOVE. `overwrite`: whether an existing item already at `to_url`
/// should be silently replaced (`Overwrite: T`) or should fail the
/// whole operation instead (`Overwrite: F`). These genuinely need to
/// differ by caller, not just be hardcoded one way -- rename/move-
/// pairing wants F (an unexpectedly-already-existing target is a real,
/// meaningful signal something's off, not something to paper over), but
/// finalizing a temp-file-first upload by moving the temp object onto
/// the real target needs T, since overwriting an existing file is the
/// ordinary case for every re-upload of already-synced content, not an
/// edge case.
///
/// `mtime`: X-OC-Mtime for this MOVE specifically -- confirmed
/// necessary directly (not assumed from the docs alone): upload's own
/// X-OC-Mtime on the PUT to the temp location does NOT survive the
/// finalizing MOVE that follows it -- the temp object arrives with the
/// right mtime, then loses it the moment it's moved onto the real
/// path, landing back at "now" regardless. Sending it again here, on
/// the MOVE itself, is what actually makes it stick. Only
/// upload_temp_first's own finalize call should ever pass Some here --
/// rename/move-pairing wants the server to keep whatever mtime the
/// object already has, not overwrite it with an unrelated value, so
/// both of those call sites pass None.
pub fn move_item(from_url: &str, to_url: &str, overwrite: bool, mtime: Option<i64>, username: &str, password: &str) -> Result<(), WebDavError> {
    let method = reqwest::Method::from_bytes(b"MOVE").expect("MOVE is a valid HTTP method token");
    let response = retry_once(|| {
        let mut request = metadata_client()
            .request(method.clone(), from_url)
            .basic_auth(username, Some(password))
            .header("Destination", to_url)
            .header("Overwrite", if overwrite { "T" } else { "F" });
        if let Some(mtime) = mtime.filter(|&t| t > 0) {
            request = request.header("X-OC-Mtime", mtime.to_string());
        }
        Ok(request.send()?)
    })?;
    let status = response.status().as_u16();
    if status != 201 && status != 204 {
        return Err(WebDavError::UnexpectedStatus { path: from_url.to_string(), status });
    }
    Ok(())
}

/// Wraps a Read (the local source file) to call `on_progress` as bytes
/// pass through -- deliberately not throttled or debounced in here;
/// the caller decides how often it actually wants to act on that.
struct ProgressReader<R> {
    inner: R,
    read_so_far: u64,
    total: u64,
    on_progress: Box<dyn FnMut(u64, u64) + Send>,
}
impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.read_so_far += n as u64;
            (self.on_progress)(self.read_so_far, self.total);
            throttle(n);
        }
        Ok(n)
    }
}

/// PUT -- uploads `local_path`'s current contents to `url`. Streamed
/// directly from disk via `reqwest::blocking::Body::new` wrapping a
/// ProgressReader, not read into memory first -- confirmed via
/// reqwest's own source that Body::new takes any `Read + Send +
/// 'static`, built specifically for this "don't buffer the whole body"
/// case, which matters here given real multi-gigabyte files already
/// present in this app's own test data. Returns the new ETag if the
/// server's response included one (PUT responses commonly do); None
/// otherwise, leaving it to the caller to fall back to a separate
/// query if it actually needs one.
///
/// Sends X-OC-Mtime -- a Nextcloud-specific extension (confirmed
/// directly against Nextcloud's own developer docs and real server
/// source, not assumed from general WebDAV): without it, "the current
/// upload date will be used as modification date" server-side,
/// regardless of what the uploaded content's own local mtime actually
/// is -- the mirror-image gap of the one download() itself already had
/// (see that function's own doc comment). Only sent when local_path's
/// own mtime is a genuine positive integer -- confirmed via a real,
/// reported Nextcloud server error ("X-OC-MTime header must be a valid
/// positive integer") that the server validates this strictly and
/// rejects the *entire* PUT outright on a bad value, so omitting the
/// header entirely (falling back to the server's own upload-time
/// default) is the safe choice for the rare local mtime that isn't a
/// clean positive value, not sending something that might not pass
/// validation.
pub fn upload(
    local_path: &std::path::Path,
    url: &str,
    username: &str,
    password: &str,
    mut on_progress: impl FnMut(u64, u64) + Send + 'static,
) -> Result<Option<String>, WebDavError> {
    let file = std::fs::File::open(local_path)?;
    let metadata = file.metadata()?;
    let total = metadata.len();
    let mtime = metadata.modified().ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .filter(|&secs| secs > 0);
    let reader = ProgressReader { inner: file, read_so_far: 0, total, on_progress: Box::new(move |a, b| on_progress(a, b)) };
    let body = reqwest::blocking::Body::new(reader);

    let mut request = transfer_client()
        .put(url)
        .basic_auth(username, Some(password));
    if let Some(mtime) = mtime {
        request = request.header("X-OC-Mtime", mtime.to_string());
    }
    let response = request.body(body).send()?;
    let status = response.status().as_u16();
    if status != 201 && status != 204 {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    }
    Ok(response.headers().get("etag").and_then(|v| v.to_str().ok()).map(|s| s.trim_matches('"').to_string()))
}

/// Whole-body-in-memory GET -- deliberately not the general download
/// path (see download's own doc comment for why that one streams to
/// disk instead): only meant for genuinely small files a caller needs
/// the actual bytes of right away, not written anywhere on disk.
pub fn get_to_memory(url: &str, username: &str, password: &str) -> Result<Vec<u8>, WebDavError> {
    let response = retry_once(|| {
        Ok(metadata_client()
            .get(url)
            .basic_auth(username, Some(password))
            .send()?)
    })?;
    let status = response.status().as_u16();
    if status != 200 {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    }
    Ok(read_throttled_to_vec(response)?)
}

/// XXH3-128 of `url`'s current content, computed incrementally as the
/// response streams in -- never buffers the whole body in memory, and
/// uses transfer_client (no overall request timeout), not
/// metadata_client -- both real, confirmed problems without this: this
/// is what sync.rs's own contents_match uses for the actual byte-level
/// verification step, on files that have already passed a
/// max_file_size_bytes check but can still legitimately be sized in the
/// gigabytes (a real multi-GB ISO in this app's own test tree is
/// exactly what surfaced this). metadata_client's own 30-second overall
/// timeout can't complete a multi-gigabyte transfer at all -- the
/// request was failing every single attempt, forever, the file staying
/// permanently stuck mid-verification rather than ever resolving.
/// Buffering the whole thing into a Vec<u8> first (the shape
/// get_to_memory itself is for) would also mean holding a full extra
/// copy of a potentially multi-gigabyte file in memory at once, on top
/// of whatever the local side's own read holds -- a real concern given
/// this app's own target of a memory-constrained mobile device, not
/// just a desktop machine with memory to spare.
///
/// XXH3, not SHA256 (what this used to be) -- this is pure content-
/// equality comparison, never a security boundary (nothing here is
/// verifying integrity against a signature or a value from an untrusted
/// source), so a fast, non-cryptographic hash is the right tool, not an
/// unnecessary cost. Confirmed several times faster than SHA256 on the
/// same input via real, independent benchmarking elsewhere in the
/// ecosystem, not assumed. 128-bit output, not the crate's own 64-bit
/// default -- collision risk for this use case is already astronomically
/// low either way (there's no adversary deliberately constructing a
/// collision, unlike a security context), but 128 bits costs almost
/// nothing extra for a wider safety margin given this doubles as the
/// deciding factor in whether a real, uploaded/downloaded file gets
/// treated as identical to another.
pub fn hash_content(url: &str, username: &str, password: &str) -> Result<u128, WebDavError> {
    let mut response = transfer_client()
        .get(url)
        .basic_auth(username, Some(password))
        .send()?;
    let status = response.status().as_u16();
    if status != 200 {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    }
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = response.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        throttle(n);
    }
    Ok(hasher.digest128())
}

/// What a Range-scoped hash request actually got back -- see
/// hash_content_range's own doc comment for the full reasoning.
pub enum RangeHashOutcome {
    /// Server honored the Range request (206) -- this is the XXH3-128 of
    /// exactly the requested byte span, nothing more.
    Hash(u128),
    /// Server responded 200 instead of 206 -- ignored the Range header
    /// entirely (some servers simply don't support partial content on
    /// this endpoint) rather than genuinely failing the request. Not an
    /// error: a perfectly valid, successful response, just not the one
    /// this specific call needed. The caller's own job to decide what
    /// "no Range support" means for it -- for sync.rs's own chunked
    /// comparison, that means abandoning chunking for this file entirely
    /// and falling back to a single, whole-file hash instead, not
    /// treating this one response as if it were the requested chunk (it
    /// isn't -- a 200 here is the *entire* file's content, which would
    /// silently corrupt the comparison if hashed as though it were just
    /// the requested slice).
    RangeNotSupported,
}

/// XXH3-128 of just the `start..=end` byte range of `url`'s current
/// content (inclusive on both ends, matching HTTP's own Range header
/// convention), computed the same streamed, incremental way as
/// hash_content -- see its own doc comment for why streaming matters at
/// all. Confirmed working Range support against this app's own real
/// server already, via download's own resumable-download usage; this is
/// the same header, same mechanism, just requesting a bounded span
/// instead of an open-ended "everything from here on" one.
pub fn hash_content_range(url: &str, username: &str, password: &str, start: u64, end: u64) -> Result<RangeHashOutcome, WebDavError> {
    let mut response = transfer_client()
        .get(url)
        .basic_auth(username, Some(password))
        .header("Range", format!("bytes={start}-{end}"))
        .send()?;
    let status = response.status().as_u16();
    if status == 200 {
        return Ok(RangeHashOutcome::RangeNotSupported);
    }
    if status != 206 {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    }
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = response.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        throttle(n);
    }
    Ok(RangeHashOutcome::Hash(hasher.digest128()))
}

/// GET -- downloads `url` to `local_path`, streamed in fixed-size
/// chunks rather than `.bytes()`'s own whole-body-in-memory approach,
/// for the same "real multi-gigabyte files exist" reason as upload's
/// own streaming -- reqwest's blocking Response implements Read
/// directly, so this reads straight from it without needing any
/// intermediate buffering step of its own. Writes to a sibling temp
/// file and renames into place only on complete success -- same
/// temp-file-then-rename discipline this app's own upload_temp_first
/// already applies on the local->remote direction, so a failed or
/// interrupted download never leaves a partial file sitting at the
/// real target path.
///
/// Resumable: if a previous attempt left a partial temp file behind (a
/// network drop, an app restart mid-transfer), and `expected_etag` is
/// given, this requests just the remainder via `Range`, guarded by
/// `If-Range` against that same ETag -- the server only honors the
/// partial request if its own current ETag still matches, so a file
/// that changed remotely in between can never get corrupted by
/// splicing new bytes onto stale ones; it just falls back to `200` and
/// a fresh restart, the same as if no partial file existed at all.
/// `expected_etag: None` skips the resume attempt entirely and always
/// starts fresh -- resuming without something to validate against
/// would be trusting the partial file's own bytes are still correct
/// with nothing to actually confirm that.
pub fn download(
    url: &str,
    local_path: &std::path::Path,
    username: &str,
    password: &str,
    expected_etag: Option<&str>,
    mut on_progress: impl FnMut(u64, u64),
) -> Result<Option<String>, WebDavError> {
    // Sibling temp file, same directory, same discipline
    // upload_temp_first already follows on the local->remote direction:
    // write fully, then rename into place. Built by appending a suffix
    // to the whole filename, not by replacing the extension -- correct
    // regardless of whether local_path has an extension at all, unlike
    // a with_extension(...) based approach would be without extra
    // care. Deterministic, not per-attempt-random -- a retry needs to
    // find the *same* leftover partial file a previous attempt left
    // behind, not a fresh, differently-named one every time.
    let temp_name = format!("{}.webdav-download-tmp", local_path.file_name().and_then(|n| n.to_str()).unwrap_or("download"));
    let temp_path = local_path.with_file_name(temp_name);

    let resume_from = if expected_etag.is_some() {
        std::fs::metadata(&temp_path).map(|m| m.len()).unwrap_or(0)
    } else {
        0
    };

    let mut request = transfer_client()
        .get(url)
        .basic_auth(username, Some(password));
    if resume_from > 0 {
        if let Some(etag) = expected_etag {
            request = request.header("If-Range", format!("\"{etag}\""));
        }
        request = request.header("Range", format!("bytes={resume_from}-"));
    }
    let mut response = request.send()?;
    let status = response.status().as_u16();

    let (mut out_file, mut so_far): (std::fs::File, u64) = if status == 206 {
        // Server honored the resume -- append to what's already there
        // rather than overwriting it.
        let file = std::fs::OpenOptions::new().append(true).open(&temp_path)?;
        (file, resume_from)
    } else if status == 200 {
        // Either no resume was attempted, or the server ignored or
        // invalidated the one requested (ETag mismatch via If-Range,
        // or it simply doesn't support Range at all) -- either way
        // this response is the whole file starting from byte 0, so the
        // temp file needs to start over fresh, not have this appended
        // onto whatever bytes might already be sitting in it.
        let file = std::fs::File::create(&temp_path)?;
        (file, 0)
    } else {
        return Err(WebDavError::UnexpectedStatus { path: url.to_string(), status });
    };

    let total: u64 = if status == 206 {
        // Content-Length on a 206 response is just the size of the
        // remainder being sent this request, not the whole file --
        // Content-Range's own total (the part after '/') is what
        // progress reporting actually needs to stay meaningful across
        // a resume, continuing from wherever it left off rather than
        // restarting the percentage from zero.
        response.headers().get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.rsplit('/').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    } else {
        response.headers().get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    };
    // Parsed before the body is consumed below -- headers are still
    // borrowable at this point, response.read() hasn't started yet.
    // None here just means "couldn't confirm one" (header missing, or
    // in some other format this GET happened to return) -- the local
    // file keeps whatever mtime the write itself produces in that case,
    // same as it always did, rather than this failing the whole
    // download over a detail that isn't essential to it.
    let remote_mtime = response.headers().get("last-modified")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_http_date);
    // Same reasoning, same header, as webdav::upload's own precedent --
    // a real, confirmed gap without this: every caller needing the new
    // ETag after a download (resolve_keep_remote and
    // resolve_keep_both_rename_local, both in sync.rs) previously had to
    // make a *separate* stat() call afterward to get it, which was both
    // a redundant network round trip and a genuine, if narrow, race --
    // if the remote file changed again in the window between this
    // download finishing and that follow-up stat call running, the
    // stored ETag would end up describing a different version than what
    // was actually downloaded. Pulling it straight from this same
    // response's own headers closes both problems at once: no second
    // call needed, and the ETag returned is guaranteed to describe
    // exactly the bytes just received, not whatever happened to be
    // there microseconds later.
    let etag = response.headers().get("etag").and_then(|v| v.to_str().ok()).map(|s| s.trim_matches('"').to_string());

    let mut buf = [0u8; 65536];
    loop {
        let n = response.read(&mut buf)?;
        if n == 0 {
            break;
        }
        out_file.write_all(&buf[..n])?;
        so_far += n as u64;
        on_progress(so_far, total);
        throttle(n);
    }
    out_file.sync_all()?;
    // Set on the still-open handle, before the rename below -- real,
    // reported gap without this: a downloaded file's local mtime was
    // simply whatever moment the write happened to finish at, not the
    // server's own Last-Modified, unlike a real desktop sync client
    // (confirmed against Nextcloud's own, which does preserve this).
    // Matters for anything that sorts by date, and for avoiding a file
    // looking "just edited" locally when it's actually old content
    // freshly pulled down. Best-effort: a failure here doesn't fail the
    // download itself over a detail that isn't essential to it, the
    // same tolerance remote_mtime's own parse above already has.
    if let Some(mtime) = remote_mtime {
        let _ = out_file.set_modified(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(mtime.max(0) as u64));
    }
    drop(out_file);
    std::fs::rename(&temp_path, local_path)?;
    Ok(etag)
}

/// Nextcloud's own chunk size for chunked uploads -- comfortably within
/// the documented [5MB, 5GB] per-chunk bound (confirmed directly
/// against Nextcloud's own developer docs, not assumed), and small
/// enough to keep well under the 10000-chunk ceiling even for very
/// large files (a 100GB file at this size is exactly 10000 chunks, the
/// documented limit) while still giving meaningful resumability
/// granularity -- an interruption loses at most one chunk's worth of
/// progress, not the whole transfer.
const CHUNK_SIZE: u64 = 10 * 1024 * 1024; // 10 MiB

/// One chunk of a chunked upload -- a plain PUT with the two headers
/// Nextcloud's own chunking API requires alongside it (confirmed
/// directly against their docs, and a real, working third-party
/// example using the identical shape): `Destination` (the eventual
/// final path, not this chunk's own URL) and `OC-Total-Length` (the
/// whole file's size, so the server can check quota against the
/// complete upload rather than only discovering an overage at the very
/// end, where it would waste every chunk already sent).
fn upload_chunk(chunk_url: &str, data: Vec<u8>, final_url: &str, total_length: u64, username: &str, password: &str) -> Result<(), WebDavError> {
    // Throttled as one whole-chunk unit, not streamed byte-by-byte --
    // consistent with upload_chunked's own existing progress-reporting
    // granularity, which already only updates once per whole chunk, not
    // per byte within one.
    throttle(data.len());
    let response = transfer_client()
        .put(chunk_url)
        .basic_auth(username, Some(password))
        .header("Destination", final_url)
        .header("OC-Total-Length", total_length.to_string())
        .body(data)
        .send()?;
    let status = response.status().as_u16();
    if status != 201 && status != 204 {
        return Err(WebDavError::UnexpectedStatus { path: chunk_url.to_string(), status });
    }
    Ok(())
}

/// Uploads `local_path` via Nextcloud's own chunking API instead of a
/// single PUT -- confirmed directly against their developer docs, not
/// assumed: `MKCOL` a per-upload chunking folder, `PUT` each chunk
/// individually, then a finalizing `MOVE` from the folder's own
/// special `.file` path onto the real target, carrying `X-OC-Mtime`
/// (same header, same reasoning, as the plain upload path's own) and
/// `OC-Total-Length` again for final verification. On success, the
/// chunks and the folder itself are deleted server-side automatically
/// (documented behavior, not something this function needs to do
/// itself).
///
/// Resumable: `chunk_folder_url` is expected to be deterministic for a
/// given (connection, path) pair, not freshly randomized per attempt
/// (see this function's own caller in sync.rs for how) -- a retry
/// naturally lands on the same folder a previous, interrupted attempt
/// used. Before uploading anything, this lists whatever's already
/// there and verifies each existing chunk is EXACTLY the size expected
/// for its position given local_path's own current size -- only the
/// longest verified, contiguous prefix starting from chunk 1 is
/// trusted (chunks are assembled in order, so an isolated later chunk
/// without everything before it is useless either way); anything after
/// the first gap or size mismatch gets re-uploaded. A local file that
/// changed size since a previous partial attempt (edited again before
/// the upload finished) naturally fails this check at chunk 1 itself,
/// since the expected chunk sizes for a different total length don't
/// match at all -- overwriting the whole stale folder chunk-by-chunk as
/// upload proceeds is the safe response to that, not trying to salvage
/// mismatched pieces from an earlier, different version of the file.
pub fn upload_chunked(
    local_path: &std::path::Path,
    chunk_folder_url: &str,
    final_url: &str,
    username: &str,
    password: &str,
    mut on_progress: impl FnMut(u64, u64) + Send + 'static,
) -> Result<Option<String>, WebDavError> {
    let total_length = std::fs::metadata(local_path)?.len();
    let num_chunks = total_length.div_ceil(CHUNK_SIZE).max(1);

    let expected_chunk_size = |chunk_number: u64| -> u64 {
        if chunk_number == num_chunks {
            total_length - (num_chunks - 1) * CHUNK_SIZE
        } else {
            CHUNK_SIZE
        }
    };

    // A listing failure (the folder doesn't exist at all -- a genuinely
    // fresh upload, the ordinary case) reads the same as "nothing to
    // resume from," not an error worth propagating.
    let existing = list(chunk_folder_url, username, password).unwrap_or_default();

    let mut resume_from_chunk: u64 = 1;
    while resume_from_chunk <= num_chunks {
        let name = format!("{resume_from_chunk:05}");
        match existing.get(&name) {
            Some(entry) if entry.size as u64 == expected_chunk_size(resume_from_chunk) => {
                resume_from_chunk += 1;
            }
            _ => break,
        }
    }

    if resume_from_chunk == 1 {
        // Nothing usable to resume from -- create the folder (or
        // tolerate it already existing from a stale, now-being-
        // overwritten previous attempt, the same way sync.rs's own
        // directory creation already tolerates this elsewhere).
        match mkcol(chunk_folder_url, username, password) {
            Ok(()) | Err(WebDavError::UnexpectedStatus { status: 405 | 409, .. }) => {}
            Err(e) => return Err(e),
        }
    }

    let mut bytes_done = (resume_from_chunk - 1) * CHUNK_SIZE;
    on_progress(bytes_done, total_length);

    let mut file = std::fs::File::open(local_path)?;
    file.seek(SeekFrom::Start(bytes_done))?;
    let mut buf = vec![0u8; CHUNK_SIZE as usize];

    for chunk_number in resume_from_chunk..=num_chunks {
        let this_chunk_size = expected_chunk_size(chunk_number) as usize;
        file.read_exact(&mut buf[..this_chunk_size])?;
        let chunk_url = format!("{}/{chunk_number:05}", chunk_folder_url.trim_end_matches('/'));
        upload_chunk(&chunk_url, buf[..this_chunk_size].to_vec(), final_url, total_length, username, password)?;
        bytes_done += this_chunk_size as u64;
        on_progress(bytes_done, total_length);
    }

    // Finalize: MOVE from the folder's own special .file path onto the
    // real target -- confirmed directly against Nextcloud's own docs
    // and a real, working third-party example, not guessed at.
    let assemble_url = format!("{}/.file", chunk_folder_url.trim_end_matches('/'));
    let mtime = std::fs::metadata(local_path)?.modified().ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .filter(|&secs| secs > 0);

    let mut request = transfer_client()
        .request(reqwest::Method::from_bytes(b"MOVE").expect("MOVE is a valid HTTP method token"), &assemble_url)
        .basic_auth(username, Some(password))
        .header("Destination", final_url)
        .header("OC-Total-Length", total_length.to_string());
    if let Some(mtime) = mtime {
        request = request.header("X-OC-Mtime", mtime.to_string());
    }
    let response = request.send()?;
    let status = response.status().as_u16();
    if status != 201 && status != 204 {
        return Err(WebDavError::UnexpectedStatus { path: assemble_url, status });
    }
    Ok(response.headers().get("etag").and_then(|v| v.to_str().ok()).map(|s| s.trim_matches('"').to_string()))
}

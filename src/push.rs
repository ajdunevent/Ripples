// src/push.rs
//
// Tier 1 (notify_push) and Tier 2 (the ETag precheck) of the three-tier
// change-detection design -- see main.rs's own scheduling for how these
// two fit together with Tier 3, the existing full crawl, which remains
// the unconditional fallback and the only thing a first-ever sync round
// (or a connection notify_push/ETag can't confirm anything about) ever
// relies on.
//
// Every function here fails soft, on purpose: a network error, a
// malformed response, a server that simply doesn't have notify_push
// installed -- none of it is treated as a hard error anywhere in this
// file. Both tiers are optional, additive optimizations layered on top
// of a full crawl that already works correctly without either of them,
// so "couldn't confirm this optimization is available" always means
// "fall back to what already works," never "propagate an error that
// could interrupt a real sync round."
//
// Deliberately synchronous/blocking throughout (reqwest's blocking
// client, tungstenite) -- this app has zero async-runtime-of-its-own
// anywhere else, and the one place any of this runs continuously (the
// WebSocket listener) does so on its own, dedicated OS thread with
// nothing else to do, the same shape tray.rs's own ksni integration
// already established for the same underlying reason (a separate
// reactor that doesn't run on glib's MainContext). reqwest's own
// `blocking` client manages its own internal async runtime per call
// entirely on its own -- nothing here needs to be aware of that at all.

use crate::webdav;

/// A client with an explicit, generous-but-bounded overall timeout for
/// every request this module makes -- a handful of small, one-off
/// requests, never legitimately slow under normal conditions, so a
/// hung connection (dropped packets rather than an outright rejection,
/// a stalled TLS handshake, a server that accepts the connection but
/// never responds) reads as a clear failure within a reasonable window
/// rather than blocking forever with no error and nothing to log.
///
/// Built once and cached, not reconstructed on every call -- see
/// webdav.rs's own metadata_client/transfer_client, cached the exact
/// same way for the exact same reason (confirmed directly against
/// reqwest's own documentation: "The Client holds a connection pool
/// internally, so it is advised that you create one and reuse it").
/// Less impactful here than there given how infrequently this module's
/// own calls actually happen (see run_blocking's own doc comment --
/// capabilities discovery once per connection per session, the ETag
/// check once per round), but the same real cost applies regardless:
/// every call through a freshly-built client pays a full TCP+TLS
/// handshake it wouldn't need to if a live connection to the same host
/// was already sitting in the pool from a moment earlier.
fn client() -> reqwest::blocking::Client {
    static CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("a minimal client with just a timeout configured should never fail to build")
    }).clone()
}

/// Runs a blocking closure on a fresh, one-off OS thread and returns its
/// result asynchronously -- for the handful of blocking HTTP calls this
/// module makes (discover_capabilities, root_etag_unchanged), which
/// would otherwise block glib's own MainContext -- and with it, the
/// entire UI -- if called directly from an async function running on
/// it. Only ever used for occasional, one-shot calls (capabilities
/// discovery once per connection per session, the ETag check once per
/// round), not anything high-frequency enough for a fresh thread per
/// call to be wasteful.
pub async fn run_blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = async_channel::bounded(1);
    std::thread::spawn(move || {
        let result = f();
        let _ = tx.send_blocking(result);
    });
    rx.recv().await.expect("the spawned thread always sends exactly one result before finishing")
}

/// What a given server's own OCS capabilities endpoint actually reports.
/// A successful response from this endpoint at all is itself the
/// confirmation this is a Nextcloud/ownCloud server -- it's a
/// Nextcloud/ownCloud-specific OCS API, not part of WebDAV/HTTP, so a
/// generic WebDAV server has no /ocs/ namespace to respond from in the
/// first place. That's exactly the gate the ETag tier needs (see
/// journal::connection_root_etag's own doc comment for why: the
/// property that tier relies on -- a directory's ETag reflecting
/// changes anywhere in its whole subtree -- is documented
/// Nextcloud/ownCloud server behavior, not a generic WebDAV guarantee).
#[derive(Debug, Clone, Default)]
pub struct PushCapabilities {
    pub websocket_url: Option<String>,
    pub pre_auth_url: Option<String>,
    pub is_nextcloud_or_owncloud: bool,
    /// None whenever is_nextcloud_or_owncloud is false (a plain WebDAV
    /// server never gets this far -- see the is_object() check just
    /// above this field's own construction) -- but also None for a
    /// genuine Nextcloud/ownCloud response whose own capabilities lack a
    /// real "theming" object, confirmed as a real, distinct case
    /// (ownCloud's own OCS capabilities response shares the same
    /// baseline as Nextcloud's, but never includes "theming" at all --
    /// that app is Nextcloud-specific). See discover_capabilities' own
    /// has_real_theming for exactly where that distinction is drawn.
    pub theme: Option<crate::config::ServerTheme>,
}

/// Hits the server's own OCS capabilities endpoint once -- a single,
/// cheap GET, not part of the regular sync round's own file/directory
/// traffic at all. Must be authenticated -- confirmed directly (not
/// assumed) that an anonymous request to this exact endpoint never
/// includes the notify_push block at all, even when the app is
/// genuinely installed and working; only a request authenticated as a
/// real account gets it. See this module's own top-level doc comment
/// for why every failure path here returns PushCapabilities::default()
/// (nothing available) rather than propagating an error.
pub fn discover_capabilities(server_url: &str, username: &str, password: &str) -> PushCapabilities {
    let url = format!("{}/ocs/v2.php/cloud/capabilities", server_url.trim_end_matches('/'));
    let result = client().get(&url)
        .basic_auth(username, Some(password))
        .header("Accept", "application/json")
        .header("OCS-APIRequest", "true")
        .send();

    let response = match result {
        Ok(r) => r,
        Err(_) => return PushCapabilities::default(),
    };
    let Ok(text) = response.text() else {
        return PushCapabilities::default();
    };
    let json: serde_json::Value = match serde_json::from_str(&text) {
        Ok(j) => j,
        Err(_) => return PushCapabilities::default(),
    };

    let capabilities = &json["ocs"]["data"]["capabilities"];
    if !capabilities.is_object() {
        // No capabilities object at all in an otherwise-successful
        // response is itself informative -- not a Nextcloud/ownCloud
        // server (or at least not one behaving as expected), so nothing
        // here can be trusted either.
        return PushCapabilities::default();
    }

    let theming = &capabilities["theming"];
    // A real, confirmed distinction, not a hypothetical -- the
    // "theming" app is Nextcloud-specific, not part of the OCS
    // capabilities baseline both Nextcloud and ownCloud otherwise share
    // (the check just above, on "capabilities" itself, passes for
    // either). An ownCloud server's own capabilities response is
    // missing "theming" entirely, which serde_json's own indexing
    // treats as Value::Null here, not as an object with all-empty
    // fields -- theming.is_object() is what actually tells the two
    // apart. Getting this wrong (as an earlier version of this function
    // did) meant treating that Null the same as a real, present-but-
    // empty theming object: every field's own get() call already
    // returns None for a Null parent (serde_json indexing into Null
    // itself yields Null, and .as_str() on that is None too), so the
    // fallback-color logic below would still run to completion and
    // return Some(a theme) -- Nextcloud's own generic default colors,
    // not the server's real branding, silently presented as if they
    // were.
    let has_real_theming = theming.is_object();
    let get = |key: &str| theming[key].as_str().map(String::from);
    // primary computed first now, so it can serve as background's own
    // fallback below -- background legitimately isn't always a hex
    // color at all: an admin can configure a background *image*
    // instead of a plain color (confirmed directly against a real
    // server's own capabilities response, where "background" was a
    // full image URL and "background-plain" was false), which
    // safe_color_pair already correctly rejects as invalid hex. Falling
    // back to the accent/primary color in that case -- a real,
    // admin-configured color either way -- is a much closer match to
    // the server's actual branding than falling back to a generic,
    // unrelated default white/dark-gray the way this used to.
    // #0082c9 confirmed against Nextcloud's own real source (the
    // widely-documented default primary) as primary's own fallback --
    // there's nothing further to fall back to for primary itself.
    let (primary, primary_text) = safe_color_pair(get("color"), get("color-text"), "#0082c9", "#ffffff");
    let (background, background_text) = safe_color_pair(get("background"), get("background-text"), &primary, &primary_text);

    let notify_push = &capabilities["notify_push"];
    PushCapabilities {
        websocket_url: notify_push["endpoints"]["websocket"].as_str().map(String::from),
        pre_auth_url: notify_push["endpoints"]["pre_auth"].as_str().map(String::from),
        is_nextcloud_or_owncloud: true,
        theme: has_real_theming.then(|| crate::config::ServerTheme { background, background_text, primary, primary_text }),
    }
}

/// Whether `s` is a well-formed "#rrggbb" hex color string -- anything
/// else (a CSS named color, a malformed value, an empty string, plain
/// garbage) is rejected outright, since this string later gets
/// interpolated directly into generated CSS with no further escaping.
/// Trusting an unvalidated server response there risks either silently
/// broken CSS (a stray character ending the rule block early) or, in
/// the specific and confirmed real case this app already hit once, a
/// color pairing that renders text invisible against its own
/// background.
fn is_valid_hex_color(s: &str) -> bool {
    s.len() == 7 && s.starts_with('#') && s.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit)
}

/// A rough, not WCAG-certified, perceived-brightness value -- weights
/// match the standard luma coefficients WCAG's own relative luminance
/// formula uses, just applied directly to raw 0-255 channel values
/// rather than gamma-linearized ones. Simpler, and sufficient for
/// catching "these two colors are dangerously close," which is the
/// actual, confirmed failure mode this app has already hit -- not a
/// tool for certifying a specific contrast ratio.
fn perceived_brightness(hex: &str) -> Option<i32> {
    let component = |i: usize| i32::from_str_radix(&hex[i..i + 2], 16).ok();
    let (r, g, b) = (component(1)?, component(3)?, component(5)?);
    Some((r * 299 + g * 587 + b * 114) / 1000)
}

/// Validates a background/text color pair from the server, falling back
/// to `(default_bg, default_fg)` -- the same confirmed Nextcloud
/// defaults used elsewhere in this function -- whenever either color is
/// missing or malformed, or whenever both are well-formed but too close
/// in brightness to read safely together. A real, confirmed failure
/// mode this app already hit once, not a hypothetical -- an earlier
/// version of this feature applied a background/text pairing directly
/// with no validation at all, and a real bug (unrelated to the server
/// response itself, but exactly the kind of outcome a bad response
/// could equally cause) produced an invisible headerbar.
///
/// A pair that fails the brightness check still falls back to the
/// *whole* default pair, background included, not just a substituted
/// text color -- pairing a real, admin-chosen background with a
/// fallback text color has no more guarantee of matching the admin's
/// actual intent than falling back entirely does, so there's nothing
/// meaningfully "more correct" about a partial fallback here.
fn safe_color_pair(bg: Option<String>, fg: Option<String>, default_bg: &str, default_fg: &str) -> (String, String) {
    let fallback = || (default_bg.to_string(), default_fg.to_string());
    let (Some(bg), Some(fg)) = (bg, fg) else { return fallback() };
    if !is_valid_hex_color(&bg) || !is_valid_hex_color(&fg) {
        return fallback();
    }
    match (perceived_brightness(&bg), perceived_brightness(&fg)) {
        // 125 is the same "minimum brightness difference" heuristic
        // from the older W3C accessibility guidelines -- a real,
        // commonly-cited threshold, not an arbitrary number, even
        // though this whole check is deliberately rougher than a full
        // WCAG contrast-ratio calculation.
        (Some(bg_b), Some(fg_b)) if (bg_b - fg_b).abs() >= 125 => (bg, fg),
        _ => fallback(),
    }
}

/// Mixes `color` toward `target` by `ratio` (0.0..=1.0), linear RGB
/// interpolation per channel, rounded to the nearest integer. Generalized
/// from an earlier version that only mixed toward white -- dark mode
/// needs the same operation mixing toward Nextcloud's own dark main
/// background (#171717) instead, confirmed against Nextcloud's real
/// DarkTheme.php source (--color-main-background-blur: 85%) and an exact
/// match against one real measured server sidebar color at that same
/// ratio. Light mode's 80%-toward-white pairing was separately confirmed
/// the same way, against two different real background/sidebar color
/// pairs from a real server. Returns None only if `color` or `target`
/// isn't a well-formed "#rrggbb" string to begin with.
pub fn mix_toward(color: &str, target: &str, ratio: f64) -> Option<String> {
    let parse = |c: &str| -> Option<(u8, u8, u8)> {
        let hex = c.strip_prefix('#')?;
        if hex.len() != 6 {
            return None;
        }
        let component = |i: usize| u8::from_str_radix(&hex[i..i + 2], 16).ok();
        Some((component(0)?, component(2)?, component(4)?))
    };
    let (r, g, b) = parse(color)?;
    let (tr, tg, tb) = parse(target)?;
    let mix = |c: u8, t: u8| ((c as f64) * (1.0 - ratio) + (t as f64) * ratio).round() as u8;
    Some(format!("#{:02x}{:02x}{:02x}", mix(r, tr), mix(g, tg), mix(b, tb)))
}

/// Exchanges the account's own password for a short-lived notify_push
/// token via the pre_auth endpoint -- see goa.rs's own
/// get_password_for_push_auth for why this is the one, deliberate place
/// this app ever touches a raw credential, and why the password itself
/// never travels any further than this one HTTP request: the token this
/// returns, not the password, is what the WebSocket connection actually
/// authenticates with (see connect_and_listen below) -- confirmed
/// directly from notify_push's own DEVELOPING.md: "send an empty string
/// as username over the websocket, send the token from the pre_auth
/// request as password."
/// Distinguishes a rejected credential from any other failure -- the two
/// need genuinely different handling. A 401/403 means the credential
/// itself is bad (the app password was revoked, the account needs
/// re-authenticating in GNOME Settings -> Online Accounts) -- retrying
/// with the exact same, still-bad credential accomplishes nothing.
/// Anything else (network error, malformed response, a transient
/// server-side problem) is a completely ordinary reason to just retry
/// later with the same credential, which is very likely still fine.
pub enum PreAuthError {
    AuthenticationFailed,
    Other(String),
}

impl std::fmt::Display for PreAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PreAuthError::AuthenticationFailed => write!(f, "credential rejected (401/403)"),
            PreAuthError::Other(e) => write!(f, "{e}"),
        }
    }
}

pub fn exchange_pre_auth_token(pre_auth_url: &str, username: &str, password: &zeroize::Zeroizing<String>) -> Result<String, PreAuthError> {
    let result = client().post(pre_auth_url)
        .basic_auth(username, Some(password.as_str()))
        .send();

    let response = match result {
        Ok(r) => r,
        Err(e) => return Err(PreAuthError::Other(format!("pre_auth request failed: {e}"))),
    };
    // reqwest, unlike ureq, doesn't turn a 4xx/5xx status into an Err on
    // its own -- .send() only fails for a genuine transport problem, so
    // this needs an explicit status check here rather than trying to
    // pattern-match it out of the Err arm above the way ureq's own
    // StatusCode variant allowed.
    let status = response.status().as_u16();
    if status == 401 || status == 403 {
        return Err(PreAuthError::AuthenticationFailed);
    }
    response.text().map_err(|e| PreAuthError::Other(format!("couldn't read pre_auth response: {e}")))
}

/// One cheap, depth-0 PROPFIND on the remote root, asking only for its
/// own ETag -- a thin wrapper around webdav::stat, which already does
/// exactly this same query for sync.rs's own purposes. None on any
/// failure (network error, malformed response, missing ETag in the
/// reply) -- every caller treats "couldn't fetch" the same as "don't
/// trust this," never as a stand-in for a real value.
pub fn fetch_root_etag(remote_root_url: &str, username: &str, password: &str) -> Option<String> {
    webdav::stat(remote_root_url, username, password).ok()?.etag
}

/// Returns true (skip the full crawl) only when the check actually
/// completed AND the fetched ETag genuinely matches `stored_etag` --
/// every failure of fetch_root_etag itself falls through to false,
/// meaning a real crawl, never defaulting to "assume unchanged." An
/// unnecessary crawl is a cost; a wrongly-skipped one is a correctness
/// risk, and those aren't symmetric -- see this module's own top-level
/// doc comment.
pub fn root_etag_unchanged(remote_root_url: &str, username: &str, password: &str, stored_etag: &str) -> bool {
    fetch_root_etag(remote_root_url, username, password).is_some_and(|etag| etag == stored_etag)
}

/// One notify_push message this app actually cares about -- everything
/// else the server might send (notify_activity, notify_notification) is
/// ignored entirely rather than threaded through as its own variant
/// nobody would consume.
pub enum PushMessage {
    /// The WebSocket connection and auth handshake both succeeded --
    /// main.rs uses this to flip notify_push_connected to true and log
    /// the capability change (see status::SyncEvent::NotifyPushCapability).
    Connected,
    /// The server reported a filesystem change somewhere in this
    /// account -- not which file, just that something changed. Sent
    /// either because this server doesn't support notify_file_id at all
    /// (an older notify_push, pre-0.4), or because it does but couldn't
    /// resolve the specific IDs for this particular change (see
    /// DEVELOPING.md's own wording: "In cases where the push server
    /// doesn't know which files have changed, it will send the regular
    /// notify_file message" -- confirmed directly from the real
    /// upstream doc, not assumed). Enough on its own to trigger a real
    /// sync round (which itself may then use the ETag tier to confirm
    /// cheaply, or fall through to a full crawl) -- see main.rs's own
    /// handling.
    FileChanged,
    /// The finer-grained variant, opted into via `listen notify_file_id`
    /// right after authenticating (see run_listener's own connection
    /// setup) -- one or more oc:fileid values (as plain decimal
    /// strings, matching how RemoteEntry::fileid already stores them
    /// everywhere else in this app) that changed. main.rs checks each
    /// one against journal::find_by_fileid; a match means the exact
    /// local path is already known, so the round that follows can be
    /// properly scoped instead of a full crawl. An ID this app has
    /// never seen before (a genuinely new file, or a connection that's
    /// simply never synced it) falls back to the same full-check
    /// treatment as FileChanged, for that connection only.
    FileIdsChanged(Vec<String>),
    /// The connection dropped or was never established -- the listener
    /// thread's own loop, below, handles reconnection itself; this is
    /// purely informational, so main.rs can log the capability change
    /// (see status::SyncEvent::NotifyPushCapability) rather than
    /// silently going quiet with no visible explanation.
    Disconnected,
    /// Distinct from a plain Disconnected -- the pre_auth exchange
    /// specifically got a 401/403 back, meaning the credential itself
    /// was rejected (an app password revoked, most likely), not a
    /// transient network/server issue. The listener thread keeps
    /// retrying regardless (a human re-authenticating the account would
    /// make the very next attempt succeed, and there's no way to detect
    /// that happening other than trying again), but main.rs uses this to
    /// surface something more useful than a generic "disconnected" --
    /// see the Disconnected status row's own subtitle handling.
    AuthenticationFailed,
}

/// A password request from the dedicated push-listener thread, answered
/// by the main, glib-driven thread. This round-trip exists because
/// GoaWatcher::get_password_for_push_auth is async, driven by gio's own
/// D-Bus machinery -- it only actually completes when polled by glib's
/// own MainContext, not something callable directly from a plain OS
/// thread with no event loop of its own at all. `async_channel` is used
/// for both directions specifically because it supports blocking
/// send/recv (for this thread) and async send/recv (for the main
/// thread's own response) on the very same channel type, rather than
/// needing two different channel crates for the two ends of one bridge.
/// Distinguishes "this account isn't the one this app cares about
/// anymore" from any other, ordinary reason a password request could
/// fail -- the two need genuinely different handling on run_listener's
/// own end (see its own doc comment on the loop below): an account
/// that's been switched away from or removed outright should make this
/// thread exit for good, not keep retrying every 30 seconds against a
/// server connection nothing here will ever act on again. By request --
/// reuses the existing password-request round-trip that already happens
/// at the start of every single reconnect attempt, rather than adding a
/// second, separate check/channel just for this: the main thread already
/// has to look this account up to answer the request at all, so
/// answering "you're not active anymore" instead of a real password (or
/// a generic failure) costs nothing extra, and the very next reconnect
/// attempt -- already going to happen anyway -- is what actually
/// notices and acts on it, rather than needing a dedicated shutdown
/// signal threaded in from wherever an account might become inactive.
pub enum PasswordRequestError {
    AccountNoLongerActive,
    Other(String),
}

pub struct PasswordRequest {
    pub account_id: String,
    pub respond_to: async_channel::Sender<Result<zeroize::Zeroizing<String>, PasswordRequestError>>,
}

/// Runs forever on its own dedicated thread (spawned by the caller via
/// std::thread::spawn, matching tray.rs's own precedent for exactly the
/// same underlying reason: this is a genuinely separate reactor, not
/// something that runs on glib's own MainContext) -- connects, exchanges
/// a fresh pre_auth token for each connection attempt (see this
/// function's own reconnect loop), authenticates, and blocks on
/// incoming messages, forwarding only the ones main.rs actually needs
/// to act on via `tx`. Reconnects automatically after any error or
/// disconnect, with a short pause between attempts to avoid hammering a
/// server that's actually down rather than just blipping -- never gives
/// up permanently on its own; the caller's own thread simply keeps
/// running for the lifetime of the process.
///
/// Each reconnect attempt requests a fresh password via
/// `password_requests` (see PasswordRequest's own doc comment for why
/// this is a round-trip to the main thread, not a direct call) rather
/// than holding one in this thread's own memory for the whole session --
/// this thread never retains a credential any longer than the single
/// pre_auth exchange actually needs it for.
/// What a single reconnect attempt actually decided, once the inner
/// closure below finishes -- distinct from a bare Result<(), String>
/// specifically so AccountNoLongerActive (see PasswordRequestError's own
/// doc comment) can propagate all the way out to run_listener's own
/// outer loop and end the thread for good, rather than being
/// indistinguishable from any other, ordinary reason to sleep and retry.
enum ReconnectOutcome {
    Retry(String),
    StopListening,
}

pub fn run_listener(
    websocket_url: String,
    pre_auth_url: String,
    account_id: String,
    username: String,
    password_requests: async_channel::Sender<PasswordRequest>,
    tx: async_channel::Sender<(String, PushMessage)>,
) {
    loop {
        let outcome = (|| -> Result<(), ReconnectOutcome> {
            let (respond_tx, respond_rx) = async_channel::bounded(1);
            password_requests.send_blocking(PasswordRequest { account_id: account_id.clone(), respond_to: respond_tx })
                .map_err(|_| ReconnectOutcome::Retry("main thread's password-request channel closed".to_string()))?;
            let password = match respond_rx.recv_blocking() {
                Ok(Ok(password)) => password,
                Ok(Err(PasswordRequestError::AccountNoLongerActive)) => return Err(ReconnectOutcome::StopListening),
                Ok(Err(PasswordRequestError::Other(e))) => return Err(ReconnectOutcome::Retry(format!("couldn't retrieve password: {e}"))),
                Err(_) => return Err(ReconnectOutcome::Retry("main thread never responded to password request".to_string())),
            };

            let token = match exchange_pre_auth_token(&pre_auth_url, &username, &password) {
                Ok(t) => t,
                Err(PreAuthError::AuthenticationFailed) => {
                    let _ = tx.send_blocking((account_id.clone(), PushMessage::AuthenticationFailed));
                    return Err(ReconnectOutcome::Retry("credential rejected -- this account likely needs re-authenticating in GNOME Settings -> Online Accounts".to_string()));
                }
                Err(PreAuthError::Other(e)) => return Err(ReconnectOutcome::Retry(e)),
            };
            // password itself goes out of scope right here, right after
            // this one exchange -- Zeroizing actively clears it from
            // memory at this point, not just deallocates it.

            let (mut socket, _response) = tungstenite::connect(&websocket_url)
                .map_err(|e| ReconnectOutcome::Retry(format!("couldn't connect: {e}")))?;

            socket.send(tungstenite::Message::Text(String::new().into()))
                .map_err(|e| ReconnectOutcome::Retry(format!("couldn't send username: {e}")))?;
            socket.send(tungstenite::Message::Text(token.into()))
                .map_err(|e| ReconnectOutcome::Retry(format!("couldn't send token: {e}")))?;

            // Opts into the finer-grained variant -- see PushMessage::FileIdsChanged's
            // own doc comment. Confirmed directly against notify_push's
            // own DEVELOPING.md, not assumed: "Starting with version 0.4
            // you can opt into receiving the changed file ids for file
            // update notifications by sending `listen notify_file_id`
            // over the websocket." Best-effort, not required to succeed:
            // an older server (pre-0.4) simply won't recognize this
            // message at all and keeps sending plain notify_file the
            // same as it always did -- there's no capability check
            // needed here, sending it unconditionally and tolerating a
            // server that ignores it is the same "purely additive
            // bonus, never a requirement" shape oc:fileid itself already
            // follows.
            let _ = socket.send(tungstenite::Message::Text("listen notify_file_id".into()));

            let _ = tx.send_blocking((account_id.clone(), PushMessage::Connected));

            loop {
                match socket.read() {
                    Ok(tungstenite::Message::Text(text)) => {
                        if text == "notify_file" {
                            let _ = tx.send_blocking((account_id.clone(), PushMessage::FileChanged));
                        } else if let Some(rest) = text.strip_prefix("notify_file_id") {
                            // "followed by a json encoded array of file
                            // ids" (DEVELOPING.md's own wording) -- one
                            // combined text frame, prefix and payload
                            // together, matching the same shape the
                            // `listen notify_file_id` request itself
                            // already uses (one string, space-separated,
                            // not two separate sends). Falls back to the
                            // plain FileChanged treatment on anything
                            // that doesn't parse as expected, rather
                            // than silently dropping the notification --
                            // worst case this behaves exactly like a
                            // server that never supported the
                            // fine-grained variant at all, not a lost
                            // update.
                            match serde_json::from_str::<Vec<i64>>(rest.trim()) {
                                Ok(ids) => {
                                    let ids: Vec<String> = ids.into_iter().map(|id| id.to_string()).collect();
                                    let _ = tx.send_blocking((account_id.clone(), PushMessage::FileIdsChanged(ids)));
                                }
                                Err(_) => {
                                    let _ = tx.send_blocking((account_id.clone(), PushMessage::FileChanged));
                                }
                            }
                        }
                        // Anything else (notify_activity,
                        // notify_notification, etc) is deliberately
                        // ignored -- see PushMessage's own doc comment.
                    }
                    Ok(_) => {} // non-text frames (ping/pong/binary/close) -- nothing this app needs to act on directly
                    Err(e) => return Err(ReconnectOutcome::Retry(format!("connection lost: {e}"))),
                }
            }
        })();

        match outcome {
            Ok(()) => {}
            Err(ReconnectOutcome::Retry(e)) => {
                eprintln!("push[{account_id}]: {e} -- will retry");
            }
            // By request -- see PasswordRequestError's own doc comment
            // for the full reasoning. No sleep, no further reconnect
            // attempt: this account isn't the one this app cares about
            // anymore, so there's nothing left for this thread to do.
            Err(ReconnectOutcome::StopListening) => {
                eprintln!("push[{account_id}]: account no longer active -- listener thread exiting for good");
                let _ = tx.send_blocking((account_id.clone(), PushMessage::Disconnected));
                return;
            }
        }
        let _ = tx.send_blocking((account_id.clone(), PushMessage::Disconnected));
        std::thread::sleep(std::time::Duration::from_secs(30));
    }
}

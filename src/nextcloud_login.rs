// src/nextcloud_login.rs
//
// Nextcloud's own documented Login Flow v2 (docs.nextcloud.com/server/
// stable/developer_manual/client_apis/LoginFlow/index.html#login-flow-v2),
// confirmed directly against that page rather than assumed from general
// knowledge -- the whole point of v2 over the original Login Flow is that
// it needs no embedded webview at all: the app opens the server's own
// login URL in the user's regular, external browser (2FA, SSO, client
// certificates all just work, since it's a real browser session, not
// something this app has to understand), and gets an app password back
// by polling a separate endpoint once the user finishes there.
//
// Three-step shape, all three confirmed against the real API:
//   1. POST <server>/index.php/login/v2 (anonymous) -> a poll token/
//      endpoint, plus the URL to open in the browser.
//   2. Open that URL in the default browser.
//   3. POST <poll endpoint> with the token, repeatedly -- 404 until the
//      user finishes in the browser, then exactly one 200 with the
//      server address, login name, and app password to actually connect
//      with.

use std::time::Duration;

/// Minimal, cached client for this module's own occasional, unauthenticated
/// calls -- same reasoning, same shape, as push.rs's own client() (cached
/// for the same, real reason: reqwest's own docs recommend reusing one
/// Client rather than paying a fresh TCP+TLS handshake per call), and
/// run_blocking below is push.rs's own copy too, duplicated rather than
/// imported across modules for the same small-helper reasoning goa.rs's
/// and sync.rs's own now_hms give.
fn client() -> reqwest::blocking::Client {
    static CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            // Nextcloud's own Login Flow v2 docs confirm this directly:
            // the consent page's own "grant access to ___" text is
            // filled in from the request's own User-Agent header, not a
            // separate, dedicated app-name parameter -- with no explicit
            // header set here, that's been left to reqwest's own
            // default, which is either blank or not something Nextcloud
            // can display as a real name, hence the empty "" reported.
            .user_agent("Ripples")
            .build()
            .expect("a minimal client with just a timeout configured should never fail to build")
    }).clone()
}

async fn run_blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = async_channel::bounded(1);
    std::thread::spawn(move || {
        let result = f();
        let _ = tx.send_blocking(result);
    });
    rx.recv().await.expect("the spawned thread always sends exactly one result before finishing")
}

/// How often to poll -- 1 second, matching nc_py_api's own default `step`
/// for this exact same flow (a real, independent Nextcloud client library
/// choosing the same value is a reasonable signal this is an accepted,
/// server-friendly interval, not something invented here).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// 20 minutes, matching the server's own documented token lifetime
/// exactly -- tracked client-side so this gives up cleanly with a clear
/// Timeout once the token could no longer possibly still be valid, rather
/// than continuing to poll indefinitely against a token the server has
/// already discarded (the docs don't specify what the poll endpoint
/// actually returns after expiry, so relying on that behavior being
/// well-defined isn't safe to assume).
const TOKEN_LIFETIME: Duration = Duration::from_secs(20 * 60);

#[derive(Debug, Clone)]
pub struct LoginFlowStart {
    pub login_url: String,
    poll_token: String,
    poll_endpoint: String,
}

#[derive(Debug, Clone)]
pub struct LoginFlowResult {
    pub server: String,
    pub login_name: String,
    pub app_password: String,
}

/// A non-GOA account, entirely owned and managed by this app rather than
/// GNOME Online Accounts -- the result of a completed Login Flow v2,
/// plus a stable, locally-generated identity so it can be referenced
/// the same way a GOA account's own account_id already is (SyncConnection
/// itself is agnostic to which kind of account it points to; see
/// AccountSource's own doc comment in main.rs for the full reasoning).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DirectNextcloudAccount {
    /// Deterministically derived from (server, login_name) via
    /// direct_account_id below, not a random UUID -- deliberately stable
    /// across separate logins to the same account, including a delete-
    /// then-re-add cycle, so a preserved SyncConnection from before
    /// naturally reconnects to this account again rather than becoming
    /// permanently orphaned under an identity that could never recur.
    /// Same reasoning, same intended effect, as switching a GOA account
    /// away and back already has for free (GOA's own account_id is
    /// already stable on its own) -- this exists specifically to give a
    /// direct account that same property, which a random UUID never
    /// could.
    pub account_id: String,
    /// Exactly what the poll response's own "server" field returned --
    /// already a plain http(s):// address, per Login Flow v2's own
    /// documented response shape, not something this app needs to parse
    /// out of a davs:// URI the way GOA's own files_uri requires.
    pub server: String,
    /// The poll response's own "loginName" -- confirmed, directly from
    /// Nextcloud's own docs, that this can genuinely differ from the
    /// account's real username (an email address used to log in, for
    /// instance), the same caveat GOA's own webdav_username field
    /// exists to handle.
    pub login_name: String,
}

/// Derives a stable account_id from (server, login_name) -- see
/// DirectNextcloudAccount::account_id's own doc comment for the full
/// reasoning on why this needs to be deterministic rather than a fresh
/// UUID per login. A free function, not a method, since it has to be
/// callable before a DirectNextcloudAccount exists at all (right after
/// a successful LoginFlowResult, while actually constructing one).
///
/// Deliberately not attempting to normalize away every superficial
/// difference between two logins that a human would call "the same
/// account" -- a login_name genuinely changing between two logins to
/// the same underlying server account (a different alias, say) would
/// still produce a different id here, and that's accepted rather than
/// solved: Login Flow v2's own response gives no other, more stable
/// identifier to key on instead. server.trim_end_matches('/') is the
/// one normalization applied, guarding against the otherwise-identical
/// case of a server address differing only in a trailing slash.
///
/// "direct:" prefix -- not required for uniqueness against a GOA
/// account_id (GOA's own ids look like "account_1780931114_0", already
/// a different shape), but makes the two kinds visually
/// distinguishable at a glance in [debug] output/journal entries,
/// rather than relying on remembering which numeric-suffix pattern
/// belongs to which source.
pub fn direct_account_id(server: &str, login_name: &str) -> String {
    format!("direct:{}:{login_name}", server.trim_end_matches('/'))
}

impl DirectNextcloudAccount {
    /// A short, human-readable label for this account -- "login_name@
    /// host", the same shape a GOA account's own presentation_identity
    /// typically takes, so the two read consistently wherever an
    /// account's own display name is needed regardless of which kind it
    /// is.
    pub fn display_name(&self) -> String {
        let host = self.server
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');
        format!("{}@{host}", self.login_name)
    }

    /// This account's own WebDAV root -- {server}/remote.php/webdav/,
    /// matching exactly the path shape this app's own webdav.rs
    /// confirms GOA's davs_to_https already produces for a GOA account
    /// (see that function's own doc comment, whose real example ends in
    /// exactly "/remote.php/webdav/") -- the same, older, implicitly-
    /// scoped-to-whoever-authenticates endpoint, not the newer /remote.
    /// php/dav/files/{username}/ shape, so this app's own existing
    /// WebDAV client code needs no branching at all between a GOA
    /// account's own base URL and a direct one's.
    pub fn webdav_base_url(&self) -> String {
        format!("{}/remote.php/webdav/", self.server.trim_end_matches('/'))
    }
}

#[derive(Debug)]
pub enum LoginFlowError {
    /// The initial POST to start the flow failed outright -- couldn't
    /// reach the server at all, not a server-side rejection.
    StartRequestFailed(String),
    /// The server responded to the start request, but not with 200, or
    /// the 200 body didn't parse as the expected JSON shape. Likely
    /// means the given URL isn't actually a Nextcloud/ownCloud server,
    /// or is running a version old enough not to support Login Flow v2
    /// at all.
    UnexpectedStartResponse(String),
    /// The 20-minute window closed with no successful poll -- the user
    /// either didn't finish in the browser, or took too long.
    TimedOut,
    /// The polling loop was told to stop before completing (the user
    /// closed the dialog, cancelled explicitly) -- not a real failure,
    /// just an intentional early exit.
    Cancelled,
}

impl std::fmt::Display for LoginFlowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoginFlowError::StartRequestFailed(e) => write!(f, "couldn't reach the server: {e}"),
            LoginFlowError::UnexpectedStartResponse(e) => write!(f, "server didn't respond the way a Nextcloud/ownCloud server should: {e}"),
            LoginFlowError::TimedOut => write!(f, "login wasn't completed in time (20 minutes) -- try again"),
            LoginFlowError::Cancelled => write!(f, "login cancelled"),
        }
    }
}

/// JSON shape of the POST /index.php/login/v2 response -- confirmed
/// directly against the real, current Nextcloud developer docs, not
/// assumed. `#[derive(Deserialize)]` field names match the response's
/// own JSON keys exactly (poll.token, poll.endpoint, login), so no
/// #[serde(rename)] is needed anywhere here.
#[derive(serde::Deserialize)]
struct StartResponse {
    poll: PollInfo,
    login: String,
}

#[derive(serde::Deserialize)]
struct PollInfo {
    token: String,
    endpoint: String,
}

/// JSON shape of a completed (200) poll response -- same reasoning as
/// StartResponse above. A 404 (not yet completed) never reaches this
/// type at all; see poll_until_complete's own handling of that case.
#[derive(serde::Deserialize)]
struct PollSuccessResponse {
    server: String,
    #[serde(rename = "loginName")]
    login_name: String,
    #[serde(rename = "appPassword")]
    app_password: String,
}

/// Starts a new Login Flow v2 session against `server_url`. Doesn't open
/// the browser itself -- that's UI-layer responsibility (main.rs, via
/// gtk4::UriLauncher), kept separate from this module's own, plain HTTP
/// logic so this stays testable and reusable without needing a live GTK
/// window to call it.
///
/// `server_url` doesn't need to be a fully well-formed URL -- leading/
/// trailing whitespace is trimmed, a trailing slash is dropped, and a
/// missing "http://"/"https://" scheme is filled in (defaulting to
/// https) before this is ever used, so a bare hostname like
/// "example.com" works the same as "https://example.com" would. See
/// this function's own body for the full reasoning.
pub async fn start(server_url: &str) -> Result<LoginFlowStart, LoginFlowError> {
    // Both guards below exist for the same underlying reason: don't
    // assume a human typed a fully well-formed URL.
    //
    // Scheme prepended automatically if missing -- a bare hostname like
    // "example.com" (no "http://"/"https://" at all) is a common, easy
    // thing to type, and would otherwise fail here with a confusing,
    // low-level URL-parse-style error rather than actually starting the
    // login flow. Defaults specifically to https, not http -- a real
    // Nextcloud instance should overwhelmingly be running behind TLS in
    // practice, and anyone who genuinely needs plain http (a local/dev
    // server, say) still gets it by typing the scheme explicitly
    // themselves, which this leaves untouched rather than overriding.
    // Checked case-insensitively, since schemes are case-insensitive per
    // RFC 3986. Trimmed first too, not just the existing trailing-slash
    // guard below -- a leading/trailing space from a copy-paste is just
    // as easy a mistake to make as leaving the scheme off entirely.
    let trimmed = server_url.trim();
    let with_scheme = if trimmed.to_ascii_lowercase().starts_with("http://") || trimmed.to_ascii_lowercase().starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    };
    // trim_end_matches('/') so a URL the user typed with a trailing
    // slash doesn't produce a doubled "//index.php" below -- the same
    // kind of guard this session's own path-display work already
    // established the value of getting right rather than assuming input
    // is already clean.
    let base = with_scheme.trim_end_matches('/').to_string();
    let start_url = format!("{base}/index.php/login/v2");

    let response = run_blocking(move || client().post(&start_url).send()).await
        .map_err(|e| LoginFlowError::StartRequestFailed(e.to_string()))?;

    let status = response.status();
    if !status.is_success() {
        return Err(LoginFlowError::UnexpectedStartResponse(format!("HTTP {status}")));
    }

    // .text() + serde_json::from_str, not response.json::<T>() -- that
    // convenience method needs reqwest's own "json" feature, which isn't
    // enabled here (this app's own Cargo.toml deliberately keeps reqwest
    // to just ["blocking"], the same minimal-dependencies philosophy the
    // async-channel entry right above it states explicitly). serde_json
    // is already a direct dependency regardless, so this needs nothing
    // new.
    let body = run_blocking(move || response.text()).await
        .map_err(|e| LoginFlowError::UnexpectedStartResponse(e.to_string()))?;
    let parsed: StartResponse = serde_json::from_str(&body)
        .map_err(|e| LoginFlowError::UnexpectedStartResponse(e.to_string()))?;

    Ok(LoginFlowStart {
        login_url: parsed.login,
        poll_token: parsed.poll.token,
        poll_endpoint: parsed.poll.endpoint,
    })
}

/// Polls `flow`'s own endpoint every POLL_INTERVAL until the user
/// finishes in the browser, TOKEN_LIFETIME passes with no success, or
/// `cancelled` reports true (checked at the start of every iteration,
/// including the first -- so a cancellation requested before this was
/// even called takes effect immediately rather than requiring one wasted
/// poll first).
///
/// Caller's own job to have already opened `flow.login_url` in the
/// browser before calling this -- this function only polls, it doesn't
/// launch anything itself.
pub async fn poll_until_complete(flow: &LoginFlowStart, cancelled: &impl Fn() -> bool) -> Result<LoginFlowResult, LoginFlowError> {
    let deadline = std::time::Instant::now() + TOKEN_LIFETIME;
    loop {
        if cancelled() {
            return Err(LoginFlowError::Cancelled);
        }
        if std::time::Instant::now() >= deadline {
            return Err(LoginFlowError::TimedOut);
        }

        let poll_endpoint = flow.poll_endpoint.clone();
        let poll_token = flow.poll_token.clone();
        let response = run_blocking(move || {
            client().post(&poll_endpoint).form(&[("token", poll_token.as_str())]).send()
        }).await;

        match response {
            Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                // Not yet completed -- the documented, expected shape of
                // "still waiting," not an error. Falls through to the
                // sleep below and tries again next iteration.
            }
            Ok(response) if response.status().is_success() => {
                let parsed: Result<PollSuccessResponse, ()> = match run_blocking(move || response.text()).await {
                    Ok(body) => serde_json::from_str(&body).map_err(|_| ()),
                    Err(_) => Err(()),
                };
                match parsed {
                    Ok(result) => {
                        return Ok(LoginFlowResult {
                            server: result.server,
                            login_name: result.login_name,
                            app_password: result.app_password,
                        });
                    }
                    // A malformed 200 is unexpected enough to be worth
                    // surfacing rather than silently retrying forever --
                    // but not a StartRequestFailed-style connectivity
                    // problem, so it's folded into TimedOut rather than
                    // adding a whole new variant for a case this
                    // unlikely to actually happen in practice.
                    Err(_) => return Err(LoginFlowError::TimedOut),
                }
            }
            // Any other status, or the request failing outright
            // (network hiccup mid-poll) -- treated the same as "still
            // waiting," not a hard failure. A transient connectivity
            // blip here shouldn't abandon the whole flow when the very
            // next poll, a second later, might well succeed.
            _ => {}
        }

        glib::timeout_future(POLL_INTERVAL).await;
    }
}

/// Credential storage for a direct account's own app password -- see
/// Cargo.toml's own oo7 entry for the full reasoning on why oo7
/// specifically. "service" + "account_id" attributes together are this
/// app's own chosen key shape for finding the right item back later;
/// oo7 itself doesn't mandate any particular attribute names, these are
/// just what this app decided on. account_id is expected to already be
/// a generated, per-account UUID (DirectNextcloudAccount's own field),
/// so it alone is already unique -- "service" is included mainly so
/// this app's own items are visibly, unambiguously attributable to
/// Ripples specifically if anyone ever inspects the shared keyring by
/// hand (via seahorse or similar), not because account_id alone risks
/// colliding with anything.
const KEYRING_SERVICE: &str = "net.dunevent.Ripples";

fn keyring_attributes(account_id: &str) -> [(&'static str, &str); 2] {
    [("service", KEYRING_SERVICE), ("account_id", account_id)]
}
// Passed as &attrs directly at each of the three use sites below, not
// &attrs[..] -- oo7's own AsAttributes trait requires Self: Sized (an
// implicit bound the compiler enforces unless a trait explicitly opts
// out with ?Sized), so it can only be implemented for the fixed-size
// array type this function returns ([(&str, &str); N]) in the first
// place, never for the unsized slice type [(&str, &str)] that [..]
// would produce -- confirmed directly from a real compile error hit
// when this file first tried the [..] version. oo7's own README example
// (`let attributes = &[("attribute", "value")];`) matches this exactly:
// Rust's own default inference for an unannotated array-literal
// reference is already a fixed-size array reference, the same shape
// this function returns, not a slice.

/// Stores (or replaces, if one already exists for this account_id) the
/// app password for a direct Nextcloud account. Plain String error, not
/// LoginFlowError -- this isn't part of the login flow itself, and the
/// only realistic caller (main.rs, right after a successful poll) just
/// needs a line to show the user if it fails, the same reasoning
/// build_sync_target_for_connection's own doc comment gives for the
/// same choice.
pub async fn store_app_password(account_id: &str, app_password: &str) -> Result<(), String> {
    let keyring = oo7::Keyring::new().await.map_err(|e| e.to_string())?;
    let attrs = keyring_attributes(account_id);
    keyring.create_item(
        "Ripples Nextcloud App Password",
        &attrs,
        zeroize::Zeroizing::new(app_password.as_bytes().to_vec()),
        true, // replace -- re-authenticating an existing direct account should overwrite its old app password, not error out or leave two items behind
    ).await.map_err(|e| e.to_string())
}

/// Retrieves a stored app password. None (not an error) if this
/// account_id was never stored, or was already deleted -- both genuinely
/// expected, ordinary outcomes for a caller checking "do I already have
/// credentials for this account," not failure conditions. Returns
/// Zeroizing<String>, not a plain String -- oo7::Secret itself already
/// zeroizes on drop, but converting it to an owned String via
/// String::from_utf8 creates a separate allocation that guarantee
/// wouldn't cover; wrapping immediately here, rather than leaving that
/// to whichever caller happens to store the result, keeps the value
/// protected for its entire remaining lifetime instead of only from
/// whatever point a caller remembers to wrap it at.
pub async fn get_app_password(account_id: &str) -> Result<Option<zeroize::Zeroizing<String>>, String> {
    let keyring = oo7::Keyring::new().await.map_err(|e| e.to_string())?;
    let attrs = keyring_attributes(account_id);
    let items = keyring.search_items(&attrs).await.map_err(|e| e.to_string())?;
    let Some(item) = items.first() else { return Ok(None) };
    let secret = item.secret().await.map_err(|e| e.to_string())?;
    String::from_utf8(secret.as_bytes().to_vec())
        .map(|s| Some(zeroize::Zeroizing::new(s)))
        .map_err(|e| format!("stored app password wasn't valid UTF-8: {e}"))
}

/// Removes a stored app password -- called when a direct account is
/// deleted from this app, so a removed account doesn't leave an orphaned
/// credential sitting in the keyring indefinitely. Deleting attributes
/// that don't currently match any item is not treated as an error by
/// oo7 itself (confirmed against its own docs: "Remove items that
/// matches the attributes," with no failure case documented for zero
/// matches), so this is safe to call even if nothing was ever stored.
pub async fn delete_app_password(account_id: &str) -> Result<(), String> {
    let keyring = oo7::Keyring::new().await.map_err(|e| e.to_string())?;
    let attrs = keyring_attributes(account_id);
    keyring.delete(&attrs).await.map_err(|e| e.to_string())
}

/// Revokes this account's own app password server-side -- confirmed
/// directly against Nextcloud's own official developer manual (Login
/// Flow v2 docs), not assumed: "When an account on a client is removed
/// for housekeeping it is desired to destroy the apptoken in use," via
/// `DELETE <server>/ocs/v2.php/core/apppassword`, authenticated as that
/// same app password -- the token being revoked authenticates its own
/// revocation, matching that same page's own documented example
/// exactly (`curl -u username:app-password -X DELETE`).
///
/// Best-effort by design, not propagated as a user-facing error -- that
/// same documentation is explicit about this too: "If a non 200 status
/// code is returned the client should still proceed with removing the
/// account." Called from remove_direct_account (main.rs) before this
/// account's own server/credential is lost to this app for good -- a
/// real, reported gap without this: this app used to only ever clean up
/// its own, local keyring entry (delete_app_password, just above),
/// leaving the app password itself still fully valid and listed in
/// Nextcloud's own security settings indefinitely, accumulating one
/// stale entry per add/remove cycle. Confirmed against Nextcloud's own
/// community forum as a widely-reported problem across many clients,
/// not something unique to this app -- but confirming the pattern is
/// common elsewhere doesn't make it correct, only unsurprising.
pub async fn revoke_app_password(server: &str, login_name: &str, app_password: &str) -> Result<(), String> {
    let url = format!("{}/ocs/v2.php/core/apppassword", server.trim_end_matches('/'));
    let login_name = login_name.to_string();
    let app_password = app_password.to_string();
    let response = run_blocking(move || {
        client()
            .delete(&url)
            .basic_auth(login_name, Some(app_password))
            .header("OCS-APIREQUEST", "true")
            .send()
    }).await.map_err(|e| e.to_string())?;
    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        Err(format!("HTTP {status}"))
    }
}

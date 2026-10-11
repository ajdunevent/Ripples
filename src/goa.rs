// src/goa.rs
//
// Watches org.gnome.OnlineAccounts on the session bus for accounts whose
// provider type is "owncloud" (GOA's Nextcloud provider), mirroring the
// design from the earlier Qt prototype but built entirely on gio-rs's own
// DBusConnection -- no zbus, no Tokio. Everything here runs on the default
// glib::MainContext, the same one GTK itself is pumping, so there is
// exactly one event loop in the whole process.
//
// As before: no account management here, only observation. Accounts are
// added/removed in GNOME Settings -> Online Accounts.

use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

use gio::prelude::*;
use glib::Variant;

const GOA_SERVICE: &str = "org.gnome.OnlineAccounts";
const GOA_OBJECT_MANAGER_PATH: &str = "/org/gnome/OnlineAccounts";
const OBJECT_MANAGER_IFACE: &str = "org.freedesktop.DBus.ObjectManager";
const ACCOUNT_IFACE: &str = "org.gnome.OnlineAccounts.Account";
const FILES_IFACE: &str = "org.gnome.OnlineAccounts.Files";

/// HH:MM:SS, local time -- exact same format as main.rs's own now_hms
/// (duplicated here rather than shared, for the same reasoning sync.rs's
/// own copy already gives: that one's private to main.rs and this module
/// has no other reason to depend on it).
fn now_hms() -> String {
    glib::DateTime::now_local().ok()
        .and_then(|d| d.format("%H:%M:%S").ok())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

// Only ever used by get_password_for_http_auth below -- the one,
// deliberate, narrow place this app touches a raw credential at all.
const PASSWORD_BASED_IFACE: &str = "org.gnome.OnlineAccounts.PasswordBased";
// GOA's "owncloud" provider type already covers both Nextcloud and actual
// ownCloud servers (one C provider implementation, shared type string) --
// no change needed there, already worked before this list existed.
// "webdav" is GOA's separate generic-WebDAV provider, built on similar
// code but a distinct type string -- added explicitly since our own sync
// logic is already protocol-generic (nothing here calls anything
// Nextcloud-specific), so accepting it costs us nothing.
const ACCEPTED_PROVIDER_TYPES: &[&str] = &["owncloud", "webdav"];

#[derive(Debug, Clone, Default)]
pub struct GoaAccountInfo {
    pub account_id: String,
    pub object_path: String,
    pub identity: String,
    pub presentation_identity: String,
    pub server_url: Option<String>,
    /// The raw Files:Uri as GOA reports it (e.g.
    /// "davs://user@cloud.example.com:443/remote.php/webdav/") -- this is
    /// what sync.rs actually mounts and builds child paths against.
    /// server_url above is the reconstructed bare https:// root, useful
    /// for display, but reversing it a second time to get back to a
    /// davs:// mount URI would be redundant and lossy (drops the
    /// account's specific WebDAV path shape). Keep both; use the right
    /// one for the right purpose.
    pub files_uri: Option<String>,
    pub attention_needed: bool,
}

// Every variant's fields are read by nobody right now -- the one
// current subscriber (main.rs) deliberately treats every GoaEvent the
// same way (a plain "go re-read watcher.accounts() and refresh," no
// branching on which variant or what it carries -- see that loop's own
// comment). Kept anyway, not stripped down to unit variants: the whole
// point of naming FilesUnavailable separately from AccountUnavailable,
// say, is for a human reading this enum or a future debug log to see
// which account and what happened, not for today's one consumer to
// branch on it. #[allow(dead_code)] here is a deliberate acknowledgment
// of that, not a suppressed oversight.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum GoaEvent {
    AccountAvailable { account_id: String, presentation_identity: String },
    AccountUnavailable { account_id: String },
    ServerUrlResolved { account_id: String, server_url: String },
    /// The account itself is still there, but its "Files" capability got
    /// switched off in GNOME Settings -> Online Accounts -- distinct from
    /// AccountUnavailable, which is a full account removal. main.rs
    /// treats this the same way as any other GoaEvent (just a "go
    /// refresh" signal), but it's named separately from
    /// AccountUnavailable since the two mean genuinely different things
    /// to anyone reading the event log later.
    FilesUnavailable { account_id: String },
}

pub struct GoaWatcher {
    connection: gio::DBusConnection,
    accounts: Rc<RefCell<HashMap<String, GoaAccountInfo>>>,
    events_tx: async_channel::Sender<GoaEvent>,
    // Kept alive for the lifetime of the watcher -- dropping these would
    // unsubscribe (SignalSubscription is an RAII guard as of gio 0.22).
    _added_sub: gio::SignalSubscription,
    _removed_sub: gio::SignalSubscription,
}

impl GoaWatcher {
    /// Connects to the session bus, does an initial scan, and starts
    /// watching for changes. Emits events on `events_tx` as accounts
    /// appear/disappear/resolve their server URL. Errors here mean the
    /// session bus itself is unreachable, not that GOA isn't running --
    /// GOA not running yet is a normal, recoverable state (see note below).
    pub async fn new(events_tx: async_channel::Sender<GoaEvent>) -> Result<Self, glib::Error> {
        let connection = gio::bus_get_future(gio::BusType::Session).await?;
        let accounts = Rc::new(RefCell::new(HashMap::new()));

        // NOTE: goa-daemon is D-Bus-activatable and can be idle-exited by
        // the bus. Unlike the Qt version's QDBusServiceWatcher, we don't
        // separately track its NameOwnerChanged here -- GetManagedObjects
        // below will simply auto-activate it if it's not running, and if
        // it exits later, our signal subscriptions just go quiet rather
        // than erroring (matching how any other D-Bus client handles an
        // activatable service). Worth revisiting with an explicit
        // NameOwnerChanged watch if silent staleness ever turns out to be
        // a real problem in practice.

        let added_sub = {
            let accounts = accounts.clone();
            let events_tx = events_tx.clone();
            connection.subscribe_to_signal(
                Some(GOA_SERVICE),
                Some(OBJECT_MANAGER_IFACE),
                Some("InterfacesAdded"),
                Some(GOA_OBJECT_MANAGER_PATH),
                None,
                gio::DBusSignalFlags::NONE,
                move |signal| {
                    handle_interfaces_added(&accounts, &events_tx, signal.parameters);
                },
            )
        };

        let removed_sub = {
            let accounts = accounts.clone();
            let events_tx = events_tx.clone();
            connection.subscribe_to_signal(
                Some(GOA_SERVICE),
                Some(OBJECT_MANAGER_IFACE),
                Some("InterfacesRemoved"),
                Some(GOA_OBJECT_MANAGER_PATH),
                None,
                gio::DBusSignalFlags::NONE,
                move |signal| {
                    handle_interfaces_removed(&accounts, &events_tx, signal.parameters);
                },
            )
        };

        let watcher = Self {
            connection,
            accounts,
            events_tx,
            _added_sub: added_sub,
            _removed_sub: removed_sub,
        };

        watcher.initial_scan().await?;
        Ok(watcher)
    }

    async fn initial_scan(&self) -> Result<(), glib::Error> {
        let reply = self.connection.call_future(
            Some(GOA_SERVICE),
            GOA_OBJECT_MANAGER_PATH,
            OBJECT_MANAGER_IFACE,
            "GetManagedObjects",
            None,
            Some(glib::VariantTy::new("(a{oa{sa{sv}}})").unwrap()),
            gio::DBusCallFlags::NONE,
            -1,
        ).await?;

        // Unwrap the single-element tuple GetManagedObjects returns.
        let objects_variant = reply.child_value(0);
        let objects: HashMap<String, HashMap<String, HashMap<String, Variant>>> =
            objects_variant.get().ok_or_else(|| {
                glib::Error::new(gio::IOErrorEnum::InvalidArgument, "unexpected GetManagedObjects reply shape")
            })?;

        for (object_path, interfaces) in objects {
            apply_account_interfaces(&self.accounts, &self.events_tx, &object_path, &interfaces);
        }

        Ok(())
    }

    pub fn accounts(&self) -> Vec<GoaAccountInfo> {
        self.accounts.borrow().values().cloned().collect()
    }

    /// One account by ID, not the whole list -- the new WebDAV-based
    /// sync path (see webdav.rs and sync_target's own setup in sync.rs)
    /// needs exactly this account's files_uri to build a base URL from,
    /// and cloning the entire map just to find one entry would be
    /// needless work on top of needless allocation.
    pub fn account_info(&self, account_id: &str) -> Option<GoaAccountInfo> {
        self.accounts.borrow().get(account_id).cloned()
    }

    /// Retrieves this account's own password and returns it wrapped so
    /// it can be used and dropped without lingering in memory longer
    /// than necessary. Used to be a narrow, two-caller exception (see
    /// this file's own git history) back when ensure_mounted's own
    /// GVfs "shadow mount" machinery handled credentials for everything
    /// else -- that's gone now (see webdav.rs's own top-level doc
    /// comment for the full migration), so this is the one and only
    /// credential path left in the app: every caller that used to go
    /// through a mount (sync.rs's own sync_target, folder_picker.rs's
    /// remote browsing) now fetches through here directly instead.
    ///
    /// Returns Zeroizing<String>, not a plain String -- the caller's
    /// entire reason for existing is to hold this for as short a time as
    /// possible (one HTTP POST), and Zeroizing actively overwrites the
    /// underlying memory the moment it drops, rather than just
    /// deallocating it the way an ordinary String's Drop would.
    ///
    /// NOTE: the exact `id` argument GetPassword expects hasn't been
    /// confirmed against a live GOA instance -- an empty string is
    /// passed here, matching GOA's own D-Bus introspection convention
    /// for providers (Nextcloud/ownCloud/webdav-type accounts among
    /// them) that only ever have one, undifferentiated credential to
    /// return. Worth confirming this against real hardware before
    /// relying on it in production.
    pub async fn get_password_for_http_auth(&self, account_id: &str) -> Result<zeroize::Zeroizing<String>, glib::Error> {
        let object_path = {
            let accounts = self.accounts.borrow();
            accounts.get(account_id)
                .map(|a| a.object_path.clone())
                .ok_or_else(|| glib::Error::new(gio::IOErrorEnum::NotFound, "account not found"))?
        };

        let reply = self.connection.call_future(
            Some(GOA_SERVICE),
            &object_path,
            PASSWORD_BASED_IFACE,
            "GetPassword",
            Some(&(String::new(),).to_variant()),
            Some(glib::VariantTy::new("(s)").unwrap()),
            gio::DBusCallFlags::NONE,
            -1,
        ).await?;

        let password: String = reply.child_value(0).get().ok_or_else(|| {
            glib::Error::new(gio::IOErrorEnum::InvalidArgument, "unexpected GetPassword reply shape")
        })?;

        Ok(zeroize::Zeroizing::new(password))
    }
}

fn handle_interfaces_added(
    accounts: &Rc<RefCell<HashMap<String, GoaAccountInfo>>>,
    events_tx: &async_channel::Sender<GoaEvent>,
    params: &Variant,
) {
    // params is (o, a{sa{sv}})
    let Some(object_path) = params.child_value(0).get::<String>() else { return };
    let Some(interfaces) = params.child_value(1).get::<HashMap<String, HashMap<String, Variant>>>() else { return };
    apply_account_interfaces(accounts, events_tx, &object_path, &interfaces);
}

fn handle_interfaces_removed(
    accounts: &Rc<RefCell<HashMap<String, GoaAccountInfo>>>,
    events_tx: &async_channel::Sender<GoaEvent>,
    params: &Variant,
) {
    // params is (o, as) -- object path + list of interface names removed
    let Some(object_path) = params.child_value(0).get::<String>() else { return };
    let Some(removed_interfaces) = params.child_value(1).get::<Vec<String>>() else { return };

    let id = account_id_from_path(&object_path);

    // Only a full account removal (Account interface itself gone) counts
    // as AccountUnavailable -- losing just Files/Mail/etc while Account
    // remains means the user flipped a capability switch, not removed
    // the account.
    if removed_interfaces.iter().any(|i| i == ACCOUNT_IFACE) {
        let existed = accounts.borrow_mut().remove(&id).is_some();
        if existed {
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] goa: account {id} interface removed -> AccountUnavailable", now_hms());
            }
            let _ = events_tx.try_send(GoaEvent::AccountUnavailable { account_id: id });
        } else if crate::is_debug_enabled() {
            eprintln!("[{}] [debug] goa: account {id} interface removed, but wasn't tracked -- ignoring", now_hms());
        }
        return; // account is gone entirely -- nothing else to check below
    }

    // Files specifically getting toggled off (GNOME Settings -> Online
    // Accounts -> that account -> "Files" switch) previously fell
    // through here and did nothing, leaving files_uri/server_url as
    // whatever they were before the toggle -- which is exactly what let
    // a Files-disabled account keep showing up as usable. Clear the
    // stale values so accounts() correctly stops reporting this one as
    // having Files available.
    if removed_interfaces.iter().any(|i| i == FILES_IFACE) {
        let had_files = {
            let mut accounts_mut = accounts.borrow_mut();
            match accounts_mut.get_mut(&id) {
                Some(entry) => {
                    let had = entry.files_uri.is_some();
                    entry.files_uri = None;
                    entry.server_url = None;
                    had
                }
                None => false,
            }
        };
        if had_files {
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] goa: account {id} Files interface removed -> FilesUnavailable", now_hms());
            }
            let _ = events_tx.try_send(GoaEvent::FilesUnavailable { account_id: id });
        }
    }
}

fn apply_account_interfaces(
    accounts: &Rc<RefCell<HashMap<String, GoaAccountInfo>>>,
    events_tx: &async_channel::Sender<GoaEvent>,
    object_path: &str,
    interfaces: &HashMap<String, HashMap<String, Variant>>,
) {
    let id = account_id_from_path(object_path);

    if let Some(account_props) = interfaces.get(ACCOUNT_IFACE) {
        let provider_type = account_props.get("ProviderType")
            .and_then(|v| v.get::<String>())
            .unwrap_or_default();

        if !ACCEPTED_PROVIDER_TYPES.contains(&provider_type.as_str()) {
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] goa: account {id} provider_type={provider_type:?} not in ACCEPTED_PROVIDER_TYPES, ignoring", now_hms());
            }
            return; // not a Nextcloud/ownCloud account -- ignore
        }

        let identity = account_props.get("Identity").and_then(|v| v.get::<String>()).unwrap_or_default();
        let presentation_identity = account_props.get("PresentationIdentity").and_then(|v| v.get::<String>()).unwrap_or_default();
        let attention_needed = account_props.get("AttentionNeeded").and_then(|v| v.get::<bool>()).unwrap_or(false);

        let was_known = accounts.borrow().contains_key(&id);

        // The clean value computed here (not GOA's raw presentation_identity
        // above) is what actually gets stored and sent in the event below --
        // see clean_presentation_identity's own doc comment for why GOA's
        // own value isn't trusted directly. Uses whatever server_url is
        // already known at this point (None on first-ever discovery, in
        // which case this falls back to GOA's raw value until the Files
        // block below resolves one and corrects it).
        let clean_identity = {
            let mut accounts_mut = accounts.borrow_mut();
            let entry = accounts_mut.entry(id.clone()).or_default();
            entry.account_id = id.clone();
            entry.object_path = object_path.to_string();
            let clean = clean_presentation_identity(&identity, entry.server_url.as_deref(), &presentation_identity);
            entry.identity = identity;
            entry.presentation_identity = clean.clone();
            entry.attention_needed = attention_needed;
            clean
        };

        if !was_known {
            if crate::is_debug_enabled() {
                eprintln!("[{}] [debug] goa: account {id} ({clean_identity}) newly seen -> AccountAvailable", now_hms());
            }
            let _ = events_tx.try_send(GoaEvent::AccountAvailable {
                account_id: id.clone(),
                presentation_identity: clean_identity,
            });
        }
    }

    if let Some(files_props) = interfaces.get(FILES_IFACE) {
        if accounts.borrow().contains_key(&id) {
            let files_uri = files_props.get("Uri").and_then(|v| v.get::<String>()).unwrap_or_default();
            if let Some(server_url) = reconstruct_server_url(&files_uri) {
                let changed = {
                    let mut accounts_mut = accounts.borrow_mut();
                    let entry = accounts_mut.get_mut(&id).unwrap();
                    let changed = entry.server_url.as_deref() != Some(server_url.as_str());
                    entry.server_url = Some(server_url.clone());
                    entry.files_uri = Some(files_uri.clone());
                    // Recomputed now that the host is actually known --
                    // this is what corrects the display string for the
                    // common case (a fresh account, where this block runs
                    // right after the Account block above stored identity,
                    // but before any host was known to build a clean value
                    // from yet).
                    let clean = clean_presentation_identity(&entry.identity, Some(&server_url), &entry.presentation_identity);
                    entry.presentation_identity = clean;
                    changed
                };
                if changed {
                    if crate::is_debug_enabled() {
                        eprintln!("[{}] [debug] goa: account {id} server_url resolved/changed -> {server_url} -> ServerUrlResolved", now_hms());
                    }
                    let _ = events_tx.try_send(GoaEvent::ServerUrlResolved { account_id: id, server_url });
                } else if crate::is_debug_enabled() {
                    eprintln!("[{}] [debug] goa: account {id} Files interface update, server_url unchanged ({server_url})", now_hms());
                }
            }
        }
    }
}

fn account_id_from_path(object_path: &str) -> String {
    object_path.rsplit('/').next().unwrap_or(object_path).to_string()
}

/// Reverses GOA's own scheme-swap + WebDAV-endpoint-append logic to
/// recover the bare server root from a Files:Uri like
/// "davs://user@cloud.example.com/remote.php/dav/files/user/". Not a
/// documented, stable contract -- just the observable current shape.
/// Plain string handling rather than the `url` crate -- this is a narrow,
/// well-defined transform and doesn't need a general-purpose URL parser
/// as a dependency for it.
/// Builds a clean, uniform "identity@host" display string, falling back
/// to GOA's own raw PresentationIdentity when the host isn't known yet
/// (before Files has resolved) or identity itself is empty. Exists
/// because GOA formats PresentationIdentity inconsistently between
/// provider types -- confirmed against a real webdav-type account:
/// owncloud-type gives a clean "user@host", but webdav-type included the
/// full DAV path ("user@host/remote.php/dav/files/user"), which showed
/// up verbatim as the account row's title in the Connections tab. Rather
/// than special-case webdav specifically, this sidesteps GOA's own
/// formatting entirely and builds the display string from data already
/// trusted elsewhere (Identity, and the host reconstruct_server_url
/// already derives) -- uniform for every provider type, not just the
/// one that happened to prompt this.
fn clean_presentation_identity(identity: &str, server_url: Option<&str>, raw_presentation_identity: &str) -> String {
    let host = server_url.and_then(|u| u.split_once("://")).map(|(scheme, host)| {
        // Strip the port only when it's the scheme's own default --
        // that specific case is redundant/implied, not informative. A
        // genuinely non-standard port (someone self-hosting on :8443,
        // say) stays visible, since that IS real, relevant information
        // a default port isn't. GOA's own native PresentationIdentity
        // never showed a port even when present, which is what this
        // restores parity with for the common case, without losing the
        // non-default-port case entirely -- confirmed as a real
        // regression once observed in practice (a Nextcloud account
        // showing "user@cloud.example.com:443").
        match scheme {
            "https" => host.strip_suffix(":443").unwrap_or(host),
            "http" => host.strip_suffix(":80").unwrap_or(host),
            _ => host,
        }
    });
    match host {
        Some(host) if !identity.is_empty() => format!("{identity}@{host}"),
        _ => raw_presentation_identity.to_string(),
    }
}

fn reconstruct_server_url(files_uri: &str) -> Option<String> {
    let (scheme, rest) = files_uri.split_once("://")?;

    let new_scheme = match scheme {
        "davs" => "https",
        "dav" => "http",
        other => {
            eprintln!("goa: unexpected Files:Uri scheme '{other}' -- GOA version may have changed this");
            other
        }
    };

    // rest is "user@host:port/path..." or "host:port/path..." -- keep
    // everything up to (and not including) the first '/', which is the
    // authority section; drop any userinfo since a bare server root has
    // no business embedding a username.
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    let authority = authority.rsplit_once('@').map(|(_, host)| host).unwrap_or(authority);

    if !path.contains("/remote.php") {
        eprintln!("goa: Files:Uri path doesn't contain /remote.php as expected: {path}");
        return Some(format!("{new_scheme}://{authority}{path}")); // best-effort fallback
    }

    Some(format!("{new_scheme}://{authority}"))
}

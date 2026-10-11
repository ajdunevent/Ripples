//! System tray icon (StatusNotifierItem via `ksni`), gated behind the
//! "System Tray" setting (`config::AppConfig::tray_enabled`).
//!
//! One complete, hand-drawn icon per status -- not a constant background
//! with a status glyph layered on top via `overlay_icon_name`. That
//! overlay approach was tried first and abandoned: confirmed via direct
//! D-Bus introspection that ksni was sending the correct data, and
//! confirmed via the real GNOME Shell AppIndicator extension source that
//! it does build a `Gio.Emblem`/`Gio.EmblemedIcon` from it -- but nothing
//! ever rendered, most likely because `St.Icon` (the actual panel-icon
//! widget) doesn't paint emblems the way Nautilus or other GTK contexts
//! do. Checking the real Nextcloud desktop client's own source settled
//! it: its apparent "cloud + badge" look is not an overlay at all --
//! `Theme::syncStateIcon` just maps each status to one pre-drawn asset
//! and hands the whole thing to `QSystemTrayIcon::setIcon` directly.
//! This mirrors that proven approach.
//!
//! Icons are raw ARGB32 pixel data (`icon_pixmap()`), embedded directly
//! into the binary via `include_bytes!` at compile time -- NOT a
//! filesystem path (`icon_name()`/`icon_theme_path()`) the SNI host
//! looks up at runtime. This replaced an earlier version that did use a
//! filesystem path (`CARGO_MANIFEST_DIR`-relative, pointing at an
//! `icons/` directory alongside `Cargo.toml`) -- that approach genuinely
//! worked for `cargo run`, but only because the source tree stays where
//! it was built; it would have broken entirely once packaged and
//! installed on a different machine, or even the same machine with the
//! source tree later moved. Embedding the bytes directly removes that
//! fragility completely: the data lives inside the compiled binary
//! itself, so there's no separate "install the icon files somewhere"
//! step ever needed, packaged or not.
//!
//! Source SVGs get rasterized to PNG once, offline (via `rsvg-convert`),
//! at four sizes (16/24/32/48px). `icon_pixmap()` returns a
//! `Vec<ksni::Icon>` specifically so the host can pick whichever size
//! best matches its actual panel size/scaling factor, rather than
//! handing it one size and hoping its own scaling looks acceptable. A
//! single-48px-only version was tried first, on the theory that
//! downscaling a large bitmap should always look clean regardless of
//! target size, unlike upscaling a small one -- confirmed false in
//! practice: the actual host's own downscaling from 48px visibly didn't
//! look good at the sizes a real panel actually renders at, so this
//! reverted back to providing every size a host might want natively
//! rather than asking it to scale anything. Decoding and the
//! RGBA-to-ARGB byte-order conversion follow the exact pattern ksni's
//! own documentation shows for exactly this use case (a static inlined
//! icon via the `image` crate).
//!
//! Two color sets, light and dark, per status -- see
//! config::AppConfig::tray_icon_dark's own doc comment for why this is a
//! manual Settings choice rather than automatic detection (no reliable
//! way to query a host panel's actual background color exists). Both
//! sets are embedded; `icon_pixmap()` picks between them at runtime
//! based on `RippleTray::icon_dark`, updated live via
//! `TrayUpdate::IconColor` the same way a status change already is.
//!
//! Runs on its own dedicated OS thread, not `glib::MainContext::
//! spawn_local` -- ksni's `async-io` feature still doesn't run ON
//! glib's own event loop, it's a genuinely separate reactor (confirmed
//! via real ksni source: the feature ties to `zbus/async-io`, an
//! independent runtime from glib's), so this is the one place in the
//! whole app that isn't single-threaded `Rc`/`RefCell`. Everything
//! crossing that boundary goes through `async_channel` in small owned
//! messages (a plain `TrayStatus` enum one way, a `TrayCommand` enum the
//! other), never a shared `Rc` -- `Rc` isn't `Send`, so `RippleTray`
//! itself holds no reference to `AppState` at all. See
//! `main.rs::spawn_tray_if_enabled` for the actual thread-spawn and
//! channel wiring; this file only holds the `ksni::Tray` implementation
//! and the icon-selection logic.

use crate::status;
use std::sync::LazyLock;

/// Decodes one embedded PNG (RGBA) into a ksni::Icon (ARGB32, network
/// byte order) -- same conversion ksni's own docs show for exactly this
/// "static inlined icon via the image crate" use case.
fn decode(bytes: &[u8]) -> ksni::Icon {
    let img = image::load_from_memory_with_format(bytes, image::ImageFormat::Png)
        .expect("bundled tray icon PNG should always be valid -- generated and embedded at build time, never user-supplied");
    let (width, height) = image::GenericImageView::dimensions(&img);
    let mut data = img.into_rgba8().into_vec();
    for pixel in data.chunks_exact_mut(4) {
        pixel.rotate_right(1); // rgba -> argb
    }
    ksni::Icon { width: width as i32, height: height as i32, data }
}

/// One status's full icon set, one color -- all four sizes (see this
/// module's own doc comment for why: an all-48px version was tried and
/// found to look visibly worse in practice than letting the host pick a
/// natively-rendered size). Built once and reused: re-decoding on every
/// status/color change would be wasteful given these never change at
/// runtime. `$suffix` is "light" or "dark", matching the filename
/// convention (`{status}-{suffix}-{size}.png`).
macro_rules! icon_set {
    ($name:literal, $suffix:literal) => {
        LazyLock::new(|| {
            vec![
                decode(include_bytes!(concat!("../assets/icons/tray/", $name, "-", $suffix, "-16.png"))),
                decode(include_bytes!(concat!("../assets/icons/tray/", $name, "-", $suffix, "-24.png"))),
                decode(include_bytes!(concat!("../assets/icons/tray/", $name, "-", $suffix, "-32.png"))),
                decode(include_bytes!(concat!("../assets/icons/tray/", $name, "-", $suffix, "-48.png"))),
            ]
        })
    };
}

static NO_ACCOUNT_ICONS_LIGHT: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-no-account", "light");
static NO_ACCOUNT_ICONS_DARK: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-no-account", "dark");
static PAUSED_ICONS_LIGHT: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-paused", "light");
static PAUSED_ICONS_DARK: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-paused", "dark");
static MONITORING_ICONS_LIGHT: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-monitoring", "light");
static MONITORING_ICONS_DARK: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-monitoring", "dark");
static SYNCING_ICONS_LIGHT: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-syncing", "light");
static SYNCING_ICONS_DARK: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-syncing", "dark");
static DISCONNECTED_ICONS_LIGHT: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-disconnected", "light");
static DISCONNECTED_ICONS_DARK: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-disconnected", "dark");
static ATTENTION_ICONS_LIGHT: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-attention", "light");
static ATTENTION_ICONS_DARK: LazyLock<Vec<ksni::Icon>> = icon_set!("net.dunevent.Ripples-attention", "dark");

/// Which status is currently showing -- replaces what used to be a bare
/// icon-name String, back when icon_name()/icon_theme_path() did a
/// filesystem-based lookup instead of icon_pixmap() carrying the actual
/// pixel data. A plain Copy enum is both simpler and cheaper to send
/// across the channel than a String ever was.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrayStatus {
    NoAccount,
    Paused,
    Monitoring,
    Syncing,
    Disconnected,
    Attention,
}

impl TrayStatus {
    fn icons(self, dark: bool) -> &'static Vec<ksni::Icon> {
        match (self, dark) {
            (TrayStatus::NoAccount, false) => &NO_ACCOUNT_ICONS_LIGHT,
            (TrayStatus::NoAccount, true) => &NO_ACCOUNT_ICONS_DARK,
            (TrayStatus::Paused, false) => &PAUSED_ICONS_LIGHT,
            (TrayStatus::Paused, true) => &PAUSED_ICONS_DARK,
            (TrayStatus::Monitoring, false) => &MONITORING_ICONS_LIGHT,
            (TrayStatus::Monitoring, true) => &MONITORING_ICONS_DARK,
            (TrayStatus::Syncing, false) => &SYNCING_ICONS_LIGHT,
            (TrayStatus::Syncing, true) => &SYNCING_ICONS_DARK,
            (TrayStatus::Disconnected, false) => &DISCONNECTED_ICONS_LIGHT,
            (TrayStatus::Disconnected, true) => &DISCONNECTED_ICONS_DARK,
            (TrayStatus::Attention, false) => &ATTENTION_ICONS_LIGHT,
            (TrayStatus::Attention, true) => &ATTENTION_ICONS_DARK,
        }
    }
}

/// Picks which status is currently showing. Same priority order as
/// `update_status_ui`'s own Status row text (no account -> no
/// connections -> conflicts/pending actions -> paused -> sync state),
/// since it's the same real-world priority: something needing setup or
/// a look outranks whatever the sync state itself would otherwise show.
///
/// Takes plain values, not `&AppState` -- `AppState` and its fields are
/// private to main.rs's module scope by design (not something this
/// change should casually widen just for the tray), so main.rs reads its
/// own state and calls this with the results already extracted, keeping
/// the actual selection LOGIC here in one place rather than duplicated
/// wherever it's needed.
///
/// `needs_attention` deliberately doesn't map to ksni's own `status()`/
/// `NeedsAttention` mechanism -- just an icon swap, no protocol-level
/// status change, per an explicit choice not to request that.
///
/// No longer takes upload/download direction -- there are only enough
/// hand-drawn icons for one generic "syncing" state, not three separate
/// ones for upload-only/download-only/both. This is scoped to THIS
/// function specifically: `AppState::round_uploaded`/`round_downloaded`
/// and the Status tab's own "(uploading)"/"(downloading)"/"(uploading &
/// downloading)" text label are untouched and still meaningfully
/// distinct there -- text has no such limit the way a single small icon
/// does, so that distinction still earns its keep even though the tray
/// can't show it.
pub fn select_status(
    no_account_or_connections: bool,
    needs_attention: bool,
    paused: bool,
    sync_state: status::SyncState,
) -> TrayStatus {
    if no_account_or_connections {
        return TrayStatus::NoAccount;
    }
    if needs_attention {
        return TrayStatus::Attention;
    }
    if paused {
        return TrayStatus::Paused;
    }
    match sync_state {
        status::SyncState::Disconnected => TrayStatus::Disconnected,
        // Idle and Scanning both map to the same existing Monitoring
        // icon set -- status.rs's own split between "genuinely idle,
        // listening for a trigger" and "actively walking/comparing
        // right now" is a real, useful distinction in the Status tab's
        // own text, but doesn't warrant new tray artwork on its own; the
        // tray's job is coarser-grained (syncing vs. not) than the
        // Status row's.
        status::SyncState::Idle | status::SyncState::Scanning => TrayStatus::Monitoring,
        status::SyncState::Syncing => TrayStatus::Syncing,
    }
}

/// Sent from the main thread to the tray's own thread.
pub enum TrayUpdate {
    Icon(TrayStatus),
    IconColor(bool),
    SyncNowEnabled(bool),
    /// Whether the app is actually paused right now (metered/power-save/
    /// user-initiated, same three conditions select_status's own third
    /// argument already folds together) -- sent as its own value rather
    /// than derived from `status` on the tray's own side, same reasoning
    /// as SyncNowEnabled just above (see its own doc comment):
    /// TrayStatus::Paused only ever shows when paused is the single
    /// highest-priority thing to display at all, per select_status's own
    /// priority order (no-account, THEN needs_attention, THEN paused,
    /// THEN sync state) -- a real, reachable case (pause the app while
    /// an existing conflict/permission error is still unresolved) means
    /// the app can genuinely be paused while `status` shows Attention
    /// instead, which `self.status == TrayStatus::Paused` alone can't
    /// tell apart from "not paused at all." Confirmed as a real bug
    /// without this: the Pause/Resume menu item's own label would read
    /// "Pause Syncing" -- implying the app is currently running -- while
    /// actually paused, and clicking it would silently unpause rather
    /// than do what the visible label suggested.
    Paused(bool),
    /// Whether the main window is currently visible -- drives the
    /// Show/Hide Ripples menu label and the left-click toggle behavior.
    WindowVisibility(bool),
    Shutdown,
}

/// Sent from the tray's own thread back to the main thread whenever the
/// user interacts with it. The tray has no access to `AppState` or any
/// GTK widget (wrong thread for both), so every click just becomes one
/// of these, and the actual behavior -- showing or hiding the window
/// depending on its current state, the real quit sequence (dropping the
/// hold guard, then `app.quit()`) -- stays exactly where it already
/// lives in `main.rs`, never duplicated here.
pub enum TrayCommand {
    ToggleVisibility,
    PauseToggle,
    SyncNow,
    Quit,
}

/// The actual `ksni::Tray` implementation. Deliberately minimal: just
/// the current status, which of the two color sets to use, and a
/// sender to report clicks back to the main thread.
pub struct RippleTray {
    pub status: TrayStatus,
    pub icon_dark: bool,
    /// Mirrors the Status tab's own Sync Now button sensitivity exactly
    /// -- same conditions, computed in the same place in main.rs's own
    /// update_status_ui, sent over as its own TrayUpdate variant rather
    /// than derived from `status` here: TrayStatus::Monitoring already
    /// conflates Idle and Scanning into one value (see select_status's
    /// own doc comment), which isn't the distinction this needs --
    /// Scanning should disable this item the same way real activity
    /// does, Idle should not, and TrayStatus alone can't tell those
    /// apart.
    pub sync_now_enabled: bool,
    /// Whether the app is actually paused right now -- see TrayUpdate::
    /// Paused's own doc comment for why this can't be derived from
    /// `status` here, same class of problem sync_now_enabled just above
    /// already has a doc comment about.
    pub paused: bool,
    /// Mirrors the main window's actual current visibility -- drives
    /// the Show/Hide Ripples menu label (and left-click's toggle
    /// behavior in activate() below). Updated via TrayUpdate::
    /// WindowVisibility the same way icon_dark/status already are.
    pub window_visible: bool,
    pub command_tx: async_channel::Sender<TrayCommand>,
}

impl ksni::Tray for RippleTray {
    fn id(&self) -> String {
        "net.dunevent.Ripples".to_string()
    }

    fn title(&self) -> String {
        "Ripples".to_string()
    }

    // Raw pixel data, not a themed icon name -- see this module's own
    // doc comment for why (no filesystem dependency at all, packaged or
    // not). Returns all four bundled sizes for whichever color set is
    // currently selected; the host picks whichever best matches its
    // actual panel size/scaling.
    fn icon_pixmap(&self) -> Vec<ksni::Icon> {
        self.status.icons(self.icon_dark).clone()
    }

    // Left-click: toggles the main window's visibility, same as the
    // Show/Hide Ripples menu item below.
    fn activate(&mut self, _x: i32, _y: i32) {
        let _ = self.command_tx.try_send(TrayCommand::ToggleVisibility);
    }

    fn menu(&self) -> Vec<ksni::MenuItem<Self>> {
        use ksni::menu::*;
        vec![
            StandardItem {
                label: if self.window_visible { "Hide Ripples" } else { "Show Ripples" }.into(),
                activate: Box::new(|this: &mut Self| {
                    let _ = this.command_tx.try_send(TrayCommand::ToggleVisibility);
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Sync Now".into(),
                enabled: self.sync_now_enabled,
                activate: Box::new(|this: &mut Self| {
                    let _ = this.command_tx.try_send(TrayCommand::SyncNow);
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: if self.paused { "Resume Syncing" } else { "Pause Syncing" }.into(),
                activate: Box::new(|this: &mut Self| {
                    let _ = this.command_tx.try_send(TrayCommand::PauseToggle);
                }),
                ..Default::default()
            }
            .into(),
            MenuItem::Separator,
            StandardItem {
                label: "Quit".into(),
                activate: Box::new(|this: &mut Self| {
                    let _ = this.command_tx.try_send(TrayCommand::Quit);
                }),
                ..Default::default()
            }
            .into(),
        ]
    }
}

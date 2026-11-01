//! Flatpak sandbox detection and the portal calls that replace host-side
//! file writes inside it.

use std::path::Path;

/// `/.flatpak-info` exists in every Flatpak sandbox and nowhere else.
pub fn is_flatpak() -> bool {
    Path::new("/.flatpak-info").exists()
}

enum FsAccess {
    Host,
    Home,
    Limited,
}

/// Reads the `filesystems=` list from the `[Context]` group of
/// `/.flatpak-info`. That list already reflects user overrides.
fn fs_access() -> FsAccess {
    let Ok(info) = std::fs::read_to_string("/.flatpak-info") else { return FsAccess::Limited };
    let mut in_context = false;
    let mut home = false;
    for line in info.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_context = line == "[Context]";
            continue;
        }
        let Some(list) = in_context.then(|| line.strip_prefix("filesystems=")).flatten() else { continue };
        for entry in list.split(';').filter(|e| !e.is_empty()) {
            if entry.starts_with('!') {
                continue;
            }
            match entry.split(':').next().unwrap_or("") {
                "host" => return FsAccess::Host,
                "home" => home = true,
                _ => {}
            }
        }
    }
    if home { FsAccess::Home } else { FsAccess::Limited }
}

/// Text for the local-folder step of the Add Sync Connection wizard, or
/// `None` outside a sandbox or when the sandbox can see the whole host.
pub fn filesystem_hint() -> Option<String> {
    if !is_flatpak() {
        return None;
    }
    let scope = match fs_access() {
        FsAccess::Host => return None,
        FsAccess::Home => "Ripples is running in a Flatpak sandbox and can only access your home folder.",
        FsAccess::Limited => "Ripples is running in a Flatpak sandbox and cannot access your home folder.",
    };
    Some(format!(
        "{scope} To sync a folder elsewhere, grant access first with Flatseal or: flatpak override --user --filesystem=/path/to/folder net.dunevent.Ripples"
    ))
}

/// Registers or removes autostart through the Background portal. Err when
/// the request fails or the user declines.
pub async fn set_autostart(enabled: bool) -> Result<(), String> {
    let background = ashpd::desktop::background::Background::request()
        .reason("Keep files in sync after login")
        .auto_start(enabled)
        .dbus_activatable(false)
        .command(["ripples", "--hidden"])
        .send()
        .await
        .map_err(|e| e.to_string())?
        .response()
        .map_err(|e| e.to_string())?;
    if background.auto_start() == enabled {
        Ok(())
    } else {
        Err("autostart request was declined".to_string())
    }
}

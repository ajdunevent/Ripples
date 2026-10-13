// src/watchdog.rs
//
// Opt-in (--watchdog) diagnostics for main-thread stalls. A glib timer on the
// main loop updates a heartbeat; a watcher thread notices when it goes stale,
// and when the loop recovers it reports how long the stall lasted, how much
// CPU the main thread used during it, and what it was doing. Reports go to
// the session log as SyncEvent::Diagnostic. Disabled by default: the timer and
// watcher thread wake the CPU on a fixed schedule.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::status::SyncEvent;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const WATCHER_INTERVAL: Duration = Duration::from_millis(250);
const STALL_THRESHOLD_MS: u64 = 2_000;
const SLOW_OP_THRESHOLD: Duration = Duration::from_millis(250);
// USER_HZ on Linux; /proc reports thread CPU time in these ticks.
const CLOCK_TICKS_PER_SEC: f64 = 100.0;

static ENABLED: OnceLock<bool> = OnceLock::new();
static START: OnceLock<Instant> = OnceLock::new();
static SINK: OnceLock<async_channel::Sender<SyncEvent>> = OnceLock::new();
static HEARTBEAT_MS: AtomicU64 = AtomicU64::new(0);
static PHASE: Mutex<&'static str> = Mutex::new("none");
static NOTE: Mutex<String> = Mutex::new(String::new());

pub fn set_enabled(enabled: bool) {
    let _ = ENABLED.set(enabled);
}

pub fn enabled() -> bool {
    ENABLED.get().copied().unwrap_or(false)
}

fn now_ms() -> u64 {
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Records a stall report in the session log (and on stderr).
pub fn report(message: String) {
    eprintln!("[watchdog] {message}");
    if let Some(tx) = SINK.get() {
        let _ = tx.try_send(SyncEvent::Diagnostic { message });
    }
}

/// Last-known position of the sync engine, included in stall reports.
pub fn note(label: &str, detail: &str) {
    if enabled() {
        *lock(&NOTE) = format!("{label}: {detail}");
    }
}

/// Marks a synchronous main-thread section. The phase is included in stall
/// reports, and a section running longer than SLOW_OP_THRESHOLD is reported
/// when it ends. Main thread only.
pub struct Section {
    name: &'static str,
    previous: &'static str,
    started: Option<Instant>,
}

pub fn section(name: &'static str) -> Section {
    if !enabled() {
        return Section { name, previous: "none", started: None };
    }
    let previous = std::mem::replace(&mut *lock(&PHASE), name);
    Section { name, previous, started: Some(Instant::now()) }
}

impl Drop for Section {
    fn drop(&mut self) {
        let Some(started) = self.started else { return };
        *lock(&PHASE) = self.previous;
        let elapsed = started.elapsed();
        if elapsed >= SLOW_OP_THRESHOLD {
            report(format!("Slow main-thread operation: {} took {} ms", self.name, elapsed.as_millis()));
        }
    }
}

/// SQLite profile callback; installed by journal::open when enabled.
pub fn report_slow_statement(sql: &str, elapsed: Duration) {
    if elapsed >= SLOW_OP_THRESHOLD {
        let sql: String = sql.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(80).collect();
        report(format!("Slow SQLite statement: {} ms: {sql}", elapsed.as_millis()));
    }
}

/// Starts the heartbeat and watcher thread. Must be called on the main
/// thread, once; later calls are ignored.
pub fn start(sink: async_channel::Sender<SyncEvent>) {
    if !enabled() || SINK.set(sink).is_err() {
        return;
    }
    // "/proc/thread-self" resolves to "<pid>/task/<tid>".
    let Some(tid) = std::fs::read_link("/proc/thread-self").ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
    else {
        eprintln!("[watchdog] couldn't determine main thread id; watchdog not started");
        return;
    };

    HEARTBEAT_MS.store(now_ms(), Ordering::Relaxed);
    glib::timeout_add_local(HEARTBEAT_INTERVAL, || {
        HEARTBEAT_MS.store(now_ms(), Ordering::Relaxed);
        glib::ControlFlow::Continue
    });
    std::thread::spawn(move || watch(&tid));
}

struct Stall {
    last_heartbeat_ms: u64,
    cpu_ticks_before: u64,
    running: u32,
    disk_wait: u32,
    sleeping: u32,
    phase: &'static str,
    note: String,
}

fn watch(tid: &str) {
    let stat_path = format!("/proc/self/task/{tid}/stat");
    let mut seen_heartbeat = HEARTBEAT_MS.load(Ordering::Relaxed);
    let mut cpu_at_heartbeat = thread_stat(&stat_path).map(|(_, t)| t).unwrap_or(0);
    let mut stall: Option<Stall> = None;

    loop {
        std::thread::sleep(WATCHER_INTERVAL);
        let heartbeat = HEARTBEAT_MS.load(Ordering::Relaxed);
        let sample = thread_stat(&stat_path);

        if heartbeat != seen_heartbeat {
            if let Some(s) = stall.take() {
                let cpu_secs = sample.map(|(_, t)| t.saturating_sub(s.cpu_ticks_before)).unwrap_or(0) as f64 / CLOCK_TICKS_PER_SEC;
                report(format!(
                    "UI thread stalled ~{:.1} s; main thread used {:.1} s CPU; samples R={} D={} S={}; phase: {}; last position: {}",
                    heartbeat.saturating_sub(s.last_heartbeat_ms) as f64 / 1000.0,
                    cpu_secs, s.running, s.disk_wait, s.sleeping, s.phase,
                    if s.note.is_empty() { "none" } else { &s.note },
                ));
            }
            seen_heartbeat = heartbeat;
            cpu_at_heartbeat = sample.map(|(_, t)| t).unwrap_or(cpu_at_heartbeat);
            continue;
        }

        if now_ms().saturating_sub(heartbeat) < STALL_THRESHOLD_MS {
            continue;
        }
        let s = stall.get_or_insert_with(|| {
            eprintln!("[watchdog] UI thread not responding for over {} s", STALL_THRESHOLD_MS / 1000);
            Stall {
                last_heartbeat_ms: heartbeat,
                cpu_ticks_before: cpu_at_heartbeat,
                running: 0, disk_wait: 0, sleeping: 0,
                phase: "none",
                note: String::new(),
            }
        });
        match sample.map(|(state, _)| state) {
            Some('R') => s.running += 1,
            Some('D') => s.disk_wait += 1,
            Some(_) => s.sleeping += 1,
            None => {}
        }
        let phase = *lock(&PHASE);
        if phase != "none" {
            s.phase = phase;
        }
        s.note = lock(&NOTE).clone();
    }
}

/// (state, utime + stime in clock ticks) for one thread, from /proc.
fn thread_stat(path: &str) -> Option<(char, u64)> {
    let text = std::fs::read_to_string(path).ok()?;
    // The comm field is parenthesized and may contain spaces; skip past it.
    let rest = text.get(text.rfind(')')? + 1..)?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let state = fields.first()?.chars().next()?;
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((state, utime + stime))
}

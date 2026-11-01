// src/watchdog.rs
//
// Opt-in (--watchdog) diagnostics for main-thread stalls. A glib timer on the
// main loop updates a heartbeat; a watcher thread notices when it goes stale,
// and when the loop recovers it reports how long the stall lasted, how much
// CPU the main thread used during it, and what it was doing. Reports go to
// the session log as SyncEvent::Diagnostic. Disabled by default: the timer and
// watcher thread wake the CPU on a fixed schedule.
//
// A second diagnostic tracks sync work in flight. Each walked item registers on
// entry and unregisters on exit. When no item has completed for STUCK_AFTER, the
// watcher logs what is still in flight and the step each item last reported.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::status::SyncEvent;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const WATCHER_INTERVAL: Duration = Duration::from_millis(250);
const STALL_THRESHOLD_MS: u64 = 2_000;
// Sampling starts earlier than the report threshold so short-lived states are
// still captured once a stall crosses it.
const SAMPLE_AFTER_MS: u64 = 500;
// A stall is held back this long after recovery so a late-delivered suspend
// signal can still cancel it.
const REPORT_DELAY_MS: u64 = 1_000;
const SLOW_OP_THRESHOLD: Duration = Duration::from_millis(250);
// No completed item for this long, with work still in flight, counts as stuck.
const STUCK_AFTER: Duration = Duration::from_secs(300);
// A stuck state that persists is reported again at this interval.
const STUCK_REPEAT: Duration = Duration::from_secs(1800);
const STUCK_REPORT_MAX_ENTRIES: usize = 20;
// USER_HZ on Linux; /proc reports thread CPU time in these ticks.
const CLOCK_TICKS_PER_SEC: f64 = 100.0;

static ENABLED: OnceLock<bool> = OnceLock::new();
static START: OnceLock<Instant> = OnceLock::new();
static SINK: OnceLock<async_channel::Sender<SyncEvent>> = OnceLock::new();
static HEARTBEAT_MS: AtomicU64 = AtomicU64::new(0);
static PHASE: Mutex<&'static str> = Mutex::new("none");
static NOTE: Mutex<String> = Mutex::new(String::new());
static SLEEP_EVENT_MS: AtomicU64 = AtomicU64::new(0);
static NEXT_TRACK_ID: AtomicU64 = AtomicU64::new(1);
static COMPLETED: AtomicU64 = AtomicU64::new(0);
static PERMITS_IN_USE: AtomicU32 = AtomicU32::new(0);
static IN_FLIGHT: Mutex<Option<HashMap<u64, Entry>>> = Mutex::new(None);

struct Entry {
    kind: &'static str,
    path: String,
    started: Instant,
    step: &'static str,
    step_since: Instant,
    // True while the entry only waits for work it started elsewhere (children
    // of a directory); such entries are summarized rather than listed.
    waiting_on_children: bool,
}

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

/// Publishes the app-wide permit count for stuck-state reports.
pub fn set_permits_in_use(n: u32) {
    if enabled() {
        PERMITS_IN_USE.store(n, Ordering::Relaxed);
    }
}

/// Registers one unit of sync work (an item or a directory walk) until the
/// returned guard is dropped. Also records it as the last-known position of the
/// sync engine, included in stall reports. Dropping the guard counts as
/// progress, including when the future holding it is cancelled.
pub fn track(kind: &'static str, path: &str) -> Track {
    if !enabled() {
        return Track { id: 0 };
    }
    *lock(&NOTE) = format!("{kind}: {path}");
    let id = NEXT_TRACK_ID.fetch_add(1, Ordering::Relaxed);
    let now = Instant::now();
    lock(&IN_FLIGHT).get_or_insert_with(HashMap::new).insert(id, Entry {
        kind,
        path: path.to_string(),
        started: now,
        step: "started",
        step_since: now,
        waiting_on_children: false,
    });
    Track { id }
}

pub struct Track {
    id: u64,
}

impl Track {
    /// Records the operation this entry is about to wait on.
    pub fn step(&self, label: &'static str) {
        self.set(label, false);
    }

    /// Like step, for an entry that only waits on the children it spawned.
    pub fn children(&self, label: &'static str) {
        self.set(label, true);
    }

    fn set(&self, label: &'static str, waiting_on_children: bool) {
        if self.id == 0 {
            return;
        }
        if let Some(entry) = lock(&IN_FLIGHT).as_mut().and_then(|m| m.get_mut(&self.id)) {
            entry.step = label;
            entry.step_since = Instant::now();
            entry.waiting_on_children = waiting_on_children;
        }
    }
}

impl Drop for Track {
    fn drop(&mut self) {
        if self.id == 0 {
            return;
        }
        if let Some(m) = lock(&IN_FLIGHT).as_mut() {
            m.remove(&self.id);
        }
        COMPLETED.fetch_add(1, Ordering::Relaxed);
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

/// Starts the heartbeat, the suspend listener and the watcher thread. Must be
/// called on the main thread, once; later calls are ignored.
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
    glib::MainContext::default().spawn_local(subscribe_to_sleep_signal());
    std::thread::spawn(move || watch(&tid));
}

/// Records every logind PrepareForSleep signal (going to sleep and resuming).
async fn subscribe_to_sleep_signal() {
    let connection = match gio::bus_get_future(gio::BusType::System).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[watchdog] couldn't connect to the system bus for suspend events: {e}");
            return;
        }
    };
    let subscription = connection.subscribe_to_signal(
        Some("org.freedesktop.login1"),
        Some("org.freedesktop.login1.Manager"),
        Some("PrepareForSleep"),
        Some("/org/freedesktop/login1"),
        None,
        gio::DBusSignalFlags::NONE,
        |_| mark_sleep_event(),
    );
    // Dropping the subscription would unsubscribe; it stays for the life of the process.
    std::mem::forget(subscription);
}

fn mark_sleep_event() {
    SLEEP_EVENT_MS.store(now_ms().max(1), Ordering::Relaxed);
}

struct Stall {
    last_heartbeat_ms: u64,
    cpu_ticks_before: u64,
    boot_before: boot_time::Instant,
    mono_before: Instant,
    running: u32,
    disk_wait: u32,
    sleeping: u32,
    phase: &'static str,
    note: String,
    announced: bool,
    // "wchan/syscall" -> samples
    blocked_in: std::collections::HashMap<String, u32>,
}

struct Finished {
    stall: Stall,
    resumed_ms: u64,
    cpu_ticks_after: u64,
    suspended: bool,
    decide_at_ms: u64,
}

fn watch(tid: &str) {
    let task_dir = format!("/proc/self/task/{tid}");
    let stat_path = format!("{task_dir}/stat");
    let mut seen_heartbeat = HEARTBEAT_MS.load(Ordering::Relaxed);
    let mut cpu_at_heartbeat = thread_stat(&stat_path).map(|(_, t)| t).unwrap_or(0);
    let mut boot_at_heartbeat = boot_time::Instant::now();
    let mut mono_at_heartbeat = Instant::now();
    let mut stall: Option<Stall> = None;
    let mut finished: Option<Finished> = None;
    let mut progress = ProgressWatch::new();

    loop {
        std::thread::sleep(WATCHER_INTERVAL);
        progress.check();
        let heartbeat = HEARTBEAT_MS.load(Ordering::Relaxed);
        let sample = thread_stat(&stat_path);

        if let Some(f) = &finished {
            if now_ms() >= f.decide_at_ms {
                let f = finished.take().unwrap();
                conclude(f);
            }
        }

        if heartbeat != seen_heartbeat {
            if let Some(s) = stall.take() {
                let duration = heartbeat.saturating_sub(s.last_heartbeat_ms);
                if duration >= STALL_THRESHOLD_MS {
                    // CLOCK_BOOTTIME advances during suspend, CLOCK_MONOTONIC does not.
                    let boot_elapsed = s.boot_before.elapsed();
                    let mono_elapsed = s.mono_before.elapsed();
                    let suspended = boot_elapsed.saturating_sub(mono_elapsed) > Duration::from_secs(1);
                    // A flush of any previous undecided stall is not needed: a new
                    // stall cannot begin within REPORT_DELAY_MS of recovery.
                    finished = Some(Finished {
                        cpu_ticks_after: sample.map(|(_, t)| t).unwrap_or(s.cpu_ticks_before),
                        stall: s,
                        resumed_ms: heartbeat,
                        suspended,
                        decide_at_ms: now_ms() + REPORT_DELAY_MS,
                    });
                }
            }
            seen_heartbeat = heartbeat;
            cpu_at_heartbeat = sample.map(|(_, t)| t).unwrap_or(cpu_at_heartbeat);
            boot_at_heartbeat = boot_time::Instant::now();
            mono_at_heartbeat = Instant::now();
            continue;
        }

        let age = now_ms().saturating_sub(heartbeat);
        if age < SAMPLE_AFTER_MS {
            continue;
        }
        let s = stall.get_or_insert_with(|| Stall {
            last_heartbeat_ms: heartbeat,
            cpu_ticks_before: cpu_at_heartbeat,
            boot_before: boot_at_heartbeat,
            mono_before: mono_at_heartbeat,
            running: 0, disk_wait: 0, sleeping: 0,
            phase: "none",
            note: String::new(),
            announced: false,
            blocked_in: Default::default(),
        });
        if age >= STALL_THRESHOLD_MS && !s.announced {
            s.announced = true;
            eprintln!("[watchdog] UI thread not responding for over {} s", STALL_THRESHOLD_MS / 1000);
        }
        match sample.map(|(state, _)| state) {
            Some('R') => s.running += 1,
            Some('D') => s.disk_wait += 1,
            Some(_) => s.sleeping += 1,
            None => {}
        }
        *s.blocked_in.entry(blocked_in(&task_dir)).or_insert(0) += 1;
        let phase = *lock(&PHASE);
        if phase != "none" {
            s.phase = phase;
        }
        s.note = lock(&NOTE).clone();
    }
}

/// Detects a walk that has stopped completing items. Uses the monotonic clock,
/// which excludes time spent suspended.
struct ProgressWatch {
    completed: u64,
    since: Instant,
    last_report: Option<Instant>,
}

impl ProgressWatch {
    fn new() -> Self {
        Self { completed: COMPLETED.load(Ordering::Relaxed), since: Instant::now(), last_report: None }
    }

    fn check(&mut self) {
        let completed = COMPLETED.load(Ordering::Relaxed);
        if completed != self.completed {
            *self = Self { completed, since: Instant::now(), last_report: None };
            return;
        }
        let idle = self.since.elapsed();
        if idle < STUCK_AFTER {
            return;
        }
        if self.last_report.is_some_and(|t| t.elapsed() < STUCK_REPEAT) {
            return;
        }
        let Some(message) = describe_in_flight(idle) else {
            // Nothing in flight: the walk finished or has not started.
            self.since = Instant::now();
            return;
        };
        self.last_report = Some(Instant::now());
        report(message);
    }
}

fn describe_in_flight(idle: Duration) -> Option<String> {
    let guard = lock(&IN_FLIGHT);
    let map = guard.as_ref().filter(|m| !m.is_empty())?;
    let mut active: Vec<&Entry> = map.values().filter(|e| !e.waiting_on_children).collect();
    active.sort_by(|a, b| a.step_since.cmp(&b.step_since));
    let mut waiting: Vec<&Entry> = map.values().filter(|e| e.waiting_on_children).collect();
    waiting.sort_by(|a, b| a.started.cmp(&b.started));

    let listed = active.iter().take(STUCK_REPORT_MAX_ENTRIES).map(|e| {
        format!("[{} {} for {} s, entry {} s old] {}",
            e.kind, e.step, e.step_since.elapsed().as_secs(), e.started.elapsed().as_secs(), e.path)
    }).collect::<Vec<_>>().join("; ");
    let hidden = active.len().saturating_sub(STUCK_REPORT_MAX_ENTRIES);
    let oldest_waiting = waiting.first().map(|e| format!(" (oldest: {})", e.path)).unwrap_or_default();
    Some(format!(
        "No item completed for {} s; {} in flight, {} waiting on children{}; {} completed since start; {} shared permits in use. Waiting longest first: {}{}",
        idle.as_secs(), map.len(), waiting.len(), oldest_waiting, COMPLETED.load(Ordering::Relaxed),
        PERMITS_IN_USE.load(Ordering::Relaxed),
        if listed.is_empty() { "none" } else { &listed },
        if hidden > 0 { format!("; and {hidden} more") } else { String::new() },
    ))
}

fn conclude(f: Finished) {
    let s = f.stall;
    let sleep_event = SLEEP_EVENT_MS.load(Ordering::Relaxed);
    let near_sleep_event = sleep_event != 0
        && sleep_event + 2_000 >= s.last_heartbeat_ms
        && sleep_event <= f.resumed_ms + REPORT_DELAY_MS + 2_000;
    if f.suspended || near_sleep_event {
        eprintln!("[watchdog] ignored a ~{:.1} s pause caused by system suspend",
            f.resumed_ms.saturating_sub(s.last_heartbeat_ms) as f64 / 1000.0);
        return;
    }
    let cpu_secs = f.cpu_ticks_after.saturating_sub(s.cpu_ticks_before) as f64 / CLOCK_TICKS_PER_SEC;
    let mut blocked: Vec<_> = s.blocked_in.iter().collect();
    blocked.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
    let blocked = blocked.iter().take(3).map(|(k, n)| format!("{k} x{n}")).collect::<Vec<_>>().join(", ");
    report(format!(
        "UI thread stalled ~{:.1} s; main thread used {:.1} s CPU; samples R={} D={} S={}; blocked in: {}; phase: {}; last position: {}",
        f.resumed_ms.saturating_sub(s.last_heartbeat_ms) as f64 / 1000.0,
        cpu_secs, s.running, s.disk_wait, s.sleeping, blocked, s.phase,
        if s.note.is_empty() { "none" } else { &s.note },
    ));
}

/// "<kernel wait symbol>/<syscall number>" for the main thread, from /proc.
fn blocked_in(task_dir: &str) -> String {
    let wchan = std::fs::read_to_string(format!("{task_dir}/wchan")).unwrap_or_default();
    let syscall = std::fs::read_to_string(format!("{task_dir}/syscall")).unwrap_or_default();
    let syscall = syscall.split_whitespace().next().unwrap_or("?");
    let wchan = wchan.trim();
    format!("{}/{}", if wchan.is_empty() { "?" } else { wchan }, syscall)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_flight_report_lists_leaves_and_summarizes_waiting_entries() {
        set_enabled(true);
        let parent = track("walk", "a");
        parent.children("walking files");
        let leaf = track("item", "a/b.txt");
        leaf.step("waiting for permit (upload)");

        let message = describe_in_flight(Duration::from_secs(301)).expect("work is in flight");
        assert!(message.contains("2 in flight, 1 waiting on children (oldest: a)"), "{message}");
        assert!(message.contains("[item waiting for permit (upload) for 0 s"), "{message}");
        assert!(message.contains("a/b.txt"), "{message}");
        assert!(!message.contains("walking files"), "{message}");

        let before = COMPLETED.load(Ordering::Relaxed);
        drop(leaf);
        drop(parent);
        assert_eq!(COMPLETED.load(Ordering::Relaxed), before + 2);
        assert!(describe_in_flight(Duration::from_secs(301)).is_none());
    }
}

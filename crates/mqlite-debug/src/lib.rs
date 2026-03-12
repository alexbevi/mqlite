use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Component {
    Cli,
    Ipc,
    Wire,
    Server,
    Storage,
    Catalog,
    Query,
    Exec,
    Bson,
}

#[derive(Debug, Clone)]
pub struct SessionHandle {
    inner: Arc<SessionInner>,
}

#[derive(Debug)]
struct SessionInner {
    label: String,
    started_at: Instant,
    started_process: ProcessSnapshot,
    state: Mutex<SessionState>,
}

#[derive(Debug, Default)]
struct SessionState {
    spans: BTreeMap<(Component, String), SpanTotals>,
    counters: BTreeMap<(Component, String), u64>,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct SpanTotals {
    calls: u64,
    total: Duration,
    max: Duration,
}

#[derive(Debug)]
pub struct InstallGuard {
    installed: bool,
}

#[derive(Debug)]
pub struct SpanGuard {
    component: Component,
    operation: &'static str,
    started_at: Option<Instant>,
    session: Option<SessionHandle>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugReport {
    pub label: String,
    pub wall_ms: f64,
    pub process_start: ProcessSnapshot,
    pub process_end: ProcessSnapshot,
    pub process_delta: ProcessDelta,
    pub metadata: BTreeMap<String, String>,
    pub spans: Vec<SpanReport>,
    pub counters: Vec<CounterReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanReport {
    pub component: Component,
    pub operation: String,
    pub calls: u64,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CounterReport {
    pub component: Component,
    pub name: String,
    pub value: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessSnapshot {
    pub pid: u32,
    pub available_parallelism: Option<usize>,
    pub user_cpu_ms: Option<u64>,
    pub system_cpu_ms: Option<u64>,
    pub max_rss: Option<u64>,
    pub max_rss_unit: Option<&'static str>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessDelta {
    pub user_cpu_ms: Option<u64>,
    pub system_cpu_ms: Option<u64>,
}

thread_local! {
    static CURRENT_SESSION: RefCell<Vec<SessionHandle>> = const { RefCell::new(Vec::new()) };
}

pub fn session(label: impl Into<String>) -> SessionHandle {
    SessionHandle {
        inner: Arc::new(SessionInner {
            label: label.into(),
            started_at: Instant::now(),
            started_process: ProcessSnapshot::capture(),
            state: Mutex::new(SessionState::default()),
        }),
    }
}

pub fn install(session: &SessionHandle) -> InstallGuard {
    CURRENT_SESSION.with(|current| current.borrow_mut().push(session.clone()));
    InstallGuard { installed: true }
}

pub fn current() -> Option<SessionHandle> {
    CURRENT_SESSION.with(|current| current.borrow().last().cloned())
}

pub fn span(component: Component, operation: &'static str) -> SpanGuard {
    SpanGuard {
        component,
        operation,
        started_at: current().map(|_| Instant::now()),
        session: current(),
    }
}

pub fn record_duration(component: Component, operation: &str, duration: Duration) {
    if let Some(session) = current() {
        session.record_duration(component, operation, duration);
    }
}

pub fn add_counter(component: Component, name: &str, value: u64) {
    if let Some(session) = current() {
        let mut state = session.inner.state.lock();
        *state
            .counters
            .entry((component, name.to_string()))
            .or_default() += value;
    }
}

pub fn set_metadata(key: &str, value: impl Into<String>) {
    if let Some(session) = current() {
        session
            .inner
            .state
            .lock()
            .metadata
            .insert(key.to_string(), value.into());
    }
}

impl SessionHandle {
    pub fn report(&self) -> DebugReport {
        let state = self.inner.state.lock();
        let process_end = ProcessSnapshot::capture();
        DebugReport {
            label: self.inner.label.clone(),
            wall_ms: duration_ms(self.inner.started_at.elapsed()),
            process_delta: ProcessDelta::between(&self.inner.started_process, &process_end),
            process_start: self.inner.started_process.clone(),
            process_end,
            metadata: state.metadata.clone(),
            spans: state
                .spans
                .iter()
                .map(|((component, operation), totals)| SpanReport {
                    component: *component,
                    operation: operation.clone(),
                    calls: totals.calls,
                    total_ms: duration_ms(totals.total),
                    avg_ms: duration_ms(totals.total) / totals.calls.max(1) as f64,
                    max_ms: duration_ms(totals.max),
                })
                .collect(),
            counters: state
                .counters
                .iter()
                .map(|((component, name), value)| CounterReport {
                    component: *component,
                    name: name.clone(),
                    value: *value,
                })
                .collect(),
        }
    }

    pub fn record_duration(&self, component: Component, operation: &str, duration: Duration) {
        let mut state = self.inner.state.lock();
        let totals = state
            .spans
            .entry((component, operation.to_string()))
            .or_default();
        totals.calls += 1;
        totals.total += duration;
        totals.max = totals.max.max(duration);
    }

    pub fn record_counter(&self, component: Component, name: &str, value: u64) {
        let mut state = self.inner.state.lock();
        *state
            .counters
            .entry((component, name.to_string()))
            .or_default() += value;
    }

    pub fn insert_metadata(&self, key: &str, value: impl Into<String>) {
        self.inner
            .state
            .lock()
            .metadata
            .insert(key.to_string(), value.into());
    }
}

impl Drop for InstallGuard {
    fn drop(&mut self) {
        if !self.installed {
            return;
        }
        CURRENT_SESSION.with(|current| {
            current.borrow_mut().pop();
        });
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        let Some(started_at) = self.started_at.take() else {
            return;
        };
        let Some(session) = self.session.take() else {
            return;
        };
        session.record_duration(self.component, self.operation, started_at.elapsed());
    }
}

impl ProcessSnapshot {
    pub fn capture() -> Self {
        let available_parallelism = std::thread::available_parallelism().ok().map(usize::from);
        #[cfg(unix)]
        {
            capture_unix_process_snapshot(available_parallelism)
        }

        #[cfg(not(unix))]
        {
            Self {
                pid: std::process::id(),
                available_parallelism,
                user_cpu_ms: None,
                system_cpu_ms: None,
                max_rss: None,
                max_rss_unit: None,
            }
        }
    }
}

impl ProcessDelta {
    fn between(start: &ProcessSnapshot, end: &ProcessSnapshot) -> Self {
        Self {
            user_cpu_ms: delta_option(start.user_cpu_ms, end.user_cpu_ms),
            system_cpu_ms: delta_option(start.system_cpu_ms, end.system_cpu_ms),
        }
    }
}

#[cfg(unix)]
fn capture_unix_process_snapshot(available_parallelism: Option<usize>) -> ProcessSnapshot {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return ProcessSnapshot {
            pid: std::process::id(),
            available_parallelism,
            user_cpu_ms: None,
            system_cpu_ms: None,
            max_rss: None,
            max_rss_unit: None,
        };
    }

    let usage = unsafe { usage.assume_init() };
    ProcessSnapshot {
        pid: std::process::id(),
        available_parallelism,
        user_cpu_ms: Some(timeval_ms(usage.ru_utime)),
        system_cpu_ms: Some(timeval_ms(usage.ru_stime)),
        max_rss: Some(usage.ru_maxrss as u64),
        max_rss_unit: Some(unix_rss_unit()),
    }
}

#[cfg(unix)]
fn timeval_ms(timeval: libc::timeval) -> u64 {
    let seconds = u64::try_from(timeval.tv_sec).unwrap_or_default();
    let micros = u64::try_from(timeval.tv_usec).unwrap_or_default();
    seconds.saturating_mul(1000) + micros / 1000
}

#[cfg(unix)]
fn unix_rss_unit() -> &'static str {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        "bytes"
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        "kilobytes"
    }
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn delta_option(start: Option<u64>, end: Option<u64>) -> Option<u64> {
    match (start, end) {
        (Some(start), Some(end)) => Some(end.saturating_sub(start)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::{Component, add_counter, install, session, set_metadata, span};

    #[test]
    fn aggregates_spans_counters_and_metadata() {
        let session = session("test");
        set_metadata("before", "ignored");
        {
            let _guard = install(&session);
            set_metadata("command", "find");
            add_counter(Component::Storage, "pageCacheHit", 3);
            add_counter(Component::Storage, "pageCacheHit", 2);
            let _span = span(Component::Server, "dispatch");
            thread::sleep(Duration::from_millis(2));
        }

        let report = session.report();
        assert_eq!(report.label, "test");
        assert_eq!(
            report.metadata.get("command").map(String::as_str),
            Some("find")
        );
        assert!(
            report
                .counters
                .iter()
                .any(|counter| counter.name == "pageCacheHit" && counter.value == 5)
        );
        assert!(
            report
                .spans
                .iter()
                .any(|span| span.operation == "dispatch" && span.calls == 1)
        );
    }

    #[test]
    fn nested_installations_restore_previous_session() {
        let outer = session("outer");
        let inner = session("inner");
        {
            let _outer = install(&outer);
            {
                let _inner = install(&inner);
                let _span = span(Component::Server, "inner");
            }
            let _span = span(Component::Server, "outer");
        }

        let outer_report = outer.report();
        let inner_report = inner.report();
        assert!(
            outer_report
                .spans
                .iter()
                .any(|span| span.operation == "outer")
        );
        assert!(
            inner_report
                .spans
                .iter()
                .any(|span| span.operation == "inner")
        );
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryState {
    pub durable_lsn: u64,
    pub wal_bytes: u64,
    pub truncated_tail: bool,
}

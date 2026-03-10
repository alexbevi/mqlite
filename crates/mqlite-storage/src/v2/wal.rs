#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WalRecordKind {
    Begin = 1,
    PageImage = 2,
    Commit = 3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecordHeader {
    pub kind: WalRecordKind,
    pub transaction_id: u64,
    pub lsn: u64,
    pub payload_len: u32,
}

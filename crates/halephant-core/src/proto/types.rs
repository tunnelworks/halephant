/// PostgreSQL object identifier.
pub type Oid = u32;

/// Wire format code for column values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

/// Transaction status indicator from `ReadyForQuery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Not in a transaction block (`'I'`).
    Idle,
    /// In a transaction block (`'T'`).
    InTransaction,
    /// In a failed transaction block (`'E'`).
    Failed,
}

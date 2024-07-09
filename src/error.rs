use fbtree::prelude::TxnStorageStatus;

#[derive(Debug)]
pub enum ExecError {
    FieldOp(String),
    Conversion(String),
    Catalog(String),
    Storage(String),
    Pipeline(String),
}

impl From<TxnStorageStatus> for ExecError {
    fn from(status: TxnStorageStatus) -> ExecError {
        ExecError::Storage(status.into())
    }
}

use txn_storage::Status;

#[derive(Debug)]
pub enum ExecError {
    FieldOp(String),
    Conversion(String),
    Catalog(String),
    Storage(String),
    Pipeline(String),
}

impl From<Status> for ExecError {
    fn from(status: Status) -> ExecError {
        ExecError::Storage(status.into())
    }
}

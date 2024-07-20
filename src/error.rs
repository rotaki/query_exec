use fbtree::{
    access_method::AccessMethodError,
    prelude::{AppendOnlyStoreError, PagedHashMapError, TreeStatus, TxnStorageStatus},
};

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

impl From<AccessMethodError> for ExecError {
    fn from(err: AccessMethodError) -> ExecError {
        ExecError::Storage("AccessMethodError".to_string())
    }
}

impl From<AppendOnlyStoreError> for ExecError {
    fn from(err: AppendOnlyStoreError) -> ExecError {
        ExecError::Storage("AppendOnlyStoreError".to_string())
    }
}

impl From<TreeStatus> for ExecError {
    fn from(err: TreeStatus) -> ExecError {
        ExecError::Storage("TreeStatus".to_string())
    }
}

impl From<PagedHashMapError> for ExecError {
    fn from(err: PagedHashMapError) -> ExecError {
        ExecError::Storage("PagedHashMapError".to_string())
    }
}

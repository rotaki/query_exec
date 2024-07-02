use std::sync::Arc;

use txn_storage::TxnStorageTrait;

use crate::{
    catalog::CatalogRef, error::ExecError, expression::prelude::PhysicalRelExpr, tuple::Tuple,
    Field,
};

mod bp_based;
mod bytecode_expr;
mod volcano;

pub mod prelude {
    pub use super::volcano::VolcanoIterator;
    pub use super::Executor;
}

// 'a is the lifetime of the iterator
// This ensures that the iterator lives as long as the executor
pub trait Executor<'a, T: TxnStorageTrait<'a>> {
    type Buffer: ResultBufferTrait;

    fn new(catalog: CatalogRef, storage: Arc<T>, physical_plan: PhysicalRelExpr) -> Self;
    fn to_pretty_string(&self) -> String;
    fn execute(&mut self, txn: &T::TxnHandle) -> Result<Self::Buffer, ExecError>;
}

pub trait ResultBufferTrait {
    fn insert(&self, tuple: Tuple);
    fn get(&self, key: Vec<Field>) -> Vec<Tuple>;
    fn to_iter<'a>(&'a self) -> Box<dyn ResultIterator<'a> + 'a>;
}

pub trait ResultIterator<'a> {
    fn next(&self) -> Option<Tuple>;
}

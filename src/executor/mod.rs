use std::sync::Arc;

use fbtree::prelude::TxnStorageTrait;

use crate::{
    catalog::CatalogRef, error::ExecError, expression::prelude::PhysicalRelExpr, tuple::Tuple,
};

mod bytecode_expr;
mod pipeline_in_mem;
mod volcano;

pub mod prelude {
    pub use super::pipeline_in_mem::PipelineQueue;
    pub use super::volcano::VolcanoIterator;
    pub use super::{Executor, TupleBuffer, TupleBufferIter};
}



pub trait Executor<T: TxnStorageTrait> {
    type Buffer: TupleBuffer;
    fn new(catalog: CatalogRef, storage: Arc<T>, physical_plan: PhysicalRelExpr) -> Self;
    fn to_pretty_string(&self) -> String;
    fn execute(self, txn: &T::TxnHandle) -> Result<Arc<Self::Buffer>, ExecError>;
}

pub trait TupleBuffer {
    type Iter: TupleBufferIter;
    fn num_tuples(&self) -> usize;
    fn iter(self: &Arc<Self>) -> Self::Iter;
}

pub trait TupleBufferIter {
    fn next(&self) -> Result<Option<Tuple>, ExecError>;
}

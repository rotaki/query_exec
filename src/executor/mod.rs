use std::sync::Arc;

use fbtree::prelude::TxnStorageTrait;

use crate::{error::ExecError, tuple::Tuple, quantile_lib::QuantileMethod};

mod bytecode_expr;
mod inmem_pipeline;
pub mod ondisk_pipeline;
mod volcano;

pub mod prelude {
    pub use super::inmem_pipeline::InMemPipelineGraph;
    pub use super::ondisk_pipeline::{MemoryPolicy, OnDiskPipelineGraph};
    pub use super::volcano::VolcanoIterator;
    pub use super::{Executor, TupleBuffer, TupleBufferIter};
}

pub trait Executor<T: TxnStorageTrait> {
    type Buffer: TupleBuffer;
    fn to_pretty_string(&self) -> String;
    fn execute(self, txn: &T::TxnHandle) -> Result<Arc<Self::Buffer>, ExecError>;
    fn quantile_generation_execute(
        self, 
        txn: &T::TxnHandle,
        data_source: &str,
        query_id: u8,
        methods: &[QuantileMethod],
        num_quantiles_per_run: usize,
        estimated_store_json: &str,
        actual_store_json: &str,
        evaluation_json: &str) -> Result<Arc<Self::Buffer>, ExecError>;
}

pub trait TupleBuffer {
    type Iter: TupleBufferIter;
    fn num_tuples(&self) -> usize;
    fn iter(self: &Arc<Self>) -> Self::Iter;
}

pub trait TupleBufferIter {
    fn next(&self) -> Result<Option<Tuple>, ExecError>;
}

use std::{
    cell::UnsafeCell,
    cmp::Reverse,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytecode_expr::ByteCodeExpr;
use txn_storage::TxnStorageTrait;

use crate::{
    catalog::CatalogRef, error::ExecError, expression::prelude::PhysicalRelExpr, tuple::Tuple,
    Field,
};

mod bytecode_expr;
mod pipeline;
mod volcano;

pub mod prelude {
    pub use super::pipeline::PipelineQueue;
    pub use super::volcano::VolcanoIterator;
    pub use super::Executor;
}
use std::collections::BinaryHeap;

use txn_storage::{ContainerId, DatabaseId, ScanOptions};

use crate::{
    expression::AggOp,
    prelude::{DataType, SchemaRef},
    rwlatch::RwLatch,
    tuple::IsNull,
    ColumnId,
};

pub trait Executor<T: TxnStorageTrait> {
    fn new(catalog: CatalogRef, storage: Arc<T>, physical_plan: PhysicalRelExpr) -> Self;
    fn to_pretty_string(&self) -> String;
    fn execute(&mut self, txn: &T::TxnHandle) -> Result<Arc<TupleBuffer<T>>, ExecError>;
}

pub enum TupleBuffer<T: TxnStorageTrait> {
    TxnStorage(
        SchemaRef, // Schema of the container. Needed to find the primary key.
        DatabaseId,
        ContainerId,
        Arc<T>,
    ),
    InMemTupleVec(
        RwLatch, // Latch to protect the vector
        UnsafeCell<Vec<Tuple>>,
    ),
    Runs(
        RwLatch,
        UnsafeCell<Vec<(Arc<TupleBuffer<T>>, Vec<(ColumnId, bool, bool)>)>>,
    ), // column_id, ascending, nulls_first
    InMemHashTable(
        RwLatch,
        Vec<ByteCodeExpr>, // Hash key expressions
        UnsafeCell<bool>,  // Has Nulls. Used in mark join.
        UnsafeCell<HashMap<Vec<Field>, Vec<Tuple>>>,
    ),
    InMemHashAggregateTable(
        RwLatch,
        Vec<ColumnId>,                                        // Group by columns
        Vec<(AggOp, ColumnId)>,                               // Aggregation operations
        UnsafeCell<bool>,                                     // Has Nulls. Used in mark join.
        UnsafeCell<HashMap<Vec<Field>, (usize, Vec<Field>)>>, // Count and aggregate values
    ),
}

impl<T: TxnStorageTrait> TupleBuffer<T> {
    pub fn shared(&self) {
        match self {
            TupleBuffer::TxnStorage(..) => {}
            TupleBuffer::InMemTupleVec(latch, _) => latch.shared(),
            TupleBuffer::Runs(latch, _) => latch.shared(),
            TupleBuffer::InMemHashTable(latch, _, _, _) => latch.shared(),
            TupleBuffer::InMemHashAggregateTable(latch, _, _, _, _) => latch.shared(),
        }
    }

    pub fn exclusive(&self) {
        match self {
            TupleBuffer::TxnStorage(..) => {}
            TupleBuffer::InMemTupleVec(latch, _) => latch.exclusive(),
            TupleBuffer::Runs(latch, _) => latch.exclusive(),
            TupleBuffer::InMemHashTable(latch, _, _, _) => latch.exclusive(),
            TupleBuffer::InMemHashAggregateTable(latch, _, _, _, _) => latch.exclusive(),
        }
    }

    pub fn release_shared(&self) {
        match self {
            TupleBuffer::TxnStorage(..) => {}
            TupleBuffer::InMemTupleVec(latch, _) => latch.release_shared(),
            TupleBuffer::Runs(latch, _) => latch.release_shared(),
            TupleBuffer::InMemHashTable(latch, _, _, _) => latch.release_shared(),
            TupleBuffer::InMemHashAggregateTable(latch, _, _, _, _) => latch.release_shared(),
        }
    }

    pub fn release_exclusive(&self) {
        match self {
            TupleBuffer::TxnStorage(..) => {}
            TupleBuffer::InMemTupleVec(latch, _) => latch.release_exclusive(),
            TupleBuffer::Runs(latch, _) => latch.release_exclusive(),
            TupleBuffer::InMemHashTable(latch, _, _, _) => latch.release_exclusive(),
            TupleBuffer::InMemHashAggregateTable(latch, _, _, _, _) => latch.release_exclusive(),
        }
    }

    pub fn txn_storage(
        schema: SchemaRef,
        db_id: DatabaseId,
        c_id: ContainerId,
        storage: Arc<T>,
    ) -> Self {
        TupleBuffer::TxnStorage(schema, db_id, c_id, storage)
    }

    pub fn vec() -> Self {
        TupleBuffer::InMemTupleVec(RwLatch::default(), UnsafeCell::new(Vec::new()))
    }

    pub fn runs(runs: Vec<(Arc<TupleBuffer<T>>, Vec<(ColumnId, bool, bool)>)>) -> Self {
        TupleBuffer::Runs(RwLatch::default(), UnsafeCell::new(runs))
    }

    pub fn hash_table(exprs: Vec<ByteCodeExpr>) -> Self {
        TupleBuffer::InMemHashTable(
            RwLatch::default(),
            exprs,
            UnsafeCell::new(false),
            UnsafeCell::new(HashMap::new()),
        )
    }

    pub fn hash_aggregate_table(group_by: Vec<ColumnId>, agg_op: Vec<(AggOp, ColumnId)>) -> Self {
        TupleBuffer::InMemHashAggregateTable(
            RwLatch::default(),
            group_by,
            agg_op,
            UnsafeCell::new(false),
            UnsafeCell::new(HashMap::new()),
        )
    }

    pub fn has_null(&self) -> bool {
        self.shared();
        let result = match self {
            TupleBuffer::TxnStorage(..) => false,
            TupleBuffer::InMemTupleVec(_, _) => false,
            TupleBuffer::Runs(_, _) => false,
            TupleBuffer::InMemHashTable(_, _, has_null, _) => unsafe { *has_null.get() },
            TupleBuffer::InMemHashAggregateTable(_, _, _, has_null, _) => unsafe {
                *has_null.get()
            },
        };
        self.release_shared();
        result
    }

    pub fn append(&self, tuple: Tuple) -> Result<(), ExecError> {
        self.exclusive();
        match self {
            TupleBuffer::TxnStorage(..) => {
                panic!("TupleBuffer::append() is not supported for TupleBuffer::TxnStorage")
            }
            TupleBuffer::InMemTupleVec(_, vec) => {
                unsafe { &mut *vec.get() }.push(tuple);
            }
            TupleBuffer::Runs(_, _) => {
                panic!("TupleBuffer::push() is not supported for TupleBuffer::Runs")
            }
            TupleBuffer::InMemHashTable(_, exprs, has_null, table) => {
                let key = exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                if key.iter().any(|f| f.is_null()) {
                    *unsafe { &mut *has_null.get() } = true;
                    self.release_exclusive();
                    return Ok(()); // Tuple with null keys are not added to the hash table.
                }
                let table = unsafe { &mut *table.get() };
                match table.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().push(tuple);
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(vec![tuple]);
                    }
                }
            }
            TupleBuffer::InMemHashAggregateTable(_, group_by, agg_op, has_null, table) => {
                let key = tuple.get_cols(&group_by);
                if key.iter().any(|f| f.is_null()) {
                    *unsafe { &mut *has_null.get() } = true;
                    self.release_exclusive();
                    return Ok(()); // Tuple with null keys are not added to the hash table.
                }
                let table = unsafe { &mut *table.get() };
                match table.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        let (count, agg_vals) = entry.get_mut();
                        *count += 1;
                        for (idx, (op, col)) in agg_op.iter().enumerate() {
                            let val = tuple.get(*col);
                            match op {
                                AggOp::Sum => {
                                    agg_vals[idx] = (&agg_vals[idx] + val)?;
                                }
                                AggOp::Avg => {
                                    agg_vals[idx] = (&agg_vals[idx] + val)?;
                                }
                                AggOp::Max => {
                                    agg_vals[idx] = (agg_vals[idx].clone()).max(val.clone());
                                }
                                AggOp::Min => {
                                    agg_vals[idx] = (agg_vals[idx].clone()).min(val.clone());
                                }
                                AggOp::Count => {
                                    if !val.is_null() {
                                        agg_vals[idx] = (&agg_vals[idx] + &Field::Int(Some(1)))?;
                                    }
                                }
                            }
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let mut agg_vals = Vec::with_capacity(agg_op.len());
                        for (op, col) in agg_op {
                            let val = tuple.get(*col);
                            match op {
                                AggOp::Sum | AggOp::Avg | AggOp::Max | AggOp::Min => {
                                    agg_vals.push(val.clone())
                                }
                                AggOp::Count => {
                                    if val.is_null() {
                                        agg_vals.push(Field::Int(Some(0)))
                                    } else {
                                        agg_vals.push(Field::Int(Some(1)))
                                    }
                                }
                            }
                        }
                        entry.insert((1, agg_vals));
                    }
                }
            }
        }
        self.release_exclusive();
        Ok(())
    }

    pub fn iter_key(self: &Arc<Self>, key: Vec<Field>) -> Option<TupleBufferIter<T>> {
        self.shared(); // Shared latch must be released when iterator is dropped.
        match self.as_ref() {
            TupleBuffer::TxnStorage(..) => {
                panic!("TupleBuffer::iter_key() is not supported for TupleBuffer::TxnStorage")
            }
            TupleBuffer::InMemTupleVec(_, _) => {
                panic!("TupleBuffer::iter_key() is not supported for TupleBuffer::TupleVec")
            }
            TupleBuffer::Runs(_, _) => {
                panic!("TupleBuffer::iter_key() is not supported for TupleBuffer::Runs")
            }
            TupleBuffer::InMemHashTable(_, _, _, table) => {
                // SAFETY: The lock ensures that the reference is valid.
                let table = unsafe { &*table.get() };
                if let Some(tuples) = table.get(&key) {
                    let iter = tuples.iter();
                    Some(TupleBufferIter::vec(Arc::clone(self), iter))
                } else {
                    None
                }
            }
            TupleBuffer::InMemHashAggregateTable(..) => {
                unimplemented!("TupleBuffer::iter_key() is not implemented for TupleBuffer::InMemHashAggregateTable.
                This would be beneficial if aggregated table is used as a build side in hash join.")
            }
        }
    }

    pub fn iter_all(self: &Arc<Self>) -> TupleBufferIter<T> {
        self.shared(); // Shared latch must be released when iterator is dropped.
        match self.as_ref() {
            TupleBuffer::TxnStorage(_, db_id, c_id, storage) => {
                let txn = storage.begin_txn(&db_id, Default::default()).unwrap();
                let iter = storage
                    .scan_range(&txn, c_id, ScanOptions::default())
                    .unwrap();
                TupleBufferIter::scan(storage.clone(), txn, iter)
            }
            TupleBuffer::InMemTupleVec(_, vec) => {
                TupleBufferIter::vec(Arc::clone(self), unsafe { &*vec.get() }.iter())
            }
            TupleBuffer::Runs(_, runs) => {
                let runs = unsafe { &*runs.get() };
                let runs = runs
                    .iter()
                    .map(|(buf, order)| (buf.iter_all(), order.clone()))
                    .collect();
                TupleBufferIter::merge(Arc::clone(self), runs)
            }
            TupleBuffer::InMemHashTable(_, _, _, table) => {
                let table = unsafe { &*table.get() };
                TupleBufferIter::hash_table(Arc::clone(self), table.iter())
            }
            TupleBuffer::InMemHashAggregateTable(_, group_by, agg_op, _, table) => {
                let table = unsafe { &*table.get() };
                TupleBufferIter::hash_aggregate_table(
                    Arc::clone(self),
                    group_by.clone(),
                    agg_op.clone(),
                    table.iter(),
                )
            }
        }
    }
}

pub enum TupleBufferIter<T: TxnStorageTrait> {
    TxnStorage(Arc<T>, T::TxnHandle, T::IteratorHandle),
    TupleVec(
        Arc<TupleBuffer<T>>, // Buffer with shared latch
        Mutex<std::slice::Iter<'static, Tuple>>,
    ),
    HashTable(
        Arc<TupleBuffer<T>>,                              // Buffer with shared latch
        Mutex<()>, // Guard to ensure that the iterator is mutable by many threads
        UnsafeCell<Option<(&'static Vec<Tuple>, usize)>>, // Current tuples and index
        UnsafeCell<std::collections::hash_map::Iter<'static, Vec<Field>, Vec<Tuple>>>,
    ),
    HashAggregateTable(
        Arc<TupleBuffer<T>>,    // Buffer with shared latch
        Mutex<()>,              // Guard to ensure that the iterator is mutable by many threads
        UnsafeCell<bool>, // Has output. Used to return a single tuple of NULLs if there is no output.
        Vec<ColumnId>,    // Group by columns. Required to create NULL tuples.
        Vec<(AggOp, ColumnId)>, // Aggregation operations. Required to compute AVG. (SUM / COUNT)
        UnsafeCell<std::collections::hash_map::Iter<'static, Vec<Field>, (usize, Vec<Field>)>>,
    ),
    MergeScan(
        Arc<TupleBuffer<T>>,                                 // Buffer with shared latch
        Mutex<BinaryHeap<Reverse<(Vec<u8>, usize, Tuple)>>>, // Guard to ensure that the iterator is mutable by many threads
        Vec<(TupleBufferIter<T>, Vec<(ColumnId, bool, bool)>)>,
    ),
}

impl<T: TxnStorageTrait> Drop for TupleBufferIter<T> {
    fn drop(&mut self) {
        match self {
            TupleBufferIter::TxnStorage(storage, txn, _) => {
                storage.commit_txn(txn, false).unwrap();
            }
            TupleBufferIter::TupleVec(latched_buffer, ..)
            | TupleBufferIter::HashTable(latched_buffer, ..)
            | TupleBufferIter::HashAggregateTable(latched_buffer, ..)
            | TupleBufferIter::MergeScan(latched_buffer, ..) => {
                latched_buffer.release_shared();
            }
        }
    }
}

impl<T: TxnStorageTrait> TupleBufferIter<T> {
    pub fn scan(storage: Arc<T>, txn: T::TxnHandle, iter: T::IteratorHandle) -> Self {
        TupleBufferIter::TxnStorage(storage, txn, iter)
    }

    pub fn vec(
        latched_buffer: Arc<TupleBuffer<T>>,
        iter: std::slice::Iter<'static, Tuple>,
    ) -> Self {
        TupleBufferIter::TupleVec(latched_buffer, Mutex::new(iter))
    }

    pub fn hash_table(
        latched_buffer: Arc<TupleBuffer<T>>,
        table: std::collections::hash_map::Iter<'static, Vec<Field>, Vec<Tuple>>,
    ) -> Self {
        TupleBufferIter::HashTable(
            latched_buffer,
            Mutex::new(()),
            UnsafeCell::new(None),
            UnsafeCell::new(table),
        )
    }

    pub fn hash_aggregate_table(
        latched_buffer: Arc<TupleBuffer<T>>,
        group_by: Vec<ColumnId>,
        agg_op: Vec<(AggOp, ColumnId)>,
        table: std::collections::hash_map::Iter<'static, Vec<Field>, (usize, Vec<Field>)>,
    ) -> Self {
        TupleBufferIter::HashAggregateTable(
            latched_buffer,
            Mutex::new(()),
            UnsafeCell::new(false),
            group_by,
            agg_op,
            UnsafeCell::new(table),
        )
    }

    pub fn merge(
        latched_buffer: Arc<TupleBuffer<T>>,
        runs: Vec<(TupleBufferIter<T>, Vec<(ColumnId, bool, bool)>)>,
    ) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, (iter, sort_cols)) in runs.iter().enumerate() {
            if let Some(tuple) = iter.next() {
                let key = tuple.to_normalized_key_bytes(&sort_cols);
                heap.push(Reverse((key, i, tuple)));
            }
        }
        TupleBufferIter::MergeScan(latched_buffer, Mutex::new(heap), runs)
    }

    pub fn next(&self) -> Option<Tuple> {
        match self {
            TupleBufferIter::TxnStorage(storage, _, iter) => {
                storage.iter_next(iter).unwrap().map(|(_, val)| {
                    let tuple = Tuple::from_bytes(&val);
                    tuple
                })
            }
            TupleBufferIter::TupleVec(_, iter) => iter.lock().unwrap().next().map(|t| t.copy()),
            TupleBufferIter::MergeScan(_, heap, runs) => {
                let heap = &mut *heap.lock().unwrap();
                if let Some(Reverse((_, i, tuple))) = heap.pop() {
                    let (iter, sort_cols) = &runs[i];
                    if let Some(next) = iter.next() {
                        let key = next.to_normalized_key_bytes(&sort_cols);
                        heap.push(Reverse((key, i, next)));
                    }
                    Some(tuple.copy())
                } else {
                    None
                }
            }
            TupleBufferIter::HashTable(_, mutex, current_vec, table) => {
                let _guard = mutex.lock().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                let current_vec = unsafe { &mut *current_vec.get() };
                let table = unsafe { &mut *table.get() };
                if current_vec.is_none() {
                    *current_vec = table.next().map(|(_, tuples)| (tuples, 0));
                }
                if let Some((tuples, i)) = current_vec {
                    if let Some(tuple) = tuples.get(*i) {
                        *i += 1;
                        Some(tuple.copy())
                    } else {
                        // Here, we do not simply call self.next() here to avoid a deadlock on the mutex.
                        *current_vec = table.next().map(|(_, tuples)| (tuples, 0));
                        if let Some((tuples, i)) = current_vec {
                            if let Some(tuple) = tuples.get(*i) {
                                *i += 1;
                                Some(tuple.copy())
                            } else {
                                unreachable!("The new tuples vector should not be empty")
                            }
                        } else {
                            // There is no more tuples in the hash table.
                            None
                        }
                    }
                } else {
                    None
                }
            }
            TupleBufferIter::HashAggregateTable(_, mutex, has_output, group_by, agg_op, table) => {
                let _guard = mutex.lock().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                let has_output = unsafe { &mut *has_output.get() };
                let table = unsafe { &mut *table.get() };
                if let Some((group_by, (count, agg_vals))) = table.next() {
                    *has_output = true;
                    // Create a tuple with group by columns and aggregation values.
                    // Check if there is any AVG being computed.
                    let group_by_len = group_by.len();
                    let mut fields = Vec::with_capacity(group_by_len + agg_vals.len());
                    fields.extend(group_by.iter().cloned());
                    for (idx, (op, _)) in agg_op.iter().enumerate() {
                        let val = match op {
                            AggOp::Sum | AggOp::Max | AggOp::Min | AggOp::Count => {
                                agg_vals[idx].clone()
                            }
                            AggOp::Avg => {
                                if *count == 0 {
                                    unreachable!("The count should not be zero")
                                } else {
                                    (&agg_vals[idx] / &Field::Float(Some(*count as f64))).unwrap()
                                }
                            }
                        };
                        fields.push(val);
                    }
                    Some(Tuple::from_fields(fields))
                } else {
                    if *has_output {
                        // There is no more tuples in the hash table.
                        None
                    } else {
                        // Return a single tuple of NULLs.
                        *has_output = true;
                        let mut fields = Vec::new();
                        for _ in group_by {
                            fields.push(Field::null(&DataType::Unknown));
                        }
                        for _ in agg_op {
                            fields.push(Field::null(&DataType::Unknown));
                        }
                        Some(Tuple::from_fields(fields))
                    }
                }
            }
        }
    }
}

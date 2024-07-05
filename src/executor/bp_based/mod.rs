use std::{
    cell::UnsafeCell,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    hash::Hash,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
};

use crate::{
    error::ExecError,
    expression::AggOp,
    prelude::{DataType, SchemaRef},
    tuple::{FromBool, IsNull, Tuple},
    ColumnId, Field,
};

use super::bytecode_expr::ByteCodeExpr;

pub enum TupleBuffer {
    InMemTupleVec(Arc<RwLock<()>>, UnsafeCell<Vec<Tuple>>),
    Runs(
        Arc<RwLock<()>>,
        UnsafeCell<Vec<(Arc<TupleBuffer>, Vec<(ColumnId, bool, bool)>)>>,
    ), // column_id, ascending, nulls_first
    InMemHashTable(
        Arc<RwLock<()>>,
        Vec<ByteCodeExpr>, // Hash key expressions
        UnsafeCell<bool>,  // Has Nulls. Used in mark join.
        UnsafeCell<HashMap<Vec<Field>, Vec<Tuple>>>,
    ),
    InMemHashAggregateTable(
        Arc<RwLock<()>>,
        Vec<ColumnId>,                                        // Group by columns
        Vec<(AggOp, ColumnId)>,                               // Aggregation operations
        UnsafeCell<bool>,                                     // Has Nulls. Used in mark join.
        UnsafeCell<HashMap<Vec<Field>, (usize, Vec<Field>)>>, // Count and aggregate values
    ),
}

impl TupleBuffer {
    pub fn vec() -> Self {
        TupleBuffer::InMemTupleVec(Arc::new(RwLock::new(())), UnsafeCell::new(Vec::new()))
    }

    pub fn runs(runs: Vec<(Arc<TupleBuffer>, Vec<(ColumnId, bool, bool)>)>) -> Self {
        TupleBuffer::Runs(Arc::new(RwLock::new(())), UnsafeCell::new(runs))
    }

    pub fn hash_table(exprs: Vec<ByteCodeExpr>) -> Self {
        TupleBuffer::InMemHashTable(
            Arc::new(RwLock::new(())),
            exprs,
            UnsafeCell::new(false),
            UnsafeCell::new(HashMap::new()),
        )
    }

    pub fn hash_aggregate_table(group_by: Vec<ColumnId>, agg_op: Vec<(AggOp, ColumnId)>) -> Self {
        TupleBuffer::InMemHashAggregateTable(
            Arc::new(RwLock::new(())),
            group_by,
            agg_op,
            UnsafeCell::new(false),
            UnsafeCell::new(HashMap::new()),
        )
    }

    pub fn has_null(&self) -> bool {
        match self {
            TupleBuffer::InMemTupleVec(_, _) => false,
            TupleBuffer::Runs(_, _) => false,
            TupleBuffer::InMemHashTable(lock, _, has_null, _) => {
                let _guard = lock.read().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                unsafe { *has_null.get() }
            }
            TupleBuffer::InMemHashAggregateTable(_, _, _, has_null, _) => {
                // SAFETY: The lock ensures that the reference is valid.
                unsafe { *has_null.get() }
            }
        }
    }

    pub fn append(&self, tuple: Tuple) -> Result<(), ExecError> {
        match self {
            TupleBuffer::InMemTupleVec(lock, vec) => {
                let _guard = lock.write().unwrap();
                unsafe { &mut *vec.get() }.push(tuple);
            }
            TupleBuffer::Runs(_, _) => {
                panic!("TupleBuffer::push() is not supported for TupleBuffer::Runs")
            }
            TupleBuffer::InMemHashTable(lock, exprs, has_null, table) => {
                let _guard = lock.write().unwrap();
                let key = exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                if key.iter().any(|f| f.is_null()) {
                    *unsafe { &mut *has_null.get() } = true;
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
            TupleBuffer::InMemHashAggregateTable(lock, group_by, agg_op, has_null, table) => {
                let _guard = lock.write().unwrap();
                let key = tuple.get_cols(&group_by);
                if key.iter().any(|f| f.is_null()) {
                    *unsafe { &mut *has_null.get() } = true;
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
        Ok(())
    }

    pub fn iter_key(&self, key: Vec<Field>) -> Option<TupleBufferIter> {
        match self {
            TupleBuffer::InMemTupleVec(_, _) => {
                panic!("TupleBuffer::iter_key() is not supported for TupleBuffer::TupleVec")
            }
            TupleBuffer::Runs(_, _) => {
                panic!("TupleBuffer::iter_key() is not supported for TupleBuffer::Runs")
            }
            TupleBuffer::InMemHashTable(lock, _, _, table) => {
                let _guard = lock.read().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                let table = unsafe { &*table.get() };
                if let Some(tuples) = table.get(&key) {
                    let iter = tuples.iter();
                    Some(TupleBufferIter::vec(_guard, iter))
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

    pub fn iter_all(&self) -> TupleBufferIter {
        match self {
            TupleBuffer::InMemTupleVec(lock, vec) => {
                let _guard = lock.read().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                TupleBufferIter::vec(_guard, unsafe { &*vec.get() }.iter())
            }
            TupleBuffer::Runs(lock, runs) => {
                let _guard = lock.read().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                let runs = unsafe { &*runs.get() };
                if runs.len() == 1 {
                    return runs[0].0.iter_all();
                } else {
                    let runs = runs
                        .iter()
                        .map(|(buf, order)| (buf.iter_all(), order.clone()))
                        .collect();
                    TupleBufferIter::merge(_guard, runs)
                }
            }
            TupleBuffer::InMemHashTable(lock, _, _, table) => {
                let _guard = lock.read().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                let table = unsafe { &*table.get() };
                TupleBufferIter::hash_table(_guard, table.iter())
            }
            TupleBuffer::InMemHashAggregateTable(lock, group_by, agg_op, _, table) => {
                let _guard = lock.read().unwrap();
                // SAFETY: The lock ensures that the reference is valid.
                let table = unsafe { &*table.get() };
                TupleBufferIter::hash_aggregate_table(
                    _guard,
                    group_by.clone(),
                    agg_op.clone(),
                    table.iter(),
                )
            }
        }
    }
}

pub enum TupleBufferIter<'a> {
    TupleVec(RwLockReadGuard<'a, ()>, Mutex<std::slice::Iter<'a, Tuple>>),
    HashTable(
        RwLockReadGuard<'a, ()>,
        Mutex<()>, // Guard to ensure that the iterator is mutable by many threads
        UnsafeCell<Option<(&'a Vec<Tuple>, usize)>>, // Current tuples and index
        UnsafeCell<std::collections::hash_map::Iter<'a, Vec<Field>, Vec<Tuple>>>,
    ),
    HashAggregateTable(
        RwLockReadGuard<'a, ()>,
        Mutex<()>,        // Guard to ensure that the iterator is mutable by many threads
        UnsafeCell<bool>, // Has output. Used to return a single tuple of NULLs if there is no output.
        Vec<ColumnId>,    // Group by columns. Required to create NULL tuples.
        Vec<(AggOp, ColumnId)>, // Aggregation operations. Required to compute AVG. (SUM / COUNT)
        UnsafeCell<std::collections::hash_map::Iter<'a, Vec<Field>, (usize, Vec<Field>)>>,
    ),
    MergeScan(
        RwLockReadGuard<'a, ()>, // Guard to ensure that the runs are not modified during the merge
        Mutex<BinaryHeap<Reverse<(Vec<u8>, usize, Tuple)>>>, // Guard to ensure that the iterator is mutable by many threads
        Vec<(TupleBufferIter<'a>, Vec<(ColumnId, bool, bool)>)>,
    ),
}

impl<'a> TupleBufferIter<'a> {
    pub fn vec(guard: RwLockReadGuard<'a, ()>, iter: std::slice::Iter<'a, Tuple>) -> Self {
        TupleBufferIter::TupleVec(guard, Mutex::new(iter))
    }

    pub fn hash_table(
        guard: RwLockReadGuard<'a, ()>,
        table: std::collections::hash_map::Iter<'a, Vec<Field>, Vec<Tuple>>,
    ) -> Self {
        TupleBufferIter::HashTable(
            guard,
            Mutex::new(()),
            UnsafeCell::new(None),
            UnsafeCell::new(table),
        )
    }

    pub fn hash_aggregate_table(
        guard: RwLockReadGuard<'a, ()>,
        group_by: Vec<ColumnId>,
        agg_op: Vec<(AggOp, ColumnId)>,
        table: std::collections::hash_map::Iter<'a, Vec<Field>, (usize, Vec<Field>)>,
    ) -> Self {
        TupleBufferIter::HashAggregateTable(
            guard,
            Mutex::new(()),
            UnsafeCell::new(false),
            group_by,
            agg_op,
            UnsafeCell::new(table),
        )
    }

    pub fn merge(
        guard: RwLockReadGuard<'a, ()>,
        runs: Vec<(TupleBufferIter<'a>, Vec<(ColumnId, bool, bool)>)>,
    ) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, (iter, sort_cols)) in runs.iter().enumerate() {
            if let Some(tuple) = iter.next() {
                let key = tuple.to_normalized_key_bytes(&sort_cols);
                heap.push(Reverse((key, i, tuple)));
            }
        }
        TupleBufferIter::MergeScan(guard, Mutex::new(heap), runs)
    }

    pub fn next(&self) -> Option<Tuple> {
        match self {
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

// Pipeline iterators are non-blocking.
pub enum PipelineNonBlocking<'a> {
    Scan(PScanIter<'a>),
    Filter(PFilterIter<'a>),
    Project(PProjectIter<'a>),
    Map(PMapIter<'a>),
    HashJoin(PHashJoinIter<'a>),
    NestedLoopJoin(PNestedLoopJoinIter<'a>),
}

impl<'a> PipelineNonBlocking<'a> {
    pub fn rewind(&mut self) {
        match self {
            PipelineNonBlocking::Scan(iter) => iter.rewind(),
            PipelineNonBlocking::Filter(iter) => iter.rewind(),
            PipelineNonBlocking::Project(iter) => iter.rewind(),
            PipelineNonBlocking::Map(iter) => iter.rewind(),
            PipelineNonBlocking::HashJoin(iter) => iter.rewind(),
            PipelineNonBlocking::NestedLoopJoin(iter) => iter.rewind(),
        }
    }

    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        let context = unsafe { std::mem::transmute(context) };
        match self {
            PipelineNonBlocking::Scan(iter) => iter.next(context),
            PipelineNonBlocking::Filter(iter) => iter.next(context),
            PipelineNonBlocking::Project(iter) => iter.next(context),
            PipelineNonBlocking::Map(iter) => iter.next(context),
            PipelineNonBlocking::HashJoin(iter) => iter.next(context),
            PipelineNonBlocking::NestedLoopJoin(iter) => iter.next(context),
        }
    }
}

pub struct PScanIter<'a> {
    id: PipelineID,
    iter: Option<TupleBufferIter<'a>>,
}

impl<'a> PScanIter<'a> {
    pub fn new(id: PipelineID) -> Self {
        Self { id, iter: None }
    }

    pub fn rewind(&mut self) {
        self.iter = None;
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        if let Some(iter) = &mut self.iter {
            Ok(iter.next())
        } else {
            self.iter = context.get(&self.id).map(|buf| buf.iter_all());
            Ok(self.iter.as_ref().and_then(|iter| iter.next()))
        }
    }
}

pub struct PFilterIter<'a> {
    input: Box<PipelineNonBlocking<'a>>,
    expr: ByteCodeExpr,
}

impl<'a> PFilterIter<'a> {
    pub fn new(input: Box<PipelineNonBlocking<'a>>, expr: ByteCodeExpr) -> Self {
        Self { input, expr }
    }

    pub fn rewind(&mut self) {
        self.input.rewind();
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        while let Some(tuple) = self.input.next(context)? {
            if self.expr.eval(&tuple)? == Field::from_bool(true) {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }
}

pub struct PProjectIter<'a> {
    input: Box<PipelineNonBlocking<'a>>,
    column_indices: Vec<ColumnId>,
}

impl<'a> PProjectIter<'a> {
    pub fn new(input: Box<PipelineNonBlocking<'a>>, column_indices: Vec<ColumnId>) -> Self {
        Self {
            input,
            column_indices,
        }
    }

    pub fn rewind(&mut self) {
        self.input.rewind();
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        if let Some(tuple) = self.input.next(context)? {
            let new_tuple = tuple.project(&self.column_indices);
            Ok(Some(new_tuple))
        } else {
            Ok(None)
        }
    }
}

pub struct PMapIter<'a> {
    input: Box<PipelineNonBlocking<'a>>,
    exprs: Vec<ByteCodeExpr>,
}

impl<'a> PMapIter<'a> {
    pub fn new(input: Box<PipelineNonBlocking<'a>>, exprs: Vec<ByteCodeExpr>) -> Self {
        Self { input, exprs }
    }

    pub fn rewind(&mut self) {
        self.input.rewind();
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        if let Some(mut tuple) = self.input.next(context)? {
            for expr in &self.exprs {
                tuple.push(expr.eval(&tuple)?);
            }
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }
}

pub enum PHashJoinIter<'a> {
    Inner(PHashJoinInnerIter<'a>),
    RightOuter(PHashJoinRightOuterIter<'a>), // Probe side is the right
    RightSemi(PHashJoinRightSemiIter<'a>),   // Probe side is the right
    RightAnti(PHashJoinRightAntiIter<'a>),   // Probe side is the right
    RightMark(PHashJoinRightMarkIter<'a>),   // Probe side is the right
}

impl<'a> PHashJoinIter<'a> {
    pub fn rewind(&mut self) {
        match self {
            PHashJoinIter::Inner(iter) => iter.rewind(),
            PHashJoinIter::RightOuter(iter) => iter.rewind(),
            PHashJoinIter::RightSemi(iter) => iter.rewind(),
            PHashJoinIter::RightAnti(iter) => iter.rewind(),
            PHashJoinIter::RightMark(iter) => iter.rewind(),
        }
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        match self {
            PHashJoinIter::Inner(iter) => iter.next(context),
            PHashJoinIter::RightOuter(iter) => iter.next(context),
            PHashJoinIter::RightSemi(iter) => iter.next(context),
            PHashJoinIter::RightAnti(iter) => iter.next(context),
            PHashJoinIter::RightMark(iter) => iter.next(context),
        }
    }
}

pub struct PHashJoinInnerIter<'a> {
    probe_side: Box<PipelineNonBlocking<'a>>,
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
    current: Option<(Tuple, TupleBufferIter<'a>)>,
}

impl<'a> PHashJoinInnerIter<'a> {
    pub fn new(
        probe_side: Box<PipelineNonBlocking<'a>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            probe_side,
            build_side,
            exprs,
            current: None,
        }
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
        self.current = None;
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        if let Some((probe, build_iter)) = &mut self.current {
            if let Some(build) = build_iter.next() {
                let result = build.merge_mut(&probe);
                return Ok(Some(result));
            }
        }
        // Reset the current tuple and build iterator.
        loop {
            // Iterate the probe side until a match is found.
            // If match is found
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
                if let Some(iter) = build_iter {
                    // There should be at least one tuple in the build side iterator.
                    if let Some(build) = iter.next() {
                        let result = build.merge_mut(&probe);
                        self.current = Some((probe, iter));
                        return Ok(Some(result));
                    } else {
                        unreachable!("The build side returned an empty iterator")
                    }
                }
                // No match found. Continue to the next probe tuple.
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PHashJoinRightOuterIter<'a> {
    probe_side: Box<PipelineNonBlocking<'a>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
    current: Option<(Tuple, TupleBufferIter<'a>)>,
    nulls: Tuple,
}

impl<'a> PHashJoinRightOuterIter<'a> {
    pub fn new(
        probe_side: Box<PipelineNonBlocking<'a>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
        nulls: Tuple,
    ) -> Self {
        Self {
            probe_side,
            build_side,
            exprs,
            current: None,
            nulls,
        }
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
        self.current = None;
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        if let Some((probe, build_iter)) = &mut self.current {
            if let Some(build) = build_iter.next() {
                let result = build.merge_mut(&probe);
                return Ok(Some(result));
            }
        }
        // Reset the current tuple and build iterator.
        if let Some(probe) = self.probe_side.next(context)? {
            let key = self
                .exprs
                .iter()
                .map(|expr| expr.eval(&probe))
                .collect::<Result<Vec<_>, _>>()?;

            let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
            let result = if let Some(iter) = build_iter {
                // Try to iterate the build side once to check if there is any match.
                let result = if let Some(build) = iter.next() {
                    let result = build.merge_mut(&probe);
                    self.current = Some((probe, iter));
                    result
                } else {
                    // There should be at least one tuple in the build side iterator.
                    unreachable!("The build side returned an empty iterator");
                };
                result
            } else {
                self.current = None;
                // No match found. Output the probe tuple with nulls for the build side.
                self.nulls.merge(&probe)
            };
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

pub struct PHashJoinRightSemiIter<'a> {
    probe_side: Box<PipelineNonBlocking<'a>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<'a> PHashJoinRightSemiIter<'a> {
    pub fn new(
        probe_side: Box<PipelineNonBlocking<'a>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            probe_side,
            build_side,
            exprs,
        }
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        // If there is a match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        loop {
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
                if let Some(iter) = build_iter {
                    if iter.next().is_some() {
                        return Ok(Some(probe));
                    } else {
                        unreachable!("The build side returned an empty iterator")
                    }
                }
                // No match found. Continue to the next probe tuple.
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PHashJoinRightAntiIter<'a> {
    probe_side: Box<PipelineNonBlocking<'a>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<'a> PHashJoinRightAntiIter<'a> {
    pub fn new(
        probe_side: Box<PipelineNonBlocking<'a>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            probe_side,
            build_side,
            exprs,
        }
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        // If there is no match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        loop {
            if let Some(probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let build_iter = context.get(&self.build_side).unwrap().iter_key(key);
                if let Some(_iter) = build_iter {
                    // Match found. Continue to the next probe tuple.
                } else {
                    return Ok(Some(probe));
                }
                // No match found. Continue to the next probe tuple.
            } else {
                return Ok(None);
            }
        }
    }
}

// Mark join is similar to semi/anti join but returns all the probe tuples.
// The tuples are marked with a boolean value indicating if there is a match in the build side.
// If there is a matching tuple, the mark is true.
// If there is no matching tuple, if the build side has nulls, the mark is null.
// Otherwise, the mark is false.
pub struct PHashJoinRightMarkIter<'a> {
    probe_side: Box<PipelineNonBlocking<'a>>, // Probe side is the right. All tuples in the probe side will be preserved.
    build_side: PipelineID,
    exprs: Vec<ByteCodeExpr>,
}

impl<'a> PHashJoinRightMarkIter<'a> {
    pub fn new(
        probe_side: Box<PipelineNonBlocking<'a>>,
        build_side: PipelineID,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            probe_side,
            build_side,
            exprs,
        }
    }

    pub fn rewind(&mut self) {
        self.probe_side.rewind();
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        // If there is a match in the build side, output the probe tuple.
        // Otherwise go to the next probe tuple
        let build_side = context.get(&self.build_side).unwrap();
        loop {
            if let Some(mut probe) = self.probe_side.next(context)? {
                let key = self
                    .exprs
                    .iter()
                    .map(|expr| expr.eval(&probe))
                    .collect::<Result<Vec<_>, _>>()?;
                let build_iter = build_side.iter_key(key);
                let mark = if let Some(iter) = build_iter {
                    if iter.next().is_some() {
                        Field::from_bool(true)
                    } else {
                        unreachable!("The build side returned an empty iterator")
                    }
                } else {
                    if build_side.has_null() {
                        Field::null(&DataType::Boolean)
                    } else {
                        Field::from_bool(false)
                    }
                };
                probe.push(mark);
                return Ok(Some(probe));
            } else {
                return Ok(None);
            }
        }
    }
}

pub struct PNestedLoopJoinIter<'a> {
    outer: Box<PipelineNonBlocking<'a>>, // Outer loop of NLJ
    inner: Box<PipelineNonBlocking<'a>>, // Inner loop of NLJ
    current_outer: Option<Tuple>,
    current_inner: Option<Tuple>,
}

impl<'a> PNestedLoopJoinIter<'a> {
    pub fn new(outer: Box<PipelineNonBlocking<'a>>, inner: Box<PipelineNonBlocking<'a>>) -> Self {
        Self {
            outer,
            inner,
            current_outer: None,
            current_inner: None,
        }
    }

    pub fn rewind(&mut self) {
        self.outer.rewind();
        self.inner.rewind();
        self.current_outer = None;
        self.current_inner = None;
    }

    pub fn next(
        &mut self,
        context: &'static HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        loop {
            // Set the outer
            if self.current_outer.is_none() {
                match self.outer.next(context)? {
                    Some(tuple) => {
                        self.current_outer = Some(tuple);
                        self.inner.rewind();
                    }
                    None => return Ok(None),
                }
            }
            // Set the inner
            if self.current_inner.is_none() {
                match self.inner.next(context)? {
                    Some(tuple) => {
                        self.current_inner = Some(tuple);
                    }
                    None => {
                        self.current_outer = None;
                        continue;
                    }
                }
            }

            // Outer and inner are set. Merge the tuples.
            let result = self
                .current_outer
                .as_ref()
                .unwrap()
                .merge(self.current_inner.as_ref().unwrap());
            self.current_inner = None;
            return Ok(Some(result));
        }
    }
}

pub enum PipelineBlocking<'a> {
    Dummy(PipelineNonBlocking<'a>),
    InMemSort(InMemSort<'a>),
    InMemHashTableCreation(InMemHashTableCreation<'a>),
    InMemHashAggregate(InMemHashAggregation<'a>),
}

impl<'a> PipelineBlocking<'a> {
    pub fn dummy(exec_plan: PipelineNonBlocking<'a>) -> Self {
        PipelineBlocking::Dummy(exec_plan)
    }

    pub fn in_mem_sort(
        exec_plan: PipelineNonBlocking<'a>,
        sort_cols: Vec<(ColumnId, bool, bool)>,
    ) -> Self {
        PipelineBlocking::InMemSort(InMemSort {
            exec_plan,
            sort_cols,
        })
    }

    pub fn in_mem_hash_table_creation(
        exec_plan: PipelineNonBlocking<'a>,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        PipelineBlocking::InMemHashTableCreation(InMemHashTableCreation { exec_plan, exprs })
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Arc<TupleBuffer>, ExecError> {
        match self {
            PipelineBlocking::Dummy(plan) => {
                let output = Arc::new(TupleBuffer::vec());
                while let Some(tuple) = plan.next(&context)? {
                    output.append(tuple)?;
                }
                Ok(output)
            }
            PipelineBlocking::InMemSort(sort) => sort.execute(&context),
            PipelineBlocking::InMemHashTableCreation(creation) => creation.execute(&context),
            PipelineBlocking::InMemHashAggregate(agg) => agg.execute(&context),
        }
    }
}

impl<'a> From<PipelineNonBlocking<'a>> for PipelineBlocking<'a> {
    fn from(plan: PipelineNonBlocking<'a>) -> Self {
        PipelineBlocking::Dummy(plan)
    }
}

pub struct InMemSort<'a> {
    exec_plan: PipelineNonBlocking<'a>,
    sort_cols: Vec<(ColumnId, bool, bool)>, // ColumnId, ascending, nulls_first
}

impl<'a> InMemSort<'a> {
    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Arc<TupleBuffer>, ExecError> {
        let output = Arc::new(TupleBuffer::vec());
        let mut tuples = Vec::new();
        while let Some(tuple) = self.exec_plan.next(&context)? {
            tuples.push(tuple);
        }
        tuples.sort_by(|a, b| {
            a.to_normalized_key_bytes(&self.sort_cols)
                .cmp(&b.to_normalized_key_bytes(&self.sort_cols))
        });
        for tuple in tuples {
            output.append(tuple)?;
        }
        Ok(output)
    }
}

pub struct InMemHashTableCreation<'a> {
    exec_plan: PipelineNonBlocking<'a>,
    exprs: Vec<ByteCodeExpr>, // Hash key expressions
}

impl<'a> InMemHashTableCreation<'a> {
    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Arc<TupleBuffer>, ExecError> {
        let output = Arc::new(TupleBuffer::hash_table(self.exprs.clone()));
        while let Some(tuple) = self.exec_plan.next(&context)? {
            output.append(tuple)?;
        }
        Ok(output)
    }
}

pub struct InMemHashAggregation<'a> {
    exec_plan: PipelineNonBlocking<'a>,
    group_by: Vec<ColumnId>,
    agg_op: Vec<(AggOp, ColumnId)>,
}

impl<'a> InMemHashAggregation<'a> {
    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Arc<TupleBuffer>, ExecError> {
        let output = Arc::new(TupleBuffer::hash_aggregate_table(
            self.group_by.clone(),
            self.agg_op.clone(),
        ));
        while let Some(tuple) = self.exec_plan.next(&context)? {
            output.append(tuple)?;
        }
        Ok(output)
    }
}

pub type PipelineID = u16;

pub struct Pipeline<'a> {
    id: PipelineID,
    context: HashMap<PipelineID, Arc<TupleBuffer>>,
    exec_plan: PipelineBlocking<'a>,
}

impl<'a> Pipeline<'a> {
    pub fn new(
        id: PipelineID,
        execution_plan: PipelineBlocking<'a>,
        output: Arc<TupleBuffer>,
    ) -> Self {
        Self {
            id,
            context: HashMap::new(),
            exec_plan: execution_plan,
        }
    }

    /// Set context
    pub fn set_context(&mut self, id: PipelineID, buffer: Arc<TupleBuffer>) {
        self.context.insert(id, buffer);
    }

    pub fn execute(&mut self) -> Result<Arc<TupleBuffer>, ExecError> {
        self.exec_plan.execute(&self.context)
    }
}

#[cfg(test)]
mod tests {
    use crate::{executor::bytecode_expr::colidx_expr, tuple};

    use super::*;

    fn check_result(
        actual: &mut Vec<Tuple>,
        expected: &mut Vec<Tuple>,
        sorted: bool,
        verbose: bool,
    ) {
        if sorted {
            let tuple_len = actual[0].fields().len();
            let sort_cols = (0..tuple_len)
                .map(|i| (i as ColumnId, true, false))
                .collect::<Vec<(ColumnId, bool, bool)>>(); // ColumnId, ascending, nulls_first
            actual.sort_by(|a, b| {
                a.to_normalized_key_bytes(&sort_cols)
                    .cmp(&b.to_normalized_key_bytes(&sort_cols))
            });
            expected.sort_by(|a, b| {
                a.to_normalized_key_bytes(&sort_cols)
                    .cmp(&b.to_normalized_key_bytes(&sort_cols))
            });
        }
        let actual_string = actual
            .iter()
            .map(|t| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        let expected_string = expected
            .iter()
            .map(|t: &Tuple| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        if verbose {
            println!("--- Result ---\n{}", actual_string);
            println!("--- Expected ---\n{}", expected_string);
        }
        assert_eq!(
            actual, expected,
            "\n--- Result ---\n{}\n--- Expected ---\n{}\n",
            actual_string, expected_string
        );
    }

    #[test]
    fn test_pipeline_scan() {
        let input = Arc::new(TupleBuffer::vec());
        let tuple = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        input.append(tuple.copy());

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            1,
            PipelineNonBlocking::Scan(PScanIter::new(0)).into(),
            output,
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![tuple];
        check_result(&mut actual, &mut expected, false, true);
    }

    #[test]
    fn test_pipeline_merge_scan() {
        // Prepare two sorted inputs.
        let input1 = Arc::new(TupleBuffer::vec());
        let mut expected1 = vec![
            Tuple::from_fields(vec![1.into(), "a".into()]),
            Tuple::from_fields(vec![3.into(), "b".into()]),
            Tuple::from_fields(vec![5.into(), "c".into()]),
        ];
        for tuple in expected1.iter() {
            input1.append(tuple.copy()).unwrap();
        }

        let input2 = Arc::new(TupleBuffer::vec());
        let mut expected2 = vec![
            Tuple::from_fields(vec![2.into(), "d".into()]),
            Tuple::from_fields(vec![4.into(), "e".into()]),
            Tuple::from_fields(vec![6.into(), "f".into()]),
        ];
        for tuple in expected2.iter() {
            input2.append(tuple.copy()).unwrap();
        }

        // Merge the two inputs.
        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            1,
            PipelineNonBlocking::Scan(PScanIter::new(0)).into(),
            output,
        );

        let runs = Arc::new(TupleBuffer::runs(vec![
            (input1, vec![(0, true, false)]),
            (input2, vec![(0, true, false)]),
        ]));

        pipeline.set_context(0, runs);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        expected1.append(&mut expected2);
        // expected1.sort();

        check_result(&mut actual, &mut expected1, true, true);
    }

    #[test]
    fn test_pipeline_merge_scan_with_3_inputs() {
        // Prepare three sorted inputs.
        let input1 = Arc::new(TupleBuffer::vec());
        let mut expected1 = vec![
            Tuple::from_fields(vec![1.into(), "a".into()]),
            Tuple::from_fields(vec![4.into(), "d".into()]),
            Tuple::from_fields(vec![7.into(), "g".into()]),
        ];
        for tuple in expected1.iter() {
            input1.append(tuple.copy()).unwrap();
        }

        let input2 = Arc::new(TupleBuffer::vec());
        let mut expected2 = vec![
            Tuple::from_fields(vec![2.into(), "b".into()]),
            Tuple::from_fields(vec![5.into(), "e".into()]),
            Tuple::from_fields(vec![8.into(), "h".into()]),
        ];
        for tuple in expected2.iter() {
            input2.append(tuple.copy()).unwrap();
        }

        let input3 = Arc::new(TupleBuffer::vec());
        let mut expected3 = vec![
            Tuple::from_fields(vec![3.into(), "c".into()]),
            Tuple::from_fields(vec![6.into(), "f".into()]),
            Tuple::from_fields(vec![9.into(), "i".into()]),
        ];
        for tuple in expected3.iter() {
            input3.append(tuple.copy()).unwrap();
        }

        // Merge the three inputs.
        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            1,
            PipelineNonBlocking::Scan(PScanIter::new(0)).into(),
            output,
        );

        let runs = Arc::new(TupleBuffer::runs(vec![
            (input1, vec![(0, true, false)]),
            (input2, vec![(0, true, false)]),
            (input3, vec![(0, true, false)]),
        ]));

        pipeline.set_context(0, runs);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        expected1.append(&mut expected2);
        expected1.append(&mut expected3);

        check_result(&mut actual, &mut expected1, true, true);
    }

    #[test]
    fn test_hash_table_iter_key() {
        let exprs = vec![colidx_expr(0)];
        let input = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec!["a".into(), 1.into()]);
        let tuple2 = Tuple::from_fields(vec!["b".into(), 2.into()]);
        let tuple3 = Tuple::from_fields(vec!["a".into(), 3.into()]);
        let tuple4 = Tuple::from_fields(vec!["b".into(), 4.into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let iter_key = input.iter_key(vec!["a".into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter_key.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1.copy(), tuple3.copy()];
        check_result(&mut actual, &mut expected, true, true);

        let iter_key = input.iter_key(vec!["b".into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter_key.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple2.copy(), tuple4.copy()];
        check_result(&mut actual, &mut expected, true, true);

        assert!(input.iter_key(vec!["c".into()]).is_none());

        let iter_all = input.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter_all.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1, tuple2, tuple3, tuple4];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hash_table_iter_all() {
        let exprs = vec![colidx_expr(0)];
        let input = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), 2.into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), 4.into()]);
        let tuple3 = Tuple::from_fields(vec![1.into(), 2.into()]);
        let tuple4 = Tuple::from_fields(vec![3.into(), 4.into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            1,
            PipelineNonBlocking::Scan(PScanIter::new(0)).into(),
            output,
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1, tuple3, tuple2, tuple4];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_inner() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![1.into(), "c".into()]);
        let tuple4 = Tuple::from_fields(vec![3.into(), "d".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();
        input1.append(tuple3.copy()).unwrap();
        input1.append(tuple4.copy()).unwrap();

        // Probe input
        let input0 = Arc::new(TupleBuffer::vec());
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            2,
            PipelineNonBlocking::HashJoin(PHashJoinIter::Inner(PHashJoinInnerIter {
                probe_side: Box::new(PipelineNonBlocking::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
                current: None,
            }))
            .into(),
            output,
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![3.into(), "b".into(), 3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![3.into(), "b".into(), 3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![1.into(), "c".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), "c".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![3.into(), "d".into(), 3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![3.into(), "d".into(), 3.into(), 4.into(), 5.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_outer() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = Arc::new(TupleBuffer::vec());
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            2,
            PipelineNonBlocking::HashJoin(PHashJoinIter::RightOuter(PHashJoinRightOuterIter {
                probe_side: Box::new(PipelineNonBlocking::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
                current: None,
                nulls: Tuple::from_fields(vec![Field::Int(None), Field::String(None)]),
            }))
            .into(),
            output,
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![
                Field::Int(None),
                Field::String(None),
                3.into(),
                4.into(),
                5.into(),
            ]),
            Tuple::from_fields(vec![
                Field::Int(None),
                Field::String(None),
                3.into(),
                4.into(),
                5.into(),
            ]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_semi() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = Arc::new(TupleBuffer::vec());
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            2,
            PipelineNonBlocking::HashJoin(PHashJoinIter::RightSemi(PHashJoinRightSemiIter {
                probe_side: Box::new(PipelineNonBlocking::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
            output,
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]),
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_anti() {
        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = Arc::new(TupleBuffer::vec());
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            2,
            PipelineNonBlocking::HashJoin(PHashJoinIter::RightAnti(PHashJoinRightAntiIter {
                probe_side: Box::new(PipelineNonBlocking::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
            output,
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_mark_without_nulls() {
        // Mark join is similar to semi/anti join.
        // It returns all the probe side tuples with an additional mark at the end of the tuple.
        // If there is a matching tuple, the mark is true.
        // If there is no matching tuple, if the build side has nulls, the mark is null.
        // Otherwise, the mark is false.

        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        // Probe input
        let input0 = Arc::new(TupleBuffer::vec());
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            2,
            PipelineNonBlocking::HashJoin(PHashJoinIter::RightMark(PHashJoinRightMarkIter {
                probe_side: Box::new(PipelineNonBlocking::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
            output,
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::from_bool(false)]),
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::from_bool(false)]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_hashjoin_mark_with_nulls() {
        // Mark join is similar to semi/anti join.
        // It returns all the probe side tuples with an additional mark at the end of the tuple.
        // If there is a matching tuple, the mark is true.
        // If there is no matching tuple, if the build side has nulls, the mark is null.
        // Otherwise, the mark is false.

        let exprs = vec![colidx_expr(0)];
        // Build input
        let input1 = Arc::new(TupleBuffer::hash_table(exprs));
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![Field::Int(None), "c".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();
        input1.append(tuple3.copy()).unwrap();

        // Probe input
        let input0 = Arc::new(TupleBuffer::vec());
        let tuple5 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple6 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        let tuple7 = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        let tuple8 = Tuple::from_fields(vec![3.into(), 4.into(), 5.into()]);
        input0.append(tuple5.copy()).unwrap();
        input0.append(tuple6.copy()).unwrap();
        input0.append(tuple7.copy()).unwrap();
        input0.append(tuple8.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            2,
            PipelineNonBlocking::HashJoin(PHashJoinIter::RightMark(PHashJoinRightMarkIter {
                probe_side: Box::new(PipelineNonBlocking::Scan(PScanIter::new(0))),
                build_side: 1,
                exprs: vec![colidx_expr(0)],
            }))
            .into(),
            output,
        );
        pipeline.set_context(0, input0); // Probe input
        pipeline.set_context(1, input1); // Build input

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::Boolean(None)]),
            Tuple::from_fields(vec![1.into(), 2.into(), 3.into(), Field::from_bool(true)]),
            Tuple::from_fields(vec![3.into(), 4.into(), 5.into(), Field::Boolean(None)]),
        ];

        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_in_mem_sort() {
        let input = Arc::new(TupleBuffer::vec());
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![2.into(), "c".into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            1,
            PipelineBlocking::in_mem_sort(
                PipelineNonBlocking::Scan(PScanIter::new(0)),
                vec![(0, true, false)],
            ),
            output,
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![tuple1, tuple3, tuple2];
        check_result(&mut actual, &mut expected, false, true);
    }

    #[test]
    fn test_pipeline_in_mem_hashtable_creation() {
        let input = Arc::new(TupleBuffer::vec());
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![3.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![1.into(), "c".into()]);
        let tuple4 = Tuple::from_fields(vec![3.into(), "d".into()]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let output = Arc::new(TupleBuffer::hash_table(vec![colidx_expr(0)]));
        let mut pipeline = Pipeline::new(
            1,
            PipelineBlocking::in_mem_hash_table_creation(
                PipelineNonBlocking::Scan(PScanIter::new(0)),
                vec![colidx_expr(0)],
            ),
            output,
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();

        let iter = result.iter_key(vec![1.into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple1.copy(), tuple3.copy()];
        check_result(&mut actual, &mut expected, true, true);

        let iter = result.iter_key(vec![3.into()]).unwrap();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }
        let mut expected = vec![tuple2.copy(), tuple4.copy()];
        check_result(&mut actual, &mut expected, true, true);

        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![tuple1, tuple2, tuple3, tuple4];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_in_mem_hash_aggregation() {
        let input = Arc::new(TupleBuffer::vec());
        let tuple1 = Tuple::from_fields(vec![1.into(), 10.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![1.into(), 10.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![2.into(), 20.into(), "a".into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), 20.into(), Field::String(None)]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            1,
            PipelineBlocking::InMemHashAggregate(InMemHashAggregation {
                exec_plan: PipelineNonBlocking::Scan(PScanIter::new(0)),
                group_by: vec![0],
                agg_op: vec![(AggOp::Sum, 1), (AggOp::Count, 2)],
            }),
            output,
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 20.into(), 2.into()]),
            Tuple::from_fields(vec![2.into(), 40.into(), 1.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_in_mem_hash_aggregation_with_null_group() {
        let input = Arc::new(TupleBuffer::vec());
        let tuple1 = Tuple::from_fields(vec![1.into(), 10.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![1.into(), 10.into(), "b".into()]);
        let tuple3 = Tuple::from_fields(vec![Field::Int(None), 20.into(), "a".into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), 20.into(), Field::String(None)]);
        input.append(tuple1.copy()).unwrap();
        input.append(tuple2.copy()).unwrap();
        input.append(tuple3.copy()).unwrap();
        input.append(tuple4.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            1,
            PipelineBlocking::InMemHashAggregate(InMemHashAggregation {
                exec_plan: PipelineNonBlocking::Scan(PScanIter::new(0)),
                group_by: vec![0],
                agg_op: vec![(AggOp::Sum, 1), (AggOp::Count, 2)],
            }),
            output,
        );
        pipeline.set_context(0, input);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), 20.into(), 2.into()]),
            Tuple::from_fields(vec![2.into(), 20.into(), 0.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }

    #[test]
    fn test_pipeline_nested_loop_join() {
        let input1 = Arc::new(TupleBuffer::vec());
        let tuple1 = Tuple::from_fields(vec![1.into(), "a".into()]);
        let tuple2 = Tuple::from_fields(vec![2.into(), "b".into()]);
        input1.append(tuple1.copy()).unwrap();
        input1.append(tuple2.copy()).unwrap();

        let input2 = Arc::new(TupleBuffer::vec());
        let tuple3 = Tuple::from_fields(vec![1.into(), 10.into()]);
        let tuple4 = Tuple::from_fields(vec![2.into(), 20.into()]);
        input2.append(tuple3.copy()).unwrap();
        input2.append(tuple4.copy()).unwrap();

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(
            2,
            PipelineNonBlocking::NestedLoopJoin(PNestedLoopJoinIter::new(
                Box::new(PipelineNonBlocking::Scan(PScanIter::new(0))),
                Box::new(PipelineNonBlocking::Scan(PScanIter::new(1))),
            ))
            .into(),
            output,
        );
        pipeline.set_context(0, input1);
        pipeline.set_context(1, input2);

        let result = pipeline.execute().unwrap();
        let iter = result.iter_all();
        let mut actual = Vec::new();
        while let Some(tuple) = iter.next() {
            actual.push(tuple);
        }

        let mut expected = vec![
            Tuple::from_fields(vec![1.into(), "a".into(), 1.into(), 10.into()]),
            Tuple::from_fields(vec![2.into(), "b".into(), 1.into(), 10.into()]),
            Tuple::from_fields(vec![1.into(), "a".into(), 2.into(), 20.into()]),
            Tuple::from_fields(vec![2.into(), "b".into(), 2.into(), 20.into()]),
        ];
        check_result(&mut actual, &mut expected, true, true);
    }
}

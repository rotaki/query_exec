use std::{collections::HashMap, sync::Arc};

use txn_storage::prelude::*;

use crate::{
    catalog::{
        self,
        prelude::{ColumnDef, DataType},
        CatalogRef, Schema, SchemaRef,
    },
    error::ExecError,
    expression::{prelude::PhysicalRelExpr, AggOp, Expression, JoinType},
    log, log_info, log_trace,
    tuple::{FromBool, IsNull, Tuple},
    ColumnId, Field,
};

use super::{bytecode_expr::ByteCodeExpr, Executor};

type Key = Vec<u8>;

pub enum VolcanoIterator<T: TxnStorageTrait> {
    Scan(ScanIter<T>),
    Filter(FilterIter<T>),
    Project(ProjectIter<T>),
    Map(MapIter<T>),
    HashAggregate(HashAggregateIter<T>),
    Sort(SortIter<T>),
    Limit(LimitIter<T>),
    HashJoin(HashJoinIter<T>),
    NestedLoopJoin(NestedLoopJoin<T>),
}

impl<T: TxnStorageTrait> Executor<T> for VolcanoIterator<T> {
    fn new(catalog: CatalogRef, storage: Arc<T>, physical_plan: PhysicalRelExpr) -> Self {
        let mut physical_to_op = PhysicalRelExprToOpIter::new(storage);
        physical_to_op.to_executable(catalog, physical_plan)
    }

    fn to_pretty_string(&self) -> String {
        let mut out = String::new();
        self.print_inner(0, &mut out);
        out
    }

    fn execute(&mut self, txn: &T::TxnHandle) -> Result<Vec<Tuple>, ExecError> {
        let mut results = Vec::new();
        loop {
            log_trace!("------------ VolcanoIterator::next ------------");
            match self.next(txn)? {
                Some((_, tuple)) => {
                    results.push(tuple);
                }
                None => {
                    return Ok(results);
                }
            }
        }
    }
}

impl<T: TxnStorageTrait> VolcanoIterator<T> {
    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        // (key, tuple)
        match self {
            VolcanoIterator::Scan(iter) => {
                log_trace!("ScanIter::next");
                iter.next(txn)
            }
            VolcanoIterator::Filter(iter) => {
                log_trace!("FilterIter::next");
                iter.next(txn)
            }
            VolcanoIterator::Project(iter) => {
                log_trace!("ProjectIter::next");
                iter.next(txn)
            }
            VolcanoIterator::HashAggregate(iter) => {
                log_trace!("HashAggregateIter::next");
                iter.next(txn)
            }
            VolcanoIterator::Sort(iter) => {
                log_trace!("SortIter::next");
                iter.next(txn)
            }
            VolcanoIterator::Limit(iter) => {
                log_trace!("LimitIter::next");
                iter.next(txn)
            }
            VolcanoIterator::HashJoin(iter) => {
                log_trace!("HashJoin::next");
                iter.next(txn)
            }
            VolcanoIterator::NestedLoopJoin(iter) => {
                log_trace!("NestedLoopJoin::next");
                iter.next(txn)
            }
            VolcanoIterator::Map(iter) => {
                log_trace!("MapIter::next");
                iter.next(txn)
            }
        }
    }

    // Returns the output schema of the operator
    fn schema(&self) -> SchemaRef {
        match self {
            VolcanoIterator::Scan(iter) => iter.schema(),
            VolcanoIterator::Filter(iter) => iter.schema(),
            VolcanoIterator::Project(iter) => iter.schema(),
            VolcanoIterator::HashAggregate(iter) => iter.schema(),
            VolcanoIterator::Sort(iter) => iter.schema(),
            VolcanoIterator::Limit(iter) => iter.schema(),
            VolcanoIterator::HashJoin(iter) => iter.schema(),
            VolcanoIterator::NestedLoopJoin(iter) => iter.schema(),
            VolcanoIterator::Map(iter) => iter.schema(),
        }
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            VolcanoIterator::Scan(iter) => iter.print_inner(indent, out),
            VolcanoIterator::Filter(iter) => iter.print_inner(indent, out),
            VolcanoIterator::Project(iter) => iter.print_inner(indent, out),
            VolcanoIterator::HashAggregate(iter) => iter.print_inner(indent, out),
            VolcanoIterator::Sort(iter) => iter.print_inner(indent, out),
            VolcanoIterator::Limit(iter) => iter.print_inner(indent, out),
            VolcanoIterator::HashJoin(iter) => iter.print_inner(indent, out),
            VolcanoIterator::NestedLoopJoin(iter) => iter.print_inner(indent, out),
            VolcanoIterator::Map(iter) => iter.print_inner(indent, out),
        }
    }
}

pub struct ScanIter<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub storage: Arc<T>,
    pub c_id: ContainerId,
    pub column_indices: Vec<ColumnId>,
    pub iter: Option<T::IteratorHandle>,
}

impl<T: TxnStorageTrait> ScanIter<T> {
    pub fn new(
        schema: SchemaRef,
        storage: Arc<T>,
        c_id: ContainerId,
        column_indices: Vec<ColumnId>,
    ) -> Self {
        Self {
            schema,
            storage,
            c_id,
            column_indices,
            iter: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        if self.iter.is_none() {
            self.iter = Some(
                self.storage
                    .scan_range(txn, &self.c_id, ScanOptions::default())
                    .unwrap(),
            );
        }
        let iter = self.iter.as_ref().unwrap();
        if let Some((k, v)) = self.storage.iter_next(iter)? {
            let tuple = Tuple::from_bytes(&v);
            let tuple = tuple.project(&self.column_indices);
            log_trace!("ScanIter::next: c_id({}), tuple: {:?}", self.c_id, tuple);
            Ok(Some((k, tuple)))
        } else {
            self.iter = None;
            log_trace!("ScanIter::next: None");
            Ok(None)
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> scan(c_id({}), ",
            " ".repeat(indent),
            self.c_id
        ));
        let mut split = "";
        out.push_str("[");
        for col_id in &self.column_indices {
            out.push_str(split);
            out.push_str(&format!("{:?}", col_id));
            split = ", ";
        }
        out.push_str("])\n");
    }
}

pub struct FilterIter<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub input: Box<VolcanoIterator<T>>,
    pub expr: ByteCodeExpr,
}

impl<T: TxnStorageTrait> FilterIter<T> {
    pub fn new(schema: SchemaRef, input: Box<VolcanoIterator<T>>, expr: ByteCodeExpr) -> Self {
        Self {
            schema,
            input,
            expr,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        loop {
            match self.input.next(txn)? {
                Some((k, tuple)) => {
                    if self.expr.eval(&tuple)? == Field::from_bool(true) {
                        log_info!("FilterIter::next: tuple: {:?}", tuple);
                        return Ok(Some((k, tuple)));
                    }
                }
                None => {
                    log_trace!("FilterIter::next: None");
                    return Ok(None);
                }
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> filter({:?})", " ".repeat(indent), self.expr));
        out.push_str("\n");
        self.input.print_inner(indent + 2, out);
    }
}

pub struct ProjectIter<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub input: Box<VolcanoIterator<T>>,
    pub column_indices: Vec<ColumnId>,
}

impl<T: TxnStorageTrait> ProjectIter<T> {
    pub fn new(
        schema: SchemaRef,
        input: Box<VolcanoIterator<T>>,
        column_indices: Vec<ColumnId>,
    ) -> Self {
        Self {
            schema,
            input,
            column_indices,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        match self.input.next(txn)? {
            Some((k, tuple)) => {
                let new_tuple = tuple.project(&self.column_indices);
                Ok(Some((k, new_tuple)))
            }
            None => {
                log_trace!("ProjectIter::next: None");
                Ok(None)
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> project(", " ".repeat(indent)));
        let mut split = "";
        out.push_str("[");
        for col_id in &self.column_indices {
            out.push_str(split);
            out.push_str(&format!("{:?}", col_id));
            split = ", ";
        }
        out.push_str("]");
        out.push_str(")\n");
        self.input.print_inner(indent + 2, out);
    }
}

pub struct MapIter<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub input: Box<VolcanoIterator<T>>,
    pub exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait> MapIter<T> {
    pub fn new(
        schema: SchemaRef,
        input: Box<VolcanoIterator<T>>,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            input,
            exprs,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        match self.input.next(txn)? {
            Some((k, mut tuple)) => {
                for expr in &self.exprs {
                    tuple.push(expr.eval(&tuple)?);
                }
                Ok(Some((k, tuple)))
            }
            None => {
                log_trace!("MapIter::next: None");
                Ok(None)
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> map(", " ".repeat(indent)));
        let mut split = "";
        out.push_str("[");
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("]");
        out.push_str(")\n");
        self.input.print_inner(indent + 2, out);
    }
}

pub struct HashAggregateIter<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub input: Box<VolcanoIterator<T>>,
    pub group_by: Vec<ColumnId>,
    pub agg_op: Vec<(AggOp, ColumnId)>,
    pub agg_result: Option<Vec<Tuple>>,
}

impl<T: TxnStorageTrait> HashAggregateIter<T> {
    pub fn new(
        schema: SchemaRef,
        input: Box<VolcanoIterator<T>>,
        group_by: Vec<ColumnId>,
        agg_op: Vec<(AggOp, ColumnId)>,
    ) -> Self {
        Self {
            schema,
            input,
            group_by,
            agg_op,
            agg_result: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Vec<u8>, Tuple)>, ExecError> {
        if self.agg_result.is_none() {
            // Pull tuples from the child and aggregate them
            let mut agg_result: HashMap<Vec<Field>, (Vec<Field>, usize)> = HashMap::new();
            while let Some((_, tuple)) = self.input.next(txn)? {
                let group_key = tuple.get_cols(&self.group_by);
                // If any of the group by columns are null, skip this tuple
                if group_key.iter().any(|f| f.is_null()) {
                    continue;
                }
                match agg_result.get_mut(&group_key) {
                    Some((agg_vals, count)) => {
                        for (idx, (op, col)) in self.agg_op.iter().enumerate() {
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
                        *count += 1;
                    }
                    None => {
                        let mut agg_vals = Vec::with_capacity(self.agg_op.len());
                        for (op, col) in &self.agg_op {
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
                        agg_result.insert(group_key, (agg_vals, 1));
                    }
                }
            }
            self.agg_result = {
                let mut agg_result_vec = agg_result
                    .drain()
                    .map(|(mut group_key, (mut agg_vals, count))| {
                        // Divide the sum by the count to get the average
                        for (idx, (op, _)) in self.agg_op.iter().enumerate() {
                            if *op == AggOp::Avg {
                                agg_vals[idx] =
                                    (&agg_vals[idx] / &Field::Float(Some(count as f64))).unwrap();
                            }
                        }
                        group_key.append(&mut agg_vals);
                        Tuple::from_fields(group_key)
                    })
                    .collect::<Vec<_>>();

                // No tuples were aggregated, return a single tuple with nulls.
                if agg_result_vec.is_empty() {
                    let mut result = Vec::with_capacity(self.group_by.len() + self.agg_op.len());
                    for _ in 0..self.group_by.len() {
                        result.push(Field::null(&DataType::Unknown));
                    }
                    for (op, _) in &self.agg_op {
                        match op {
                            AggOp::Count => result.push(Field::Int(Some(0))),
                            _ => result.push(Field::null(&DataType::Unknown)),
                        }
                    }
                    let tuple = Tuple::from_fields(result);
                    agg_result_vec.push(tuple);
                }
                Some(agg_result_vec)
            }
        }

        // Go through the aggregated results and return them.
        // Pop the first key-value pair from the hashmap and return it.
        // If the hashmap is empty, return None and remove the hashmap.
        let agg_result = self.agg_result.as_mut().unwrap();
        if agg_result.is_empty() {
            self.agg_result = None;
            log_trace!("HashAggregateIter::next: None");
            Ok(None)
        } else {
            let tuple = agg_result.pop().unwrap();
            Ok(Some((vec![], tuple)))
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> hash_aggregate(", " ".repeat(indent)));
        let mut split = "";
        out.push_str("[");
        for col_id in &self.group_by {
            out.push_str(split);
            out.push_str(&format!("{:?}", col_id));
            split = ", ";
        }
        out.push_str("], ");
        out.push_str("[");
        split = "";
        for (op, col_id) in &self.agg_op {
            out.push_str(split);
            out.push_str(&format!("{:?}({:?})", op, col_id));
            split = ", ";
        }
        out.push_str("])\n");
        self.input.print_inner(indent + 2, out);
    }
}

pub struct SortIter<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub input: Box<VolcanoIterator<T>>,
    pub sort_cols: Vec<(ColumnId, bool, bool)>, // (col_id, asc, nulls_first)
    pub buffer: Option<Vec<(Vec<u8>, Tuple)>>,
}

impl<T: TxnStorageTrait> SortIter<T> {
    pub fn new(
        schema: SchemaRef,
        input: Box<VolcanoIterator<T>>,
        sort_cols: Vec<(ColumnId, bool, bool)>,
    ) -> Self {
        Self {
            schema,
            input,
            sort_cols,
            buffer: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        if self.buffer.is_none() {
            let mut sort_buffer = Vec::new();
            while let Some((k, tuple)) = self.input.next(txn)? {
                sort_buffer.push((tuple.to_normalized_key_bytes(&self.sort_cols), (k, tuple)));
            }
            // Sort in reverse order so we can pop the last element
            sort_buffer.sort_by(|a, b| b.0.cmp(&a.0));
            self.buffer = {
                let res = sort_buffer
                    .into_iter()
                    .map(|(_, tuple)| tuple)
                    .collect::<Vec<_>>();
                Some(res)
            }
        }

        match self.buffer.as_mut().unwrap().pop() {
            Some((k, tuple)) => Ok(Some((k, tuple))),
            None => {
                self.buffer = None;
                log_trace!("SortIter::next: None");
                Ok(None)
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> sort(", " ".repeat(indent)));
        let mut split = "";
        out.push_str("[");
        for (col_id, asc, nulls_first) in &self.sort_cols {
            out.push_str(split);
            out.push_str(&format!(
                "{:?} {}{}",
                col_id,
                if *asc { "asc" } else { "desc" },
                if *nulls_first { " nulls first" } else { "" }
            ));
            split = ", ";
        }
        out.push_str("])\n");
        self.input.print_inner(indent + 2, out);
    }
}

pub struct LimitIter<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub input: Box<VolcanoIterator<T>>,
    pub limit: usize,
    pub current: Option<usize>,
}

impl<T: TxnStorageTrait> LimitIter<T> {
    pub fn new(schema: SchemaRef, input: Box<VolcanoIterator<T>>, limit: usize) -> Self {
        Self {
            schema,
            input,
            limit,
            current: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        if self.current.is_none() {
            self.current = Some(0);
        }
        if self.current.unwrap() >= self.limit {
            self.current = None;
            log_trace!("LimitIter::next: None");
            Ok(None)
        } else {
            self.current = Some(self.current.unwrap() + 1);
            self.input.next(txn)
        }
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> limit({})", " ".repeat(indent), self.limit));
        out.push_str("\n");
        self.input.print_inner(indent + 2, out);
    }
}

enum HashJoinIter<T: TxnStorageTrait> {
    Inner(HashJoinInner<T>),
    RightOuter(HashJoinRightOuter<T>),
    RightSemi(HashJoinRightSemi<T>),
    RightAnti(HashJoinRightAnti<T>),
}

impl<T: TxnStorageTrait> HashJoinIter<T> {
    pub fn new(
        schema: SchemaRef,
        join_type: JoinType,
        left: Box<VolcanoIterator<T>>,
        right: Box<VolcanoIterator<T>>,
        left_exprs: Vec<ByteCodeExpr>,
        right_exprs: Vec<ByteCodeExpr>,
        filter: Option<ByteCodeExpr>,
    ) -> Self {
        match join_type {
            JoinType::Inner => Self::Inner(HashJoinInner::new(
                schema,
                left,
                right,
                left_exprs,
                right_exprs,
                filter,
            )),
            JoinType::RightOuter => Self::RightOuter(HashJoinRightOuter::new(
                schema,
                left,
                right,
                left_exprs,
                right_exprs,
                filter,
            )),
            JoinType::RightSemi => Self::RightSemi(HashJoinRightSemi::new(
                schema,
                left,
                right,
                left_exprs,
                right_exprs,
                filter,
            )),
            JoinType::RightAnti => Self::RightAnti(HashJoinRightAnti::new(
                schema,
                left,
                right,
                left_exprs,
                right_exprs,
                filter,
            )),
            other => {
                panic!("Unimplemented join type: {:?}", other);
            }
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Inner(inner) => inner.schema(),
            Self::RightOuter(inner) => inner.schema.clone(),
            Self::RightSemi(inner) => inner.schema.clone(),
            Self::RightAnti(inner) => inner.schema.clone(),
        }
    }

    pub fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        match self {
            Self::Inner(inner) => inner.next(txn),
            Self::RightOuter(inner) => inner.next(txn),
            Self::RightSemi(inner) => inner.next(txn),
            Self::RightAnti(inner) => inner.next(txn),
        }
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            Self::Inner(inner) => inner.print_inner(indent, out),
            Self::RightOuter(inner) => inner.print_inner(indent, out),
            Self::RightSemi(inner) => inner.print_inner(indent, out),
            Self::RightAnti(inner) => inner.print_inner(indent, out),
        }
    }
}

struct HashJoinInner<T: TxnStorageTrait> {
    schema: SchemaRef,
    left: Box<VolcanoIterator<T>>,
    right: Box<VolcanoIterator<T>>,
    left_exprs: Vec<ByteCodeExpr>,
    right_exprs: Vec<ByteCodeExpr>,
    filter: Option<ByteCodeExpr>,
    buffer: Option<HashMap<Vec<Field>, Vec<Tuple>>>,
    current_right: Option<Tuple>,
    current_right_fields: Option<Vec<Field>>,
    current_idx: Option<usize>,
}

impl<T: TxnStorageTrait> HashJoinInner<T> {
    pub fn new(
        schema: SchemaRef,
        left: Box<VolcanoIterator<T>>,
        right: Box<VolcanoIterator<T>>,
        left_exprs: Vec<ByteCodeExpr>,
        right_exprs: Vec<ByteCodeExpr>,
        filter: Option<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            left,
            right,
            left_exprs,
            right_exprs,
            filter,
            buffer: None,
            current_right: None,
            current_right_fields: None,
            current_idx: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        if self.buffer.is_none() {
            let mut hash_table: HashMap<Vec<Field>, Vec<Tuple>> = HashMap::new();
            while let Some((_, tuple)) = self.left.next(txn)? {
                let fields: Vec<Field> = self
                    .left_exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                hash_table
                    .entry(fields)
                    .or_insert_with(Vec::new)
                    .push(tuple);
            }
            self.buffer = Some(hash_table);
        }

        loop {
            if self.current_right.is_none() {
                match self.right.next(txn)? {
                    Some((_, tuple)) => {
                        let right_fields = self
                            .right_exprs
                            .iter()
                            .map(|expr| expr.eval(&tuple))
                            .collect::<Result<Vec<_>, _>>()?;
                        self.current_right = Some(tuple);
                        self.current_right_fields = Some(right_fields);
                        self.current_idx = Some(0);
                    }
                    None => {
                        self.current_idx = None;
                        self.current_right_fields = None;
                        self.buffer = None;
                        return Ok(None);
                    }
                }
            }

            let current_right = self.current_right.as_ref().unwrap();
            let current_right_fields = self.current_right_fields.as_ref().unwrap();
            let current_idx = self.current_idx.unwrap();
            let hash_table = self.buffer.as_mut().unwrap();
            if let Some(left_tuples) = hash_table.get_mut(current_right_fields) {
                if current_idx < left_tuples.len() {
                    let left_tuple = &left_tuples[current_idx];
                    let new_tuple = left_tuple.merge(current_right);
                    self.current_idx = Some(current_idx + 1);
                    if let Some(filter) = &self.filter {
                        if filter.eval(&new_tuple)? == Field::from_bool(true) {
                            return Ok(Some((vec![], new_tuple)));
                        } else {
                            continue;
                        }
                    } else {
                        return Ok(Some((vec![], new_tuple)));
                    }
                } else {
                    self.current_right = None;
                    self.current_right_fields = None;
                    self.current_idx = None;
                }
            } else {
                self.current_right = None;
                self.current_right_fields = None;
                self.current_idx = None;
            }
        }
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> hash_join_inner(\n", " ".repeat(indent)));
        out.push_str(&format!("{}left_exprs: [", " ".repeat(indent + 4)));
        let mut split = "";
        for expr in &self.left_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("]\n");
        out.push_str(&format!("{}right_exprs: [", " ".repeat(indent + 4)));
        split = "";
        for expr in &self.right_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        split = "";
        out.push_str(&format!("{}filter: [", " ".repeat(indent + 4)));
        if let Some(filter) = &self.filter {
            out.push_str(&format!("{:?}", filter));
        }
        out.push_str("]\n");
        out.push_str(&format!("{})\n", " ".repeat(indent)));
        self.left.print_inner(indent + 2, out);
        self.right.print_inner(indent + 2, out);
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct HashJoinRightOuter<T: TxnStorageTrait> {
    schema: SchemaRef,
    left: Box<VolcanoIterator<T>>,
    right: Box<VolcanoIterator<T>>,
    left_exprs: Vec<ByteCodeExpr>,
    right_exprs: Vec<ByteCodeExpr>,
    filter: Option<ByteCodeExpr>,
    buffer: Option<HashMap<Vec<Field>, Vec<Tuple>>>,
    current_right: Option<Tuple>,
    current_right_fields: Option<Vec<Field>>,
    current_idx: Option<usize>,
    null_tuple: Option<Tuple>,
}

impl<T: TxnStorageTrait> HashJoinRightOuter<T> {
    fn new(
        schema: SchemaRef,
        left: Box<VolcanoIterator<T>>,
        right: Box<VolcanoIterator<T>>,
        left_exprs: Vec<ByteCodeExpr>,
        right_exprs: Vec<ByteCodeExpr>,
        filter: Option<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            left,
            right,
            left_exprs,
            right_exprs,
            filter,
            buffer: None,
            current_right: None,
            current_right_fields: None,
            current_idx: None,
            null_tuple: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        if self.buffer.is_none() {
            let mut hash_table: HashMap<Vec<Field>, Vec<Tuple>> = HashMap::new();
            while let Some((_, tuple)) = self.left.next(txn)? {
                let fields = self
                    .left_exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                hash_table
                    .entry(fields)
                    .or_insert_with(Vec::new)
                    .push(tuple);
            }
            self.buffer = Some(hash_table);
        }

        if self.null_tuple.is_none() {
            let left_schema = self.left.schema();
            let left_null_tuple = Tuple::from_fields(
                left_schema
                    .columns()
                    .iter()
                    .map(|col| Field::null(col.data_type()))
                    .collect::<Vec<_>>(),
            );
            self.null_tuple = Some(left_null_tuple);
        }

        loop {
            if self.current_right.is_none() {
                match self.right.next(txn)? {
                    Some((_, tuple)) => {
                        let right_fields = self
                            .right_exprs
                            .iter()
                            .map(|expr| expr.eval(&tuple))
                            .collect::<Result<Vec<_>, _>>()?;
                        self.current_right = Some(tuple);
                        self.current_right_fields = Some(right_fields);
                        self.current_idx = Some(0);
                    }
                    None => {
                        self.current_idx = None;
                        self.current_right_fields = None;
                        self.buffer = None;
                        return Ok(None);
                    }
                }
            }

            let current_right = self.current_right.as_ref().unwrap();
            let current_right_fields = self.current_right_fields.as_ref().unwrap();
            let current_idx = self.current_idx.unwrap();
            let hash_table = self.buffer.as_mut().unwrap();
            if let Some(left_tuples) = hash_table.get_mut(current_right_fields) {
                if current_idx < left_tuples.len() {
                    let left_tuple = &left_tuples[current_idx];
                    let new_tuple = left_tuple.merge(current_right);
                    self.current_idx = Some(current_idx + 1);
                    if let Some(filter) = &self.filter {
                        if filter.eval(&new_tuple)? == Field::from_bool(true) {
                            return Ok(Some((vec![], new_tuple)));
                        } else {
                            continue;
                        }
                    } else {
                        return Ok(Some((vec![], new_tuple)));
                    }
                } else {
                    self.current_right = None;
                    self.current_right_fields = None;
                    self.current_idx = None;
                }
            } else {
                let current_right = self.current_right.take().unwrap();
                let new_tuple = self.null_tuple.as_ref().unwrap().merge(&current_right);
                self.current_right = None;
                self.current_right_fields = None;
                self.current_idx = None;
                if let Some(filter) = &self.filter {
                    if filter.eval(&new_tuple)? == Field::from_bool(true) {
                        return Ok(Some((vec![], new_tuple)));
                    } else {
                        continue;
                    }
                } else {
                    return Ok(Some((vec![], new_tuple)));
                }
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> hash_join_right_outer(\n",
            " ".repeat(indent)
        ));
        out.push_str(&format!("{}left_exprs: [", " ".repeat(indent + 4)));
        let mut split = "";
        for expr in &self.left_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("]\n");
        out.push_str(&format!("{}right_exprs: [", " ".repeat(indent + 4)));
        split = "";
        for expr in &self.right_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        split = "";
        out.push_str(&format!("{}filter: [", " ".repeat(indent + 4)));
        if let Some(filter) = &self.filter {
            out.push_str(&format!("{:?}", filter));
        }
        out.push_str("]\n");
        out.push_str(&format!("{})\n", " ".repeat(indent)));
        self.left.print_inner(indent + 2, out);
        self.right.print_inner(indent + 2, out);
    }
}

struct HashJoinRightSemi<T: TxnStorageTrait> {
    schema: SchemaRef,
    left: Box<VolcanoIterator<T>>,
    right: Box<VolcanoIterator<T>>,
    left_exprs: Vec<ByteCodeExpr>,
    right_exprs: Vec<ByteCodeExpr>,
    filter: Option<ByteCodeExpr>,
    buffer: Option<HashMap<Vec<Field>, Vec<Tuple>>>,
}

impl<T: TxnStorageTrait> HashJoinRightSemi<T> {
    fn new(
        schema: SchemaRef,
        left: Box<VolcanoIterator<T>>,
        right: Box<VolcanoIterator<T>>,
        left_exprs: Vec<ByteCodeExpr>,
        right_exprs: Vec<ByteCodeExpr>,
        filter: Option<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            left,
            right,
            left_exprs,
            right_exprs,
            filter,
            buffer: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        if self.buffer.is_none() {
            let mut hash_table: HashMap<Vec<Field>, Vec<Tuple>> = HashMap::new();
            while let Some((_, tuple)) = self.left.next(txn)? {
                let fields = self
                    .left_exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                hash_table
                    .entry(fields)
                    .or_insert_with(Vec::new)
                    .push(tuple);
            }
            self.buffer = Some(hash_table);
        }

        loop {
            if let Some((key, tuple)) = self.right.next(txn)? {
                let fields = self
                    .right_exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                let hash_table = self.buffer.as_mut().unwrap();
                if let Some(_) = hash_table.get_mut(&fields) {
                    if let Some(filter) = &self.filter {
                        if filter.eval(&tuple)? == Field::from_bool(true) {
                            return Ok(Some((key, tuple)));
                        } else {
                            continue;
                        }
                    } else {
                        return Ok(Some((key, tuple)));
                    }
                }
            } else {
                self.buffer = None;
                return Ok(None);
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> hash_join_right_semi(\n", " ".repeat(indent)));
        out.push_str(&format!("{}left_exprs: [", " ".repeat(indent + 4)));
        let mut split = "";
        for expr in &self.left_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("]\n");
        out.push_str(&format!("{}right_exprs: [", " ".repeat(indent + 4)));
        split = "";
        for expr in &self.right_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        split = "";
        out.push_str(&format!("{}filter: [", " ".repeat(indent + 4)));
        if let Some(filter) = &self.filter {
            out.push_str(&format!("{:?}", filter));
        }
        out.push_str("]\n");
        out.push_str(&format!("{})\n", " ".repeat(indent)));
        self.left.print_inner(indent + 2, out);
        self.right.print_inner(indent + 2, out);
    }
}

struct HashJoinRightAnti<T: TxnStorageTrait> {
    schema: SchemaRef,
    left: Box<VolcanoIterator<T>>,
    right: Box<VolcanoIterator<T>>,
    left_exprs: Vec<ByteCodeExpr>,
    right_exprs: Vec<ByteCodeExpr>,
    filter: Option<ByteCodeExpr>,
    buffer: Option<HashMap<Vec<Field>, Vec<Tuple>>>,
}

impl<T: TxnStorageTrait> HashJoinRightAnti<T> {
    fn new(
        schema: SchemaRef,
        left: Box<VolcanoIterator<T>>,
        right: Box<VolcanoIterator<T>>,
        left_exprs: Vec<ByteCodeExpr>,
        right_exprs: Vec<ByteCodeExpr>,
        filter: Option<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            left,
            right,
            left_exprs,
            right_exprs,
            filter,
            buffer: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        if self.buffer.is_none() {
            let mut hash_table: HashMap<Vec<Field>, Vec<Tuple>> = HashMap::new();
            while let Some((_, tuple)) = self.left.next(txn)? {
                let fields = self
                    .left_exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                hash_table
                    .entry(fields)
                    .or_insert_with(Vec::new)
                    .push(tuple);
            }
            self.buffer = Some(hash_table);
        }

        loop {
            if let Some((key, tuple)) = self.right.next(txn)? {
                let fields = self
                    .right_exprs
                    .iter()
                    .map(|expr| expr.eval(&tuple))
                    .collect::<Result<Vec<_>, _>>()?;
                let hash_table = self.buffer.as_mut().unwrap();
                if let Some(_) = hash_table.get_mut(&fields) {
                    // If the tuple is in the hash table, skip it
                    continue;
                } else {
                    if let Some(filter) = &self.filter {
                        if filter.eval(&tuple)? == Field::from_bool(true) {
                            return Ok(Some((key, tuple)));
                        } else {
                            continue;
                        }
                    } else {
                        return Ok(Some((key, tuple)));
                    }
                }
            } else {
                self.buffer = None;
                return Ok(None);
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}-> hash_join_right_anti(\n", " ".repeat(indent)));
        out.push_str(&format!("{}left_exprs: [", " ".repeat(indent + 4)));
        let mut split = "";
        for expr in &self.left_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("]\n");
        out.push_str(&format!("{}right_exprs: [", " ".repeat(indent + 4)));
        split = "";
        for expr in &self.right_exprs {
            out.push_str(split);
            out.push_str(&format!("{:?}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        split = "";
        out.push_str(&format!("{}filter: [", " ".repeat(indent + 4)));
        if let Some(filter) = &self.filter {
            out.push_str(&format!("{:?}", filter));
        }
        out.push_str("]\n");
        out.push_str(&format!("{})\n", " ".repeat(indent)));
        self.left.print_inner(indent + 2, out);
        self.right.print_inner(indent + 2, out);
    }
}

pub struct NestedLoopJoin<T: TxnStorageTrait> {
    pub schema: SchemaRef,
    pub join_type: JoinType,
    pub left: Box<VolcanoIterator<T>>,
    pub right: Box<VolcanoIterator<T>>,
    pub filter: Option<ByteCodeExpr>,
    pub current_left: Option<Tuple>,
    pub current_right: Option<Tuple>,
}

impl<T: TxnStorageTrait> NestedLoopJoin<T> {
    pub fn new(
        schema: SchemaRef,
        join_type: JoinType,
        left: Box<VolcanoIterator<T>>,
        right: Box<VolcanoIterator<T>>,
        filter: Option<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            join_type,
            left,
            right,
            filter,
            current_left: None,
            current_right: None,
        }
    }

    fn next(&mut self, txn: &T::TxnHandle) -> Result<Option<(Key, Tuple)>, ExecError> {
        loop {
            if self.current_left.is_none() {
                match self.left.next(txn)? {
                    Some((_, tuple)) => {
                        self.current_left = Some(tuple);
                    }
                    None => {
                        log_trace!("NestedLoopJoin::next: None");
                        return Ok(None);
                    }
                }
            }

            if self.current_right.is_none() {
                match self.right.next(txn)? {
                    Some((_, tuple)) => {
                        self.current_right = Some(tuple);
                    }
                    None => {
                        self.current_left = None;
                        continue;
                    }
                }
            }

            let left_tuple = self.current_left.as_ref().unwrap();
            let right_tuple = self.current_right.as_ref().unwrap();
            let new_tuple = left_tuple.merge(right_tuple);
            if let Some(filter) = &self.filter {
                if filter.eval(&new_tuple)? == Field::from_bool(true) {
                    self.current_right = None;
                    return Ok(Some((vec![], new_tuple)));
                } else {
                    self.current_right = None;
                }
            } else {
                self.current_right = None;
                return Ok(Some((vec![], new_tuple)));
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!(
            "{}-> nl_join({})\n",
            " ".repeat(indent),
            self.join_type
        ));
        self.left.print_inner(indent + 2, out);
        self.right.print_inner(indent + 2, out);
    }
}

pub struct PhysicalRelExprToOpIter<T: TxnStorageTrait> {
    pub storage: Arc<T>,
}

type ColIdToIdx = HashMap<ColumnId, usize>;

impl<T: TxnStorageTrait> PhysicalRelExprToOpIter<T> {
    pub fn new(storage: Arc<T>) -> Self {
        Self { storage }
    }

    pub fn to_executable(
        &mut self,
        catalog: CatalogRef,
        expr: PhysicalRelExpr,
    ) -> VolcanoIterator<T> {
        let (op, _col_id_to_idx) = self.to_executable_inner(catalog, expr).unwrap();
        op
    }

    fn to_executable_inner(
        &mut self,
        catalog: CatalogRef,
        expr: PhysicalRelExpr,
    ) -> Result<(VolcanoIterator<T>, ColIdToIdx), ExecError> {
        match expr {
            PhysicalRelExpr::Scan {
                db_id: _,
                c_id,
                table_name: _,
                column_indices,
            } => {
                let col_id_to_idx = column_indices
                    .iter()
                    .enumerate()
                    .map(|(idx, col_id)| (*col_id, idx))
                    .collect();
                let schema = catalog
                    .get_schema(c_id)
                    .ok_or(ExecError::Catalog(format!("Schema not found")))?;
                let schema = Arc::new(schema.project(&column_indices));
                let scan = ScanIter::new(schema, self.storage.clone(), c_id, column_indices);
                Ok((VolcanoIterator::Scan(scan), col_id_to_idx))
            }
            PhysicalRelExpr::Rename { src, src_to_dest } => {
                let (input_op, col_id_to_idx) = self.to_executable_inner(catalog, *src)?;
                let col_id_to_idx = src_to_dest
                    .iter()
                    .map(|(src, dest)| (*dest, col_id_to_idx[src]))
                    .collect();
                Ok((input_op, col_id_to_idx))
            }
            PhysicalRelExpr::Select { src, predicates } => {
                let (input_op, col_id_to_idx) = self.to_executable_inner(catalog, *src)?;
                let predicate = Expression::merge_conjunction(predicates);
                let expr: ByteCodeExpr = ByteCodeExpr::from_ast(predicate, &col_id_to_idx)?;
                let filter = FilterIter::new(input_op.schema(), Box::new(input_op), expr);
                Ok((VolcanoIterator::Filter(filter), col_id_to_idx))
            }
            PhysicalRelExpr::Project { src, column_names } => {
                let (input_op, col_id_to_idx) = self.to_executable_inner(catalog, *src)?;
                let (column_indices, new_col_id_to_idx) = {
                    let mut new_col_id_to_idx = HashMap::new();
                    let mut column_indices = Vec::new();
                    for (idx, col_id) in column_names.iter().enumerate() {
                        new_col_id_to_idx.insert(*col_id, idx);
                        column_indices.push(col_id_to_idx[col_id]);
                    }
                    (column_indices, new_col_id_to_idx)
                };
                let schema = input_op.schema().project(&column_indices);
                let project =
                    ProjectIter::new(Arc::new(schema), Box::new(input_op), column_indices);
                Ok((VolcanoIterator::Project(project), new_col_id_to_idx))
            }
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
            } => {
                let (input_op, col_id_to_idx) = self.to_executable_inner(catalog, *src)?;
                let group_by_indices = group_by
                    .iter()
                    .map(|col_id| col_id_to_idx[col_id])
                    .collect();
                let agg_op_indices = aggrs
                    .iter()
                    .map(|(_dest, (src, op))| (*op, col_id_to_idx[src]))
                    .collect();
                // Project the group by and aggregation columns
                let input_schema = input_op.schema();
                let mut schema = input_schema.project(&group_by_indices);
                for &(op, col) in &agg_op_indices {
                    match op {
                        AggOp::Count => {
                            schema.push_column(ColumnDef::new("dest", DataType::Int, false))
                        }
                        AggOp::Sum | AggOp::Max | AggOp::Min => {
                            let col_type = input_schema.get_column(col).data_type();
                            schema.push_column(ColumnDef::new("dest", col_type.clone(), true))
                        }
                        AggOp::Avg => {
                            // float
                            schema.push_column(ColumnDef::new("dest", DataType::Float, true))
                        }
                    }
                }
                let hash_agg = HashAggregateIter::new(
                    Arc::new(schema),
                    Box::new(input_op),
                    group_by_indices,
                    agg_op_indices,
                );
                let mut new_col_id_to_idx: HashMap<usize, usize> = group_by
                    .iter()
                    .enumerate()
                    .map(|(idx, col_id)| (*col_id, idx))
                    .collect();
                let group_by_len = group_by.len();
                for (idx, (dest, _)) in aggrs.iter().enumerate() {
                    new_col_id_to_idx.insert(*dest, group_by_len + idx);
                }
                Ok((VolcanoIterator::HashAggregate(hash_agg), new_col_id_to_idx))
            }
            PhysicalRelExpr::Map { input, exprs } => {
                let (input_op, mut col_id_to_idx) = self.to_executable_inner(catalog, *input)?;
                let mut bytecode_exprs = Vec::new();
                let input_schema = input_op.schema();
                let mut new_cols = Vec::new();
                for (dest, expr) in exprs {
                    let expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx)?;
                    bytecode_exprs.push(expr);
                    col_id_to_idx.insert(dest, col_id_to_idx.len());
                    new_cols.push(ColumnDef::new("dest", DataType::Unknown, true));
                }
                let schema = input_schema.merge(&Schema::new(new_cols, Vec::new()));
                let map = MapIter::new(Arc::new(schema), Box::new(input_op), bytecode_exprs);
                Ok((VolcanoIterator::Map(map), col_id_to_idx))
            }
            PhysicalRelExpr::Sort { src, column_names } => {
                let (input_op, col_id_to_idx) = self.to_executable_inner(catalog, *src)?;
                let sort_cols = column_names
                    .iter()
                    .map(|(col_id, asc, nulls_first)| (col_id_to_idx[col_id], *asc, *nulls_first))
                    .collect();
                let schema = input_op.schema();
                let sort = SortIter::new(schema, Box::new(input_op), sort_cols);
                Ok((VolcanoIterator::Sort(sort), col_id_to_idx))
            }
            PhysicalRelExpr::HashJoin {
                join_type,
                left,
                right,
                equalities,
                filter,
            } => {
                let (left_op, left_col_id_to_idx) =
                    self.to_executable_inner(catalog.clone(), *left)?;
                let left_len = left_col_id_to_idx.len();
                let (right_op, right_col_id_to_idx) = self.to_executable_inner(catalog, *right)?;
                let mut left_exprs = Vec::new();
                let mut right_exprs = Vec::new();
                for (left_expr, right_expr) in equalities {
                    left_exprs.push(ByteCodeExpr::from_ast(left_expr, &left_col_id_to_idx)?);
                    right_exprs.push(ByteCodeExpr::from_ast(right_expr, &right_col_id_to_idx)?);
                }
                let (iter, col_id_to_idx) = match join_type {
                    JoinType::Inner | JoinType::CrossJoin => {
                        let mut col_id_to_idx = left_col_id_to_idx;
                        for (col_name, col_idx) in right_col_id_to_idx {
                            let res = col_id_to_idx.insert(col_name, col_idx + left_len);
                            assert_eq!(res, None);
                        }
                        let filter = if filter.is_empty() {
                            None
                        } else {
                            Some(ByteCodeExpr::from_ast(
                                Expression::merge_conjunction(filter),
                                &col_id_to_idx,
                            )?)
                        };
                        let schema = left_op.schema().merge(&right_op.schema());
                        let hash_join = HashJoinIter::new(
                            Arc::new(schema),
                            join_type,
                            Box::new(left_op),
                            Box::new(right_op),
                            left_exprs,
                            right_exprs,
                            filter,
                        );
                        (VolcanoIterator::HashJoin(hash_join), col_id_to_idx)
                    }
                    JoinType::LeftOuter => {
                        let mut col_id_to_idx = left_col_id_to_idx;
                        for (col_name, col_idx) in right_col_id_to_idx {
                            let res = col_id_to_idx.insert(col_name, col_idx + left_len);
                            assert_eq!(res, None);
                        }
                        let filter = if filter.is_empty() {
                            None
                        } else {
                            Some(ByteCodeExpr::from_ast(
                                Expression::merge_conjunction(filter),
                                &col_id_to_idx,
                            )?)
                        };
                        // Make the right side columns nullable
                        let right_schema = right_op.schema().make_nullable();
                        let schema = left_op.schema().merge(&right_schema);
                        let hash_join = HashJoinIter::new(
                            Arc::new(schema),
                            join_type,
                            Box::new(left_op),
                            Box::new(right_op),
                            left_exprs,
                            right_exprs,
                            filter,
                        );
                        (VolcanoIterator::HashJoin(hash_join), col_id_to_idx)
                    }
                    JoinType::RightOuter => {
                        let mut col_id_to_idx = left_col_id_to_idx;
                        for (col_name, col_idx) in right_col_id_to_idx {
                            let res = col_id_to_idx.insert(col_name, col_idx + left_len);
                            assert_eq!(res, None);
                        }
                        let filter = if filter.is_empty() {
                            None
                        } else {
                            Some(ByteCodeExpr::from_ast(
                                Expression::merge_conjunction(filter),
                                &col_id_to_idx,
                            )?)
                        };
                        // Make the left side columns nullable
                        let left_schema = left_op.schema().make_nullable();
                        let schema = left_schema.merge(&right_op.schema());
                        let hash_join = HashJoinIter::new(
                            Arc::new(schema),
                            join_type,
                            Box::new(left_op),
                            Box::new(right_op),
                            left_exprs,
                            right_exprs,
                            filter,
                        );
                        (VolcanoIterator::HashJoin(hash_join), col_id_to_idx)
                    }
                    JoinType::LeftSemi | JoinType::LeftAnti => {
                        // LeftSemi/LeftAnti only returns the left side
                        let filter = if filter.is_empty() {
                            None
                        } else {
                            Some(ByteCodeExpr::from_ast(
                                Expression::merge_conjunction(filter),
                                &left_col_id_to_idx,
                            )?)
                        };
                        let hash_join = HashJoinIter::new(
                            left_op.schema(),
                            join_type,
                            Box::new(left_op),
                            Box::new(right_op),
                            left_exprs,
                            right_exprs,
                            filter,
                        );
                        (VolcanoIterator::HashJoin(hash_join), left_col_id_to_idx)
                    }
                    JoinType::RightSemi | JoinType::RightAnti => {
                        // RightSemi/RightAnti only returns the right side
                        let filter = if filter.is_empty() {
                            None
                        } else {
                            Some(ByteCodeExpr::from_ast(
                                Expression::merge_conjunction(filter),
                                &right_col_id_to_idx,
                            )?)
                        };
                        let hash_join = HashJoinIter::new(
                            right_op.schema(),
                            join_type,
                            Box::new(left_op),
                            Box::new(right_op),
                            left_exprs,
                            right_exprs,
                            filter,
                        );
                        (VolcanoIterator::HashJoin(hash_join), right_col_id_to_idx)
                    }
                    JoinType::FullOuter => {
                        // FullOuter is not supported yet
                        unimplemented!("FullOuter is not supported yet")
                    }
                };
                Ok((iter, col_id_to_idx))
            }
            PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left,
                right,
                predicates,
            } => {
                let (left_op, left_col_id_to_idx) =
                    self.to_executable_inner(catalog.clone(), *left)?;
                let left_len = left_col_id_to_idx.len();
                let (right_op, right_col_id_to_idx) = self.to_executable_inner(catalog, *right)?;
                let mut col_id_to_idx = left_col_id_to_idx;
                for (col_name, col_idx) in right_col_id_to_idx {
                    col_id_to_idx.insert(col_name, col_idx + left_len);
                }
                let filter = if predicates.is_empty() {
                    None
                } else {
                    Some(ByteCodeExpr::from_ast(
                        Expression::merge_conjunction(predicates),
                        &col_id_to_idx,
                    )?)
                };
                let schema = left_op.schema().merge(&right_op.schema());
                let cross_join = NestedLoopJoin::new(
                    Arc::new(schema),
                    join_type,
                    Box::new(left_op),
                    Box::new(right_op),
                    filter,
                );
                Ok((VolcanoIterator::NestedLoopJoin(cross_join), col_id_to_idx))
            }
            PhysicalRelExpr::FlatMap { .. } => {
                // FlatMap is not supported yet
                unimplemented!("FlatMap is not supported yet")
            }
        }
    }
}

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    u32,
};

use fbtree::{
    access_method::hash_fbt::HashFosterBtreeIter,
    bp::{ContainerKey, EvictionPolicy, MemPool},
    prelude::{HashFosterBtree, TreeStatus, TxnStorageTrait},
};

use crate::{
    catalog::{DataType, SchemaRef},
    error::ExecError,
    executor::bytecode_expr::ByteCodeExpr,
    expression::AggOp,
    tuple::{IsNull, Tuple},
    ColumnId, Field,
};

use super::{disk_buffer::OnDiskBuffer, MemoryPolicy, NonBlockingOp, PipelineID};

pub struct HashTable<E: EvictionPolicy, M: MemPool<E>> {
    hash_index: Arc<HashFosterBtree<E, M>>,
    exprs: Vec<ByteCodeExpr>,
    has_null: AtomicBool,
}

impl<E: EvictionPolicy, M: MemPool<E>> HashTable<E, M> {
    pub fn new(
        c_key: ContainerKey,
        mem_pool: Arc<M>,
        num_buckets: usize,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            hash_index: Arc::new(HashFosterBtree::new(c_key, mem_pool, num_buckets)),
            exprs,
            has_null: AtomicBool::new(false),
        }
    }

    pub fn num_kvs(&self) -> usize {
        self.hash_index.num_kvs()
    }

    pub fn has_null(&self) -> bool {
        self.has_null.load(Ordering::Acquire)
    }

    pub fn insert(&self, tuple: &Tuple) -> Result<(), ExecError> {
        // Generate key from tuple
        let key = self
            .exprs
            .iter()
            .map(|expr| expr.eval(tuple))
            .collect::<Result<Vec<_>, _>>()?;
        if key.iter().any(|f| f.is_null()) {
            self.has_null.store(true, Ordering::Release);
            return Ok(());
        }
        let key_bytes = Tuple::from_fields(key).to_bytes();

        // Insert tuple into hash table
        let tuple_bytes = tuple.to_bytes();
        self.hash_index.append(&key_bytes, &tuple_bytes)?;

        Ok(())
    }

    pub fn iter_key(&self, key: &[Field]) -> Option<HashFosterBtreeIter<E, M>> {
        let key_bytes = Tuple::from_fields(key.into()).to_bytes();
        // Search the index for the first key. If it does not exist, return None
        if self.hash_index.check_key(&key_bytes) {
            Some(self.hash_index.scan_with_prefix(&key_bytes))
        } else {
            None
        }
    }
}

pub struct OnDiskHashTableCreation<T: TxnStorageTrait, E: EvictionPolicy + 'static, M: MemPool<E>> {
    schema: SchemaRef,
    exec_plan: NonBlockingOp<T, E, M>,
    exprs: Vec<ByteCodeExpr>,
}

impl<T: TxnStorageTrait, E: EvictionPolicy + 'static, M: MemPool<E>>
    OnDiskHashTableCreation<T, E, M>
{
    pub fn new(
        schema: SchemaRef,
        exec_plan: NonBlockingOp<T, E, M>,
        exprs: Vec<ByteCodeExpr>,
    ) -> Self {
        Self {
            schema,
            exec_plan,
            exprs,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.exec_plan.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->hash_table_disk(", " ".repeat(indent)));
        let mut split = "";
        out.push('[');
        for expr in &self.exprs {
            out.push_str(split);
            out.push_str(&format!("{}", expr));
            split = ", ";
        }
        out.push_str("])\n");
        self.exec_plan.print_inner(indent + 2, out);
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, E, M>>>,
        policy: &Arc<MemoryPolicy>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Arc<OnDiskBuffer<T, E, M>>, ExecError> {
        let output = Arc::new(HashTable::new(
            dest_c_key,
            mem_pool.clone(),
            10,
            self.exprs.clone(),
        ));
        while let Some(tuple) = self.exec_plan.next(context)? {
            output.insert(&tuple)?;
        }
        Ok(Arc::new(OnDiskBuffer::HashTable(output)))
    }
}

pub struct HashAggregateTable<E: EvictionPolicy, M: MemPool<E>> {
    hash_index: Arc<HashFosterBtree<E, M>>,
    group_by: Vec<ColumnId>,
    agg_op: Vec<(AggOp, ColumnId)>,
    has_null: AtomicBool,
}

impl<E: EvictionPolicy + 'static, M: MemPool<E>> HashAggregateTable<E, M> {
    fn serialize_tuple(count: usize, t: &Tuple) -> Vec<u8> {
        let mut result = count.to_be_bytes().to_vec();
        let mut tuple_bytes = t.to_bytes();
        result.append(&mut tuple_bytes);
        result
    }

    fn deserialize_tuple(value: &[u8]) -> (usize, Tuple) {
        let count = usize::from_be_bytes(value[..8].try_into().unwrap());
        let tuple = Tuple::from_bytes(&value[8..]);
        (count, tuple)
    }

    fn merge_tuples(acc: &[u8], incoming: &[u8], agg_op: Vec<(AggOp, ColumnId)>) -> Vec<u8> {
        let (acc_count, mut acc) = HashAggregateTable::<E, M>::deserialize_tuple(acc);
        let (incoming_count, incoming) = HashAggregateTable::<E, M>::deserialize_tuple(incoming);
        assert!(incoming_count == 1);
        let count = acc_count + 1;
        for (idx, (op, _)) in agg_op.iter().enumerate() {
            let acc_field = acc.get_mut(idx);
            let incoming_field = incoming.get(idx);
            match op {
                AggOp::Sum => {
                    *acc_field = (acc_field.clone() + incoming_field.clone()).unwrap();
                }
                AggOp::Avg => {
                    *acc_field = (acc_field.clone() + incoming_field.clone()).unwrap();
                }
                AggOp::Max => {
                    *acc_field = (acc_field.clone()).max(incoming_field.clone());
                }
                AggOp::Min => {
                    *acc_field = (acc_field.clone()).min(incoming_field.clone());
                }
                AggOp::Count => {
                    if !incoming_field.is_null() {
                        *acc_field = (acc_field.clone() + Field::Int(Some(1))).unwrap();
                    }
                }
            }
        }
        Self::serialize_tuple(count, &acc)
    }

    pub fn new(
        c_key: ContainerKey,
        mem_pool: Arc<M>,
        num_buckets: usize,
        group_by: Vec<ColumnId>,
        agg_op: Vec<(AggOp, ColumnId)>,
    ) -> Self {
        Self {
            hash_index: Arc::new(HashFosterBtree::new(c_key, mem_pool, num_buckets)),
            group_by,
            agg_op,
            has_null: AtomicBool::new(false),
        }
    }

    pub fn num_kvs(&self) -> usize {
        self.hash_index.num_kvs()
    }

    pub fn has_null(&self) -> bool {
        self.has_null.load(Ordering::Acquire)
    }

    pub fn get(&self, key: &[Field]) -> Result<Vec<Tuple>, ExecError> {
        let key_bytes = Tuple::from_fields(key.into()).to_bytes();
        let value = self.hash_index.get(&key_bytes)?;
        Ok(vec![Self::deserialize_tuple(&value).1])
    }

    pub fn insert(&self, tuple: &Tuple) -> Result<(), ExecError> {
        let key = tuple.get_cols(&self.group_by);
        if key.iter().any(|f| f.is_null()) {
            self.has_null.store(true, Ordering::Release);
            return Ok(());
        }
        let key_bytes = Tuple::from_fields(key).to_bytes();

        let mut agg_vals = Vec::with_capacity(self.agg_op.len());
        for (op, col) in &self.agg_op {
            let val = tuple.get(*col);
            match op {
                AggOp::Sum | AggOp::Avg | AggOp::Max | AggOp::Min => {
                    agg_vals.push(val.clone());
                }
                AggOp::Count => {
                    if val.is_null() {
                        agg_vals.push(Field::Int(Some(0)));
                    } else {
                        agg_vals.push(Field::Int(Some(1)));
                    }
                }
            }
        }
        let tuple_bytes = Self::serialize_tuple(1, &Tuple::from_fields(agg_vals));
        self.hash_index
            .upsert_with_merge(&key_bytes, &tuple_bytes, |acc, incoming| {
                Self::merge_tuples(acc, incoming, self.agg_op.clone())
            })?;
        Ok(())
    }

    pub fn iter(&self) -> HashAggregationTableIter<E, M> {
        HashAggregationTableIter::new(
            self.hash_index.scan(),
            self.group_by.clone(),
            self.agg_op.clone(),
        )
    }
}

pub struct HashAggregationTableIter<E: EvictionPolicy + 'static, M: MemPool<E>> {
    iter: HashFosterBtreeIter<E, M>,
    group_by: Vec<ColumnId>,
    agg_op: Vec<(AggOp, ColumnId)>,
    has_output: bool,
}

impl<E: EvictionPolicy + 'static, M: MemPool<E>> HashAggregationTableIter<E, M> {
    pub fn new(
        iter: HashFosterBtreeIter<E, M>,
        group_by: Vec<ColumnId>,
        agg_op: Vec<(AggOp, ColumnId)>,
    ) -> Self {
        Self {
            iter,
            group_by,
            agg_op,
            has_output: false,
        }
    }

    pub fn next(&mut self) -> Result<Option<Tuple>, ExecError> {
        if let Some((key, value)) = self.iter.next() {
            self.has_output = true;
            let key_tuple = Tuple::from_bytes(&key);
            let (count, value_tuple) = HashAggregateTable::<E, M>::deserialize_tuple(&value);
            let mut result = key_tuple;
            for (idx, (op, _)) in self.agg_op.iter().enumerate() {
                let val = value_tuple.get(idx);
                match op {
                    AggOp::Avg => {
                        let count_field = Field::Float(Some(count as f64));
                        let avg = (val.clone() / count_field)?;
                        result.push(avg);
                    }
                    _ => {
                        result.push(val.clone());
                    }
                }
            }
            Ok(Some(result))
        } else if self.has_output {
            Ok(None)
        } else {
            // Return a single tuple of NULLs
            self.has_output = true;
            let mut fields = Vec::new();
            for _ in self.group_by.iter() {
                fields.push(Field::null(&DataType::Unknown));
            }
            for _ in self.agg_op.iter() {
                fields.push(Field::null(&DataType::Unknown));
            }
            Ok(Some(Tuple::from_fields(fields)))
        }
    }
}

pub struct OnDiskHashAggregation<T: TxnStorageTrait, E: EvictionPolicy + 'static, M: MemPool<E>> {
    schema: SchemaRef,
    exec_plan: NonBlockingOp<T, E, M>,
    group_by: Vec<ColumnId>,
    agg_op: Vec<(AggOp, ColumnId)>,
}

impl<T: TxnStorageTrait, E: EvictionPolicy + 'static, M: MemPool<E>>
    OnDiskHashAggregation<T, E, M>
{
    pub fn new(
        schema: SchemaRef,
        exec_plan: NonBlockingOp<T, E, M>,
        group_by: Vec<ColumnId>,
        agg_op: Vec<(AggOp, ColumnId)>,
    ) -> Self {
        Self {
            schema,
            exec_plan,
            group_by,
            agg_op,
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn deps(&self) -> HashSet<PipelineID> {
        self.exec_plan.deps()
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        out.push_str(&format!("{}->hash_aggregate_disk(", " ".repeat(indent)));
        let mut split = "";
        out.push('[');
        for col_id in &self.group_by {
            out.push_str(split);
            out.push_str(&format!("{}", col_id));
            split = ", ";
        }
        out.push_str("], [");
        split = "";
        for (agg_op, col_id) in &self.agg_op {
            out.push_str(split);
            out.push_str(&format!("{:?}({})", agg_op, col_id));
            split = ", ";
        }
        out.push_str("])\n");
        self.exec_plan.print_inner(indent + 2, out);
    }

    pub fn execute(
        &mut self,
        context: &HashMap<PipelineID, Arc<OnDiskBuffer<T, E, M>>>,
        policy: &Arc<MemoryPolicy>,
        mem_pool: &Arc<M>,
        dest_c_key: ContainerKey,
    ) -> Result<Arc<OnDiskBuffer<T, E, M>>, ExecError> {
        let output = Arc::new(HashAggregateTable::new(
            dest_c_key,
            mem_pool.clone(),
            100,
            self.group_by.clone(),
            self.agg_op.clone(),
        ));
        while let Some(tuple) = self.exec_plan.next(context)? {
            output.insert(&tuple)?;
        }
        Ok(Arc::new(OnDiskBuffer::HashAggregateTable(output)))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use fbtree::bp::{get_test_bp, ContainerKey};

    use crate::{executor::bytecode_expr::colidx_expr, expression::AggOp, tuple::Tuple};

    use super::HashTable;

    #[test]
    fn test_hash_table() {
        let bp = get_test_bp(10);
        let c_key = ContainerKey::new(1, 1);
        let num_buckets = 3;

        let values = vec![
            Tuple::from_fields(vec![1.into(), 10.into()]),
            Tuple::from_fields(vec![1.into(), 11.into()]),
            Tuple::from_fields(vec![2.into(), 12.into()]),
            Tuple::from_fields(vec![2.into(), 13.into()]),
            Tuple::from_fields(vec![3.into(), 14.into()]),
            Tuple::from_fields(vec![3.into(), 15.into()]),
        ];

        let expr = vec![colidx_expr(0)];

        let hash_table = Arc::new(HashTable::new(c_key, bp, num_buckets, expr));
        for value in &values {
            hash_table.insert(value).unwrap();
        }

        let fields = vec![1.into()];
        let mut iter = hash_table.iter_key(&fields).unwrap();
        let mut result = Vec::new();
        while let Some((_, value)) = iter.next() {
            result.push(Tuple::from_bytes(&value));
        }
        println!("{:?}", result);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Tuple::from_fields(vec![1.into(), 10.into()])));
        assert!(result.contains(&Tuple::from_fields(vec![1.into(), 11.into()])));

        let fields = vec![2.into()];
        let mut iter = hash_table.iter_key(&fields).unwrap();
        let mut result = Vec::new();
        while let Some((_, value)) = iter.next() {
            result.push(Tuple::from_bytes(&value));
        }
        println!("{:?}", result);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Tuple::from_fields(vec![2.into(), 12.into()])));
        assert!(result.contains(&Tuple::from_fields(vec![2.into(), 13.into()])));

        let fields = vec![3.into()];
        let mut iter = hash_table.iter_key(&fields).unwrap();
        let mut result = Vec::new();
        while let Some((_, value)) = iter.next() {
            result.push(Tuple::from_bytes(&value));
        }
        println!("{:?}", result);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Tuple::from_fields(vec![3.into(), 14.into()])));
        assert!(result.contains(&Tuple::from_fields(vec![3.into(), 15.into()])));

        /*
        let mut scanner = hash_table.iter();
        while let Some(tuple) = scanner.next().unwrap() {
            assert!(values.contains(&tuple));
        }
        */
    }

    #[test]
    fn test_hash_aggregate_table() {
        let bp = get_test_bp(10);
        let c_key = ContainerKey::new(1, 1);
        let num_buckets = 3;

        let values = vec![
            Tuple::from_fields(vec![1.into(), 10.into(), 1.into()]),
            Tuple::from_fields(vec![1.into(), 11.into(), 10.into()]),
            Tuple::from_fields(vec![2.into(), 12.into(), 1.into()]),
            Tuple::from_fields(vec![2.into(), 13.into(), 10.into()]),
            Tuple::from_fields(vec![3.into(), 14.into(), 1.into()]),
            Tuple::from_fields(vec![3.into(), 15.into(), 10.into()]),
        ];

        let group_by = vec![0];
        let agg_op = vec![
            (AggOp::Sum, 1),
            (AggOp::Max, 2),
            (AggOp::Min, 2),
            (AggOp::Count, 2),
            (AggOp::Avg, 2),
        ];

        let hash_table = Arc::new(super::HashAggregateTable::new(
            c_key,
            bp,
            num_buckets,
            group_by,
            agg_op,
        ));
        for value in &values {
            hash_table.insert(value).unwrap();
        }

        let mut scanner = hash_table.iter();
        let mut result = Vec::new();
        while let Some(tuple) = scanner.next().unwrap() {
            result.push(tuple);
        }
        println!("{:?}", result);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&Tuple::from_fields(vec![
            1.into(),
            21.into(),
            10.into(),
            1.into(),
            2.into(),
            5.5.into()
        ])));
        assert!(result.contains(&Tuple::from_fields(vec![
            2.into(),
            25.into(),
            10.into(),
            1.into(),
            2.into(),
            5.5.into()
        ])));
        assert!(result.contains(&Tuple::from_fields(vec![
            3.into(),
            29.into(),
            10.into(),
            1.into(),
            2.into(),
            5.5.into()
        ])));
    }
}

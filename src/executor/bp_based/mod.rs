use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use super::{bytecode_expr::ByteCodeExpr, ResultBufferTrait};

use crate::{error::ExecError, log, log_info, ColumnId};

pub struct Pipeline {
    scanner: Scanner,
    output: Arc<dyn ResultBufferTrait>,
}

pub enum Scanner {
    TableScan(Arc<dyn ResultBufferTrait>),
    MergeScan(Vec<(Arc<dyn ResultBufferTrait>, Vec<(ColumnId, bool, bool)>)>), // (scanner, Vec((col_id, asc, nulls_first)))
    HashJoinScan(
        Arc<dyn ResultBufferTrait>, // Build side. The ResultBufferTrait is a hash table.
        Arc<dyn ResultBufferTrait>, // Probe side.
        Vec<ByteCodeExpr>,
    ),
}

impl Pipeline {
    pub fn new(scanner: Scanner, output: Arc<dyn ResultBufferTrait>) -> Self {
        Self { scanner, output }
    }

    pub fn execute(&mut self) -> Result<Arc<dyn ResultBufferTrait>, ExecError> {
        match &self.scanner {
            Scanner::TableScan(scanner) => {
                let iter = scanner.to_iter();
                while let Some(tuple) = iter.next() {
                    self.output.insert(tuple);
                }
            }
            Scanner::MergeScan(scanners) => {
                let mut heap = BinaryHeap::new();
                let iters = scanners
                    .iter()
                    .enumerate()
                    .map(|(i, (scanner, sort_cols))| {
                        let iter = scanner.to_iter();
                        if let Some(tuple) = iter.next() {
                            heap.push(Reverse((
                                tuple.to_normalized_key_bytes(sort_cols),
                                (i, tuple),
                            )));
                        }
                        (iter, sort_cols)
                    })
                    .collect::<Vec<_>>();
                while let Some(Reverse((_, (i, tuple)))) = heap.pop() {
                    self.output.insert(tuple);
                    let (iter, sort_cols) = &iters[i];
                    if let Some(tuple) = iter.next() {
                        heap.push(Reverse((
                            tuple.to_normalized_key_bytes(sort_cols),
                            (i, tuple),
                        )));
                    }
                }
            }
            Scanner::HashJoinScan(build_side, probe_side, join_exprs) => {
                let probe_iter = probe_side.to_iter();
                while let Some(tuple) = probe_iter.next() {
                    let key = join_exprs
                        .iter()
                        .map(|expr| expr.eval(&tuple))
                        .collect::<Result<Vec<_>, _>>()?;
                    let matching_tuples = build_side.get(key);
                    for matching_tuple in matching_tuples {
                        let new_tuple = matching_tuple.merge_mut(&tuple);
                        self.output.insert(new_tuple);
                    }
                }
            }
        }
        Ok(self.output.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        executor::{bytecode_expr::colidx_expr, ResultIterator},
        tuple::Tuple,
        Field,
    };

    use super::*;
    use std::{
        cell::UnsafeCell,
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, RwLock, RwLockReadGuard,
        },
    };

    pub struct TupleResults {
        latch: RwLock<()>,
        tuples: UnsafeCell<Vec<Tuple>>,
    }

    impl TupleResults {
        pub fn new() -> Self {
            Self {
                latch: RwLock::new(()),
                tuples: UnsafeCell::new(Vec::new()),
            }
        }
    }

    impl ResultBufferTrait for TupleResults {
        fn insert(&self, tuple: Tuple) {
            let _guard = self.latch.write().unwrap();
            unsafe {
                (*self.tuples.get()).push(tuple);
            }
        }

        fn get(&self, key: Vec<crate::Field>) -> Vec<Tuple> {
            unimplemented!("get not implemented for TupleResults")
        }

        fn to_iter<'a>(&'a self) -> Box<dyn ResultIterator<'a> + 'a> {
            let _guard = self.latch.read().unwrap();
            Box::new(TupleResultsIter {
                _buffer_guard: self.latch.read().unwrap(),
                tuples: unsafe { &*self.tuples.get() },
                current: AtomicUsize::new(0),
            })
        }
    }

    pub struct TupleResultsIter<'a> {
        _buffer_guard: RwLockReadGuard<'a, ()>,
        tuples: &'a Vec<Tuple>,
        current: AtomicUsize,
    }

    impl<'a> ResultIterator<'a> for TupleResultsIter<'a> {
        fn next(&self) -> Option<Tuple> {
            let current = self.current.fetch_add(1, Ordering::AcqRel);
            if current < self.tuples.len() {
                Some(self.tuples[current].copy())
            } else {
                None
            }
        }
    }

    pub struct HashTable {
        latch: RwLock<()>,
        exprs: Vec<ByteCodeExpr>,
        table: UnsafeCell<HashMap<Vec<Field>, Vec<Tuple>>>,
    }

    impl HashTable {
        pub fn new(exprs: Vec<ByteCodeExpr>) -> Self {
            Self {
                latch: RwLock::new(()),
                exprs,
                table: UnsafeCell::new(HashMap::new()),
            }
        }
    }

    impl ResultBufferTrait for HashTable {
        fn insert(&self, tuple: Tuple) {
            let _guard = self.latch.write().unwrap();
            let key = self
                .exprs
                .iter()
                .map(|expr| expr.eval(&tuple))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let table = unsafe { &mut *self.table.get() };
            match table.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().push(tuple);
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(vec![tuple]);
                }
            }
        }

        fn get(&self, key: Vec<crate::Field>) -> Vec<Tuple> {
            let _guard = self.latch.read().unwrap();
            let table = unsafe { &*self.table.get() };
            table
                .get(&key)
                .map(|v| v.iter().map(|t| t.copy()).collect())
                .unwrap_or_default()
        }

        fn to_iter<'a>(&'a self) -> Box<dyn ResultIterator<'a> + 'a> {
            unimplemented!("to_iter not implemented for HashTable")
        }
    }

    fn check_result(
        result: Arc<dyn ResultBufferTrait>,
        expected: &mut [Tuple],
        sorted: bool,
        verbose: bool,
    ) {
        let mut vec = Vec::new();
        let result = result.to_iter();
        while let Some(t) = result.next() {
            vec.push(t);
        }
        let mut result = vec;
        if sorted {
            result.sort();
            expected.sort();
        }
        let result_string = result
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
            println!("--- Result ---\n{}", result_string);
            println!("--- Expected ---\n{}", expected_string);
        }
        assert_eq!(
            result, expected,
            "\n--- Result ---\n{}\n--- Expected ---\n{}\n",
            result_string, expected_string
        );
    }

    #[test]
    fn test_scan_pipeline() {
        let input = Arc::new(TupleResults::new());
        let mut expected = Vec::new();
        expected.push(Tuple::from_fields(vec![1.into(), "hello".into()]));
        expected.push(Tuple::from_fields(vec![2.into(), "world".into()]));

        for tuple in &expected {
            input.insert(tuple.copy());
        }
        let scanner = Scanner::TableScan(input.clone());
        let output = Arc::new(TupleResults::new());
        let mut pipeline = Pipeline::new(scanner, output);
        let output = pipeline.execute().unwrap();

        check_result(output, &mut expected, false, false);
    }

    #[test]
    fn test_sort_merge_pipeline_2_inputs() {
        let input1 = Arc::new(TupleResults::new());
        let mut expected1 = Vec::new();
        expected1.push(Tuple::from_fields(vec![1.into(), "hello".into()]));
        expected1.push(Tuple::from_fields(vec![3.into(), "world".into()]));
        expected1.push(Tuple::from_fields(vec![5.into(), "foo".into()]));

        for tuple in &expected1 {
            input1.insert(tuple.copy());
        }

        let input2 = Arc::new(TupleResults::new());
        let mut expected2 = Vec::new();
        expected2.push(Tuple::from_fields(vec![2.into(), "world".into()]));
        expected2.push(Tuple::from_fields(vec![4.into(), "hello".into()]));
        expected2.push(Tuple::from_fields(vec![6.into(), "bar".into()]));

        for tuple in &expected2 {
            input2.insert(tuple.copy());
        }

        let scanner = Scanner::MergeScan(vec![
            (input1.clone(), vec![(0, true, false)]),
            (input2.clone(), vec![(0, true, false)]),
        ]);
        let output = Arc::new(TupleResults::new());
        let mut pipeline = Pipeline::new(scanner, output);
        let output = pipeline.execute().unwrap();

        let mut expected = expected1;
        expected.extend(expected2);
        expected.sort();
        check_result(output, &mut expected, false, false);
    }

    #[test]
    fn test_sort_merge_pipeline_3_inputs() {
        let input1 = Arc::new(TupleResults::new());
        let mut expected1 = Vec::new();
        expected1.push(Tuple::from_fields(vec![1.into(), "hello".into()]));
        expected1.push(Tuple::from_fields(vec![3.into(), "bar".into()]));
        expected1.push(Tuple::from_fields(vec![3.into(), "world".into()]));

        for tuple in &expected1 {
            input1.insert(tuple.copy());
        }

        let input2 = Arc::new(TupleResults::new());
        let mut expected2 = Vec::new();
        expected2.push(Tuple::from_fields(vec![2.into(), "world".into()]));
        expected2.push(Tuple::from_fields(vec![3.into(), "bar".into()]));
        expected2.push(Tuple::from_fields(vec![3.into(), "foo".into()]));

        for tuple in &expected2 {
            input2.insert(tuple.copy());
        }

        let input3 = Arc::new(TupleResults::new());
        let mut expected3 = Vec::new();
        expected3.push(Tuple::from_fields(vec![7.into(), "baz".into()]));
        expected3.push(Tuple::from_fields(vec![8.into(), "qux".into()]));
        expected3.push(Tuple::from_fields(vec![9.into(), "quux".into()]));

        for tuple in &expected3 {
            input3.insert(tuple.copy());
        }

        let scanner = Scanner::MergeScan(vec![
            (input1.clone(), vec![(0, true, false), (1, true, false)]),
            (input2.clone(), vec![(0, true, false), (1, true, false)]),
            (input3.clone(), vec![(0, true, false), (1, true, false)]),
        ]);
        let output = Arc::new(TupleResults::new());
        let mut pipeline = Pipeline::new(scanner, output);
        let output = pipeline.execute().unwrap();

        let mut expected = expected1;
        expected.extend(expected2);
        expected.extend(expected3);
        expected.sort();
        check_result(output, &mut expected, false, false);
    }

    #[test]
    fn test_hash_join_pipeline() {
        let build_input = Arc::new(HashTable::new(vec![colidx_expr(0)]));
        let mut build_tuples = Vec::new();
        build_tuples.push(Tuple::from_fields(vec![1.into(), "hello".into()]));
        build_tuples.push(Tuple::from_fields(vec![2.into(), "world".into()]));
        build_tuples.push(Tuple::from_fields(vec![2.into(), "world".into()])); // Duplicate
        build_tuples.push(Tuple::from_fields(vec![3.into(), "foo".into()]));
        build_tuples.push(Tuple::from_fields(vec![3.into(), "bar".into()]));
        for tuple in &build_tuples {
            build_input.insert(tuple.copy());
        }

        let probe_input = Arc::new(TupleResults::new());
        let mut probe_tuples = Vec::new();
        probe_tuples.push(Tuple::from_fields(vec![1.into(), "hello".into()]));
        probe_tuples.push(Tuple::from_fields(vec![2.into(), "world".into()]));
        probe_tuples.push(Tuple::from_fields(vec![3.into(), "foo".into()]));
        probe_tuples.push(Tuple::from_fields(vec![3.into(), "bar".into()]));
        probe_tuples.push(Tuple::from_fields(vec![4.into(), "bar".into()]));
        for tuple in &probe_tuples {
            probe_input.insert(tuple.copy());
        }

        let scanner = Scanner::HashJoinScan(
            build_input.clone(),
            probe_input.clone(),
            vec![colidx_expr(0)],
        );
        let output = Arc::new(TupleResults::new());
        let mut pipeline = Pipeline::new(scanner, output);
        let output = pipeline.execute().unwrap();

        let mut expected = Vec::new();
        expected.push(Tuple::from_fields(vec![
            1.into(),
            "hello".into(),
            1.into(),
            "hello".into(),
        ]));
        expected.push(Tuple::from_fields(vec![
            2.into(),
            "world".into(),
            2.into(),
            "world".into(),
        ]));
        expected.push(Tuple::from_fields(vec![
            2.into(),
            "world".into(),
            2.into(),
            "world".into(),
        ]));
        expected.push(Tuple::from_fields(vec![
            3.into(),
            "foo".into(),
            3.into(),
            "foo".into(),
        ]));
        expected.push(Tuple::from_fields(vec![
            3.into(),
            "foo".into(),
            3.into(),
            "bar".into(),
        ]));
        expected.push(Tuple::from_fields(vec![
            3.into(),
            "bar".into(),
            3.into(),
            "foo".into(),
        ]));
        expected.push(Tuple::from_fields(vec![
            3.into(),
            "bar".into(),
            3.into(),
            "bar".into(),
        ]));

        check_result(output, &mut expected, true, false);
    }
}

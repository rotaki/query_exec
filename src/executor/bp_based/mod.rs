use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use super::ResultBufferTrait;

use crate::{log, log_info, ColumnId};

pub struct Pipeline {
    scanner: Scanner,
    output: Arc<dyn ResultBufferTrait>,
}

pub enum Scanner {
    TableScan(Arc<dyn ResultBufferTrait>),
    MergeScan(Vec<(Arc<dyn ResultBufferTrait>, Vec<(ColumnId, bool, bool)>)>), // (scanner, Vec((col_id, asc, nulls_first)))
}

impl Pipeline {
    pub fn new(scanner: Scanner, output: Arc<dyn ResultBufferTrait>) -> Self {
        Self { scanner, output }
    }

    pub fn execute(&mut self) {
        match &self.scanner {
            Scanner::TableScan(scanner) => {
                let iter = scanner.to_iter();
                while let Some(tuple) = iter.next() {
                    self.output.push(tuple);
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
                    self.output.push(tuple);
                    let (iter, sort_cols) = &iters[i];
                    if let Some(tuple) = iter.next() {
                        heap.push(Reverse((
                            tuple.to_normalized_key_bytes(sort_cols),
                            (i, tuple),
                        )));
                    }
                }
            }
        }
    }

    pub fn get_output(&self) -> Arc<dyn ResultBufferTrait> {
        self.output.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{executor::ResultIterator, tuple::Tuple};

    use super::*;
    use std::{
        cell::UnsafeCell,
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
        fn push(&self, tuple: Tuple) {
            let _guard = self.latch.write().unwrap();
            unsafe {
                (*self.tuples.get()).push(tuple);
            }
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
            input.push(tuple.copy());
        }
        let scanner = Scanner::TableScan(input.clone());
        let output = Arc::new(TupleResults::new());
        let mut pipeline = Pipeline::new(scanner, output);
        pipeline.execute();

        let output = pipeline.get_output();
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
            input1.push(tuple.copy());
        }

        let input2 = Arc::new(TupleResults::new());
        let mut expected2 = Vec::new();
        expected2.push(Tuple::from_fields(vec![2.into(), "world".into()]));
        expected2.push(Tuple::from_fields(vec![4.into(), "hello".into()]));
        expected2.push(Tuple::from_fields(vec![6.into(), "bar".into()]));

        for tuple in &expected2 {
            input2.push(tuple.copy());
        }

        let scanner = Scanner::MergeScan(vec![
            (input1.clone(), vec![(0, true, false)]),
            (input2.clone(), vec![(0, true, false)]),
        ]);
        let output = Arc::new(TupleResults::new());
        let mut pipeline = Pipeline::new(scanner, output);
        pipeline.execute();

        let output: Arc<dyn ResultBufferTrait> = pipeline.get_output();
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
            input1.push(tuple.copy());
        }

        let input2 = Arc::new(TupleResults::new());
        let mut expected2 = Vec::new();
        expected2.push(Tuple::from_fields(vec![2.into(), "world".into()]));
        expected2.push(Tuple::from_fields(vec![3.into(), "bar".into()]));
        expected2.push(Tuple::from_fields(vec![3.into(), "foo".into()]));

        for tuple in &expected2 {
            input2.push(tuple.copy());
        }

        let input3 = Arc::new(TupleResults::new());
        let mut expected3 = Vec::new();
        expected3.push(Tuple::from_fields(vec![7.into(), "baz".into()]));
        expected3.push(Tuple::from_fields(vec![8.into(), "qux".into()]));
        expected3.push(Tuple::from_fields(vec![9.into(), "quux".into()]));

        for tuple in &expected3 {
            input3.push(tuple.copy());
        }

        let scanner = Scanner::MergeScan(vec![
            (input1.clone(), vec![(0, true, false), (1, true, false)]),
            (input2.clone(), vec![(0, true, false), (1, true, false)]),
            (input3.clone(), vec![(0, true, false), (1, true, false)]),
        ]);
        let output = Arc::new(TupleResults::new());
        let mut pipeline = Pipeline::new(scanner, output);
        pipeline.execute();

        let output: Arc<dyn ResultBufferTrait> = pipeline.get_output();
        let mut expected = expected1;
        expected.extend(expected2);
        expected.extend(expected3);
        expected.sort();
        check_result(output, &mut expected, false, false);
    }
}

use std::sync::Arc;

use super::ResultBufferTrait;

use crate::{log, log_info};

pub struct Pipeline {
    scanner: Scanner,
    output: Arc<dyn ResultBufferTrait>,
}

pub enum Scanner {
    TableScan(Arc<dyn ResultBufferTrait>),
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
    fn test_pipeline() {
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
}

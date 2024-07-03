use std::{
    cell::UnsafeCell,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
};

use crate::{error::ExecError, tuple::{FromBool, Tuple}, ColumnId, Field};

use super::{bytecode_expr::ByteCodeExpr, ResultBufferTrait};

pub enum TupleBuffer {
    TupleVec(Arc<RwLock<()>>, UnsafeCell<Vec<Tuple>>),
    Runs(
        Arc<RwLock<()>>,
        UnsafeCell<Vec<(Arc<TupleBuffer>, Vec<(ColumnId, bool, bool)>)>>,
    ), // column_id, ascending, nulls_first
}

impl TupleBuffer {
    pub fn vec() -> Self {
        TupleBuffer::TupleVec(Arc::new(RwLock::new(())), UnsafeCell::new(Vec::new()))
    }

    pub fn runs(runs: Vec<(Arc<TupleBuffer>, Vec<(ColumnId, bool, bool)>)>) -> Self {
        TupleBuffer::Runs(Arc::new(RwLock::new(())), UnsafeCell::new(runs))
    }

    pub fn push(&self, tuple: Tuple) {
        match self {
            TupleBuffer::TupleVec(lock, vec) => {
                let _guard = lock.write().unwrap();
                unsafe { &mut *vec.get() }.push(tuple);
            }
            TupleBuffer::Runs(_, _) => {
                panic!("TupleBuffer::push() is not supported for TupleBuffer::Runs")
            }
        }
    }

    pub fn iter_all(&self) -> TupleBufferIter {
        match self {
            TupleBuffer::TupleVec(lock, vec) => {
                let _guard = lock.read().unwrap();
                TupleBufferIter::vec(_guard, unsafe { &*vec.get() }.iter())
            }
            TupleBuffer::Runs(lock, runs) => {
                let _guard = lock.read().unwrap();
                let runs = unsafe { &*runs.get() };
                let runs = runs
                    .iter()
                    .map(|(buf, order)| (buf.iter_all(), order.clone()))
                    .collect();
                TupleBufferIter::merge(_guard, runs)
            }
        }
    }
}

pub enum TupleBufferIter<'a> {
    TupleVec(RwLockReadGuard<'a, ()>, Mutex<std::slice::Iter<'a, Tuple>>),
    MergeScan(
        RwLockReadGuard<'a, ()>, // Guard to ensure that the runs are not modified during the merge
        Mutex<BinaryHeap<Reverse<(Vec<u8>, usize, Tuple)>>>,
        Vec<(TupleBufferIter<'a>, Vec<(ColumnId, bool, bool)>)>,
    ),
}

impl<'a> TupleBufferIter<'a> {
    pub fn vec(guard: RwLockReadGuard<'a, ()>, iter: std::slice::Iter<'a, Tuple>) -> Self {
        TupleBufferIter::TupleVec(guard, Mutex::new(iter))
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
        }
    }
}

pub enum PipelineIterator<'a> {
    Scan(PScanIter<'a>),
    Filter(PFilterIter<'a>),
    Project(PProjectIter<'a>),
    Map(PMapIter<'a>),
}

impl<'a> PipelineIterator<'a> {
    pub fn next(
        &mut self,
        context: &HashMap<PipelineID, Arc<TupleBuffer>>,
    ) -> Result<Option<Tuple>, ExecError> {
        let context = unsafe { std::mem::transmute(context) };
        match self {
            PipelineIterator::Scan(iter) => {
                iter.next(context)
            }
            PipelineIterator::Filter(iter) => {
                iter.next(context)
            }
            PipelineIterator::Project(iter) => {
                iter.next(context)
            }
            PipelineIterator::Map(iter) => {
                iter.next(context)
            }
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
    input: Box<PipelineIterator<'a>>,
    expr: ByteCodeExpr,
}

impl<'a> PFilterIter<'a> {
    pub fn new(input: Box<PipelineIterator<'a>>, expr: ByteCodeExpr) -> Self {
        Self { input, expr }
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
    input: Box<PipelineIterator<'a>>,
    column_indices: Vec<ColumnId>,
}

impl<'a> PProjectIter<'a> {
    pub fn new(input: Box<PipelineIterator<'a>>, column_indices: Vec<ColumnId>) -> Self {
        Self {
            input,
            column_indices,
        }
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
    input: Box<PipelineIterator<'a>>,
    exprs: Vec<ByteCodeExpr>,
}

impl<'a> PMapIter<'a> {
    pub fn new(input: Box<PipelineIterator<'a>>, exprs: Vec<ByteCodeExpr>) -> Self {
        Self { input, exprs }
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

pub type PipelineID = u16;

pub struct Pipeline<'a> {
    id: PipelineID,
    context: HashMap<PipelineID, Arc<TupleBuffer>>,
    execution_plan: PipelineIterator<'a>,
    output: Arc<TupleBuffer>,
}

impl<'a> Pipeline<'a> {
    pub fn new(
        id: PipelineID,
        execution_plan: PipelineIterator<'a>,
        output: Arc<TupleBuffer>,
    ) -> Self {
        Self {
            id,
            context: HashMap::new(),
            execution_plan,
            output,
        }
    }

    /// Set context
    pub fn set_context(&mut self, id: PipelineID, buffer: Arc<TupleBuffer>) {
        self.context.insert(id, buffer);
    }

    pub fn execute(&mut self) -> Result<Arc<TupleBuffer>, ExecError> {
        while let Some(tuple) = self.execution_plan.next(&self.context)? {
            self.output.push(tuple);
        }
        Ok(self.output.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_result(actual: &mut Vec<Tuple>, expected: &mut Vec<Tuple>, sorted: bool, verbose: bool) {
        if sorted {
            actual.sort();
            expected.sort();
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
        input.push(tuple.copy());

        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(1, PipelineIterator::Scan(PScanIter::new(0)), output);
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
            input1.push(tuple.copy());
        }

        let input2 = Arc::new(TupleBuffer::vec());
        let mut expected2 = vec![
            Tuple::from_fields(vec![2.into(), "d".into()]),
            Tuple::from_fields(vec![4.into(), "e".into()]),
            Tuple::from_fields(vec![6.into(), "f".into()]),
        ];
        for tuple in expected2.iter() {
            input2.push(tuple.copy());
        }

        // Merge the two inputs.
        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(1, PipelineIterator::Scan(PScanIter::new(0)), output);

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
            input1.push(tuple.copy());
        }

        let input2 = Arc::new(TupleBuffer::vec());
        let mut expected2 = vec![
            Tuple::from_fields(vec![2.into(), "b".into()]),
            Tuple::from_fields(vec![5.into(), "e".into()]),
            Tuple::from_fields(vec![8.into(), "h".into()]),
        ];
        for tuple in expected2.iter() {
            input2.push(tuple.copy());
        }

        let input3 = Arc::new(TupleBuffer::vec());
        let mut expected3 = vec![
            Tuple::from_fields(vec![3.into(), "c".into()]),
            Tuple::from_fields(vec![6.into(), "f".into()]),
            Tuple::from_fields(vec![9.into(), "i".into()]),
        ];
        for tuple in expected3.iter() {
            input3.push(tuple.copy());
        }

        // Merge the three inputs.
        let output = Arc::new(TupleBuffer::vec());
        let mut pipeline = Pipeline::new(1, PipelineIterator::Scan(PScanIter::new(0)), output);

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
}

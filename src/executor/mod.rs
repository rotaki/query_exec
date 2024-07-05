use std::{
    cell::UnsafeCell,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock, RwLockReadGuard,
    },
};

use bytecode_expr::ByteCodeExpr;
use txn_storage::TxnStorageTrait;

use crate::{
    catalog::CatalogRef, error::ExecError, expression::prelude::PhysicalRelExpr, tuple::Tuple,
    Field,
};

mod bp_based;
mod bytecode_expr;
mod volcano;

pub mod prelude {
    pub use super::volcano::VolcanoIterator;
    pub use super::Executor;
}

// 'a is the lifetime of the iterator
// This ensures that the iterator lives as long as the executor
pub trait Executor<T: TxnStorageTrait> {
    type Buffer: ResultBufferTrait;

    fn new(catalog: CatalogRef, storage: Arc<T>, physical_plan: PhysicalRelExpr) -> Self;
    fn to_pretty_string(&self) -> String;
    fn execute(&mut self, txn: &T::TxnHandle) -> Result<Self::Buffer, ExecError>;
}

pub trait ResultBufferTrait {
    fn insert(&self, tuple: Tuple);
    fn get(&self, key: Vec<Field>) -> Vec<Tuple>;
    fn iter_key<'a>(&'a self, key: Vec<Field>) -> Box<dyn ResultIterator + 'a>;
    fn iter_all<'a>(&'a self) -> Box<dyn ResultIterator + 'a>;
}

pub trait ResultIterator {
    fn next(&self) -> Option<Tuple>;
}

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

    fn iter_key<'a>(&'a self, key: Vec<Field>) -> Box<dyn ResultIterator + 'a> {
        unimplemented!("to_iter not implemented for TupleResults")
    }

    fn iter_all<'a>(&'a self) -> Box<dyn ResultIterator + 'a> {
        let _guard = self.latch.read().unwrap();
        Box::new(TupleResultsIter {
            _buffer_guard: self.latch.read().unwrap(),
            tuples: Some(unsafe { &*self.tuples.get() }),
            current: AtomicUsize::new(0),
        })
    }
}

pub struct TupleResultsIter<'a> {
    _buffer_guard: RwLockReadGuard<'a, ()>,
    tuples: Option<&'a Vec<Tuple>>,
    current: AtomicUsize,
}

impl<'a> ResultIterator for TupleResultsIter<'a> {
    fn next(&self) -> Option<Tuple> {
        if let Some(tuples) = self.tuples {
            let current = self.current.fetch_add(1, Ordering::AcqRel);
            if current < tuples.len() {
                Some(tuples[current].copy())
            } else {
                None
            }
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

    fn iter_key<'a>(&'a self, key: Vec<Field>) -> Box<dyn ResultIterator + 'a> {
        let _guard = self.latch.read().unwrap();
        let table: &HashMap<Vec<Field>, Vec<Tuple>> = unsafe { &*self.table.get() };
        if let Some(tuples) = table.get(&key) {
            return Box::new(TupleResultsIter {
                _buffer_guard: _guard,
                tuples: Some(tuples),
                current: AtomicUsize::new(0),
            });
        } else {
            return Box::new(TupleResultsIter {
                _buffer_guard: _guard,
                tuples: None,
                current: AtomicUsize::new(0),
            });
        }
    }

    fn iter_all<'a>(&'a self) -> Box<dyn ResultIterator + 'a> {
        unimplemented!("to_iter not implemented for HashTable")
    }
}

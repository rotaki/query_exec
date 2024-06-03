mod col_id_gen;
mod schema;

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::{Arc, Mutex},
};

pub use col_id_gen::{ColIdGen, ColIdGenRef};
pub use schema::{ColumnDef, DataType, Schema, SchemaRef};
use txn_storage::ContainerId;

pub mod prelude {
    pub use super::*;
}

pub struct Table {
    name: String,
    schema: SchemaRef,
}

impl Table {
    pub fn new(name: &str, schema: SchemaRef) -> Self {
        Table {
            name: name.to_string(),
            schema,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub type TableRef = Arc<Table>;

/// Per DB catalog
pub struct Catalog {
    tables: Mutex<HashMap<ContainerId, TableRef>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_table(&self, c_id: ContainerId, table: TableRef) {
        self.tables.lock().unwrap().insert(c_id, table);
    }

    pub fn drop_table(&self, c_id: ContainerId) {
        self.tables.lock().unwrap().remove(&c_id);
    }

    pub fn get_schema(&self, c_id: ContainerId) -> Option<SchemaRef> {
        self.tables
            .lock()
            .unwrap()
            .get(&c_id)
            .map(|t| t.schema.clone())
    }

    pub fn get_table(&self, table_name: &str) -> Option<(ContainerId, TableRef)> {
        self.tables
            .lock()
            .unwrap()
            .iter()
            .find(|(_, t)| t.name == table_name)
            .map(|(c_id, t)| (*c_id, t.clone()))
    }

    pub fn is_valid_table(&self, table_name: &str) -> bool {
        self.get_table(table_name).is_some()
    }

    pub fn is_valid_column(&self, table_name: &str, column_name: &str) -> bool {
        self.get_table(table_name)
            .map(|(_, t)| t.schema().columns().iter().any(|c| c.name() == column_name))
            .unwrap_or(false)
    }
}

pub type CatalogRef = Arc<Catalog>;

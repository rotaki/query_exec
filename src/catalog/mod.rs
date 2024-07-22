mod col_id_gen;
mod schema;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub use col_id_gen::{ColIdGen, ColIdGenRef};
use fbtree::prelude::ContainerId;
pub use schema::{ColumnDef, DataType, Schema, SchemaRef};

pub mod prelude {
    pub use super::*;
}

#[derive(Debug)]
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let name_len = self.name.len() as u32;
        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(self.name.as_bytes());
        bytes.extend_from_slice(&self.schema.to_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let name_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let name = String::from_utf8(bytes[4..4 + name_len].to_vec()).unwrap();
        let schema = Schema::from_bytes(&bytes[4 + name_len..]);
        Table::new(&name, Arc::new(schema))
    }
}

pub type TableRef = Arc<Table>;

/// Per DB catalog
#[derive(Debug)]
pub struct Catalog {
    tables: Mutex<HashMap<ContainerId, TableRef>>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_catalog_to_bytes_from_bytes() {
        use super::*;
        let schema = Schema::new(
            vec![
                ColumnDef::new("c1", DataType::Int, false),
                ColumnDef::new("c2", DataType::Int, false),
            ],
            vec![0],
        );
        let table = Table::new("t1", Arc::new(schema));
        let bytes = table.to_bytes();
        let table2 = Table::from_bytes(&bytes);
        assert_eq!(table.name, table2.name);
        assert_eq!(table.schema().columns(), table2.schema().columns());
        assert_eq!(
            table.schema().primary_key_indices(),
            table2.schema().primary_key_indices()
        );
    }
}

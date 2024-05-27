use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub type SchemaRef = Arc<Schema>;

pub struct Schema {
    columns: Vec<ColumnDef>,
    primary_key: Vec<usize>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>, primary_key: Vec<usize>) -> Self {
        Schema {
            columns,
            primary_key,
        }
    }

    pub fn columns(&self) -> &Vec<ColumnDef> {
        &self.columns
    }

    pub fn get_column(&self, idx: usize) -> &ColumnDef {
        &self.columns[idx]
    }

    pub fn primary_key_indices(&self) -> &Vec<usize> {
        &self.primary_key
    }
}

#[derive(Serialize, Deserialize)]
pub struct ColumnDef {
    name: String,
    data_type: DataType,
    is_nullable: bool,
}

impl ColumnDef {
    /// Create a new column definition.
    /// name: Name of the column.
    /// data_type: Data type of the column.
    /// is_nullable: Whether the column is nullable.
    pub fn new(name: &str, data_type: DataType, is_nullable: bool) -> Self {
        ColumnDef {
            name: name.to_string(),
            data_type,
            is_nullable,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }
}

#[derive(Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int,
    Float,
    String,
    Date,
}
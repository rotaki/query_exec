use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub type SchemaRef = Arc<Schema>;

#[derive(Debug)]
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

    pub fn merge(&self, other: &Schema) -> Schema {
        let mut columns = self.columns.clone();
        columns.extend(other.columns.clone());
        Schema::new(columns, Vec::new())
    }

    pub fn project(&self, indices: &Vec<usize>) -> Schema {
        let columns = indices
            .into_iter()
            .map(|idx| self.columns[*idx].clone())
            .collect();
        Schema::new(columns, vec![])
    }

    pub fn push_column(&mut self, column: ColumnDef) {
        self.columns.push(column);
    }

    pub fn get_column(&self, idx: usize) -> &ColumnDef {
        &self.columns[idx]
    }

    pub fn primary_key_indices(&self) -> &Vec<usize> {
        &self.primary_key
    }

    pub fn make_nullable(&self) -> Schema {
        let columns = self
            .columns
            .iter()
            .map(|col| ColumnDef::new(col.name(), col.data_type().clone(), true))
            .collect();
        Schema::new(columns, self.primary_key.clone())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataType {
    Boolean = 0,
    Int,
    Float,
    String,
    Date,
    Months,
    Days,
    Unknown,
}

impl From<usize> for DataType {
    fn from(value: usize) -> Self {
        match value {
            0 => DataType::Boolean,
            1 => DataType::Int,
            2 => DataType::Float,
            3 => DataType::String,
            4 => DataType::Date,
            5 => DataType::Months,
            6 => DataType::Days,
            _ => DataType::Unknown,
        }
    }
}

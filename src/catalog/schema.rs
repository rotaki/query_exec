use std::sync::Arc;

pub type SchemaRef = Arc<Schema>;

#[derive(Debug, PartialEq, Eq)]
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
            .iter()
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let num_columns = self.columns.len() as u32;
        bytes.extend_from_slice(&num_columns.to_be_bytes());
        for col in &self.columns {
            let col_bytes = col.to_bytes();
            bytes.extend_from_slice(&(col_bytes.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&col_bytes);
        }
        let num_primary_key = self.primary_key.len() as u32;
        bytes.extend_from_slice(&num_primary_key.to_be_bytes());
        for idx in &self.primary_key {
            bytes.extend_from_slice(&idx.to_be_bytes());
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let num_columns = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let mut columns = Vec::with_capacity(num_columns);
        let mut offset = 4;
        for _ in 0..num_columns {
            let col_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            let col = ColumnDef::from_bytes(&bytes[offset + 4..offset + 4 + col_len]);
            columns.push(col);
            offset += 4 + col_len;
        }
        let num_primary_key = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4;
        let mut primary_key = Vec::with_capacity(num_primary_key);
        for _ in 0..num_primary_key {
            primary_key.push(u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize);
            offset += 4;
        }
        Schema::new(columns, primary_key)
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Schema: [")?;
        for (i, col) in self.columns.iter().enumerate() {
            write!(f, "{}: {}", col.name(), col.data_type())?;
            if i < self.columns.len() - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let name_len = self.name.len();
        bytes.extend_from_slice(&(name_len as u32).to_be_bytes());
        bytes.extend_from_slice(self.name.as_bytes());
        bytes.extend_from_slice(&(self.data_type.clone() as u8).to_be_bytes());
        bytes.extend_from_slice(&(self.is_nullable as u8).to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let name_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let name = String::from_utf8(bytes[4..4 + name_len].to_vec()).unwrap();
        let data_type = DataType::from(bytes[4 + name_len] as usize);
        let is_nullable = bytes[4 + name_len + 1] == 1;
        ColumnDef::new(&name, data_type, is_nullable)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Int => write!(f, "Int"),
            DataType::Float => write!(f, "Float"),
            DataType::String => write!(f, "String"),
            DataType::Date => write!(f, "Date"),
            DataType::Months => write!(f, "Months"),
            DataType::Days => write!(f, "Days"),
            DataType::Unknown => write!(f, "Unknown"),
        }
    }
}

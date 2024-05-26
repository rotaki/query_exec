use serde::{Deserialize, Serialize};

use crate::schema::{ColumnDef, DataType};

#[derive(Serialize, Deserialize)]
pub struct Tuple {
    bytes: Vec<Field>,
}

impl Tuple {
    pub fn with_capacity(capacity: usize) -> Self {
        Tuple {
            bytes: Vec::with_capacity(capacity),
        }
    }

    pub fn get(&self, field_idx: usize) -> &Field {
        &self.bytes[field_idx]
    }

    pub fn push(&mut self, field: Field) {
        self.bytes.push(field);
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self.bytes).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Tuple {
            bytes: bincode::deserialize(bytes).unwrap(),
        }
    }

    /// Convert the tuple to a primary key bytes.
    /// Panics if there is a null value in the primary key.
    pub fn to_primary_key_bytes(&self, primary_key_indicies: &Vec<usize>) -> Vec<u8> {
        let mut key = Vec::new();
        for idx in primary_key_indicies {
            match &self.bytes[*idx] {
                Field::Boolean(val) => key.push(val.unwrap() as u8),
                Field::Int(val) => {
                    key.extend(&val.unwrap().to_be_bytes());
                }
                Field::Float(val) => {
                    key.extend(&val.unwrap().to_be_bytes());
                }
                Field::String(val) => {
                    key.extend(val.clone().unwrap().as_bytes());
                }
                Field::Date(val) => {
                    key.extend(&val.unwrap().to_be_bytes());
                }
            }
        }
        key
    }

    pub fn to_normalized_key_bytes(
        &self,
        key_indicies: &Vec<usize>,
        nulls_first: &Vec<bool>,
    ) -> Vec<u8> {
        let mut key = Vec::new();
        for (idx, null_first) in key_indicies.iter().zip(nulls_first) {
            let null_prefix = if *null_first { 0 } else { 1 };
            let non_null_prefix = if *null_first { 1 } else { 0 };
            match &self.bytes[*idx] {
                Field::Boolean(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        key.push(*val as u8);
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::Int(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        key.extend(&val.to_be_bytes());
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::Float(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        key.extend(&val.to_be_bytes());
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::String(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        key.extend(val.as_bytes());
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::Date(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        key.extend(&val.to_be_bytes());
                    } else {
                        key.push(null_prefix);
                    }
                }
            }
        }
        key
    }
}

impl std::fmt::Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Separate the fields with '|'. Each field has a fixed width of 10 characters.
        // If the field is larger than 10 characters, truncate it with '...'.
        let mut res = String::new();
        let width = 10;
        for field in &self.bytes {
            let field_str = format!("{}", field);
            if field_str.len() > width {
                res.push_str(&field_str[..width - 3]);
                res.push_str("...");
            } else {
                res.push_str(&field_str);
                for _ in 0..width - field_str.len() {
                    res.push(' ');
                }
            }
            res.push('|');
        }
        write!(f, "{}", res)
    }
}

#[derive(Serialize, Deserialize)]
pub enum Field {
    Boolean(Option<bool>),
    Int(Option<i64>),
    Float(Option<f64>),
    String(Option<String>),
    Date(Option<i64>),
}

impl Field {
    pub fn from_str(column_def: &ColumnDef, field: &str) -> Result<Self, String> {
        let data_type = column_def.data_type();
        let is_nullable = column_def.is_nullable();
        if is_nullable && (field == "NULL" || field == "null") {
            match data_type {
                DataType::Boolean => Ok(Field::Boolean(None)),
                DataType::Int => Ok(Field::Int(None)),
                DataType::Float => Ok(Field::Float(None)),
                DataType::String => Ok(Field::String(None)),
                DataType::Date => Ok(Field::Date(None)),
            }
        } else {
            match data_type {
                DataType::Boolean => {
                    let val = field.parse::<bool>().map_err(|e| e.to_string())?;
                    Ok(Field::Boolean(Some(val)))
                }
                DataType::Int => {
                    let val = field.parse::<i64>().map_err(|e| e.to_string())?;
                    Ok(Field::Int(Some(val)))
                }
                DataType::Float => {
                    let val = field.parse::<f64>().map_err(|e| e.to_string())?;
                    Ok(Field::Float(Some(val)))
                }
                DataType::String => Ok(Field::String(Some(field.to_string()))),
                DataType::Date => {
                    let val = field.parse::<i64>().map_err(|e| e.to_string())?;
                    Ok(Field::Date(Some(val)))
                }
            }
        }
    }

    pub fn as_string(&self) -> &Option<String> {
        match self {
            Field::String(val) => val,
            _ => panic!("Field is not a string"),
        }
    }

    pub fn as_int(&self) -> &Option<i64> {
        match self {
            Field::Int(val) => val,
            _ => panic!("Field is not an int"),
        }
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Field::Boolean(val) => match val {
                Some(val) => write!(f, "{}", val),
                None => write!(f, "NULL"),
            },
            Field::Int(val) => match val {
                Some(val) => write!(f, "{}", val),
                None => write!(f, "NULL"),
            },
            Field::Float(val) => match val {
                Some(val) => write!(f, "{}", val),
                None => write!(f, "NULL"),
            },
            Field::String(val) => match val {
                Some(val) => write!(f, "{}", val),
                None => write!(f, "NULL"),
            },
            Field::Date(val) => match val {
                Some(val) => write!(f, "{}", val),
                None => write!(f, "NULL"),
            },
        }
    }
}

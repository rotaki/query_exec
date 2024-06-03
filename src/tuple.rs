use std::{
    fmt::Debug,
    hash::Hash,
    ops::{Add, Div, Mul, Sub},
};

use serde::{Deserialize, Serialize};

use crate::{
    catalog::{ColumnDef, DataType},
    error::ExecError,
};

fn f64_to_order_preserving_bytes(val: f64) -> [u8; 8] {
    let mut val_bits = val.to_bits();
    let sign = (val_bits >> 63) as u8;
    if sign == 1 {
        // Negative number so flip all the bits including the sign bit
        val_bits = !val_bits;
    } else {
        // Positive number. To distinguish between positive and negative numbers,
        // we flip the sign bit.
        val_bits ^= 1 << 63;
    }
    val_bits.to_be_bytes()
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tuple {
    fields: Vec<Field>,
}

impl Tuple {
    pub fn to_pretty_string(&self) -> String {
        // Each field has a fixed width of 20 characters.
        let width = 20;
        let mut res = String::new();
        for field in &self.fields {
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
            res.push_str(" | ");
        }
        res
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Tuple {
            fields: Vec::with_capacity(capacity),
        }
    }

    pub fn from_fields(fields: Vec<Field>) -> Self {
        Tuple { fields }
    }

    pub fn merge(&self, other: &Tuple) -> Tuple {
        let mut fields = Vec::with_capacity(self.fields.len() + other.fields.len());
        fields.extend_from_slice(&self.fields);
        fields.extend_from_slice(&other.fields);
        Tuple { fields }
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn get(&self, field_idx: usize) -> &Field {
        &self.fields[field_idx]
    }

    pub fn get_cols(&self, col_ids: &Vec<usize>) -> Vec<Field> {
        col_ids
            .iter()
            .map(|&col_id| self.fields[col_id].clone())
            .collect()
    }

    pub fn push(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self.fields).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Tuple {
            fields: bincode::deserialize(bytes).unwrap(),
        }
    }

    pub fn project(mut self, col_ids: &Vec<usize>) -> Tuple {
        let mut new_fields = Vec::with_capacity(col_ids.len());
        for &col_id in col_ids {
            new_fields.push(self.fields[col_id].take());
        }
        Tuple { fields: new_fields }
    }

    /// Convert the tuple to a primary key bytes.
    /// Panics if there is a null value in the primary key.
    pub fn to_primary_key_bytes(&self, primary_key_indicies: &Vec<usize>) -> Vec<u8> {
        let mut key = Vec::new();
        for idx in primary_key_indicies {
            match &self.fields[*idx] {
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
        key_indicies: &Vec<(usize, bool, bool)>, // (idx, asc, nulls_first)
    ) -> Vec<u8> {
        let mut key = Vec::new();
        for (idx, asc, null_first) in key_indicies.iter() {
            let null_prefix = if *null_first { 0 } else { 255 };
            let non_null_prefix = if *null_first { 255 } else { 0 };
            match &self.fields[*idx] {
                Field::Boolean(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        let val_in_bytes = if *val { 1 } else { 0 };
                        if *asc {
                            key.push(val_in_bytes);
                        } else {
                            key.push(!val_in_bytes);
                        }
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::Int(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        let val_in_bytes = val.to_be_bytes();
                        for byte in &val_in_bytes {
                            if *asc {
                                key.push(*byte);
                            } else {
                                key.push(!byte);
                            }
                        }
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::Float(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        let val_in_bytes = f64_to_order_preserving_bytes(*val);
                        for byte in &val_in_bytes {
                            if *asc {
                                key.push(*byte);
                            } else {
                                key.push(!byte);
                            }
                        }
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::String(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        let val_in_bytes = val.as_bytes();
                        for byte in val_in_bytes {
                            if *asc {
                                key.push(*byte);
                            } else {
                                key.push(!byte);
                            }
                        }
                    } else {
                        key.push(null_prefix);
                    }
                }
                Field::Date(val) => {
                    if let Some(val) = val {
                        key.push(non_null_prefix);
                        let val_in_bytes = val.to_be_bytes();
                        for byte in &val_in_bytes {
                            if *asc {
                                key.push(*byte);
                            } else {
                                key.push(!byte);
                            }
                        }
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
        for field in &self.fields {
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Field {
    Boolean(Option<bool>),
    Int(Option<i64>),
    Float(Option<f64>), // f64 should not contain f64::NAN.
    String(Option<String>),
    Date(Option<i64>),
}

impl Eq for Field {}
impl Ord for Field {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Field::Boolean(val1), Field::Boolean(val2)) => val1.cmp(val2),
            (Field::Int(val1), Field::Int(val2)) => val1.cmp(val2),
            (Field::Float(val1), Field::Float(val2)) => val1.partial_cmp(val2).unwrap(),
            (Field::String(val1), Field::String(val2)) => val1.cmp(val2),
            (Field::Date(val1), Field::Date(val2)) => val1.cmp(val2),
            _ => panic!("Cannot compare different types"),
        }
    }
}

impl Hash for Field {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Field::Boolean(val) => val.hash(state),
            Field::Int(val) => val.hash(state),
            Field::Float(val) => {
                if let Some(val) = val {
                    val.to_bits().hash(state)
                } else {
                    f64::NAN.to_bits().hash(state)
                }
            }
            Field::String(val) => val.hash(state),
            Field::Date(val) => val.hash(state),
        }
    }
}

impl Field {
    pub fn null(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => Field::Boolean(None),
            DataType::Int => Field::Int(None),
            DataType::Float => Field::Float(None),
            DataType::String => Field::String(None),
            DataType::Date => Field::Date(None),
            DataType::Unknown => {
                println!("Unknown data type, defaulting to int");
                Field::Int(None) // Default to int for unknown data type
            }
        }
    }

    pub fn take(&mut self) -> Field {
        match self {
            Field::Boolean(val) => Field::Boolean(val.take()),
            Field::Int(val) => Field::Int(val.take()),
            Field::Float(val) => Field::Float(val.take()),
            Field::String(val) => Field::String(val.take()),
            Field::Date(val) => Field::Date(val.take()),
        }
    }

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
                DataType::Unknown => Err("Unknown data type".to_string()),
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
                DataType::Unknown => Err("Unknown data type".to_string()),
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

impl Add for Field {
    type Output = Result<Field, ExecError>;

    fn add(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(val1), Field::Int(val2)) => {
                Ok(Field::Int(val1.and_then(|v1| val2.map(|v2| v1 + v2))))
            }
            (Field::Float(val1), Field::Int(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 + v2 as f64)),
            )),
            (Field::Int(val1), Field::Float(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 as f64 + v2)),
            )),
            (Field::Float(val1), Field::Float(val2)) => {
                Ok(Field::Float(val1.and_then(|v1| val2.map(|v2| v1 + v2))))
            }
            (Field::String(val1), Field::String(val2)) => {
                Ok(Field::String(val1.and_then(|v1| val2.map(|v2| v1 + &v2))))
            }
            (x, y) => Err(ExecError::FieldOp(format!(
                "Cannot add {:?} and {:?}",
                x, y
            ))),
        }
    }
}

impl Add for &Field {
    type Output = Result<Field, ExecError>;

    fn add(self, other: Self) -> Self::Output {
        self.clone().add(other.clone())
    }
}

impl Sub for Field {
    type Output = Result<Field, ExecError>;

    fn sub(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(val1), Field::Int(val2)) => {
                Ok(Field::Int(val1.and_then(|v1| val2.map(|v2| v1 - v2))))
            }
            (Field::Float(val1), Field::Int(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 - v2 as f64)),
            )),
            (Field::Int(val1), Field::Float(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 as f64 - v2)),
            )),
            (Field::Float(val1), Field::Float(val2)) => {
                Ok(Field::Float(val1.and_then(|v1| val2.map(|v2| v1 - v2))))
            }
            (x, y) => Err(ExecError::FieldOp(format!(
                "Cannot subtract {:?} and {:?}",
                x, y
            ))),
        }
    }
}

impl Sub for &Field {
    type Output = Result<Field, ExecError>;

    fn sub(self, other: Self) -> Self::Output {
        self.clone().sub(other.clone())
    }
}

impl Mul for Field {
    type Output = Result<Field, ExecError>;

    fn mul(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(val1), Field::Int(val2)) => {
                Ok(Field::Int(val1.and_then(|v1| val2.map(|v2| v1 * v2))))
            }
            (Field::Float(val1), Field::Int(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 * v2 as f64)),
            )),
            (Field::Int(val1), Field::Float(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 as f64 * v2)),
            )),
            (Field::Float(val1), Field::Float(val2)) => {
                Ok(Field::Float(val1.and_then(|v1| val2.map(|v2| v1 * v2))))
            }
            (x, y) => Err(ExecError::FieldOp(format!(
                "Cannot multiply {:?} and {:?}",
                x, y
            ))),
        }
    }
}

impl Mul for &Field {
    type Output = Result<Field, ExecError>;

    fn mul(self, other: Self) -> Self::Output {
        self.clone().mul(other.clone())
    }
}

impl Div for Field {
    type Output = Result<Field, ExecError>;

    fn div(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(val1), Field::Int(val2)) => {
                Ok(Field::Int(val1.and_then(|v1| val2.map(|v2| v1 / v2))))
            }
            (Field::Float(val1), Field::Int(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 / v2 as f64)),
            )),
            (Field::Int(val1), Field::Float(val2)) => Ok(Field::Float(
                val1.and_then(|v1| val2.map(|v2| v1 as f64 / v2)),
            )),
            (Field::Float(val1), Field::Float(val2)) => {
                Ok(Field::Float(val1.and_then(|v1| val2.map(|v2| v1 / v2))))
            }
            (x, y) => Err(ExecError::FieldOp(format!(
                "Cannot divide {:?} and {:?}",
                x, y
            ))),
        }
    }
}

impl Div for &Field {
    type Output = Result<Field, ExecError>;

    fn div(self, other: Self) -> Self::Output {
        self.clone().div(other.clone())
    }
}

pub trait FromBool {
    fn from_bool(b: bool) -> Self;
}

impl FromBool for Field {
    fn from_bool(val: bool) -> Self {
        Field::Boolean(Some(val))
    }
}

pub trait AsBool {
    fn as_bool(&self) -> Result<bool, ExecError>;
}

impl AsBool for Field {
    fn as_bool(&self) -> Result<bool, ExecError> {
        match self {
            Field::Boolean(val) => val.ok_or(ExecError::FieldOp("Field is NULL".to_string())),
            x => Err(ExecError::FieldOp(format!(
                "Cannot convert {:?} to bool",
                x
            ))),
        }
    }
}

pub trait And<Rhs = Self> {
    type Output;

    fn and(self, other: Rhs) -> Self::Output;
}

impl And for Field {
    type Output = Result<Field, ExecError>;

    fn and(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Boolean(val1), Field::Boolean(val2)) => {
                Ok(Field::Boolean(val1.and_then(|v1| val2.map(|v2| v1 && v2))))
            }
            (x, y) => Err(ExecError::FieldOp(format!(
                "Cannot AND {:?} and {:?}",
                x, y
            ))),
        }
    }
}

impl And for &Field {
    type Output = Result<Field, ExecError>;

    fn and(self, other: Self) -> Self::Output {
        self.clone().and(other.clone())
    }
}

pub trait Or<Rhs = Self> {
    type Output;
    fn or(self, other: Rhs) -> Self::Output;
}

impl Or for Field {
    type Output = Result<Field, ExecError>;
    fn or(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Boolean(val1), Field::Boolean(val2)) => {
                Ok(Field::Boolean(val1.and_then(|v1| val2.map(|v2| v1 || v2))))
            }
            (x, y) => Err(ExecError::FieldOp(format!("Cannot OR {:?} and {:?}", x, y))),
        }
    }
}

pub trait IsNull {
    fn is_null(&self) -> bool;
}

impl IsNull for Field {
    fn is_null(&self) -> bool {
        match self {
            Field::Boolean(val) => val.is_none(),
            Field::Int(val) => val.is_none(),
            Field::Float(val) => val.is_none(),
            Field::String(val) => val.is_none(),
            Field::Date(val) => val.is_none(),
        }
    }
}

impl Or for &Field {
    type Output = Result<Field, ExecError>;
    fn or(self, other: Self) -> Self::Output {
        self.clone().or(other.clone())
    }
}

impl From<i64> for Field {
    fn from(val: i64) -> Self {
        Field::Int(Some(val))
    }
}

impl From<f64> for Field {
    fn from(val: f64) -> Self {
        Field::Float(Some(val))
    }
}

impl From<bool> for Field {
    fn from(val: bool) -> Self {
        Field::Boolean(Some(val))
    }
}

impl From<String> for Field {
    fn from(val: String) -> Self {
        Field::String(Some(val))
    }
}

impl From<&str> for Field {
    fn from(val: &str) -> Self {
        Field::String(Some(val.to_string()))
    }
}

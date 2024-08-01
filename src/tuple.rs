use std::{
    fmt::Debug,
    hash::Hash,
    ops::{Add, Div, Mul, Sub},
};

use crate::{
    catalog::{ColumnDef, DataType},
    error::ExecError,
};

use bitcode::{Decode, Encode};
use chrono::{Datelike, Days, Months, NaiveDate};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode)]
pub struct Tuple {
    fields: Vec<Field>,
}

impl Tuple {
    pub fn copy(&self) -> Self {
        // We don't use Copy trait because we don't want implicit copying of the tuples.
        Tuple {
            fields: self.fields.clone(),
        }
    }

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

    pub fn merge_mut(mut self, other: &Tuple) -> Self {
        self.fields.extend_from_slice(&other.fields);
        self
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn get(&self, field_idx: usize) -> &Field {
        &self.fields[field_idx]
    }

    pub fn get_mut(&mut self, field_idx: usize) -> &mut Field {
        &mut self.fields[field_idx]
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
        bincode::serialize(&self).unwrap()
        // let mut bytes = Vec::new();
        // let num_fields = self.fields.len() as u32;
        // bytes.extend_from_slice(&num_fields.to_be_bytes());
        // for field in &self.fields {
        //     let field_bytes = field.to_bytes();
        //     let field_len = field_bytes.len() as u32;
        //     bytes.extend_from_slice(&field_len.to_be_bytes());
        //     bytes.extend_from_slice(&field_bytes);
        // }
        // bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
        // let num_fields = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        // let mut offset = 4;
        // let mut fields = Vec::with_capacity(num_fields);
        // for _ in 0..num_fields {
        //     let field_len = u32::from_be_bytes([
        //         bytes[offset],
        //         bytes[offset + 1],
        //         bytes[offset + 2],
        //         bytes[offset + 3],
        //     ]) as usize;
        //     let field_bytes = &bytes[offset + 4..offset + 4 + field_len];
        //     fields.push(Field::from_bytes(field_bytes));
        //     offset += 4 + field_len;
        // }
        // Tuple { fields }
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
                    let val = val.unwrap().num_days_from_ce();
                    key.extend(&val.to_be_bytes());
                }
                Field::Months(val) => {
                    let val = val.unwrap();
                    key.extend(&val.to_be_bytes());
                }
                Field::Days(val) => {
                    let val = val.unwrap();
                    key.extend(&val.to_be_bytes());
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
                        let val_in_bytes = val.num_days_from_ce().to_be_bytes();
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
                Field::Months(val) => {
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
                Field::Days(val) => {
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

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Field {
    Boolean(Option<bool>),
    Int(Option<i64>),
    Float(Option<f64>), // f64 should not contain f64::NAN.
    String(Option<String>),
    Date(Option<NaiveDate>),
    Months(Option<u32>),
    Days(Option<u64>),
}

impl Field {
    pub fn to_bytes(&self) -> Vec<u8> {
        bitcode::encode(self)
        // bincode::serialize(&self).unwrap()
        /*
        let mut bytes = Vec::new();
        // [type, is_null, value]
        match self {
            Field::Boolean(val) => {
                bytes.push(0);
                bytes.push(val.is_none() as u8);
                if let Some(val) = val {
                    bytes.push(*val as u8);
                }
            }
            Field::Int(val) => {
                bytes.push(1);
                bytes.push(val.is_none() as u8);
                if let Some(val) = val {
                    bytes.extend_from_slice(&val.to_be_bytes());
                }
            }
            Field::Float(val) => {
                bytes.push(2);
                bytes.push(val.is_none() as u8);
                if let Some(val) = val {
                    bytes.extend_from_slice(&val.to_be_bytes());
                }
            }
            Field::String(val) => {
                bytes.push(3);
                bytes.push(val.is_none() as u8);
                if let Some(val) = val {
                    let val_bytes = val.as_bytes();
                    let len = val_bytes.len() as u32;
                    bytes.extend_from_slice(&len.to_be_bytes());
                    bytes.extend_from_slice(val_bytes);
                }
            }
            Field::Date(val) => {
                bytes.push(4);
                bytes.push(val.is_none() as u8);
                if let Some(val) = val {
                    let val_bytes = val.num_days_from_ce().to_be_bytes();
                    bytes.extend_from_slice(&val_bytes);
                }
            }
            Field::Months(val) => {
                bytes.push(5);
                bytes.push(val.is_none() as u8);
                if let Some(val) = val {
                    bytes.extend_from_slice(&val.to_be_bytes());
                }
            }
            Field::Days(val) => {
                bytes.push(6);
                bytes.push(val.is_none() as u8);
                if let Some(val) = val {
                    bytes.extend_from_slice(&val.to_be_bytes());
                }
            }
        }
        bytes
        */
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        bitcode::decode(bytes).unwrap()
        // bincode::deserialize(bytes).unwrap()
        /*
        let data_type = bytes[0];
        let is_null = bytes[1] == 1;
        let offset = 2;
        match data_type {
            0 => {
                let val = if is_null { None } else { Some(bytes[2] == 1) };
                Field::Boolean(val)
            }
            1 => {
                let val = if is_null {
                    None
                } else {
                    let mut val_bytes = [0; 8];
                    val_bytes.copy_from_slice(&bytes[2..10]);
                    Some(i64::from_be_bytes(val_bytes))
                };
                Field::Int(val)
            }
            2 => {
                let val = if is_null {
                    None
                } else {
                    let mut val_bytes = [0; 8];
                    val_bytes.copy_from_slice(&bytes[2..10]);
                    Some(f64::from_be_bytes(val_bytes))
                };
                Field::Float(val)
            }
            3 => {
                let val = if is_null {
                    None
                } else {
                    let len = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]) as usize;
                    let val_bytes = &bytes[6..6 + len];
                    Some(String::from_utf8(val_bytes.to_vec()).unwrap())
                };
                Field::String(val)
            }
            4 => {
                let val = if is_null {
                    None
                } else {
                    let val_bytes = i32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
                    Some(NaiveDate::from_num_days_from_ce_opt(val_bytes).unwrap())
                };
                Field::Date(val)
            }
            5 => {
                let val = if is_null {
                    None
                } else {
                    let val_bytes = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
                    Some(val_bytes)
                };
                Field::Months(val)
            }
            6 => {
                let val = if is_null {
                    None
                } else {
                    let val_bytes = u64::from_be_bytes([
                        bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
                        bytes[9],
                    ]);
                    Some(val_bytes)
                };
                Field::Days(val)
            }
            _ => panic!("Unknown data type"),
        }
        */
    }
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Field::Boolean(val1), Field::Boolean(val2)) => val1 == val2,
            (Field::Int(val1), Field::Int(val2)) => val1 == val2,
            (Field::Float(val1), Field::Float(val2)) => val1 == val2,
            (Field::String(val1), Field::String(val2)) => val1 == val2,
            (Field::Date(val1), Field::Date(val2)) => val1 == val2,
            (Field::Int(val1), Field::Float(val2)) => {
                if let (Some(val1), Some(val2)) = (val1, val2) {
                    (*val1 as f64).eq(val2)
                } else {
                    false
                }
            }
            (Field::Float(val1), Field::Int(val2)) => {
                if let (Some(val1), Some(val2)) = (val1, val2) {
                    val1.eq(&(*val2 as f64))
                } else {
                    false
                }
            }
            (Field::Months(val1), Field::Months(val2)) => val1 == val2,
            (Field::Days(val1), Field::Days(val2)) => val1 == val2,
            _ => false,
        }
    }
}

// Two null values comparison returns None. To check if a field is null, use Field::is_null().
impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Field::Boolean(val1), Field::Boolean(val2)) => val1
                .as_ref()
                .and_then(|v1| val2.as_ref().map(|v2| v1.cmp(v2))),
            (Field::Int(val1), Field::Int(val2)) => val1
                .as_ref()
                .and_then(|v1| val2.as_ref().map(|v2| v1.cmp(v2))),
            (Field::Float(val1), Field::Float(val2)) => val1
                .as_ref()
                .and_then(|v1| val2.as_ref().and_then(|v2| v1.partial_cmp(v2))),
            (Field::String(val1), Field::String(val2)) => val1
                .as_ref()
                .and_then(|v1| val2.as_ref().map(|v2| v1.cmp(v2))),
            (Field::Date(val1), Field::Date(val2)) => val1
                .as_ref()
                .and_then(|v1| val2.as_ref().map(|v2| v1.cmp(v2))),
            (Field::Int(val1), Field::Float(val2)) => val1
                .as_ref()
                .and_then(|v1| val2.as_ref().and_then(|v2| (*v1 as f64).partial_cmp(v2))),
            (Field::Float(val1), Field::Int(val2)) => val1
                .as_ref()
                .and_then(|v1| val2.as_ref().and_then(|v2| v1.partial_cmp(&(*v2 as f64)))),
            _ => {
                panic!("Cannot compare different types")
            }
        }
    }
}

impl Eq for Field {}

// Ord is only used for sorting tuples when testing. Two null values are equal. Null values are the smallest in the ordering.
impl Ord for Field {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (x, y) if x.is_null() && y.is_null() => std::cmp::Ordering::Equal,
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
            Field::Months(val) => val.hash(state),
            Field::Days(val) => val.hash(state),
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
            DataType::Months => Field::Months(None),
            DataType::Days => Field::Days(None),
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
            Field::Months(val) => Field::Months(val.take()),
            Field::Days(val) => Field::Days(val.take()),
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
                DataType::Months => Ok(Field::Months(None)),
                DataType::Days => Ok(Field::Days(None)),
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
                    // Date is stored as yyyy-mm-dd
                    let val = chrono::NaiveDate::parse_from_str(field, "%Y-%m-%d")
                        .map_err(|e| e.to_string())?;
                    Ok(Field::Date(Some(val)))
                }
                DataType::Months => {
                    let val = field.parse::<u32>().map_err(|e| e.to_string())?;
                    Ok(Field::Months(Some(val)))
                }
                DataType::Days => {
                    let val = field.parse::<u64>().map_err(|e| e.to_string())?;
                    Ok(Field::Days(Some(val)))
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

    pub fn extract_year(&self) -> Result<Option<i32>, ExecError> {
        match self {
            Field::Date(val) => Ok(val.map(|date| date.year())),
            _ => Err(ExecError::FieldOp("Field is not a date".to_string())),
        }
    }

    pub fn extract_month(&self) -> Result<Option<u32>, ExecError> {
        match self {
            Field::Date(val) => Ok(val.map(|date| date.month())),
            _ => Err(ExecError::FieldOp("Field is not a date".to_string())),
        }
    }

    pub fn extract_day(&self) -> Result<Option<u32>, ExecError> {
        match self {
            Field::Date(val) => Ok(val.map(|date| date.day())),
            _ => Err(ExecError::FieldOp("Field is not a date".to_string())),
        }
    }

    pub fn cast(&self, to_type: &DataType) -> Result<Field, ExecError> {
        match (self, to_type) {
            (Field::Boolean(val), DataType::Boolean) => Ok(Field::Boolean(*val)),
            (Field::Int(val), DataType::Int) => Ok(Field::Int(*val)),
            (Field::Float(val), DataType::Float) => Ok(Field::Float(*val)),
            (Field::String(val), DataType::String) => Ok(Field::String(val.clone())),
            (Field::Date(val), DataType::Date) => Ok(Field::Date(*val)),
            (Field::Months(val), DataType::Months) => Ok(Field::Months(*val)),
            (Field::Days(val), DataType::Days) => Ok(Field::Days(*val)),
            (Field::Int(val), DataType::Float) => Ok(Field::Float(val.map(|v| v as f64))),
            (Field::Float(val), DataType::Int) => Ok(Field::Int(val.map(|v| v as i64))),
            (Field::String(val), DataType::Int) => {
                let val = val.as_ref().and_then(|v| v.parse::<i64>().ok());
                Ok(Field::Int(val))
            }
            (Field::String(val), DataType::Float) => {
                let val = val.as_ref().and_then(|v| v.parse::<f64>().ok());
                Ok(Field::Float(val))
            }
            (Field::String(val), DataType::Date) => {
                let val = val
                    .as_ref()
                    .and_then(|v| chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d").ok());
                Ok(Field::Date(val))
            }
            (Field::String(val), DataType::Months) => {
                let val = val.as_ref().and_then(|v| v.parse::<u32>().ok());
                Ok(Field::Months(val))
            }
            (Field::String(val), DataType::Days) => {
                let val = val.as_ref().and_then(|v| v.parse::<u64>().ok());
                Ok(Field::Days(val))
            }
            (Field::Int(value), DataType::String) => {
                Ok(Field::String(value.map(|v| v.to_string())))
            }
            (Field::Float(value), DataType::String) => {
                Ok(Field::String(value.map(|v| v.to_string())))
            }
            (Field::Date(value), DataType::String) => {
                Ok(Field::String(value.map(|v| v.to_string())))
            }
            (Field::Months(value), DataType::String) => {
                Ok(Field::String(value.map(|v| v.to_string())))
            }
            (Field::Days(value), DataType::String) => {
                Ok(Field::String(value.map(|v| v.to_string())))
            }
            (Field::Int(value), DataType::Boolean) => Ok(Field::Boolean(value.map(|v| v != 0))),
            // Othercases, return None with the DataType
            (_, DataType::Boolean) => Ok(Field::Boolean(None)),
            (_, DataType::Int) => Ok(Field::Int(None)),
            (_, DataType::Float) => Ok(Field::Float(None)),
            (_, DataType::String) => Ok(Field::String(None)),
            (_, DataType::Date) => Ok(Field::Date(None)),
            (_, DataType::Months) => Ok(Field::Months(None)),
            (_, DataType::Days) => Ok(Field::Days(None)),
            (_, DataType::Unknown) => Err(ExecError::FieldOp("Unknown data type".to_string())),
        }
    }

    pub fn substring(&self, start: usize, len: usize) -> Result<Field, ExecError> {
        match self {
            Field::String(val) => {
                let val = val.as_ref().map(|v| {
                    let start = start.min(v.len());
                    let end = (start + len).min(v.len());
                    v[start..end].to_string()
                });
                Ok(Field::String(val))
            }
            _ => Err(ExecError::FieldOp("Field is not a string".to_string())),
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
                Some(val) => {
                    write!(f, "{}", val)
                }
                None => write!(f, "NULL"),
            },
            Field::Months(val) => match val {
                Some(val) => write!(f, "{}M", val),
                None => write!(f, "NULL"),
            },
            Field::Days(val) => match val {
                Some(val) => write!(f, "{}D", val),
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
            (Field::Date(val1), Field::Months(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v1 + Months::new(v2))),
            )),
            (Field::Date(val1), Field::Days(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v1 + Days::new(v2))),
            )),
            (Field::Months(val1), Field::Date(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v2 + Months::new(v1))),
            )),
            (Field::Days(val1), Field::Date(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v2 + Days::new(v1))),
            )),
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
            (Field::Date(val1), Field::Months(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v1 - Months::new(v2))),
            )),
            (Field::Date(val1), Field::Days(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v1 - Days::new(v2))),
            )),
            (Field::Date(val1), Field::Date(val2)) => Ok(Field::Days(
                val1.and_then(|v1| val2.map(|v2| (v1 - v2).num_days() as u64)),
            )),
            (Field::Months(val1), Field::Date(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v2 - Months::new(v1))),
            )),
            (Field::Days(val1), Field::Date(val2)) => Ok(Field::Date(
                val1.and_then(|v1| val2.map(|v2| v2 - Days::new(v1))),
            )),
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
            Field::Months(val) => val.is_none(),
            Field::Days(val) => val.is_none(),
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

impl From<(i32, u32, u32)> for Field {
    fn from(val: (i32, u32, u32)) -> Self {
        let date = NaiveDate::from_ymd_opt(val.0, val.1, val.2);
        Field::Date(date)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_to_and_from_bytes_tuple_field() {
        use super::*;
        let field = Field::Int(Some(123));
        let bytes = field.to_bytes();
        let field2 = Field::from_bytes(&bytes);
        assert_eq!(field, field2);

        let field = Field::Float(Some(123.456));
        let bytes = field.to_bytes();
        let field2 = Field::from_bytes(&bytes);
        assert_eq!(field, field2);

        let field = Field::String(Some("hello".to_string()));
        let bytes = field.to_bytes();
        let field2 = Field::from_bytes(&bytes);
        assert_eq!(field, field2);

        let field = Field::Date(Some(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()));
        let bytes = field.to_bytes();
        let field2 = Field::from_bytes(&bytes);
        assert_eq!(field, field2);

        let field = Field::Months(Some(123));
        let bytes = field.to_bytes();
        let field2 = Field::from_bytes(&bytes);
        assert_eq!(field, field2);

        let field = Field::Days(Some(123));
        let bytes = field.to_bytes();
        let field2 = Field::from_bytes(&bytes);
        assert_eq!(field, field2);

        let tuple = Tuple::from_fields(vec![
            Field::Int(Some(123)),
            Field::Float(Some(123.456)),
            Field::String(Some("hello".to_string())),
            Field::Date(Some(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap())),
            Field::Months(Some(123)),
            Field::Days(Some(123)),
            Field::Int(None),
            Field::Float(None),
            Field::String(None),
            Field::Date(None),
            Field::Months(None),
            Field::Days(None),
        ]);

        let bytes = tuple.to_bytes();
        let tuple2 = Tuple::from_bytes(&bytes);
        assert_eq!(tuple, tuple2);
    }
}

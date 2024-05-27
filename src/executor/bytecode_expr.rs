use crate::expression::prelude::*;
use crate::{
    catalog::Schema,
    error::ExecError,
    tuple::{And, Field, FromBool, Or, Tuple},
    ColumnId,
};

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ops::{Add, Div, Mul, Sub},
};

pub enum ByteCodes {
    // CONTROL FLOW
    PushLit,
    PushField,
    // MATH OPERATIONS
    Add,
    Sub,
    Mul,
    Div,
    // COMPARISON OPERATIONS
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    // LOGICAL OPERATIONS
    And,
    Or,
}

const STATIC_DISPATCHER: [DispatchFn<Field>; 14] = [
    // CONTROL FLOW
    PUSH_LIT_FN,
    PUSH_FIELD_FN,
    // MATH OPERATIONS
    ADD_FN,
    SUB_FN,
    MUL_FN,
    DIV_FN,
    // COMPARISON OPERATIONS
    EQ_FN,
    NEQ_FN,
    LT_FN,
    GT_FN,
    LTE_FN,
    GTE_FN,
    // LOGICAL OPERATIONS
    AND_FN,
    OR_FN,
];

// Utility functions
pub fn colidx_expr(colidx: usize) -> ByteCodeExpr {
    let mut expr = ByteCodeExpr::new();
    expr.add_code(ByteCodes::PushField as usize);
    expr.add_code(colidx);
    expr
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ByteCodeExpr {
    pub bytecodes: Vec<usize>,
    pub literals: Vec<Field>,
}

impl ByteCodeExpr {
    pub fn new() -> Self {
        ByteCodeExpr {
            bytecodes: Vec::new(),
            literals: Vec::new(),
        }
    }

    pub fn add_code(&mut self, code: usize) {
        self.bytecodes.push(code);
    }

    pub fn add_literal(&mut self, literal: Field) -> usize {
        let i = self.literals.len();
        self.literals.push(literal);
        i
    }

    fn is_empty(&self) -> bool {
        self.bytecodes.is_empty()
    }

    pub fn eval(&self, record: &Tuple) -> Result<Field, ExecError> {
        if self.is_empty() {
            panic!("Cannot evaluate empty expression")
        }
        let mut stack = Vec::new();
        let mut i = 0;
        let record = &record.fields();
        let bytecodes = &self.bytecodes;
        let literals = &self.literals;
        while i < bytecodes.len() {
            let opcode = bytecodes[i];
            i += 1;
            STATIC_DISPATCHER[opcode](bytecodes, &mut i, &mut stack, literals, record)?;
        }
        Ok(stack.pop().unwrap())
    }
}

type DispatchFn<T> = fn(&[usize], &mut usize, &mut Vec<T>, &[T], &[T]) -> Result<(), ExecError>;
const PUSH_LIT_FN: DispatchFn<Field> = push_lit;
const PUSH_FIELD_FN: DispatchFn<Field> = push_field;
const ADD_FN: DispatchFn<Field> = add;
const SUB_FN: DispatchFn<Field> = sub;
const MUL_FN: DispatchFn<Field> = mul;
const DIV_FN: DispatchFn<Field> = div;
const EQ_FN: DispatchFn<Field> = eq;
const NEQ_FN: DispatchFn<Field> = neq;
const LT_FN: DispatchFn<Field> = lt;
const GT_FN: DispatchFn<Field> = gt;
const LTE_FN: DispatchFn<Field> = lte;
const GTE_FN: DispatchFn<Field> = gte;
const AND_FN: DispatchFn<Field> = and;
const OR_FN: DispatchFn<Field> = or;

fn push_field<T>(
    bytecodes: &[usize],
    i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    record: &[T],
) -> Result<(), ExecError>
where
    T: Clone,
{
    stack.push(record[bytecodes[*i]].clone());
    *i += 1;
    Ok(())
}

fn push_lit<T>(
    bytecodes: &[usize],
    i: &mut usize,
    stack: &mut Vec<T>,
    literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Clone,
{
    stack.push(literals[bytecodes[*i]].clone());
    *i += 1;
    Ok(())
}

fn add<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Add<Output = Result<T, ExecError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l + r)?);
    Ok(())
}

fn sub<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Sub<Output = Result<T, ExecError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l - r)?);
    Ok(())
}

fn mul<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Mul<Output = Result<T, ExecError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l * r)?);
    Ok(())
}

fn div<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Div<Output = Result<T, ExecError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l / r)?);
    Ok(())
}

fn eq<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialEq + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l == r));
    Ok(())
}

fn neq<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialEq + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l != r));
    Ok(())
}

fn lt<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l < r));
    Ok(())
}

fn gt<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l > r));
    Ok(())
}

fn lte<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l <= r));
    Ok(())
}

fn gte<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l >= r));
    Ok(())
}

fn and<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialEq + Clone + And,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.and(&r)?);
    Ok(())
}

fn or<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialEq + Clone + Or,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.or(&r)?);
    Ok(())
}

/// Convert a physical expression to a bytecode expression.
/// This function may take in `col_id_to_idx` (mapping from the unique column ID to the
/// index of the column in the schema) to replace the column references in the physical
/// expression with the corresponding index in the schema.
///
/// # Arguments
///
/// * `expr` - The physical expression to convert.
///
/// * `col_id_to_idx` - The mapping from the unique column ID to the index of the column
///
/// # Returns
///
/// * `Result<ByteCodeExpr, CrustyError>` - The converted bytecode expression.
pub fn convert_expr_to_bytecode<P: PlanTrait>(
    expr: Expression<P>,
    col_id_to_idx: Option<&HashMap<ColumnId, ColumnId>>,
) -> Result<ByteCodeExpr, ExecError> {
    let mut bound_expr = expr;
    if let Some(col_id_to_idx) = col_id_to_idx {
        bound_expr = bound_expr.replace_variables(col_id_to_idx);
    }
    let mut bytecode_expr = ByteCodeExpr::new();
    convert_expr_to_bytecode_inner(&bound_expr, &mut bytecode_expr)?;
    Ok(bytecode_expr)
}

/// Helper function called by `convert_ast_to_bytecode` to recursively convert the
/// physical expression to a bytecode expression. This function assumes that the
/// column references in the physical expression have been replaced with the
/// corresponding index in the schema.
///
/// # Arguments
///
/// * `expr` - The physical expression to convert.
///
/// * `bytecode_expr` - A mutable reference to the bytecode expression to be
///   constructed.
///
/// # Returns
///
/// * `Result<(), CrustyError>` - Ok(()) if the conversion is successful
fn convert_expr_to_bytecode_inner<P: PlanTrait>(
    expr: &Expression<P>,
    bytecode_expr: &mut ByteCodeExpr,
) -> Result<(), ExecError> {
    match expr {
        Expression::Field { val: l } => {
            let i = bytecode_expr.add_literal(l.clone());
            bytecode_expr.add_code(ByteCodes::PushLit as usize);
            bytecode_expr.add_code(i);
        }
        Expression::Binary { op, left, right } => {
            // (a+b)-(c+d) Bytecode will be [a][b][+][c][d][+][-]
            // i, Stack
            // 0, [a]
            // 1, [a][b]
            // 2, [a+b]
            // 3, [a+b][c]
            // 4, [a+b][c][d]
            // 5, [a+b][c+d]
            // 6, [a+b-c-d]
            convert_expr_to_bytecode_inner(left, bytecode_expr)?;
            convert_expr_to_bytecode_inner(right, bytecode_expr)?;
            match op {
                BinaryOp::Add => bytecode_expr.add_code(ByteCodes::Add as usize),
                BinaryOp::Sub => bytecode_expr.add_code(ByteCodes::Sub as usize),
                BinaryOp::Mul => bytecode_expr.add_code(ByteCodes::Mul as usize),
                BinaryOp::Div => bytecode_expr.add_code(ByteCodes::Div as usize),
                BinaryOp::Eq => bytecode_expr.add_code(ByteCodes::Eq as usize),
                BinaryOp::Neq => bytecode_expr.add_code(ByteCodes::Neq as usize),
                BinaryOp::Gt => bytecode_expr.add_code(ByteCodes::Gt as usize),
                BinaryOp::Ge => bytecode_expr.add_code(ByteCodes::Gte as usize),
                BinaryOp::Lt => bytecode_expr.add_code(ByteCodes::Lt as usize),
                BinaryOp::Le => bytecode_expr.add_code(ByteCodes::Lte as usize),
                BinaryOp::And => bytecode_expr.add_code(ByteCodes::And as usize),
                BinaryOp::Or => bytecode_expr.add_code(ByteCodes::Or as usize),
            }
        }
        Expression::ColRef { id: i } => {
            bytecode_expr.add_code(ByteCodes::PushField as usize);
            bytecode_expr.add_code(*i);
        }
        // TODO: Currently does not support `Case` and `Subquery` physical expressions
        _ => {
            return Err(ExecError::Conversion(
                "Unsupported physical expression".to_string(),
            ))
        }
    }
    Ok(())
}

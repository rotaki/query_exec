use crate::expression::prelude::*;
use crate::tuple::{AsBool, IsNull};
use crate::{
    catalog::Schema,
    error::ExecError,
    tuple::{And, Field, FromBool, Or, Tuple},
    ColumnId,
};

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::{
    collections::HashMap,
    ops::{Add, Div, Mul, Sub},
};

pub enum ByteCodes {
    // CONTROL FLOW
    PushLit,
    PushField,
    // JUMP
    Jump,
    JumpIfTrue,
    JumpIfFalse,
    // COPY
    Duplicate,
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
    // IS_NULL
    IsNull,
}

const STATIC_DISPATCHER: [DispatchFn<Field>; 19] = [
    // CONTROL FLOW
    PUSH_LIT_FN,
    PUSH_FIELD_FN,
    // JUMP
    JUMP_FN,
    JUMP_IF_TRUE_FN,
    JUMP_IF_FALSE_FN,
    // COPY
    DUPLICATE_FN,
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
    // IS_NULL
    IS_NULL_FN,
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

    pub fn add_placeholder(&mut self) -> usize {
        let i = self.bytecodes.len();
        self.bytecodes.push(usize::MAX);
        i
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

    pub fn from_ast<P: PlanTrait>(
        expr: Expression<P>,
        col_id_to_idx: &HashMap<ColumnId, ColumnId>,
    ) -> Result<Self, ExecError> {
        AstToByteCode::<P>::to_bytecode(expr, col_id_to_idx)
    }
}

type DispatchFn<T> = fn(&[usize], &mut usize, &mut Vec<T>, &[T], &[T]) -> Result<(), ExecError>;
const PUSH_LIT_FN: DispatchFn<Field> = push_lit;
const PUSH_FIELD_FN: DispatchFn<Field> = push_field;
const JUMP_FN: DispatchFn<Field> = jump;
const JUMP_IF_TRUE_FN: DispatchFn<Field> = jump_if_true;
const JUMP_IF_FALSE_FN: DispatchFn<Field> = jump_if_false;
const DUPLICATE_FN: DispatchFn<Field> = duplicate;
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
const IS_NULL_FN: DispatchFn<Field> = is_null;

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

fn jump<T>(
    bytecodes: &[usize],
    i: &mut usize,
    _stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError> {
    *i = bytecodes[*i];
    Ok(())
}

fn jump_if_true<T>(
    bytecodes: &[usize],
    i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Clone + AsBool,
{
    let cond = stack.pop().unwrap();
    if cond.as_bool()? {
        *i = bytecodes[*i];
    } else {
        *i += 1;
    }
    Ok(())
}

fn jump_if_false<T>(
    bytecodes: &[usize],
    i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Clone + AsBool,
{
    let cond = stack.pop().unwrap();
    if !cond.as_bool()? {
        *i = bytecodes[*i];
    } else {
        *i += 1;
    }
    Ok(())
}

fn duplicate<T>(
    _bytecodes: &[usize],
    _i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: Clone,
{
    let v = stack.last().unwrap().clone();
    stack.push(v);
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
    T: PartialEq + Clone + And<Output = Result<T, ExecError>>,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.and(r)?);
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
    T: PartialEq + Clone + Or<Output = Result<T, ExecError>>,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.or(r)?);
    Ok(())
}

fn is_null<T>(
    bytecodes: &[usize],
    i: &mut usize,
    stack: &mut Vec<T>,
    literals: &[T],
    _record: &[T],
) -> Result<(), ExecError>
where
    T: PartialEq + Clone + IsNull + FromBool,
{
    let field = stack.pop().unwrap();
    stack.push(T::from_bool(field.is_null()));
    Ok(())
}

pub struct AstToByteCode<P: PlanTrait> {
    phantom: PhantomData<P>,
}

impl<P: PlanTrait> AstToByteCode<P> {
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
    pub fn to_bytecode(
        expr: Expression<P>,
        col_id_to_idx: &HashMap<ColumnId, ColumnId>,
    ) -> Result<ByteCodeExpr, ExecError> {
        let expr = expr.replace_variables(&col_id_to_idx);
        let mut bytecode_expr = ByteCodeExpr::new();
        convert_expr_to_bytecode(&expr, &mut bytecode_expr)?;
        Ok(bytecode_expr)
    }
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
fn convert_expr_to_bytecode<P: PlanTrait>(
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
            convert_expr_to_bytecode(left, bytecode_expr)?;
            convert_expr_to_bytecode(right, bytecode_expr)?;
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
        Expression::Case {
            expr,
            whens,
            else_expr,
        } => {
            if let Some(base) = expr {
                // [base][dup][when1][eq][jump_if_false][when2_addr][then1][jump_to_end]
                //       [dup][when2][eq][jump_if_false][when3_addr][then2][jump_to_end]...
                //       [else]
                let mut jump_end_ifs = Vec::new();

                convert_expr_to_bytecode(base, bytecode_expr)?;
                for (when, then) in whens {
                    bytecode_expr.add_code(ByteCodes::Duplicate as usize);
                    convert_expr_to_bytecode(when, bytecode_expr)?;
                    bytecode_expr.add_code(ByteCodes::Eq as usize);
                    bytecode_expr.add_code(ByteCodes::JumpIfFalse as usize);
                    let jump_if_false_addr = bytecode_expr.add_placeholder();
                    convert_expr_to_bytecode(then, bytecode_expr)?;
                    bytecode_expr.add_code(ByteCodes::Jump as usize);
                    let end_addr = bytecode_expr.add_placeholder();
                    jump_end_ifs.push(end_addr);
                    bytecode_expr.bytecodes[jump_if_false_addr] = end_addr + 1;
                }
                convert_expr_to_bytecode(else_expr, bytecode_expr)?;
                for addr in jump_end_ifs {
                    bytecode_expr.bytecodes[addr] = bytecode_expr.bytecodes.len();
                }
            } else {
                // [when1][jump_if_false][when2_addr][then1][jump_to_end]
                // [when2][jump_if_false][when3_addr][then2][jump_to_end]...
                // [else]
                let mut jump_end_ifs = Vec::new();

                for (when, then) in whens {
                    convert_expr_to_bytecode(when, bytecode_expr)?;
                    bytecode_expr.add_code(ByteCodes::JumpIfFalse as usize);
                    let jump_if_false_addr = bytecode_expr.add_placeholder();
                    convert_expr_to_bytecode(then, bytecode_expr)?;
                    bytecode_expr.add_code(ByteCodes::Jump as usize);
                    let end_addr = bytecode_expr.add_placeholder();
                    jump_end_ifs.push(end_addr);
                    bytecode_expr.bytecodes[jump_if_false_addr] = end_addr + 1;
                }

                convert_expr_to_bytecode(else_expr, bytecode_expr)?;
                for addr in jump_end_ifs {
                    bytecode_expr.bytecodes[addr] = bytecode_expr.bytecodes.len();
                }
            }
        }
        Expression::IsNull { expr } => {
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            bytecode_expr.add_code(ByteCodes::IsNull as usize);
        }
        Expression::Subquery { .. } => {
            unimplemented!("Subquery not supported in bytecode")
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::prelude::*;
    use crate::expression::Expression;
    use crate::tuple::Field;
    use std::collections::HashMap;
    use std::result;

    #[test]
    fn test_binary_operation() {
        let tuple = Tuple::from_fields(vec![1.into(), 2.into(), 3.into()]);
        // (idx0 + idx1) = idx2
        let expr = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expression::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expression::ColRef { id: 0 }),
                right: Box::new(Expression::ColRef { id: 1 }),
            }),
            right: Box::new(Expression::ColRef { id: 2 }),
        };
        let col_id_to_idx = HashMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // idx0 < idx1
        let expr = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Lt,
            left: Box::new(Expression::ColRef { id: 0 }),
            right: Box::new(Expression::ColRef { id: 1 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // idx0 > idx1
        let expr = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expression::ColRef { id: 0 }),
            right: Box::new(Expression::ColRef { id: 1 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));
    }

    #[test]
    fn test_is_null() {
        let tuple = Tuple::from_fields(vec![Field::Int(None), 2.into()]);
        // idx0 is null
        let expr = Expression::<PhysicalRelExpr>::IsNull {
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let col_id_to_idx = HashMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // idx1 is not null
        let expr = Expression::<PhysicalRelExpr>::IsNull {
            expr: Box::new(Expression::ColRef { id: 1 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));
    }

    #[test]
    fn test_case_whens() {
        // Case with base
        let tuple0 = Tuple::from_fields(vec![0.into()]);
        let tuple1 = Tuple::from_fields(vec![1.into()]);
        let tuple2 = Tuple::from_fields(vec![2.into()]);
        let tuple3 = Tuple::from_fields(vec![3.into()]);
        let tuple4 = Tuple::from_fields(vec![4.into()]);
        let tuple_null = Tuple::from_fields(vec![Field::Int(None)]);
        // Case idx0
        // when 1 then 10
        // when 2 then 20
        // when 3 then 30
        // else 40
        let expr = Expression::<PhysicalRelExpr>::Case {
            expr: Some(Box::new(Expression::ColRef { id: 0 })),
            whens: vec![
                (
                    Expression::Field { val: 1.into() },
                    Expression::Field { val: 10.into() },
                ),
                (
                    Expression::Field { val: 2.into() },
                    Expression::Field { val: 20.into() },
                ),
                (
                    Expression::Field { val: 3.into() },
                    Expression::Field { val: 30.into() },
                ),
            ],
            else_expr: Box::new(Expression::Field { val: 40.into() }),
        };
        let col_id_to_idx = HashMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result0 = bytecode_expr.eval(&tuple0).unwrap();
        assert_eq!(result0, Field::Int(Some(40)));
        let result1 = bytecode_expr.eval(&tuple1).unwrap();
        assert_eq!(result1, Field::Int(Some(10)));
        let result2 = bytecode_expr.eval(&tuple2).unwrap();
        assert_eq!(result2, Field::Int(Some(20)));
        let result3 = bytecode_expr.eval(&tuple3).unwrap();
        assert_eq!(result3, Field::Int(Some(30)));
        let result4 = bytecode_expr.eval(&tuple4).unwrap();
        assert_eq!(result4, Field::Int(Some(40)));
        let result_null = bytecode_expr.eval(&tuple_null).unwrap();
        assert_eq!(result_null, Field::Int(Some(40)));
    }

    #[test]
    fn test_whens() {
        // Case
        // when idx0 = 1 then 10
        // when idx0 = 2 then 20
        // when idx0 = 3 then 30
        // else 40
        let tuple0 = Tuple::from_fields(vec![0.into()]);
        let tuple1 = Tuple::from_fields(vec![1.into()]);
        let tuple2 = Tuple::from_fields(vec![2.into()]);
        let tuple3 = Tuple::from_fields(vec![3.into()]);
        let tuple4 = Tuple::from_fields(vec![4.into()]);
        let tuple_null = Tuple::from_fields(vec![Field::Int(None)]);

        let expr = Expression::<PhysicalRelExpr>::Case {
            expr: None,
            whens: vec![
                (
                    Expression::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(Expression::ColRef { id: 0 }),
                        right: Box::new(Expression::Field { val: 1.into() }),
                    },
                    Expression::Field { val: 10.into() },
                ),
                (
                    Expression::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(Expression::ColRef { id: 0 }),
                        right: Box::new(Expression::Field { val: 2.into() }),
                    },
                    Expression::Field { val: 20.into() },
                ),
                (
                    Expression::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(Expression::ColRef { id: 0 }),
                        right: Box::new(Expression::Field { val: 3.into() }),
                    },
                    Expression::Field { val: 30.into() },
                ),
                (
                    Expression::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(Expression::Field { val: 4.into() }),
                        right: Box::new(Expression::Field { val: 4.into() }),
                    },
                    Expression::Field { val: 40.into() },
                ),
            ],
            else_expr: Box::new(Expression::Field { val: 50.into() }),
        };
        let col_id_to_idx = HashMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result0 = bytecode_expr.eval(&tuple0).unwrap();
        assert_eq!(result0, Field::Int(Some(40)));
        let result1 = bytecode_expr.eval(&tuple1).unwrap();
        assert_eq!(result1, Field::Int(Some(10)));
        let result2 = bytecode_expr.eval(&tuple2).unwrap();
        assert_eq!(result2, Field::Int(Some(20)));
        let result3 = bytecode_expr.eval(&tuple3).unwrap();
        assert_eq!(result3, Field::Int(Some(30)));
        let result4 = bytecode_expr.eval(&tuple4).unwrap();
        assert_eq!(result4, Field::Int(Some(40)));
        let result_null = bytecode_expr.eval(&tuple_null).unwrap();
        assert_eq!(result_null, Field::Int(Some(40)));
    }
}

use crate::expression::{prelude::*, DateField};
use crate::prelude::DataType;
use crate::tuple::{AsBool, IsNull};
use crate::{
    error::ExecError,
    tuple::{And, Field, FromBool, Or, Tuple},
    ColumnId,
};

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::marker::PhantomData;

pub enum ByteCodes {
    // CONTROL FLOW
    PushLit,
    PushField,
    Pop,
    // JUMP
    Jump,
    JumpIfTrue,
    JumpIfFalse,
    JumpIfFalseOrNull,
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
    Not,
    // IS_NULL
    IsNull,
    // IS_BETWEEN
    IsBetween,
    // DATE
    ExtractYear,
    ExtractMonth,
    ExtractDay,
    // LIKE
    Like,
    // CAST
    Cast,
    // SUBSTRING
    Substring,
}

const STATIC_DISPATCHER: [DispatchFn; 29] = [
    // CONTROL FLOW
    PUSH_LIT_FN,
    PUSH_FIELD_FN,
    POP_FN,
    // JUMP
    JUMP_FN,
    JUMP_IF_TRUE_FN,
    JUMP_IF_FALSE_FN,
    JUMP_IF_FALSE_OR_NULL_FN,
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
    NOT_FN,
    // IS_NULL
    IS_NULL_FN,
    // IS_BETWEEN
    IS_BETWEEN_FN,
    // DATE
    EXTRACT_YEAR_FN,
    EXTRACT_MONTH_FN,
    EXTRACT_DAY_FN,
    // LIKE
    LIKE_FN,
    // CAST
    CAST_FN,
    // SUBSTRING
    SUBSTRING_FN,
];

// Utility functions
pub fn colidx_expr(colidx: usize) -> ByteCodeExpr {
    let mut expr = ByteCodeExpr::new();
    expr.add_code(ByteCodes::PushField as usize);
    expr.add_code(colidx);
    expr
}

type ByteCodeType = u16;

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ByteCodeExpr {
    pub bytecodes: Vec<ByteCodeType>,
    pub literals: Vec<Field>,
}

impl std::fmt::Display for ByteCodeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        s.push_str("ByteCodeExpr {");
        s.push_str("bytecodes: [");
        for i in 0..self.bytecodes.len().min(5) {
            s.push_str(&format!("{}, ", self.bytecodes[i]));
            if self.bytecodes.len() > 5 && i == 4 {
                s.push_str("...len=");
                s.push_str(&self.bytecodes.len().to_string());
                break;
            }
        }
        s.push_str("], literals: [");
        for i in 0..self.literals.len().min(5) {
            s.push_str(&format!("{}, ", self.literals[i]));
            if self.literals.len() > 5 && i == 4 {
                s.push_str("...len=");
                s.push_str(&self.literals.len().to_string());
                break;
            }
        }
        s.push_str("]}");
        write!(f, "{}", s)
    }
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
        self.bytecodes.push(ByteCodeType::MAX);
        i
    }

    pub fn add_code(&mut self, code: usize) {
        self.bytecodes.push(code as ByteCodeType);
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
        while (i as usize) < bytecodes.len() {
            let opcode = bytecodes[i as usize] as usize;
            i += 1;
            STATIC_DISPATCHER[opcode](bytecodes, &mut i, &mut stack, literals, record)?;
        }
        debug_assert_eq!(stack.len(), 1, "{:?}", stack);
        Ok(stack.pop().unwrap())
    }

    pub fn from_ast<P: PlanTrait>(
        expr: Expression<P>,
        col_id_to_idx: &BTreeMap<ColumnId, ColumnId>,
    ) -> Result<Self, ExecError> {
        AstToByteCode::<P>::to_bytecode(expr, col_id_to_idx)
    }
}

type DispatchFn = fn(
    &[ByteCodeType],
    &mut ByteCodeType,
    &mut Vec<Field>,
    &[Field],
    &[Field],
) -> Result<(), ExecError>;
const PUSH_LIT_FN: DispatchFn = push_lit;
const PUSH_FIELD_FN: DispatchFn = push_field;
const POP_FN: DispatchFn = pop;
const JUMP_FN: DispatchFn = jump;
const JUMP_IF_TRUE_FN: DispatchFn = jump_if_true;
const JUMP_IF_FALSE_FN: DispatchFn = jump_if_false;
const JUMP_IF_FALSE_OR_NULL_FN: DispatchFn = jump_if_false_or_null;
const DUPLICATE_FN: DispatchFn = duplicate;
const ADD_FN: DispatchFn = add;
const SUB_FN: DispatchFn = sub;
const MUL_FN: DispatchFn = mul;
const DIV_FN: DispatchFn = div;
const EQ_FN: DispatchFn = eq;
const NEQ_FN: DispatchFn = neq;
const LT_FN: DispatchFn = lt;
const GT_FN: DispatchFn = gt;
const LTE_FN: DispatchFn = lte;
const GTE_FN: DispatchFn = gte;
const AND_FN: DispatchFn = and;
const OR_FN: DispatchFn = or;
const NOT_FN: DispatchFn = not;
const IS_NULL_FN: DispatchFn = is_null;
const IS_BETWEEN_FN: DispatchFn = is_between;
const EXTRACT_YEAR_FN: DispatchFn = extract_year;
const EXTRACT_MONTH_FN: DispatchFn = extract_month;
const EXTRACT_DAY_FN: DispatchFn = extract_day;
const LIKE_FN: DispatchFn = like;
const CAST_FN: DispatchFn = cast;
const SUBSTRING_FN: DispatchFn = substring;

fn push_field(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    record: &[Field],
) -> Result<(), ExecError> {
    stack.push(record[bytecodes[*i as usize] as usize].clone());
    *i += 1;
    Ok(())
}

fn push_lit(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    stack.push(literals[bytecodes[*i as usize] as usize].clone());
    *i += 1;
    Ok(())
}

fn pop(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    stack.pop().unwrap();
    Ok(())
}

fn jump(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    _stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    *i = bytecodes[*i as usize];
    Ok(())
}

fn jump_if_true(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let cond = stack.pop().unwrap();
    if !cond.is_null() && cond.as_bool()? {
        *i = bytecodes[*i as usize];
    } else {
        *i += 1;
    }
    Ok(())
}

fn jump_if_false(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let cond = stack.pop().unwrap();
    if !cond.is_null() && !cond.as_bool()? {
        *i = bytecodes[*i as usize];
    } else {
        *i += 1;
    }
    Ok(())
}

fn jump_if_false_or_null(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let cond = stack.pop().unwrap();
    if cond.is_null() || !cond.as_bool()? {
        *i = bytecodes[*i as usize];
    } else {
        *i += 1;
    }
    Ok(())
}

fn duplicate(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let v = stack.last().unwrap().clone();
    stack.push(v);
    Ok(())
}

fn add(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l + r)?);
    Ok(())
}

fn sub(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l - r)?);
    Ok(())
}

fn mul(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l * r)?);
    Ok(())
}

fn div(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l / r)?);
    Ok(())
}

fn eq(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    let result = if r.is_null() || l.is_null() {
        Field::Boolean(None)
    } else {
        Field::from_bool(l == r)
    };
    stack.push(result);
    Ok(())
}

fn neq(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    let result = if r.is_null() || l.is_null() {
        Field::Boolean(None)
    } else {
        Field::from_bool(l != r)
    };
    stack.push(result);
    Ok(())
}

fn lt(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    let result = if r.is_null() || l.is_null() {
        Field::Boolean(None)
    } else {
        Field::from_bool(l < r)
    };
    stack.push(result);
    Ok(())
}

fn gt(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    let result = if r.is_null() || l.is_null() {
        Field::Boolean(None)
    } else {
        Field::from_bool(l > r)
    };
    stack.push(result);
    Ok(())
}

fn lte(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    let result = if r.is_null() || l.is_null() {
        Field::Boolean(None)
    } else {
        Field::from_bool(l <= r)
    };
    stack.push(result);
    Ok(())
}

fn gte(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    let result = if r.is_null() || l.is_null() {
        Field::Boolean(None)
    } else {
        Field::from_bool(l >= r)
    };
    stack.push(result);
    Ok(())
}

fn and(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.and(r)?);
    Ok(())
}

fn or(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.or(r)?);
    Ok(())
}

fn not(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let field = stack.pop().unwrap();
    if field.is_null() {
        stack.push(Field::Boolean(None));
    } else {
        stack.push(Field::from_bool(!field.as_bool()?));
    }
    Ok(())
}

fn is_null(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let field = stack.pop().unwrap();
    stack.push(Field::from_bool(field.is_null()));
    Ok(())
}

fn is_between(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let upper = stack.pop().unwrap();
    let lower = stack.pop().unwrap();
    let field = stack.pop().unwrap();
    let result = if field.is_null() || lower.is_null() || upper.is_null() {
        Field::Boolean(None)
    } else {
        Field::from_bool(field >= lower && field <= upper)
    };
    stack.push(result);
    Ok(())
}

fn extract_year(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let field = stack.pop().unwrap();
    stack.push(Field::Int(field.extract_year()?.map(|x| x as i64)));
    Ok(())
}

fn extract_month(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let field = stack.pop().unwrap();
    stack.push(Field::Int(field.extract_month()?.map(|x| x as i64)));
    Ok(())
}

fn extract_day(
    _bytecodes: &[ByteCodeType],
    _i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let field = stack.pop().unwrap();
    stack.push(Field::Int(field.extract_day()?.map(|x| x as i64)));
    Ok(())
}

fn like(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let pattern = &literals[bytecodes[*i as usize] as usize];
    *i += 1;
    let escape = &literals[bytecodes[*i as usize] as usize];
    *i += 1;
    let field = stack.pop().unwrap();
    if pattern.is_null() || field.is_null() {
        stack.push(Field::Boolean(None)); // NULL
    } else {
        let field_str = field.as_string().as_ref().unwrap();
        let pattern_str = pattern.as_string().as_ref().unwrap();
        let escape_str = escape.as_string();
        // %: Any string of zero or more characters.
        // _: Any single character.
        // [ ]: Any single character within the specified range ([a-f]) or set ([abcdef]).
        // [^]: Any single character not within the specified range ([^a-f]) or set ([^abcdef]).
        let result = is_like(&field_str, &pattern_str, &escape_str);
        stack.push(Field::Boolean(Some(result)));
    }
    Ok(())
}

fn cast(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let field = stack.pop().unwrap();
    let cast_type = bytecodes[*i as usize] as usize;
    *i += 1;
    let data_type = DataType::from(cast_type);
    let result = field.cast(&data_type)?;
    stack.push(result);
    Ok(())
}

fn substring(
    bytecodes: &[ByteCodeType],
    i: &mut ByteCodeType,
    stack: &mut Vec<Field>,
    _literals: &[Field],
    _record: &[Field],
) -> Result<(), ExecError> {
    let field = stack.pop().unwrap();
    let start = bytecodes[*i as usize] as usize;
    *i += 1;
    let len = bytecodes[*i as usize] as usize;
    *i += 1;
    let result = field.substring(start, len)?;
    stack.push(result);
    Ok(())
}

// Helper function to check if a field matches a LIKE pattern
fn is_like(field: &str, pattern: &str, escape_str: &Option<String>) -> bool {
    if pattern.is_empty() {
        return field.is_empty();
    }
    // Convert SQL LIKE pattern to Rust regex pattern
    let mut regex_pattern = String::new();
    // Add a ^ to match the start of the string
    regex_pattern.push('^');
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push_str("."),
            _ => {
                if let Some(esc) = escape_str {
                    // If the character is escaped, we need to check if the next character is a special character.
                    // If it is, and matches our escape character, treat the following character literally.
                    if esc == &c.to_string()
                        && chars
                            .peek()
                            .map_or(false, |&next| next == '%' || next == '_')
                    {
                        regex_pattern.push('\\'); // Escape the next special character in regex
                        if let Some(next) = chars.next() {
                            regex_pattern.push(next);
                            continue;
                        } else {
                            break;
                        }
                    }
                }
                // If no special conditions were met, escape the current character if necessary
                regex_pattern.push_str(&regex::escape(&c.to_string()));
            }
        }
    }
    // Compile the regex pattern
    let regex = regex::Regex::new(&regex_pattern).unwrap();
    // Check if the field matches the pattern
    regex.is_match(field)
}

pub struct AstToByteCode<P: PlanTrait> {
    phantom: PhantomData<P>,
}

impl<P: PlanTrait + PartialEq> AstToByteCode<P> {
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
        col_id_to_idx: &BTreeMap<ColumnId, ColumnId>,
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
            if whens.is_empty() {
                return Err(ExecError::Conversion(
                    "Case expression must have at least one when clause".to_string(),
                ));
            }
            if let Some(base) = expr {
                // [base][dup][when1][eq][jump_if_false][when2_addr][pop][then1][jump_to_end]
                //       [dup][when2][eq][jump_if_false][when3_addr][pop][then2][jump_to_end]...
                //       [pop][else]
                let mut jump_end_ifs = Vec::with_capacity(whens.len());

                convert_expr_to_bytecode(base, bytecode_expr)?;
                for (when, then) in whens {
                    bytecode_expr.add_code(ByteCodes::Duplicate as usize);
                    if let Expression::Field { val } = when {
                        if val.is_null() {
                            bytecode_expr.add_code(ByteCodes::IsNull as usize);
                        } else {
                            convert_expr_to_bytecode(when, bytecode_expr)?;
                            bytecode_expr.add_code(ByteCodes::Eq as usize);
                        }
                    } else {
                        convert_expr_to_bytecode(when, bytecode_expr)?;
                        bytecode_expr.add_code(ByteCodes::Eq as usize);
                    }
                    bytecode_expr.add_code(ByteCodes::JumpIfFalseOrNull as usize);
                    let jump_if_false_addr = bytecode_expr.add_placeholder();
                    bytecode_expr.add_code(ByteCodes::Pop as usize);
                    convert_expr_to_bytecode(then, bytecode_expr)?;
                    bytecode_expr.add_code(ByteCodes::Jump as usize);
                    let end_addr = bytecode_expr.add_placeholder();
                    jump_end_ifs.push(end_addr);
                    bytecode_expr.bytecodes[jump_if_false_addr] = end_addr as ByteCodeType + 1;
                }
                bytecode_expr.add_code(ByteCodes::Pop as usize);
                if let Some(else_expr) = else_expr {
                    convert_expr_to_bytecode(else_expr, bytecode_expr)?;
                } else {
                    let i = bytecode_expr.add_literal(Field::null(&DataType::Unknown));
                    bytecode_expr.add_code(ByteCodes::PushLit as usize);
                    bytecode_expr.add_code(i);
                }
                for addr in jump_end_ifs {
                    bytecode_expr.bytecodes[addr] = bytecode_expr.bytecodes.len() as ByteCodeType;
                }
            } else {
                // [when1][jump_if_false][when2_addr][then1][jump_to_end]
                // [when2][jump_if_false][when3_addr][then2][jump_to_end]...
                // [else]
                let mut jump_end_ifs = Vec::with_capacity(whens.len());

                for (when, then) in whens {
                    convert_expr_to_bytecode(when, bytecode_expr)?;
                    bytecode_expr.add_code(ByteCodes::JumpIfFalseOrNull as usize);
                    let jump_if_false_addr = bytecode_expr.add_placeholder();
                    convert_expr_to_bytecode(then, bytecode_expr)?;
                    bytecode_expr.add_code(ByteCodes::Jump as usize);
                    let end_addr = bytecode_expr.add_placeholder();
                    jump_end_ifs.push(end_addr);
                    bytecode_expr.bytecodes[jump_if_false_addr] = end_addr as ByteCodeType + 1;
                }
                // If else_expr is None, we should push a NULL value to the stack
                // to maintain the stack invariant
                if let Some(else_expr) = else_expr {
                    convert_expr_to_bytecode(else_expr, bytecode_expr)?;
                } else {
                    let i = bytecode_expr.add_literal(Field::null(&DataType::Unknown));
                    bytecode_expr.add_code(ByteCodes::PushLit as usize);
                    bytecode_expr.add_code(i);
                }

                for addr in jump_end_ifs {
                    bytecode_expr.bytecodes[addr] = bytecode_expr.bytecodes.len() as ByteCodeType;
                }
            }
        }
        Expression::Between { expr, lower, upper } => {
            // [expr][lower][upper][is_between]
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            convert_expr_to_bytecode(lower, bytecode_expr)?;
            convert_expr_to_bytecode(upper, bytecode_expr)?;
            bytecode_expr.add_code(ByteCodes::IsBetween as usize);
        }
        Expression::IsNull { expr } => {
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            bytecode_expr.add_code(ByteCodes::IsNull as usize);
        }
        Expression::Extract { field, expr } => {
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            match field {
                DateField::Year => {
                    bytecode_expr.add_code(ByteCodes::ExtractYear as usize);
                }
                DateField::Month => {
                    bytecode_expr.add_code(ByteCodes::ExtractMonth as usize);
                }
                DateField::Day => {
                    bytecode_expr.add_code(ByteCodes::ExtractDay as usize);
                }
            }
        }
        Expression::Like {
            expr,
            pattern,
            escape,
        } => {
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            bytecode_expr.add_code(ByteCodes::Like as usize);
            let pattern_idx = bytecode_expr.add_literal(Field::String(Some(pattern.clone())));
            let escape_idx = bytecode_expr.add_literal(Field::String(escape.clone()));
            bytecode_expr.add_code(pattern_idx);
            bytecode_expr.add_code(escape_idx);
        }
        Expression::Cast { expr, to_type } => {
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            bytecode_expr.add_code(ByteCodes::Cast as usize);
            bytecode_expr.add_code(to_type.clone() as usize);
        }
        Expression::InList { expr, list } => {
            // [base][dup][val1][eq][jump_if_true][ok]
            //       [dup][val2][eq][jump_if_true][ok]
            //       ...
            //       [pop][push_lit][false][jump][end][(ok)pop][push_lit][true][(end)]
            let mut jump_end_ifs = Vec::with_capacity(list.len());
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            for val in list {
                bytecode_expr.add_code(ByteCodes::Duplicate as usize);
                convert_expr_to_bytecode(val, bytecode_expr)?;
                bytecode_expr.add_code(ByteCodes::Eq as usize);
                bytecode_expr.add_code(ByteCodes::JumpIfTrue as usize);
                let jump_if_true_addr = bytecode_expr.add_placeholder();
                jump_end_ifs.push(jump_if_true_addr);
            }
            bytecode_expr.add_code(ByteCodes::Pop as usize);
            let i = bytecode_expr.add_literal(Field::from_bool(false));
            bytecode_expr.add_code(ByteCodes::PushLit as usize);
            bytecode_expr.add_code(i);
            bytecode_expr.add_code(ByteCodes::Jump as usize);
            let end_addr = bytecode_expr.add_placeholder();
            for addr in jump_end_ifs {
                bytecode_expr.bytecodes[addr] = bytecode_expr.bytecodes.len() as ByteCodeType;
            }
            bytecode_expr.add_code(ByteCodes::Pop as usize);
            let i = bytecode_expr.add_literal(Field::from_bool(true));
            bytecode_expr.add_code(ByteCodes::PushLit as usize);
            bytecode_expr.add_code(i);
            bytecode_expr.bytecodes[end_addr] = bytecode_expr.bytecodes.len() as ByteCodeType;
        }
        Expression::Not { expr } => {
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            bytecode_expr.add_code(ByteCodes::Not as usize);
        }
        Expression::Substring { expr, start, len } => {
            convert_expr_to_bytecode(expr, bytecode_expr)?;
            bytecode_expr.add_code(ByteCodes::Substring as usize);
            bytecode_expr.add_code(*start);
            bytecode_expr.add_code(*len);
        }
        Expression::Subquery { .. }
        | Expression::UncorrelatedAny { .. }
        | Expression::UncorrelatedExists { .. } => {
            unimplemented!("Subquery not supported in bytecode")
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::Expression;
    use crate::tuple::Field;
    use std::collections::HashMap;

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
        let col_id_to_idx = BTreeMap::new();
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
    fn test_and_or() {
        let tuple = Tuple::from_fields(vec![Field::from_bool(true), Field::from_bool(false)]);
        // idx0 and idx1
        let expr = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::And,
            left: Box::new(Expression::ColRef { id: 0 }),
            right: Box::new(Expression::ColRef { id: 1 }),
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));

        // idx0 or idx1
        let expr = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expression::ColRef { id: 0 }),
            right: Box::new(Expression::ColRef { id: 1 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));
    }

    #[test]
    fn test_is_null() {
        let tuple = Tuple::from_fields(vec![Field::Int(None), 2.into()]);
        // idx0 is null
        let expr = Expression::<PhysicalRelExpr>::IsNull {
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let col_id_to_idx = BTreeMap::new();
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
            else_expr: Some(Box::new(Expression::Field { val: 40.into() })),
        };
        let col_id_to_idx = BTreeMap::new();
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

        // Case idx0
        // when 1 then 10

        let expr = Expression::<PhysicalRelExpr>::Case {
            expr: Some(Box::new(Expression::ColRef { id: 0 })),
            whens: vec![(
                Expression::Field { val: 1.into() },
                Expression::Field { val: 10.into() },
            )],
            else_expr: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result0 = bytecode_expr.eval(&tuple0).unwrap();
        assert_eq!(result0, Field::null(&DataType::Unknown));
        let result1 = bytecode_expr.eval(&tuple1).unwrap();
        assert_eq!(result1, Field::Int(Some(10)));
        let result2 = bytecode_expr.eval(&tuple2).unwrap();
        assert_eq!(result2, Field::null(&DataType::Unknown));
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
            else_expr: Some(Box::new(Expression::Field { val: 50.into() })),
        };
        let col_id_to_idx = BTreeMap::new();
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

        // Case
        // when idx0 = 1 then 10

        let expr = Expression::<PhysicalRelExpr>::Case {
            expr: None,
            whens: vec![(
                Expression::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expression::ColRef { id: 0 }),
                    right: Box::new(Expression::Field { val: 1.into() }),
                },
                Expression::Field { val: 10.into() },
            )],
            else_expr: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result0 = bytecode_expr.eval(&tuple0).unwrap();
        assert_eq!(result0, Field::null(&DataType::Unknown));
        let result1 = bytecode_expr.eval(&tuple1).unwrap();
        assert_eq!(result1, Field::Int(Some(10)));
        let result2 = bytecode_expr.eval(&tuple2).unwrap();
        assert_eq!(result2, Field::null(&DataType::Unknown));
    }

    #[test]
    fn test_between() {
        let tuple0 = Tuple::from_fields(vec![0.into()]);
        let tuple1 = Tuple::from_fields(vec![1.into()]);
        let tuple2 = Tuple::from_fields(vec![2.into()]);
        let tuple3 = Tuple::from_fields(vec![3.into()]);
        let tuple4 = Tuple::from_fields(vec![4.into()]);
        let tuple_null = Tuple::from_fields(vec![Field::Int(None)]);

        // idx0 between 1 and 3
        let expr = Expression::<PhysicalRelExpr>::Between {
            expr: Box::new(Expression::ColRef { id: 0 }),
            lower: Box::new(Expression::Field { val: 1.into() }),
            upper: Box::new(Expression::Field { val: 3.into() }),
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result0 = bytecode_expr.eval(&tuple0).unwrap();
        assert_eq!(result0, Field::from_bool(false));
        let result1 = bytecode_expr.eval(&tuple1).unwrap();
        assert_eq!(result1, Field::from_bool(true));
        let result2 = bytecode_expr.eval(&tuple2).unwrap();
        assert_eq!(result2, Field::from_bool(true));
        let result3 = bytecode_expr.eval(&tuple3).unwrap();
        assert_eq!(result3, Field::from_bool(true));
        let result4 = bytecode_expr.eval(&tuple4).unwrap();
        assert_eq!(result4, Field::from_bool(false));
        let result_null = bytecode_expr.eval(&tuple_null).unwrap();
        assert_eq!(result_null, Field::Boolean(None));
    }

    #[test]
    fn test_extract() {
        let tuple = Tuple::from_fields(vec![(2021, 1, 1).into()]);
        // Extract year from idx0
        let expr = Expression::<PhysicalRelExpr>::Extract {
            field: DateField::Year,
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Int(Some(2021)));

        // Extract month from idx0
        let expr = Expression::<PhysicalRelExpr>::Extract {
            field: DateField::Month,
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Int(Some(1)));

        // Extract day from idx0
        let expr = Expression::<PhysicalRelExpr>::Extract {
            field: DateField::Day,
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Int(Some(1)));
    }

    #[test]
    fn test_like() {
        let tuple = Tuple::from_fields(vec!["hello world".into()]);
        // Simple match hello with hello
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "hello world".to_string(),
            escape: None,
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // Percent wildcard
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "hello %".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // Percent wildcard many
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "%lo %".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // Underscore wildcard
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "hello _orld".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // Underscore wildcard many
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "hello _____".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // Escape character
        let tuple = Tuple::from_fields(vec!["hello %world".into()]);
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "hello k%w_r%".to_string(),
            escape: Some("k".to_string()),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // No match
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "world%".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));

        // Empty pattern
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));

        // Empty string
        let tuple = Tuple::from_fields(vec!["".into()]);
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "%".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "_".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));

        // Null string
        let tuple = Tuple::from_fields(vec![Field::String(None)]);
        let expr = Expression::<PhysicalRelExpr>::Like {
            expr: Box::new(Expression::ColRef { id: 0 }),
            pattern: "hello".to_string(),
            escape: None,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Boolean(None));
    }

    #[test]
    fn test_cast() {
        let tuple = Tuple::from_fields(vec![1.into()]);
        // Cast idx0 to string
        let expr = Expression::<PhysicalRelExpr>::Cast {
            expr: Box::new(Expression::ColRef { id: 0 }),
            to_type: DataType::String,
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::String(Some("1".to_string())));

        // Cast idx0 to boolean
        let expr = Expression::<PhysicalRelExpr>::Cast {
            expr: Box::new(Expression::ColRef { id: 0 }),
            to_type: DataType::Boolean,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Boolean(Some(true)));

        // Cast idx0 to int
        let expr = Expression::<PhysicalRelExpr>::Cast {
            expr: Box::new(Expression::ColRef { id: 0 }),
            to_type: DataType::Int,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Int(Some(1)));

        // Cast idx0 to float
        let expr = Expression::<PhysicalRelExpr>::Cast {
            expr: Box::new(Expression::ColRef { id: 0 }),
            to_type: DataType::Float,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Float(Some(1.0)));

        // Cast idx0 to date
        let expr = Expression::<PhysicalRelExpr>::Cast {
            expr: Box::new(Expression::ColRef { id: 0 }),
            to_type: DataType::Date,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Date(None));

        // Float to Int
        let tuple = Tuple::from_fields(vec![1.5.into()]);
        let expr = Expression::<PhysicalRelExpr>::Cast {
            expr: Box::new(Expression::ColRef { id: 0 }),
            to_type: DataType::Int,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Int(Some(1)));
    }

    #[test]
    fn test_in_list() {
        let tuple = Tuple::from_fields(vec![1.into()]);
        // idx0 in (1, 2, 3)
        let expr = Expression::<PhysicalRelExpr>::InList {
            expr: Box::new(Expression::ColRef { id: 0 }),
            list: vec![
                Expression::Field { val: 1.into() },
                Expression::Field { val: 2.into() },
                Expression::Field { val: 3.into() },
            ],
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        // idx0 in (2, 3, 4)
        let expr = Expression::<PhysicalRelExpr>::InList {
            expr: Box::new(Expression::ColRef { id: 0 }),
            list: vec![
                Expression::Field { val: 2.into() },
                Expression::Field { val: 3.into() },
                Expression::Field { val: 4.into() },
            ],
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));

        // idx0 in ()
        let expr = Expression::<PhysicalRelExpr>::InList {
            expr: Box::new(Expression::ColRef { id: 0 }),
            list: vec![],
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));

        // NULL in (1, 2, 3, NULL)
        // This should be false because NULL != NULL
        let tuple = Tuple::from_fields(vec![Field::Int(None)]);
        let expr = Expression::<PhysicalRelExpr>::InList {
            expr: Box::new(Expression::ColRef { id: 0 }),
            list: vec![
                Expression::Field { val: 1.into() },
                Expression::Field { val: 2.into() },
                Expression::Field { val: 3.into() },
            ],
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));
    }

    #[test]
    fn test_not() {
        let tuple = Tuple::from_fields(vec![Field::from_bool(true)]);
        // not idx0
        let expr = Expression::<PhysicalRelExpr>::Not {
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(false));

        let tuple = Tuple::from_fields(vec![Field::from_bool(false)]);
        // not idx0
        let expr = Expression::<PhysicalRelExpr>::Not {
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::from_bool(true));

        let tuple = Tuple::from_fields(vec![Field::Boolean(None)]);
        // not idx0
        let expr = Expression::<PhysicalRelExpr>::Not {
            expr: Box::new(Expression::ColRef { id: 0 }),
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::Boolean(None));
    }

    #[test]
    fn test_substring() {
        let tuple = Tuple::from_fields(vec!["hello world".into()]);
        // Substring idx0 from 0 to 5
        let expr = Expression::<PhysicalRelExpr>::Substring {
            expr: Box::new(Expression::ColRef { id: 0 }),
            start: 0,
            len: 5,
        };
        let col_id_to_idx = BTreeMap::new();
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::String(Some("hello".to_string())));

        // Substring idx0 from 6 to 5
        let expr = Expression::<PhysicalRelExpr>::Substring {
            expr: Box::new(Expression::ColRef { id: 0 }),
            start: 6,
            len: 5,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::String(Some("world".to_string())));

        // Substring idx0 from 6 to 100
        let expr = Expression::<PhysicalRelExpr>::Substring {
            expr: Box::new(Expression::ColRef { id: 0 }),
            start: 6,
            len: 100,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::String(Some("world".to_string())));

        // Substring idx0 from 100 to 5
        let expr = Expression::<PhysicalRelExpr>::Substring {
            expr: Box::new(Expression::ColRef { id: 0 }),
            start: 100,
            len: 5,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::String(Some("".to_string())));

        // Substring idx0 from 0 to 100
        let expr = Expression::<PhysicalRelExpr>::Substring {
            expr: Box::new(Expression::ColRef { id: 0 }),
            start: 0,
            len: 100,
        };
        let bytecode_expr = ByteCodeExpr::from_ast(expr, &col_id_to_idx).unwrap();
        let result = bytecode_expr.eval(&tuple).unwrap();
        assert_eq!(result, Field::String(Some("hello world".to_string())));
    }
}

mod logical;
mod physical;

use crate::{prelude::DataType, tuple::Field};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::Hash,
    result,
};

pub use crate::ColumnId;

pub mod prelude {
    pub use super::logical::{HeuristicRule, HeuristicRules, HeuristicRulesRef, LogicalRelExpr};
    pub use super::physical::{LogicalToPhysicalRelExpr, PhysicalRelExpr};
    pub use super::PrePostVisitor;
    pub use super::{AggOp, BinaryOp, ColumnId, Expression, JoinType, PlanTrait};
}

// This `plan` is implemented by logical (LogicalRelExpr) and physical (PhysicalRelExpr) relational expressions.
pub trait PlanTrait: Clone + std::fmt::Debug + PartialEq {
    /// Replace the variables in the current plan with the dest_ids in the `src_to_dest` map.
    fn replace_variables(self, src_to_dest: &HashMap<ColumnId, ColumnId>) -> Self;

    /// Print the current plan with the given indentation by modifying the `out` string.
    fn print_inner(&self, indent: usize, out: &mut String);

    /// Get the set of columns that the expression node currently has.
    /// For example, `scan` would return all the columns that it reads from the table.
    /// `project` would return the columns that it projects.
    fn att(&self) -> HashSet<ColumnId>;

    /// Get the set of columns that are not bound in the expression
    /// For example, `scan` would return an empty set as all the columns are bound.
    /// For `project`, it would return columns that are not bound by determining
    /// the set difference between 1) the bounded columns returned by the child
    /// and 2) the columns that are projected and the free columns from the child.
    fn free(&self) -> HashSet<ColumnId>;
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Sub => write!(f, "-"),
            BinaryOp::Mul => write!(f, "*"),
            BinaryOp::Div => write!(f, "/"),
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::Neq => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::Le => write!(f, "<="),
            BinaryOp::Ge => write!(f, ">="),
            BinaryOp::And => write!(f, "&&"),
            BinaryOp::Or => write!(f, "||"),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggOp {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl std::fmt::Display for AggOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use AggOp::*;
        match self {
            Avg => write!(f, "AVG"),
            Count => write!(f, "COUNT"),
            Max => write!(f, "MAX"),
            Min => write!(f, "MIN"),
            Sum => write!(f, "SUM"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    CrossJoin,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    // Mark join is a special type of join that is used to evaluate Uncorrelated Subqueries.
    // Left mark join adds a mark column to the left side of the join which indicates if
    // the row is matched by the right side of the join. If there is a match, the mark column
    // is set to true. Otherwise, if the right side of the join does not contain NULL values,
    // the mark column is set to false. If the right side of the join contains NULL values,
    // the mark column is set to NULL. The same applies to the right mark join.
    // See more at:
    // * https://www.btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // * https://www.youtube.com/watch?v=ajpg_pMX620
    LeftMarkJoin(ColumnId),
    RightMarkJoin(ColumnId),
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "inner"),
            JoinType::LeftOuter => write!(f, "left_outer"),
            JoinType::RightOuter => write!(f, "right_outer"),
            JoinType::FullOuter => write!(f, "full_outer"),
            JoinType::CrossJoin => write!(f, "cross"),
            JoinType::LeftSemi => write!(f, "left_semi"),
            JoinType::RightSemi => write!(f, "right_semi"),
            JoinType::LeftAnti => write!(f, "left_anti"),
            JoinType::RightAnti => write!(f, "right_anti"),
            JoinType::LeftMarkJoin(col_id) => write!(f, "left_mark@{}", col_id),
            JoinType::RightMarkJoin(col_id) => write!(f, "right_mark@{}", col_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum DateField {
    Year,
    Month,
    Day,
}

#[derive(Debug, Clone, Hash, PartialEq)]
pub enum Expression<P: PlanTrait> {
    ColRef {
        id: ColumnId,
    },
    Field {
        val: Field,
    },
    IsNull {
        expr: Box<Expression<P>>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expression<P>>,
        right: Box<Expression<P>>,
    },
    Between {
        expr: Box<Expression<P>>,
        lower: Box<Expression<P>>,
        upper: Box<Expression<P>>,
    },
    Case {
        expr: Option<Box<Expression<P>>>,
        whens: Vec<(Expression<P>, Expression<P>)>,
        else_expr: Option<Box<Expression<P>>>,
    },
    Extract {
        field: DateField,
        expr: Box<Expression<P>>,
    },
    Like {
        expr: Box<Expression<P>>,
        pattern: String,
        escape: Option<String>,
    },
    Cast {
        expr: Box<Expression<P>>,
        to_type: DataType,
    },
    InList {
        expr: Box<Expression<P>>,
        list: Vec<Expression<P>>,
    },
    Not {
        expr: Box<Expression<P>>,
    },
    Substring {
        expr: Box<Expression<P>>,
        start: usize,
        len: usize,
    },
    Subquery {
        expr: Box<P>,
    },
    UncorrelatedAny {
        left: Box<Expression<P>>,
        comp: BinaryOp,
        right: Box<P>,
    },
    UncorrelatedExists {
        expr: Box<P>,
    },
}

impl<P: PlanTrait> Expression<P> {
    pub fn col_ref(id: ColumnId) -> Expression<P> {
        Expression::ColRef { id }
    }

    pub fn int(val: i64) -> Expression<P> {
        Expression::Field {
            val: Field::Int(Some(val)),
        }
    }

    pub fn string(val: String) -> Expression<P> {
        Expression::Field {
            val: Field::String(Some(val)),
        }
    }

    pub fn bool(val: bool) -> Expression<P> {
        Expression::Field {
            val: Field::Boolean(Some(val)),
        }
    }

    pub fn date(val: NaiveDate) -> Expression<P> {
        Expression::Field {
            val: Field::Date(Some(val)),
        }
    }

    pub fn months(val: u32) -> Expression<P> {
        Expression::Field {
            val: Field::Months(Some(val)),
        }
    }

    pub fn days(val: u64) -> Expression<P> {
        Expression::Field {
            val: Field::Days(Some(val)),
        }
    }

    pub fn float(val: f64) -> Expression<P> {
        Expression::Field {
            val: Field::Float(Some(val)),
        }
    }

    pub fn is_null(self) -> Expression<P> {
        Expression::IsNull {
            expr: Box::new(self),
        }
    }

    pub fn binary(op: BinaryOp, left: Expression<P>, right: Expression<P>) -> Expression<P> {
        if matches!(op, BinaryOp::Or) {
            if left == right {
                return left;
            }
        } else if matches!(op, BinaryOp::And) {
            if left == right {
                return left;
            }
        }
        Expression::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    pub fn eq(self, other: Expression<P>) -> Expression<P> {
        Expression::Binary {
            op: BinaryOp::Eq,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    pub fn neq(self, other: Expression<P>) -> Expression<P> {
        Expression::Binary {
            op: BinaryOp::Neq,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    pub fn add(self, other: Expression<P>) -> Expression<P> {
        Expression::Binary {
            op: BinaryOp::Add,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    pub fn extract(self, field: DateField) -> Expression<P> {
        Expression::Extract {
            field,
            expr: Box::new(self),
        }
    }

    pub fn like(self, pattern: String, escape: Option<String>) -> Expression<P> {
        Expression::Like {
            expr: Box::new(self),
            pattern,
            escape,
        }
    }

    pub fn case(
        expr: Option<Expression<P>>,
        whens: Vec<(Expression<P>, Expression<P>)>,
        else_expr: Option<Expression<P>>,
    ) -> Expression<P> {
        Expression::Case {
            expr: expr.map(Box::new),
            whens,
            else_expr: else_expr.map(Box::new),
        }
    }

    pub fn cast(self, to_type: DataType) -> Expression<P> {
        Expression::Cast {
            expr: Box::new(self),
            to_type,
        }
    }

    pub fn in_list(self, list: Vec<Expression<P>>) -> Expression<P> {
        Expression::InList {
            expr: Box::new(self),
            list,
        }
    }

    pub fn not(self) -> Expression<P> {
        Expression::Not {
            expr: Box::new(self),
        }
    }

    pub fn substring(self, start: usize, len: usize) -> Expression<P> {
        Expression::Substring {
            expr: Box::new(self),
            start,
            len,
        }
    }

    pub fn subquery(expr: P) -> Expression<P> {
        Expression::Subquery {
            expr: Box::new(expr),
        }
    }

    pub fn uncorrelated_any(left: Expression<P>, comp: BinaryOp, right: P) -> Expression<P> {
        Expression::UncorrelatedAny {
            left: Box::new(left),
            comp,
            right: Box::new(right),
        }
    }

    pub fn uncorrelated_exist(expr: P) -> Expression<P> {
        Expression::UncorrelatedExists {
            expr: Box::new(expr),
        }
    }

    pub fn between(self, lower: Expression<P>, upper: Expression<P>) -> Expression<P> {
        Expression::Between {
            expr: Box::new(self),
            lower: Box::new(lower),
            upper: Box::new(upper),
        }
    }

    pub fn has_subquery(&self) -> bool {
        match self {
            Expression::ColRef { id: _ } => false,
            Expression::Field { val: _ } => false,
            Expression::IsNull { expr } => expr.has_subquery(),
            Expression::Binary { left, right, .. } => left.has_subquery() || right.has_subquery(),
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => {
                expr.as_ref().map_or(false, |expr| expr.has_subquery())
                    || whens
                        .iter()
                        .any(|(when, then)| when.has_subquery() || then.has_subquery())
                    || else_expr.as_ref().map_or(false, |expr| expr.has_subquery())
            }
            Expression::Between { expr, lower, upper } => {
                expr.has_subquery() || lower.has_subquery() || upper.has_subquery()
            }
            Expression::Extract { field: _, expr } => expr.has_subquery(),
            Expression::Like {
                expr,
                pattern,
                escape,
            } => expr.has_subquery(),
            Expression::Cast { expr, to_type } => expr.has_subquery(),
            Expression::InList { expr, list } => {
                expr.has_subquery() || list.iter().any(|expr| expr.has_subquery())
            }
            Expression::Not { expr } => expr.has_subquery(),
            Expression::Substring { expr, start, len } => expr.has_subquery(),
            Expression::Subquery { expr: _ } => true,
            Expression::UncorrelatedAny { left, comp, right } => true,
            Expression::UncorrelatedExists { expr } => true,
        }
    }

    pub fn split_conjunction(self) -> Vec<Expression<P>> {
        match self {
            Expression::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                let mut left = left.split_conjunction();
                let mut right = right.split_conjunction();
                left.append(&mut right);
                left
            }
            _ => vec![self],
        }
    }

    pub fn merge_conjunction(exprs: Vec<Expression<P>>) -> Expression<P> {
        if exprs.is_empty() {
            panic!("Cannot merge an empty list of expressions");
        }
        let mut iter = exprs.into_iter();
        let first = iter.next().unwrap();
        iter.fold(first, |acc, expr| {
            Expression::binary(BinaryOp::And, acc, expr)
        })
    }

    /// Extract the bounded predicates from the given predicates.
    /// The bounded predicates are the predicates that are bound by the given bound columns.
    ///
    /// For example:
    /// Bound: a, b
    /// (a = 1 AND c = 2) OR  (b = 3 AND d = 4) => (a = 1) OR (b = 3)
    /// (a = 1 AND b = 2) OR  (c = 3 AND d = 4) => (a = 1 AND b = 2) OR empty => empty
    /// (a = 1 AND b = 2) OR  (a = 3 AND b = 4) => (a = 1 AND b = 2) OR (a = 3 AND b = 4)
    /// (a = 1 OR  c = 2) OR  (b = 3 AND d = 4) => empty OR b = 3 => empty
    /// (a = 1 OR  c = 2) AND (b = 3 AND d = 4) => empty AND b = 3 => b = 3
    ///
    /// The returning predicates subsumes the original predicates.
    /// This means the returning predicates express a logically larger set of rows than the original predicates.
    /// The returning predicates are predicates that are bound by the bound_cols.
    pub fn extract_bounded_predicates(&self, rel: &P) -> Vec<Expression<P>> {
        let mut result = Vec::new();
        match self {
            Expression::Binary { op, left, right } if matches!(op, BinaryOp::And) => {
                let mut left = left.extract_bounded_predicates(rel);
                let mut right = right.extract_bounded_predicates(rel);
                result.append(&mut left);
                result.append(&mut right);
            }
            Expression::Binary { op, left, right } if matches!(op, BinaryOp::Or) => {
                let mut left = left.extract_bounded_predicates(rel);
                let mut right = right.extract_bounded_predicates(rel);
                if left.is_empty() || right.is_empty() {
                    // Do nothing
                } else if left.len() == 1 && right.len() == 1 {
                    let left = left.remove(0);
                    let right = right.remove(0);
                    if left == right {
                        result.push(left);
                    } else {
                        result.push(Expression::binary(BinaryOp::Or, left, right));
                    }
                } else {
                    // Connect every left and right with an AND
                    // After that, connect the two results with an OR
                    let merged_left = Expression::merge_conjunction(left);
                    let merged_right = Expression::merge_conjunction(right);
                    result.push(Expression::binary(BinaryOp::Or, merged_left, merged_right));
                }
            }
            _ => {
                // All other cases, check if expr is bound. If bound, add to the result.
                // If not bound, ignore the expr.
                if self.bound_by(rel) {
                    result.push(self.clone());
                }
            }
        }
        result
    }

    /// Replace the variables in the expression with the new column IDs as specified in the
    /// `src_to_dest` mapping.
    pub fn replace_variables(self, src_to_dest: &HashMap<ColumnId, ColumnId>) -> Expression<P> {
        if src_to_dest.is_empty() {
            return self;
        }
        match self {
            Expression::ColRef { id } => {
                if let Some(dest) = src_to_dest.get(&id) {
                    Expression::ColRef { id: *dest }
                } else {
                    Expression::ColRef { id }
                }
            }
            Expression::Field { val } => Expression::Field { val },
            Expression::IsNull { expr } => Expression::IsNull {
                expr: Box::new(expr.replace_variables(src_to_dest)),
            },
            Expression::Binary { op, left, right } => Expression::Binary {
                op,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
            },
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => Expression::Case {
                expr: expr.map(|expr| Box::new(expr.replace_variables(src_to_dest))),
                whens: whens
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            when.replace_variables(src_to_dest),
                            then.replace_variables(src_to_dest),
                        )
                    })
                    .collect(),
                else_expr: else_expr.map(|expr| Box::new(expr.replace_variables(src_to_dest))),
            },
            Expression::Between { expr, lower, upper } => Expression::Between {
                expr: Box::new(expr.replace_variables(src_to_dest)),
                lower: Box::new(lower.replace_variables(src_to_dest)),
                upper: Box::new(upper.replace_variables(src_to_dest)),
            },
            Expression::Extract { field, expr } => Expression::Extract {
                field,
                expr: Box::new(expr.replace_variables(src_to_dest)),
            },
            Expression::Like {
                expr,
                pattern,
                escape,
            } => Expression::Like {
                expr: Box::new(expr.replace_variables(src_to_dest)),
                pattern,
                escape,
            },
            Expression::Cast { expr, to_type } => Expression::Cast {
                expr: Box::new(expr.replace_variables(src_to_dest)),
                to_type,
            },
            Expression::InList { expr, list } => {
                let expr = Box::new(expr.replace_variables(src_to_dest));
                let list = list
                    .into_iter()
                    .map(|expr| expr.replace_variables(src_to_dest))
                    .collect();
                Expression::InList { expr, list }
            }
            Expression::Not { expr } => Expression::Not {
                expr: Box::new(expr.replace_variables(src_to_dest)),
            },
            Expression::Substring { expr, start, len } => Expression::Substring {
                expr: Box::new(expr.replace_variables(src_to_dest)),
                start,
                len,
            },
            Expression::Subquery { expr } => Expression::Subquery {
                expr: Box::new(expr.replace_variables(src_to_dest)),
            },
            Expression::UncorrelatedAny { left, comp, right } => Expression::UncorrelatedAny {
                left: Box::new(left.replace_variables(src_to_dest)),
                comp,
                right: Box::new(right.replace_variables(src_to_dest)),
            },
            Expression::UncorrelatedExists { expr } => Expression::UncorrelatedExists {
                expr: Box::new(expr.replace_variables(src_to_dest)),
            },
        }
    }

    /// Replace the variables in the expression with the new expressions as specified in the
    /// `src_to_dest` mapping.
    pub(crate) fn replace_variables_with_exprs(
        self,
        src_to_dest: &HashMap<ColumnId, Expression<P>>,
    ) -> Expression<P> {
        if src_to_dest.is_empty() {
            return self;
        }
        match self {
            Expression::ColRef { id } => {
                if let Some(expr) = src_to_dest.get(&id) {
                    expr.clone()
                } else {
                    Expression::ColRef { id }
                }
            }
            Expression::Field { val } => Expression::Field { val },
            Expression::IsNull { expr } => Expression::IsNull {
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Binary { op, left, right } => Expression::Binary {
                op,
                left: Box::new(left.replace_variables_with_exprs(src_to_dest)),
                right: Box::new(right.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => Expression::Case {
                expr: expr.map(|expr| Box::new(expr.replace_variables_with_exprs(src_to_dest))),
                whens: whens
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            when.replace_variables_with_exprs(src_to_dest),
                            then.replace_variables_with_exprs(src_to_dest),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .map(|expr| Box::new(expr.replace_variables_with_exprs(src_to_dest))),
            },
            Expression::Between { expr, lower, upper } => Expression::Between {
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
                lower: Box::new(lower.replace_variables_with_exprs(src_to_dest)),
                upper: Box::new(upper.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Extract { field, expr } => Expression::Extract {
                field,
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Like {
                expr,
                pattern,
                escape,
            } => Expression::Like {
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
                pattern,
                escape,
            },
            Expression::Cast { expr, to_type } => Expression::Cast {
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
                to_type,
            },
            Expression::InList { expr, list } => {
                let expr = Box::new(expr.replace_variables_with_exprs(src_to_dest));
                let list = list
                    .into_iter()
                    .map(|expr| expr.replace_variables_with_exprs(src_to_dest))
                    .collect();
                Expression::InList { expr, list }
            }
            Expression::Not { expr } => Expression::Not {
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Substring { expr, start, len } => Expression::Substring {
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
                start,
                len,
            },
            Expression::Subquery { expr } => Expression::Subquery {
                // Do nothing for subquery
                expr,
            },
            Expression::UncorrelatedAny { left, comp, right } => Expression::UncorrelatedAny {
                left: Box::new(left.replace_variables_with_exprs(src_to_dest)),
                comp,
                right, // Do nothing for subquery
            },
            Expression::UncorrelatedExists { expr } => Expression::UncorrelatedExists {
                // Do nothing for subquery
                expr,
            },
        }
    }

    pub fn pretty_print(&self) {
        println!("{}", self.pretty_string());
    }

    pub fn pretty_string(&self) -> String {
        let mut out = String::new();
        self.print_inner(0, &mut out);
        out
    }

    pub fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            Expression::ColRef { id } => {
                out.push_str(&format!("@{}", id));
            }
            Expression::Field { val } => {
                out.push_str(&format!("{}", val));
            }
            Expression::IsNull { expr } => {
                out.push_str("is_null(");
                expr.print_inner(indent, out);
                out.push_str(")");
            }
            Expression::Binary { op, left, right } => {
                left.print_inner(indent, out);
                out.push_str(&format!("{}", op));
                right.print_inner(indent, out);
            }
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => {
                out.push_str("case ");
                expr.as_ref().map(|expr| expr.print_inner(indent, out));
                for (when, then) in whens {
                    out.push_str(" when ");
                    when.print_inner(indent, out);
                    out.push_str(" then ");
                    then.print_inner(indent, out);
                }
                else_expr.as_ref().map(|expr| {
                    out.push_str(" else ");
                    expr.print_inner(indent, out);
                });
            }
            Expression::Between { expr, lower, upper } => {
                expr.print_inner(indent, out);
                out.push_str(" between ");
                lower.print_inner(indent, out);
                out.push_str(" and ");
                upper.print_inner(indent, out);
            }
            Expression::Extract { field, expr } => {
                out.push_str(&format!("{:?}(", field));
                expr.print_inner(indent, out);
                out.push_str(")");
            }
            Expression::Like {
                expr,
                pattern,
                escape,
            } => {
                expr.print_inner(indent, out);
                out.push_str(&format!(" like '{}'", pattern));
                if let Some(escape) = escape {
                    out.push_str(&format!(" escape '{}'", escape));
                }
            }
            Expression::Cast { expr, to_type } => {
                out.push_str(&format!("cast("));
                expr.print_inner(indent, out);
                out.push_str(&format!(" as {:?})", to_type));
            }
            Expression::InList { expr, list } => {
                expr.print_inner(indent, out);
                out.push_str(" in (");
                for (i, expr) in list.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    expr.print_inner(indent, out);
                }
                out.push_str(")");
            }
            Expression::Not { expr } => {
                out.push_str("not ");
                expr.print_inner(indent, out);
            }
            Expression::Substring { expr, start, len } => {
                out.push_str("substring(");
                expr.print_inner(indent, out);
                out.push_str(&format!(", {}, {})", start, len));
            }
            Expression::Subquery { expr } => {
                out.push_str(&format!("λ.{:?}(\n", expr.free()));
                expr.print_inner(indent + 6, out);
                out.push_str(&format!("{})", " ".repeat(indent + 4)));
            }
            Expression::UncorrelatedAny { left, comp, right } => {
                left.print_inner(indent, out);
                out.push_str(&format!(" {} uncorrelated any (", comp));
                right.print_inner(indent, out);
                out.push_str(")");
            }
            Expression::UncorrelatedExists { expr } => {
                out.push_str("uncorrelated exists (");
                expr.print_inner(indent, out);
                out.push_str(")");
            }
        }
    }
}

// Free variables
// * A column in an expression that is not bound.

// Bound variables
// * A column that gives its values within an expression and is not a
//   parameter that comes from some other context

// Example:
// function(x) {x + y}
// * x is a bound variable
// * y is a free variable

impl<P: PlanTrait> Expression<P> {
    /// Get all variables in the expression.
    /// TODO: `free` might be a misleading name as in reality it returns all column
    /// IDs in the expression.
    pub fn free(&self) -> HashSet<ColumnId> {
        match self {
            Expression::ColRef { id } => {
                let mut set = HashSet::new();
                set.insert(*id);
                set
            }
            Expression::Field { val: _ } => HashSet::new(),
            Expression::IsNull { expr } => expr.free(),
            Expression::Binary { left, right, .. } => {
                let mut set = left.free();
                set.extend(right.free());
                set
            }
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => {
                let mut set = expr.as_ref().map_or(HashSet::new(), |expr| expr.free());
                for (when, then) in whens {
                    set.extend(when.free());
                    set.extend(then.free());
                }
                set.extend(
                    else_expr
                        .as_ref()
                        .map_or(HashSet::new(), |expr| expr.free()),
                );
                set
            }
            Expression::Between { expr, lower, upper } => {
                let mut set = expr.free();
                set.extend(lower.free());
                set.extend(upper.free());
                set
            }
            Expression::Extract { expr, .. } => expr.free(),
            Expression::Like { expr, .. } => expr.free(),
            Expression::Cast { expr, .. } => expr.free(),
            Expression::InList { expr, list } => {
                let mut set = expr.free();
                for expr in list {
                    set.extend(expr.free());
                }
                set
            }
            Expression::Not { expr } => expr.free(),
            Expression::Substring { expr, .. } => expr.free(),
            Expression::Subquery { expr } => expr.free(),
            Expression::UncorrelatedAny { left, right, .. } => {
                let mut set = left.free();
                set.extend(right.free());
                set
            }
            Expression::UncorrelatedExists { expr } => expr.free(),
        }
    }

    /// Check if all variables in the expression are bound (i.e., its colums refer
    /// to the attributes of the plan node). In other words, check if there are
    /// no free variables in the expression.
    pub fn bound_by(&self, rel: &P) -> bool {
        self.free().is_subset(&rel.att())
    }

    /// Check if any of the variables in the expression come from the expression
    /// node `rel`.
    pub fn intersect_with(&self, rel: &P) -> bool {
        !self.free().is_disjoint(&rel.att())
    }
}

pub trait PrePostVisitor<T> {
    fn visit_pre(&mut self, node: &T);
    fn visit_post(&mut self, node: &T);
}

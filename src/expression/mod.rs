mod logical;
mod physical;

use crate::tuple::Field;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub use crate::ColumnId;

pub mod prelude {
    pub use super::logical::LogicalRelExpr;
    pub use super::{AggOp, BinaryOp, ColumnId, Expression, JoinType, PlanTrait};
}

// This `plan` is implemented by logical (LogicalRelExpr) and physical (PhysicalRelExpr) relational expressions.
pub trait PlanTrait: Clone + std::fmt::Debug {
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
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "inner"),
            JoinType::LeftOuter => write!(f, "left_outer"),
            JoinType::RightOuter => write!(f, "right_outer"),
            JoinType::FullOuter => write!(f, "full_outer"),
            JoinType::CrossJoin => write!(f, "cross"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Expression<P: PlanTrait> {
    ColRef {
        id: ColumnId,
    },
    Field {
        val: Field,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expression<P>>,
        right: Box<Expression<P>>,
    },
    Case {
        expr: Box<Expression<P>>,
        whens: Vec<(Expression<P>, Expression<P>)>,
        else_expr: Box<Expression<P>>,
    },
    Subquery {
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

    pub fn binary(op: BinaryOp, left: Expression<P>, right: Expression<P>) -> Expression<P> {
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

    pub fn add(self, other: Expression<P>) -> Expression<P> {
        Expression::Binary {
            op: BinaryOp::Add,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    pub fn subquery(expr: P) -> Expression<P> {
        Expression::Subquery {
            expr: Box::new(expr),
        }
    }

    pub fn has_subquery(&self) -> bool {
        match self {
            Expression::ColRef { id: _ } => false,
            Expression::Field { val: _ } => false,
            Expression::Binary { left, right, .. } => left.has_subquery() || right.has_subquery(),
            Expression::Case { .. } => {
                // Currently, we don't support subqueries in the case expression
                false
            }
            Expression::Subquery { expr: _ } => true,
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

    /// Replace the variables in the expression with the new column IDs as specified in the
    /// `src_to_dest` mapping.
    pub fn replace_variables(self, src_to_dest: &HashMap<ColumnId, ColumnId>) -> Expression<P> {
        match self {
            Expression::ColRef { id } => {
                if let Some(dest) = src_to_dest.get(&id) {
                    Expression::ColRef { id: *dest }
                } else {
                    Expression::ColRef { id }
                }
            }
            Expression::Field { val } => Expression::Field { val },
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
                expr: Box::new(expr.replace_variables(src_to_dest)),
                whens: whens
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            when.replace_variables(src_to_dest),
                            then.replace_variables(src_to_dest),
                        )
                    })
                    .collect(),
                else_expr: Box::new(else_expr.replace_variables(src_to_dest)),
            },
            Expression::Subquery { expr } => Expression::Subquery {
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
        match self {
            Expression::ColRef { id } => {
                if let Some(expr) = src_to_dest.get(&id) {
                    expr.clone()
                } else {
                    Expression::ColRef { id }
                }
            }
            Expression::Field { val } => Expression::Field { val },
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
                expr: Box::new(expr.replace_variables_with_exprs(src_to_dest)),
                whens: whens
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            when.replace_variables_with_exprs(src_to_dest),
                            then.replace_variables_with_exprs(src_to_dest),
                        )
                    })
                    .collect(),
                else_expr: Box::new(else_expr.replace_variables_with_exprs(src_to_dest)),
            },
            Expression::Subquery { expr } => Expression::Subquery {
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
                expr.print_inner(indent, out);
                for (when, then) in whens {
                    out.push_str(" when ");
                    when.print_inner(indent, out);
                    out.push_str(" then ");
                    then.print_inner(indent, out);
                }
                out.push_str(" else ");
                else_expr.print_inner(indent, out);
                out.push_str(" end");
            }
            Expression::Subquery { expr } => {
                out.push_str(&format!("λ.{:?}(\n", expr.free()));
                expr.print_inner(indent + 6, out);
                out.push_str(&format!("{})", " ".repeat(indent + 4)));
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
                let mut set = expr.free();
                for (when, then) in whens {
                    set.extend(when.free());
                    set.extend(then.free());
                }
                set.extend(else_expr.free());
                set
            }
            Expression::Subquery { expr } => expr.free(),
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
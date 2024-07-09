mod aggregate;
mod flatmap;
mod hoist;
mod join;
mod map;
mod orderby;
mod project;
mod rename;
mod rules;
mod scan;
mod select;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    hash::Hash,
};

use super::prelude::*;
use crate::{ColumnId, ContainerId};

use fbtree::prelude::DatabaseId;
pub use rules::{HeuristicRule, HeuristicRules, HeuristicRulesRef};

pub mod prelude {
    pub use super::super::prelude::*;
    pub use super::{HeuristicRule, HeuristicRulesRef};
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalRelExpr {
    Scan {
        db_id: DatabaseId,
        c_id: ContainerId,
        table_name: String,
        column_indices: Vec<ColumnId>,
    },
    Select {
        // Evaluate the predicate for each row in the source
        src: Box<LogicalRelExpr>,
        predicates: BTreeSet<Expression<Self>>,
    },
    Join {
        join_type: JoinType,
        left: Box<LogicalRelExpr>,
        right: Box<LogicalRelExpr>,
        predicates: BTreeSet<Expression<Self>>,
    },
    Project {
        // Reduces the number of columns in the result
        src: Box<LogicalRelExpr>,
        cols: Vec<ColumnId>,
    },
    OrderBy {
        src: Box<LogicalRelExpr>,
        cols: Vec<(ColumnId, bool, bool)>, // (column_id, asc, nulls_first)
    },
    Aggregate {
        src: Box<LogicalRelExpr>,
        group_by: Vec<ColumnId>,
        aggrs: Vec<(ColumnId, (ColumnId, AggOp))>, // (dest_column_id, (src_column_id, agg_op)
    },
    Map {
        // Appends new columns to the result
        // This is the only operator that can have a reference to the columns of
        // the outer scope
        input: Box<LogicalRelExpr>,
        exprs: Vec<(ColumnId, Expression<LogicalRelExpr>)>, // ColumnId here refers to the destination column id
    },
    FlatMap {
        // For each row in the input, call func and append the result to the output
        input: Box<LogicalRelExpr>,
        func: Box<LogicalRelExpr>,
    },
    Rename {
        src: Box<LogicalRelExpr>,
        src_to_dest: BTreeMap<ColumnId, ColumnId>, // (src_column_id, dest_column_id)
    },
}

impl PlanTrait for LogicalRelExpr {
    /// Replace the column names in the relational expression
    /// * src_to_dest: A mapping from the source column id to the desired destination column id
    fn replace_variables(self, src_to_dest: &BTreeMap<ColumnId, ColumnId>) -> LogicalRelExpr {
        match self {
            LogicalRelExpr::Scan {
                db_id,
                c_id,
                table_name,
                column_indices: column_names,
            } => {
                let column_names = column_names
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect();
                LogicalRelExpr::Scan {
                    db_id,
                    c_id,
                    table_name,
                    column_indices: column_names,
                }
            }
            LogicalRelExpr::Select { src, predicates } => LogicalRelExpr::Select {
                src: Box::new(src.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            LogicalRelExpr::Join {
                join_type,
                left,
                right,
                predicates,
            } => LogicalRelExpr::Join {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            LogicalRelExpr::Project { src, cols } => LogicalRelExpr::Project {
                src: Box::new(src.replace_variables(src_to_dest)),
                cols: cols
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect(),
            },
            LogicalRelExpr::OrderBy { src, cols } => LogicalRelExpr::OrderBy {
                src: Box::new(src.replace_variables(src_to_dest)),
                cols: cols
                    .into_iter()
                    .map(|(id, asc, nulls_first)| {
                        (*src_to_dest.get(&id).unwrap_or(&id), asc, nulls_first)
                    })
                    .collect(),
            },
            LogicalRelExpr::Aggregate {
                src,
                group_by,
                aggrs,
            } => LogicalRelExpr::Aggregate {
                src: Box::new(src.replace_variables(src_to_dest)),
                group_by: group_by
                    .into_iter()
                    .map(|id| *src_to_dest.get(&id).unwrap_or(&id))
                    .collect(),
                aggrs: aggrs
                    .into_iter()
                    .map(|(id, (src_id, op))| {
                        (
                            *src_to_dest.get(&id).unwrap_or(&id),
                            (*src_to_dest.get(&src_id).unwrap_or(&src_id), op),
                        )
                    })
                    .collect(),
            },
            LogicalRelExpr::Map { input, exprs } => LogicalRelExpr::Map {
                input: Box::new(input.replace_variables(src_to_dest)),
                exprs: exprs
                    .into_iter()
                    .map(|(id, expr)| {
                        (
                            *src_to_dest.get(&id).unwrap_or(&id),
                            expr.replace_variables(src_to_dest),
                        )
                    })
                    .collect(),
            },
            LogicalRelExpr::FlatMap { input, func } => LogicalRelExpr::FlatMap {
                input: Box::new(input.replace_variables(src_to_dest)),
                func: Box::new(func.replace_variables(src_to_dest)),
            },
            LogicalRelExpr::Rename {
                src,
                src_to_dest: column_mappings,
            } => LogicalRelExpr::Rename {
                src: Box::new(src.replace_variables(src_to_dest)),
                src_to_dest: column_mappings
                    .into_iter()
                    .map(|(src, dest)| {
                        (
                            *src_to_dest.get(&src).unwrap_or(&src),
                            *src_to_dest.get(&dest).unwrap_or(&dest),
                        )
                    })
                    .collect(),
            },
        }
    }

    fn print_inner(&self, indent: usize, out: &mut String) {
        match self {
            LogicalRelExpr::Scan {
                db_id: _,
                c_id: _,
                table_name,
                column_indices: column_names,
            } => {
                out.push_str(&format!("{}-> scan({:?}, ", " ".repeat(indent), table_name,));
                let mut split = "";
                out.push('[');
                for col in column_names {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str("])\n");
            }
            LogicalRelExpr::Select { src, predicates } => {
                out.push_str(&format!("{}-> select(", " ".repeat(indent)));
                let mut split = "";
                for pred in predicates {
                    out.push_str(split);
                    pred.print_inner(0, out);
                    split = " && ";
                }
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
            LogicalRelExpr::Join {
                join_type,
                left,
                right,
                predicates,
            } => {
                out.push_str(&format!("{}-> {}_join(", " ".repeat(indent), join_type));
                let mut split = "";
                for pred in predicates {
                    out.push_str(split);
                    pred.print_inner(0, out);
                    split = " && ";
                }
                out.push_str(")\n");
                left.print_inner(indent + 2, out);
                right.print_inner(indent + 2, out);
            }
            LogicalRelExpr::Project { src, cols } => {
                out.push_str(&format!("{}-> project(", " ".repeat(indent)));
                let mut split = "";
                for col in cols {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
            LogicalRelExpr::OrderBy { src, cols } => {
                out.push_str(&format!("{}-> order_by({:?})\n", " ".repeat(indent), cols));
                src.print_inner(indent + 2, out);
            }
            LogicalRelExpr::Aggregate {
                src,
                group_by,
                aggrs,
            } => {
                out.push_str(&format!("{}-> aggregate(", " ".repeat(indent)));
                out.push_str("group_by: [");
                let mut split = "";
                for col in group_by {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str("], ");
                out.push_str("aggrs: [");
                let mut split = "";
                for (id, (input_id, op)) in aggrs {
                    out.push_str(split);
                    out.push_str(&format!("@{} <- {:?}(@{})", id, op, input_id));
                    split = ", ";
                }
                out.push(']');
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
            LogicalRelExpr::Map { input, exprs } => {
                out.push_str(&format!("{}-> map(\n", " ".repeat(indent)));
                for (id, expr) in exprs {
                    out.push_str(&format!("{}    @{} <- ", " ".repeat(indent), id));
                    expr.print_inner(indent, out);
                    out.push_str(",\n");
                }
                out.push_str(&format!("{})\n", " ".repeat(indent + 2)));
                input.print_inner(indent + 2, out);
            }
            LogicalRelExpr::FlatMap { input, func } => {
                out.push_str(&format!("{}-> flatmap\n", " ".repeat(indent)));
                input.print_inner(indent + 2, out);
                out.push_str(&format!("{}  λ.{:?}\n", " ".repeat(indent), func.free()));
                func.print_inner(indent + 2, out);
            }
            LogicalRelExpr::Rename {
                src,
                src_to_dest: colsk,
            } => {
                // Rename will be printed as @dest <- @src
                out.push_str(&format!("{}-> rename(", " ".repeat(indent)));
                let mut split = "";
                for (src, dest) in colsk {
                    out.push_str(split);
                    out.push_str(&format!("@{} <- @{}", dest, src));
                    split = ", ";
                }
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
        }
    }

    /// Free set of relational expression
    /// * The set of columns that are not bound in the expression
    /// * From all the columns required to compute the result, remove the columns that are
    ///   internally bound.
    ///
    /// * Examples of internally bound columns:
    ///   * The columns that are bound by the source of the expression (e.g. the columns of a table)
    ///   * The columns that are bound by the projection of the expression
    ///   * The columns that are bound by evaluating an expression
    fn free(&self) -> HashSet<ColumnId> {
        match self {
            LogicalRelExpr::Scan { .. } => HashSet::new(),
            LogicalRelExpr::Select { src, predicates } => {
                // For each predicate, identify the free columns.
                // Take the set difference of the free columns and the src attribute set.
                let mut set = src.free();
                for pred in predicates {
                    set.extend(pred.free());
                }
                set.difference(&src.att()).cloned().collect()
            }
            LogicalRelExpr::Join {
                left,
                right,
                predicates,
                ..
            } => {
                let mut set = left.free();
                set.extend(right.free());
                for pred in predicates {
                    set.extend(pred.free());
                }
                set.difference(&left.att().union(&right.att()).cloned().collect())
                    .cloned()
                    .collect()
            }
            LogicalRelExpr::Project { src, cols } => {
                let mut set = src.free();
                for col in cols {
                    set.insert(*col);
                }
                set.difference(&src.att()).cloned().collect()
            }
            LogicalRelExpr::OrderBy { src, cols } => {
                let mut set = src.free();
                for (id, _, _) in cols {
                    set.insert(*id);
                }
                set.difference(&src.att()).cloned().collect()
            }
            LogicalRelExpr::Aggregate {
                src,
                group_by,
                aggrs,
                ..
            } => {
                let mut set = src.free();
                for id in group_by {
                    set.insert(*id);
                }
                for (_, (src_id, _)) in aggrs {
                    set.insert(*src_id);
                }
                set.difference(&src.att()).cloned().collect()
            }
            LogicalRelExpr::Map { input, exprs } => {
                let mut set = input.free();
                for (_, expr) in exprs {
                    set.extend(expr.free());
                }
                set.difference(&input.att()).cloned().collect()
            }
            LogicalRelExpr::FlatMap { input, func } => {
                let mut set = input.free();
                set.extend(func.free());
                set.difference(&input.att()).cloned().collect()
            }
            LogicalRelExpr::Rename { src, .. } => src.free(),
        }
    }

    /// Attribute set of relational expression
    /// * The set of columns that are in the result of the expression.
    /// * Attribute changes when we do a projection or map the columns to a different name.
    ///
    /// Difference between "free" and "att"
    /// * "free" is the set of columns that we need to evaluate the expression
    /// * "att" is the set of columns that we have (the column names of the result of RelExpr)
    fn att(&self) -> HashSet<ColumnId> {
        match self {
            LogicalRelExpr::Scan {
                db_id: _,
                c_id: _,
                table_name: _,
                column_indices: column_names,
            } => column_names.iter().cloned().collect(),
            LogicalRelExpr::Select { src, .. } => src.att(),
            LogicalRelExpr::Join {
                join_type,
                left,
                right,
                ..
            } => match join_type {
                JoinType::CrossJoin
                | JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter => {
                    let mut set = left.att();
                    set.extend(right.att());
                    set
                }
                JoinType::LeftSemi | JoinType::LeftAnti => left.att(),
                JoinType::RightSemi | JoinType::RightAnti => right.att(),
                JoinType::LeftMarkJoin(col_id) => {
                    let mut set = left.att();
                    set.insert(*col_id);
                    set
                }
                JoinType::RightMarkJoin(col_id) => {
                    let mut set = right.att();
                    set.insert(*col_id);
                    set
                }
            },
            LogicalRelExpr::Project { cols, .. } => cols.iter().cloned().collect(),
            LogicalRelExpr::OrderBy { src, .. } => src.att(),
            LogicalRelExpr::Aggregate {
                group_by, aggrs, ..
            } => {
                let mut set: HashSet<usize> = group_by.iter().cloned().collect();
                set.extend(aggrs.iter().map(|(id, _)| *id));
                set
            }
            LogicalRelExpr::Map { input, exprs } => {
                let mut set = input.att();
                set.extend(exprs.iter().map(|(id, _)| *id));
                set
            }
            LogicalRelExpr::FlatMap { input, func } => {
                let mut set = input.att();
                set.extend(func.att());
                set
            }
            LogicalRelExpr::Rename {
                src, src_to_dest, ..
            } => {
                let mut set = src.att();
                // rewrite the column names
                for (src, dest) in src_to_dest {
                    set.remove(src);
                    set.insert(*dest);
                }
                set
            }
        }
    }
}

impl LogicalRelExpr {
    pub fn pretty_print(&self) {
        println!("{}", self.pretty_string());
    }

    pub fn pretty_string(&self) -> String {
        let mut out = String::new();
        self.print_inner(0, &mut out);
        out
    }

    pub fn visit(&self, visitor: &mut impl PrePostVisitor<Self>) {
        match self {
            LogicalRelExpr::Scan { .. } => {
                visitor.visit_pre(self);
                visitor.visit_post(self);
            }
            LogicalRelExpr::Select { src, .. } => {
                visitor.visit_pre(self);
                src.visit(visitor);
                visitor.visit_post(self);
            }
            LogicalRelExpr::Join { left, right, .. } => {
                visitor.visit_pre(self);
                left.visit(visitor);
                right.visit(visitor);
                visitor.visit_post(self);
            }
            LogicalRelExpr::Project { src, .. } => {
                visitor.visit_pre(self);
                src.visit(visitor);
                visitor.visit_post(self);
            }
            LogicalRelExpr::OrderBy { src, .. } => {
                visitor.visit_pre(self);
                src.visit(visitor);
                visitor.visit_post(self);
            }
            LogicalRelExpr::Aggregate { src, .. } => {
                visitor.visit_pre(self);
                src.visit(visitor);
                visitor.visit_post(self);
            }
            LogicalRelExpr::Map { input, .. } => {
                visitor.visit_pre(self);
                input.visit(visitor);
                visitor.visit_post(self);
            }
            LogicalRelExpr::FlatMap { input, func } => {
                visitor.visit_pre(self);
                input.visit(visitor);
                func.visit(visitor);
                visitor.visit_post(self);
            }
            LogicalRelExpr::Rename { src, .. } => {
                visitor.visit_pre(self);
                src.visit(visitor);
                visitor.visit_post(self);
            }
        }
    }
}

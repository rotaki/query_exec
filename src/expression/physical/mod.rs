use std::collections::{HashMap, HashSet};

use super::prelude::*;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub enum PhysicalRelExpr {
    Scan {
        cid: ContainerId,
        table_name: String,
        column_names: Vec<ColumnId>,
    },
    Select {
        // Evaluate the predicate for each row in the source
        src: Box<PhysicalRelExpr>,

        // TODO: Should be a single expression(?) because usually we need to break up conjunctive selection predicates
        // However, this might not be necessary as we have already pushed down the selection predicates
        // in the translation phase (generation of logical plan from ast)
        predicates: Vec<Expression<Self>>,
    },
    CrossJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
    },
    NestedLoopJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
    },
    HashJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
    },
    SortMergeJoin {
        join_type: JoinType,
        left: Box<PhysicalRelExpr>,
        right: Box<PhysicalRelExpr>,
        predicates: Vec<Expression<Self>>,
    },
    Project {
        // Reduces the number of columns in the result
        src: Box<PhysicalRelExpr>,
        cols: Vec<ColumnId>,
    },
    Sort {
        src: Box<PhysicalRelExpr>,
        cols: Vec<(ColumnId, bool, bool)>, // (column_id, asc, nulls_first)
    },
    HashAggregate {
        src: Box<PhysicalRelExpr>,
        group_by: Vec<ColumnId>,
        aggrs: Vec<(ColumnId, (ColumnId, AggOp))>, // (dest_column_id, (src_column_id, agg_op)
    },
    Map {
        // Appends new columns to the result
        // This is the only operator that can have a reference to the columns of
        // the outer scope
        input: Box<PhysicalRelExpr>,
        exprs: Vec<(ColumnId, Expression<Self>)>,
    },
    FlatMap {
        // For each row in the input, call func and append the result to the output
        input: Box<PhysicalRelExpr>,
        func: Box<PhysicalRelExpr>,
    },
    Rename {
        src: Box<PhysicalRelExpr>,
        src_to_dest: HashMap<ColumnId, ColumnId>, // (src_column_id, dest_column_id)
    },
}

impl PlanTrait for PhysicalRelExpr {
    /// Replace the column names in the relational expression
    /// * src_to_dest: mapping from source column id to the desired destination column id
    fn replace_variables(self, src_to_dest: &HashMap<ColumnId, ColumnId>) -> PhysicalRelExpr {
        match self {
            PhysicalRelExpr::Scan {
                cid,
                table_name,
                column_names,
            } => {
                let column_names = column_names
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect();
                PhysicalRelExpr::Scan {
                    cid,
                    table_name,
                    column_names,
                }
            }
            PhysicalRelExpr::Select { src, predicates } => PhysicalRelExpr::Select {
                src: Box::new(src.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            PhysicalRelExpr::CrossJoin {
                join_type,
                left,
                right,
                predicates,
            } => PhysicalRelExpr::CrossJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left,
                right,
                predicates,
            } => PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            PhysicalRelExpr::HashJoin {
                join_type,
                left,
                right,
                predicates,
            } => PhysicalRelExpr::HashJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            PhysicalRelExpr::SortMergeJoin {
                join_type,
                left,
                right,
                predicates,
            } => PhysicalRelExpr::SortMergeJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                predicates: predicates
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            PhysicalRelExpr::Project { src, cols } => PhysicalRelExpr::Project {
                src: Box::new(src.replace_variables(src_to_dest)),
                cols: cols
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect(),
            },
            PhysicalRelExpr::Sort { src, cols } => PhysicalRelExpr::Sort {
                src: Box::new(src.replace_variables(src_to_dest)),
                cols: cols
                    .into_iter()
                    .map(|(id, asc, nulls_first)| {
                        (*src_to_dest.get(&id).unwrap_or(&id), asc, nulls_first)
                    })
                    .collect(),
            },
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
            } => PhysicalRelExpr::HashAggregate {
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
            PhysicalRelExpr::Map { input, exprs } => PhysicalRelExpr::Map {
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
            PhysicalRelExpr::FlatMap { input, func } => PhysicalRelExpr::FlatMap {
                input: Box::new(input.replace_variables(src_to_dest)),
                func: Box::new(func.replace_variables(src_to_dest)),
            },
            PhysicalRelExpr::Rename {
                src,
                src_to_dest: column_mappings,
            } => PhysicalRelExpr::Rename {
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
            PhysicalRelExpr::Scan {
                cid: _,
                table_name,
                column_names,
            } => {
                out.push_str(&format!("{}-> scan({:?}, ", " ".repeat(indent), table_name,));
                let mut split = "";
                out.push_str("[");
                for col in column_names {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str("])\n");
            }
            PhysicalRelExpr::Select { src, predicates } => {
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
            PhysicalRelExpr::CrossJoin {
                join_type,
                left,
                right,
                predicates,
            } => {
                out.push_str(&format!(
                    "{}-> Cross {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
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
            PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left,
                right,
                predicates,
            } => {
                out.push_str(&format!(
                    "{}-> Nested loop {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
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
            PhysicalRelExpr::HashJoin {
                join_type,
                left,
                right,
                predicates,
            } => {
                out.push_str(&format!(
                    "{}-> Hash {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
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
            PhysicalRelExpr::SortMergeJoin {
                join_type,
                left,
                right,
                predicates,
            } => {
                out.push_str(&format!(
                    "{}-> Sort merge {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
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
            PhysicalRelExpr::Project { src, cols } => {
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
            PhysicalRelExpr::Sort { src, cols } => {
                out.push_str(&format!("{}-> order_by({:?})\n", " ".repeat(indent), cols));
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
            } => {
                out.push_str(&format!("{}-> aggregate(", " ".repeat(indent)));
                out.push_str(&format!("group_by: [",));
                let mut split = "";
                for col in group_by {
                    out.push_str(split);
                    out.push_str(&format!("@{}", col));
                    split = ", ";
                }
                out.push_str("], ");
                out.push_str(&format!("aggrs: ["));
                let mut split = "";
                for (id, (input_id, op)) in aggrs {
                    out.push_str(split);
                    out.push_str(&format!("@{} <- {:?}(@{})", id, op, input_id));
                    split = ", ";
                }
                out.push_str("]");
                out.push_str(&format!(")\n"));
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Map { input, exprs } => {
                out.push_str(&format!("{}-> map(\n", " ".repeat(indent)));
                for (id, expr) in exprs {
                    out.push_str(&format!("{}    @{} <- ", " ".repeat(indent), id));
                    expr.print_inner(indent, out);
                    out.push_str(",\n");
                }
                out.push_str(&format!("{})\n", " ".repeat(indent + 2)));
                input.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::FlatMap { input, func } => {
                out.push_str(&format!("{}-> flatmap\n", " ".repeat(indent)));
                input.print_inner(indent + 2, out);
                out.push_str(&format!("{}  λ.{:?}\n", " ".repeat(indent), func.free()));
                func.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Rename {
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
            PhysicalRelExpr::Scan { .. } => HashSet::new(),
            PhysicalRelExpr::Select { src, predicates } => {
                // For each predicate, identify the free columns.
                // Take the set difference of the free columns and the src attribute set.
                let mut set = src.free();
                for pred in predicates {
                    set.extend(pred.free());
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::CrossJoin {
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::NestedLoopJoin {
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::HashJoin {
                left,
                right,
                predicates,
                ..
            }
            | PhysicalRelExpr::SortMergeJoin {
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
            PhysicalRelExpr::Project { src, cols } => {
                let mut set = src.free();
                for col in cols {
                    set.insert(*col);
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::Sort { src, cols } => {
                let mut set = src.free();
                for (id, _, _) in cols {
                    set.insert(*id);
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::HashAggregate {
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
            PhysicalRelExpr::Map { input, exprs } => {
                let mut set = input.free();
                for (_, expr) in exprs {
                    set.extend(expr.free());
                }
                set.difference(&input.att()).cloned().collect()
            }
            PhysicalRelExpr::FlatMap { input, func } => {
                let mut set = input.free();
                set.extend(func.free());
                set.difference(&input.att()).cloned().collect()
            }
            PhysicalRelExpr::Rename { src, .. } => src.free(),
        }
    }

    /// Attribute set of relational expression
    /// * The set of columns that are in the result of the expression.
    /// * Attribute changes when we do a projection or map the columns to a different name.
    ///
    /// Difference between "free" and "att"
    /// * "free" is the set of columns that we need to evaluate the expression
    /// * "att" is the set of columns that we have (the column names of the result of  LogicalRelExpr)
    fn att(&self) -> HashSet<ColumnId> {
        match self {
            PhysicalRelExpr::Scan {
                cid: _,
                table_name: _,
                column_names,
            } => column_names.iter().cloned().collect(),
            PhysicalRelExpr::Select { src, .. } => src.att(),
            PhysicalRelExpr::CrossJoin { left, right, .. }
            | PhysicalRelExpr::NestedLoopJoin { left, right, .. }
            | PhysicalRelExpr::HashJoin { left, right, .. }
            | PhysicalRelExpr::SortMergeJoin { left, right, .. } => {
                let mut set = left.att();
                set.extend(right.att());
                set
            }
            PhysicalRelExpr::Project { cols, .. } => cols.iter().cloned().collect(),
            PhysicalRelExpr::Sort { src, .. } => src.att(),
            PhysicalRelExpr::HashAggregate {
                group_by, aggrs, ..
            } => {
                let mut set: HashSet<usize> = group_by.iter().cloned().collect();
                set.extend(aggrs.iter().map(|(id, _)| *id));
                set
            }
            PhysicalRelExpr::Map { input, exprs } => {
                let mut set = input.att();
                set.extend(exprs.iter().map(|(id, _)| *id));
                set
            }
            PhysicalRelExpr::FlatMap { input, func } => {
                let mut set = input.att();
                set.extend(func.att());
                set
            }
            PhysicalRelExpr::Rename {
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

impl PhysicalRelExpr {
    pub fn pretty_print(&self) {
        println!("{}", self.pretty_string());
    }

    pub fn pretty_string(&self) -> String {
        let mut out = String::new();
        self.print_inner(0, &mut out);
        out
    }
}

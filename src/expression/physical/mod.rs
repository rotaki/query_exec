use std::collections::{BTreeMap, BTreeSet, HashSet};

use super::prelude::*;
use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PhysicalRelExpr {
    Scan {
        db_id: DatabaseId,
        c_id: ContainerId,
        table_name: String,
        column_indices: Vec<ColumnId>,
    },
    Select {
        // Evaluate the predicate for each row in the source
        src: Box<PhysicalRelExpr>,
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
        equalities: Vec<(Expression<Self>, Expression<Self>)>, // Left and right expressions
        filter: Vec<Expression<Self>>,
    },
    Project {
        // Reduces the number of columns in the result
        src: Box<PhysicalRelExpr>,
        column_names: Vec<ColumnId>,
    },
    Sort {
        src: Box<PhysicalRelExpr>,
        column_names: Vec<(ColumnId, bool, bool)>, // (column_id, asc, nulls_first)
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
        src_to_dest: BTreeMap<ColumnId, ColumnId>, // (src_column_id, dest_column_id)
    },
}

impl PlanTrait for PhysicalRelExpr {
    /// Replace the column names in the relational expression
    /// * src_to_dest: mapping from source column id to the desired destination column id
    fn replace_variables(self, src_to_dest: &BTreeMap<ColumnId, ColumnId>) -> PhysicalRelExpr {
        match self {
            PhysicalRelExpr::Scan {
                db_id,
                c_id,
                table_name,
                column_indices: column_names,
            } => {
                let column_names = column_names
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect();
                PhysicalRelExpr::Scan {
                    db_id,
                    c_id,
                    table_name,
                    column_indices: column_names,
                }
            }
            PhysicalRelExpr::Select { src, predicates } => PhysicalRelExpr::Select {
                src: Box::new(src.replace_variables(src_to_dest)),
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
                equalities,
                filter,
            } => PhysicalRelExpr::HashJoin {
                join_type,
                left: Box::new(left.replace_variables(src_to_dest)),
                right: Box::new(right.replace_variables(src_to_dest)),
                equalities: equalities
                    .into_iter()
                    .map(|(left, right)| {
                        (
                            left.replace_variables(src_to_dest),
                            right.replace_variables(src_to_dest),
                        )
                    })
                    .collect(),
                filter: filter
                    .into_iter()
                    .map(|pred| pred.replace_variables(src_to_dest))
                    .collect(),
            },
            PhysicalRelExpr::Project {
                src,
                column_names: cols,
            } => PhysicalRelExpr::Project {
                src: Box::new(src.replace_variables(src_to_dest)),
                column_names: cols
                    .into_iter()
                    .map(|col| *src_to_dest.get(&col).unwrap_or(&col))
                    .collect(),
            },
            PhysicalRelExpr::Sort {
                src,
                column_names: cols,
            } => PhysicalRelExpr::Sort {
                src: Box::new(src.replace_variables(src_to_dest)),
                column_names: cols
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
            PhysicalRelExpr::NestedLoopJoin {
                join_type,
                left,
                right,
                predicates,
            } => {
                out.push_str(&format!("{}-> nl_{}_join(", " ".repeat(indent), join_type));
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
                equalities,
                filter,
            } => {
                out.push_str(&format!(
                    "{}-> hash {}_join(",
                    " ".repeat(indent),
                    join_type
                ));
                let mut split = "";
                out.push_str("eq: (");
                for i in 0..equalities.len().min(5) {
                    out.push_str(split);
                    let (left, right) = &equalities[i];
                    left.print_inner(0, out);
                    out.push('=');
                    right.print_inner(0, out);
                    split = " && ";
                    if equalities.len() > 5 && i == 4 {
                        out.push_str(&format!("...len={}", equalities.len()));
                        break;
                    }
                }
                out.push_str("), filter: (");
                split = "";
                for i in 0..filter.len().min(5) {
                    out.push_str(split);
                    filter[i].print_inner(0, out);
                    split = " && ";
                    if filter.len() > 5 && i == 4 {
                        out.push_str(&format!("...len={}", filter.len()));
                        break;
                    }
                }
                out.push_str("))\n");
                left.print_inner(indent + 2, out);
                right.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Project {
                src,
                column_names: cols,
            } => {
                out.push_str(&format!("{}-> project(", " ".repeat(indent)));
                let mut split = "";
                for i in 0..cols.len().min(5) {
                    out.push_str(split);
                    out.push_str(&format!("@{}", cols[i]));
                    split = ", ";
                    if cols.len() > 5 && i == 4 {
                        out.push_str(&format!("...len={}", cols.len()));
                        break;
                    }
                }
                out.push_str(")\n");
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::Sort {
                src,
                column_names: cols,
            } => {
                out.push_str(&format!("{}-> order_by({:?})\n", " ".repeat(indent), cols));
                src.print_inner(indent + 2, out);
            }
            PhysicalRelExpr::HashAggregate {
                src,
                group_by,
                aggrs,
            } => {
                out.push_str(&format!("{}-> aggregate(", " ".repeat(indent)));
                out.push_str("group_by: [");
                let mut split = "";
                for i in 0..group_by.len().min(5) {
                    out.push_str(split);
                    out.push_str(&format!("@{}", group_by[i]));
                    split = ", ";
                    if group_by.len() > 5 && i == 4 {
                        out.push_str(&format!("...len={}", group_by.len()));
                        break;
                    }
                }
                out.push_str("], ");
                out.push_str("aggrs: [");
                let mut split = "";
                for i in 0..aggrs.len().min(5) {
                    let (id, (input_id, op)) = &aggrs[i];
                    out.push_str(split);
                    out.push_str(&format!("@{} <- {:?}(@{})", id, op, input_id));
                    split = ", ";
                    if aggrs.len() > 5 && i == 4 {
                        out.push_str(&format!("...len={}", aggrs.len()));
                        break;
                    }
                }
                out.push(']');
                out.push_str(")\n");
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
            PhysicalRelExpr::Rename { src, src_to_dest } => {
                // Rename will be printed as @dest <- @src
                out.push_str(&format!("{}-> rename(", " ".repeat(indent)));
                let mut split = "";
                let mut count = 0;
                for (src, dest) in src_to_dest {
                    out.push_str(split);
                    out.push_str(&format!("@{} <- @{}", dest, src));
                    split = ", ";
                    count += 1;
                    if count == 5 {
                        out.push_str(&format!("...len={}", src_to_dest.len()));
                        break;
                    }
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
            PhysicalRelExpr::NestedLoopJoin {
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
            PhysicalRelExpr::HashJoin {
                join_type: _,
                left,
                right,
                equalities,
                filter,
            } => {
                let mut set = left.free();
                set.extend(right.free());
                for (left, right) in equalities {
                    set.extend(left.free());
                    set.extend(right.free());
                }
                for pred in filter {
                    set.extend(pred.free());
                }
                set.difference(&left.att().union(&right.att()).cloned().collect())
                    .cloned()
                    .collect()
            }
            PhysicalRelExpr::Project {
                src,
                column_names: cols,
            } => {
                let mut set = src.free();
                for col in cols {
                    set.insert(*col);
                }
                set.difference(&src.att()).cloned().collect()
            }
            PhysicalRelExpr::Sort {
                src,
                column_names: cols,
            } => {
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
                db_id: _,
                c_id: _,
                table_name: _,
                column_indices: column_names,
            } => column_names.iter().cloned().collect(),
            PhysicalRelExpr::Select { src, .. } => src.att(),
            PhysicalRelExpr::NestedLoopJoin { left, right, .. }
            | PhysicalRelExpr::HashJoin { left, right, .. } => {
                let mut set = left.att();
                set.extend(right.att());
                set
            }
            PhysicalRelExpr::Project {
                column_names: cols, ..
            } => cols.iter().cloned().collect(),
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

    pub fn pre_post_visit<V>(&self, visitor: &mut V)
    where
        V: PrePostVisitor<PhysicalRelExpr>,
    {
        match &self {
            PhysicalRelExpr::Scan { .. } => {
                visitor.visit_pre(self);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::Select { src, .. } => {
                visitor.visit_pre(self);
                src.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::NestedLoopJoin { left, right, .. }
            | PhysicalRelExpr::HashJoin { left, right, .. } => {
                visitor.visit_pre(self);
                left.pre_post_visit(visitor);
                right.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::Project { src, .. } => {
                visitor.visit_pre(self);
                src.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::Sort { src, .. } => {
                visitor.visit_pre(self);
                src.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::HashAggregate { src, .. } => {
                visitor.visit_pre(self);
                src.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::Map { input, .. } => {
                visitor.visit_pre(self);
                input.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::FlatMap { input, func } => {
                visitor.visit_pre(self);
                input.pre_post_visit(visitor);
                func.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
            PhysicalRelExpr::Rename { src, .. } => {
                visitor.visit_pre(self);
                src.pre_post_visit(visitor);
                visitor.visit_post(self);
            }
        }
    }
}

pub struct LogicalToPhysicalRelExpr;

impl LogicalToPhysicalRelExpr {
    pub fn to_physical(&mut self, expr: LogicalRelExpr) -> PhysicalRelExpr {
        match expr {
            LogicalRelExpr::Scan {
                db_id,
                c_id,
                table_name,
                column_indices: column_names,
            } => PhysicalRelExpr::Scan {
                db_id,
                c_id,
                table_name,
                column_indices: column_names,
            },
            LogicalRelExpr::Select { src, predicates } => PhysicalRelExpr::Select {
                src: Box::new(self.to_physical(*src)),
                predicates: predicates
                    .iter()
                    .map(|pred| LogicalToPhysicalExpression.to_physical(pred))
                    .collect(),
            },
            LogicalRelExpr::Project { src, cols } => PhysicalRelExpr::Project {
                src: Box::new(self.to_physical(*src)),
                column_names: cols,
            },
            LogicalRelExpr::Join {
                join_type,
                left,
                right,
                predicates,
            } => {
                let left = Box::new(self.to_physical(*left));
                let right = Box::new(self.to_physical(*right));
                let predicates = predicates
                    .iter()
                    .map(|pred| LogicalToPhysicalExpression.to_physical(pred))
                    .collect::<BTreeSet<Expression<PhysicalRelExpr>>>();

                // Determine the equality predicates and left, right filter conditions
                let mut equalities = BTreeSet::new();
                for predicates in predicates.iter() {
                    equalities.extend(predicates.extract_equalities());
                }

                let mut filter = Vec::new();
                for pred in predicates {
                    if !equalities.contains(&pred) {
                        filter.push(pred);
                    }
                }

                let mut equality_preds = Vec::new();
                // From all the equalities, determine the ones that are bound by the left and right
                // i.e. expressions that can be used in the hash join
                for pred in equalities {
                    match pred {
                        Expression::Binary {
                            op: BinaryOp::Eq,
                            left,
                            right,
                        } if !left.has_col_ref() || !right.has_col_ref() => {
                            filter.push(Expression::Binary {
                                op: BinaryOp::Eq,
                                left,
                                right,
                            })
                        }
                        Expression::Binary {
                            op: BinaryOp::Eq,
                            left: left_expr,
                            right: right_expr,
                        } if left_expr.bound_by(&left) && right_expr.bound_by(&right) => {
                            equality_preds.push((*left_expr, *right_expr))
                        }
                        Expression::Binary {
                            op: BinaryOp::Eq,
                            left: left_expr,
                            right: right_expr,
                        } if left_expr.bound_by(&right) && right_expr.bound_by(&left) => {
                            equality_preds.push((*right_expr, *left_expr))
                        }
                        Expression::Binary {
                            op: BinaryOp::Eq,
                            left: left_expr,
                            right: right_expr,
                        } => {
                            // left_expr and right_expr are bound by one of the sources
                            filter.push(Expression::Binary {
                                op: BinaryOp::Eq,
                                left: left_expr,
                                right: right_expr,
                            })
                        }
                        _ => {
                            panic!("Join predicate is not an equality predicate")
                        }
                    }
                }

                if equality_preds.is_empty() && join_type == JoinType::CrossJoin {
                    // TODO: remove join_type == JoinType::CrossJoin condition from here.
                    // This condition is needed for executing test_uncorrelated_exists.
                    // Otherwise, the test will fail because nl_right_mark_join is not supported.
                    // If nested loop join with all the join_types are supported, we can remove this condition
                    PhysicalRelExpr::NestedLoopJoin {
                        join_type,
                        left,
                        right,
                        predicates: filter,
                    }
                } else {
                    let join_type = if join_type == JoinType::CrossJoin {
                        JoinType::Inner
                    } else {
                        join_type
                    };
                    PhysicalRelExpr::HashJoin {
                        join_type,
                        left,
                        right,
                        equalities: equality_preds,
                        filter,
                    }
                }
            }
            LogicalRelExpr::OrderBy { src, cols } => PhysicalRelExpr::Sort {
                src: Box::new(self.to_physical(*src)),
                column_names: cols,
            },
            LogicalRelExpr::Aggregate {
                src,
                group_by,
                aggrs,
            } => PhysicalRelExpr::HashAggregate {
                src: Box::new(self.to_physical(*src)),
                group_by,
                aggrs,
            },
            LogicalRelExpr::Map { input, exprs } => PhysicalRelExpr::Map {
                input: Box::new(self.to_physical(*input)),
                exprs: exprs
                    .iter()
                    .map(|(id, expr)| (*id, LogicalToPhysicalExpression.to_physical(expr)))
                    .collect(),
            },
            LogicalRelExpr::FlatMap { input, func } => PhysicalRelExpr::FlatMap {
                input: Box::new(self.to_physical(*input)),
                func: Box::new(self.to_physical(*func)),
            },
            LogicalRelExpr::Rename { src, src_to_dest } => PhysicalRelExpr::Rename {
                src: Box::new(self.to_physical(*src)),
                src_to_dest,
            },
        }
    }
}

pub struct LogicalToPhysicalExpression;

impl LogicalToPhysicalExpression {
    pub fn to_physical(&self, expr: &Expression<LogicalRelExpr>) -> Expression<PhysicalRelExpr> {
        match expr {
            Expression::ColRef { id } => Expression::ColRef { id: *id },
            Expression::Field { val } => Expression::Field { val: val.clone() },
            Expression::IsNull { expr } => Expression::IsNull {
                expr: Box::new(self.to_physical(expr)),
            },
            Expression::Binary { op, left, right } => Expression::Binary {
                op: *op,
                left: Box::new(self.to_physical(left)),
                right: Box::new(self.to_physical(right)),
            },
            Expression::Case {
                expr,
                whens,
                else_expr,
            } => Expression::Case {
                expr: expr.as_ref().map(|expr| Box::new(self.to_physical(expr))),
                whens: whens
                    .iter()
                    .map(|(when, then)| (self.to_physical(when), self.to_physical(then)))
                    .collect(),
                else_expr: else_expr
                    .as_ref()
                    .map(|expr| Box::new(self.to_physical(expr))),
            },
            Expression::Between { expr, lower, upper } => Expression::Between {
                expr: Box::new(self.to_physical(expr)),
                lower: Box::new(self.to_physical(lower)),
                upper: Box::new(self.to_physical(upper)),
            },
            Expression::Extract { field, expr } => Expression::Extract {
                field: field.clone(),
                expr: Box::new(self.to_physical(expr)),
            },
            Expression::Like {
                expr,
                pattern,
                escape,
            } => Expression::Like {
                expr: Box::new(self.to_physical(expr)),
                pattern: pattern.clone(),
                escape: escape.clone(),
            },
            Expression::Cast { expr, to_type } => Expression::Cast {
                expr: Box::new(self.to_physical(expr)),
                to_type: to_type.clone(),
            },
            Expression::InList { expr, list } => Expression::InList {
                expr: Box::new(self.to_physical(expr)),
                list: list.iter().map(|expr| self.to_physical(expr)).collect(),
            },
            Expression::Not { expr } => Expression::Not {
                expr: Box::new(self.to_physical(expr)),
            },
            Expression::Substring { expr, start, len } => Expression::Substring {
                expr: Box::new(self.to_physical(expr)),
                start: *start,
                len: *len,
            },
            Expression::Subquery { expr } => Expression::Subquery {
                expr: Box::new(LogicalToPhysicalRelExpr.to_physical(expr.as_ref().clone())),
            },
            Expression::UncorrelatedAny { left, comp, right } => Expression::UncorrelatedAny {
                left: Box::new(self.to_physical(left)),
                comp: *comp,
                right: Box::new(LogicalToPhysicalRelExpr.to_physical(right.as_ref().clone())),
            },
            Expression::Exists { expr } => Expression::Exists {
                expr: Box::new(LogicalToPhysicalRelExpr.to_physical(expr.as_ref().clone())),
            },
        }
    }
}

// Reference: https://github.com/rotaki/decorrelator

use super::super::prelude::*;
use super::prelude::*;
use crate::catalog::ColIdGenRef;
use std::collections::{HashMap, HashSet};

/// Union of free variables and columns. The order of the columns is preserved.
fn union(cols: &Vec<usize>, free: &HashSet<usize>) -> Vec<usize> {
    let mut cols = cols.clone();
    for f in free {
        if cols.contains(f) {
            continue;
        } else {
            cols.push(*f);
        }
    }
    cols
}

/// Intersection of free variables and columns. The order of the columns is preserved.
fn intersect(cols: &Vec<usize>, att: &HashSet<usize>) -> Vec<usize> {
    let mut new_cols = Vec::new();
    for c in cols.iter() {
        if att.contains(c) {
            new_cols.push(*c);
        }
    }
    new_cols
}

impl LogicalRelExpr {
    /// Apply projection to the current logical relational expression.
    /// Notice the difference from rotaki/decorrelator. The `is_wildcard` parameter
    /// is added to the function signature. This parameter is used to determine if
    /// all columns should be projected.
    /// For correctness, a vector (instead of a hashset) is also used to represent
    /// the columns to be projected so that the order of the columns is preserved.
    pub fn project(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        cols: Vec<usize>,
    ) -> LogicalRelExpr {
        let outer_refs = self.free();
        let cols_set: HashSet<usize> = cols.iter().cloned().collect();
        let att_set = self.att();
        if att_set == cols_set {
            return self;
        }

        if optimize && enabled_rules.is_enabled(&HeuristicRule::ProjectionPushdown) {
            match self {
                LogicalRelExpr::Project {
                    src,
                    cols: _no_need_cols,
                } => src.project(true, enabled_rules, col_id_gen, cols),
                LogicalRelExpr::Map {
                    input,
                    exprs: mut existing_exprs,
                } => {
                    // Remove the mappings that are not used in the projection.
                    existing_exprs.retain(|(id, _)| cols.contains(id));

                    // Pushdown the projection to the source. Note that we don't push
                    // down the projection of outer columns.
                    let mut free: HashSet<usize> = existing_exprs
                        .iter()
                        .flat_map(|(_, expr)| expr.free())
                        .collect();
                    free = free.difference(&outer_refs).cloned().collect();

                    // From the cols, remove the cols that is created by the map expressions
                    // This is like `union`, but we need to keep the order of the columns
                    let mut new_cols = union(&cols, &free);

                    new_cols = new_cols
                        .into_iter()
                        .filter(|col| !existing_exprs.iter().any(|(id, _)| *id == *col))
                        .collect();
                    let plan = input
                        .project(true, enabled_rules, col_id_gen, new_cols)
                        .map(false, enabled_rules, col_id_gen, existing_exprs);
                    if cols_set == plan.att() {
                        plan
                    } else {
                        // After pushing down the projection, remove the projection if there is still
                        // a projection node before the map node. This is to avoid unnecessary projection operations.
                        let plan = if let LogicalRelExpr::Map { input, exprs } = plan {
                            if let LogicalRelExpr::Project {
                                src,
                                cols: _no_need_cols,
                            } = *input
                            {
                                src.map(true, enabled_rules, col_id_gen, exprs)
                            } else {
                                LogicalRelExpr::Map { input, exprs }
                            }
                        } else {
                            plan
                        };
                        plan.project(false, enabled_rules, col_id_gen, cols)
                    }
                }
                LogicalRelExpr::Select { src, predicates } => {
                    // We don't push projections through selections. Selections are prioritized.
                    src.select(true, enabled_rules, col_id_gen, predicates)
                        .project(false, enabled_rules, col_id_gen, cols)
                }
                LogicalRelExpr::Join {
                    join_type,
                    left,
                    right,
                    predicates,
                } => {
                    // The necessary columns are the free variables of the predicates and the projection columns
                    let free: HashSet<usize> =
                        predicates.iter().flat_map(|pred| pred.free()).collect();
                    let new_cols = union(&cols, &free);
                    let left_proj = intersect(&new_cols, &left.att());
                    let right_proj = intersect(&new_cols, &right.att());
                    left.project(true, enabled_rules, col_id_gen, left_proj)
                        .join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            join_type,
                            right.project(true, enabled_rules, col_id_gen, right_proj),
                            predicates,
                        )
                        .project(false, enabled_rules, col_id_gen, cols)
                }
                LogicalRelExpr::OrderBy { src, cols: orderby } => {
                    // We can push down the projection through order by
                    // if the projection columns are a subset of the order by columns
                    let pushdown_cols =
                        union(&cols, &orderby.iter().map(|(col, _, _)| *col).collect());
                    let plan = src
                        .project(true, enabled_rules, col_id_gen, pushdown_cols)
                        .order_by(false, enabled_rules, col_id_gen, orderby);
                    if cols_set == plan.att() {
                        plan
                    } else {
                        // Remove the projection if it is not needed. This is to avoid unnecessary projection operations.
                        let plan = if let LogicalRelExpr::OrderBy { src, cols } = plan {
                            if let LogicalRelExpr::Project {
                                src,
                                cols: _no_need_cols,
                            } = *src
                            {
                                src.order_by(true, enabled_rules, col_id_gen, cols)
                            } else {
                                LogicalRelExpr::OrderBy { src, cols }
                            }
                        } else {
                            plan
                        };
                        plan.project(false, enabled_rules, col_id_gen, cols)
                    }
                }
                LogicalRelExpr::Rename {
                    src,
                    src_to_dest: mut existing_rename,
                } => {
                    // Remove the mappings that are not used in the projection.
                    existing_rename.retain(|_, dest| cols.contains(dest));

                    // Pushdown the projection to the source. First we need to rewrite the column names
                    let existing_rename_rev: HashMap<usize, usize> = existing_rename
                        .iter()
                        .map(|(src, dest)| (*dest, *src))
                        .collect(); // dest -> src
                    let mut new_cols = Vec::new();
                    for col in cols.iter() {
                        new_cols.push(existing_rename_rev.get(col).unwrap_or(&col).clone());
                    }

                    // Notice that we do not apply a projection node on the `rename` operator
                    // since it is not necessary. But can be added for clarity.
                    src.project(true, enabled_rules, &col_id_gen, new_cols)
                        .rename_to(existing_rename)
                }
                LogicalRelExpr::Scan {
                    db_id,
                    c_id,
                    table_name,
                    column_indices: mut column_names,
                } => {
                    column_names.retain(|col| cols.contains(col));
                    LogicalRelExpr::scan(db_id, c_id, table_name, column_names)
                }
                _ => self.project(false, enabled_rules, col_id_gen, cols),
            }
        } else {
            LogicalRelExpr::Project {
                src: Box::new(self),
                cols,
            }
        }
    }
}

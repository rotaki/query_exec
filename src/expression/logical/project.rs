// Reference: https://github.com/rotaki/decorrelator

use super::super::prelude::*;
use super::prelude::*;
use crate::catalog::ColIdGenRef;
use std::collections::{HashMap, HashSet};

fn to_vec(set: HashSet<usize>) -> Vec<usize> {
    set.into_iter().collect()
}

fn to_set(vec: Vec<usize>) -> HashSet<usize> {
    vec.into_iter().collect()
}

impl LogicalRelExpr {
    // Ordered projection
    pub fn o_project(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        cols: Vec<usize>,
    ) -> LogicalRelExpr {
        let plan = self.u_project(optimize, enabled_rules, col_id_gen, to_set(cols.clone()));

        if let LogicalRelExpr::Project {
            src,
            cols: _no_need_cols,
        } = plan
        {
            LogicalRelExpr::Project { src, cols }
        } else {
            LogicalRelExpr::Project {
                src: Box::new(plan),
                cols,
            }
        }
    }

    // Unordered projection
    pub fn u_project(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        cols: HashSet<usize>,
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
                } => src.u_project(true, enabled_rules, col_id_gen, cols),
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
                    let mut new_cols: HashSet<usize> = cols.union(&free).cloned().collect();
                    new_cols = new_cols
                        .difference(&existing_exprs.iter().map(|(id, _)| *id).collect())
                        .cloned()
                        .collect();

                    let plan = input
                        .u_project(true, enabled_rules, col_id_gen, new_cols)
                        .map(true, enabled_rules, col_id_gen, existing_exprs);
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
                                // This logic is to reduce excessive projection nodes.
                                //
                                // From:
                                // Project @10020
                                //   Map @10020 <- @10010
                                //     Project @10010
                                //       ...
                                //
                                // To:
                                // Project @10020
                                //   Map @10020 <- @10010
                                //      ...
                                let new_plan = src.map(true, enabled_rules, col_id_gen, exprs);
                                if let LogicalRelExpr::Map { input, mut exprs } = new_plan {
                                    // Special case for the following scenario:
                                    // From:
                                    // Project @10020
                                    //   Map @10020 <- @10010
                                    //     Project @10010
                                    //       Map @10010 <- @10000
                                    // To:
                                    // Project @10020
                                    //   Map @10020 <- @10000
                                    //
                                    // First, it removes the lower projection:
                                    // Project @10020
                                    //   Map @10020 <- @10010
                                    //     Map @10010 <- @10000
                                    //
                                    // The map is optimized to:
                                    // Project @10020
                                    //   Map @10020 <- @10000, @10010 <- @10000
                                    // This is the new_plan specified above.
                                    //
                                    // Finally, it removes the second map:
                                    // Project @10020
                                    //   Map @10020 <- @10000
                                    exprs.retain(|(id, _)| cols.contains(id));
                                    input.map(false, enabled_rules, col_id_gen, exprs)
                                } else {
                                    new_plan
                                }
                            } else {
                                LogicalRelExpr::Map { input, exprs }
                            }
                        } else {
                            plan
                        };
                        plan.u_project(false, enabled_rules, col_id_gen, cols)
                    }
                }
                LogicalRelExpr::Aggregate {
                    src,
                    group_by,
                    aggrs,
                } => {
                    // Remove aggregates that are not used in the projection
                    let mut new_aggrs = Vec::new();
                    for (dest, (src, op)) in aggrs {
                        if cols.contains(&dest) {
                            new_aggrs.push((dest, (src, op)));
                        }
                    }
                    let plan = src.aggregate(true, enabled_rules, col_id_gen, group_by, new_aggrs);
                    if cols_set == plan.att() {
                        plan
                    } else {
                        // Remove the projection if it is not needed. This is to avoid unnecessary projection operations.
                        let plan = if let LogicalRelExpr::Aggregate {
                            src,
                            group_by,
                            aggrs,
                        } = plan
                        {
                            if let LogicalRelExpr::Project {
                                src,
                                cols: _no_need_cols,
                            } = *src
                            {
                                src.aggregate(false, enabled_rules, col_id_gen, group_by, aggrs)
                            } else {
                                LogicalRelExpr::Aggregate {
                                    src,
                                    group_by,
                                    aggrs,
                                }
                            }
                        } else {
                            plan
                        };
                        plan.u_project(false, enabled_rules, col_id_gen, cols)
                    }
                }
                LogicalRelExpr::Select { src, predicates } => {
                    // We don't push projections through selections. Selections are prioritized.
                    src.select(true, enabled_rules, col_id_gen, predicates)
                        .u_project(false, enabled_rules, col_id_gen, cols)
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
                    let new_cols = cols.union(&free).cloned().collect();
                    let left_proj = left.att().intersection(&new_cols).cloned().collect();
                    let right_proj = right.att().intersection(&new_cols).cloned().collect();
                    left.u_project(true, enabled_rules, col_id_gen, left_proj)
                        .join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            join_type,
                            right.u_project(true, enabled_rules, col_id_gen, right_proj),
                            predicates,
                        )
                        .u_project(false, enabled_rules, col_id_gen, cols)
                }
                LogicalRelExpr::OrderBy { src, cols: orderby } => {
                    // We can push down the projection through order by
                    // if the projection columns are a subset of the order by columns
                    let pushdown_cols = cols
                        .union(&orderby.iter().map(|(col, _, _)| *col).collect())
                        .cloned()
                        .collect();
                    let plan = src
                        .u_project(true, enabled_rules, col_id_gen, pushdown_cols)
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
                        plan.u_project(false, enabled_rules, col_id_gen, cols)
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
                    let mut new_cols = HashSet::new();
                    for col in cols {
                        new_cols.insert(*existing_rename_rev.get(&col).unwrap_or(&col));
                    }
                    // Notice that we do not apply a projection node on the `rename` operator
                    // since it is not necessary. But can be added for clarity.
                    src.u_project(true, enabled_rules, col_id_gen, new_cols)
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
                _ => self.u_project(false, enabled_rules, col_id_gen, cols),
            }
        } else {
            LogicalRelExpr::Project {
                src: Box::new(self),
                cols: to_vec(cols),
            }
        }
    }
}

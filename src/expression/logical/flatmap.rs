use super::prelude::*;
use crate::catalog::ColIdGenRef;
use crate::Field;
use std::collections::{BTreeMap, BTreeSet, HashSet};

impl LogicalRelExpr {
    pub fn try_flatmap_mark(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        func: LogicalRelExpr,
        mark_id: usize,
    ) -> Option<LogicalRelExpr> {
        if optimize && enabled_rules.is_enabled(&HeuristicRule::Decorrelate) {
            if func.free().is_empty() {
                return Some(self.join(
                    true,
                    enabled_rules,
                    col_id_gen,
                    JoinType::LeftMarkJoin(mark_id),
                    func,
                    BTreeSet::new(),
                ));
            }

            // Depth first search until cardinality changing operator is found.
            // The values returned by the function are not used so we do not care
            // about projections, maps, or orderbys after the select.
            match func {
                LogicalRelExpr::Select { src, predicates } => {
                    if src.free().is_empty() {
                        return Some(self.join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            JoinType::LeftMarkJoin(mark_id),
                            *src,
                            predicates,
                        ));
                    } else {
                        None
                    }
                }
                LogicalRelExpr::Project { src, cols } => {
                    self.try_flatmap_mark(true, enabled_rules, col_id_gen, *src, mark_id)
                }
                LogicalRelExpr::Map { input, exprs } => {
                    self.try_flatmap_mark(true, enabled_rules, col_id_gen, *input, mark_id)
                }
                LogicalRelExpr::OrderBy { src, cols } => {
                    self.try_flatmap_mark(true, enabled_rules, col_id_gen, *src, mark_id)
                }
                _ => None,
            }
        } else {
            None
        }
    }

    // Flat map semi executes a function for each row in the input relation.
    // The result of the function is NOT merged with the input relation.
    // The input relation is NOT preserved in the output.
    // The output might have fewer rows than the input relation.
    // When the function returns 0 input for a row, the row is removed from the output.
    pub fn try_flatmap_semi(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        func: LogicalRelExpr,
    ) -> Option<LogicalRelExpr> {
        if optimize && enabled_rules.is_enabled(&HeuristicRule::Decorrelate) {
            if func.free().is_empty() {
                return Some(self.join(
                    true,
                    enabled_rules,
                    col_id_gen,
                    JoinType::LeftSemi,
                    func,
                    BTreeSet::new(),
                ));
            }

            // Depth first search until cardinality changing operator is found.
            // The values returned by the function are not used so we do not care
            // about projections, maps, or orderbys after the select.
            match func {
                // Cardinality changing operators
                LogicalRelExpr::Select { src, predicates } => {
                    if src.free().is_empty() {
                        return Some(self.join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            JoinType::LeftSemi,
                            *src,
                            predicates,
                        ));
                    } else {
                        None
                    }
                }
                // Non-cardinality changing operators
                LogicalRelExpr::Project { src, cols } => {
                    self.try_flatmap_semi(true, enabled_rules, col_id_gen, *src)
                }
                LogicalRelExpr::Map { input, exprs } => {
                    self.try_flatmap_semi(true, enabled_rules, col_id_gen, *input)
                }
                LogicalRelExpr::OrderBy { src, cols } => {
                    self.try_flatmap_semi(true, enabled_rules, col_id_gen, *src)
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn try_flatmap_anti(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        func: LogicalRelExpr,
    ) -> Option<LogicalRelExpr> {
        if optimize && enabled_rules.is_enabled(&HeuristicRule::Decorrelate) {
            if func.free().is_empty() {
                return Some(self.join(
                    true,
                    enabled_rules,
                    col_id_gen,
                    JoinType::LeftAnti,
                    func,
                    BTreeSet::new(),
                ));
            }

            // Depth first search until cardinality changing operator is found.
            // The values returned by the function are not used so we do not care
            // about projections, maps, or orderbys after the select.
            match func {
                // Cardinality changing operators
                LogicalRelExpr::Select { src, predicates } => {
                    if src.free().is_empty() {
                        return Some(self.join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            JoinType::LeftAnti,
                            *src,
                            predicates,
                        ));
                    } else {
                        None
                    }
                }
                // Non-cardinality changing operators
                LogicalRelExpr::Project { src, cols } => {
                    self.try_flatmap_anti(true, enabled_rules, col_id_gen, *src)
                }
                LogicalRelExpr::Map { input, exprs } => {
                    self.try_flatmap_anti(true, enabled_rules, col_id_gen, *input)
                }
                LogicalRelExpr::OrderBy { src, cols } => {
                    self.try_flatmap_anti(true, enabled_rules, col_id_gen, *src)
                }
                _ => None,
            }
        } else {
            None
        }
    }

    // Flat map outer executes a function for each row in the input relation.
    // The result of the function is merged with the input relation.
    // The input relation is preserved in the output.
    // The output might have more rows than the input relation if there are multiple matches
    // with the output of the function.
    // When the function returns 0 input for a row,
    // the row is merged with a NULL value and added to the output.
    pub fn try_flatmap_outer(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        func: LogicalRelExpr,
    ) -> Option<LogicalRelExpr> {
        if optimize && enabled_rules.is_enabled(&HeuristicRule::Decorrelate) {
            if func.free().is_empty() {
                return Some(self.join(
                    true,
                    enabled_rules,
                    col_id_gen,
                    JoinType::LeftOuter,
                    func,
                    BTreeSet::new(),
                ));
            }

            match func {
                LogicalRelExpr::Select { src, predicates }
                    if predicates.iter().all(|p| !p.has_null()) =>
                {
                    self.try_flatmap_outer(true, enabled_rules, col_id_gen, *src)
                        .map(|p| p.select(true, enabled_rules, col_id_gen, predicates))
                }
                LogicalRelExpr::Project { src, cols } => self
                    .try_flatmap_outer(true, enabled_rules, col_id_gen, *src)
                    .map(|p| {
                        p.u_project(true, enabled_rules, col_id_gen, cols.into_iter().collect())
                    }),
                LogicalRelExpr::Map { input, exprs }
                    if exprs.iter().all(|(_, expr)| !expr.has_null()) =>
                {
                    self.try_flatmap_outer(true, enabled_rules, col_id_gen, *input)
                        .map(|p| p.map(true, enabled_rules, col_id_gen, exprs))
                }
                LogicalRelExpr::OrderBy { src, cols } => self
                    .try_flatmap_outer(true, enabled_rules, col_id_gen, *src)
                    .map(|p| p.order_by(true, enabled_rules, col_id_gen, cols)),
                _ => None,
            }
        } else {
            None
        }
    }

    // Flat map executes a function for each row in the input relation.
    // The result of the function is merged with the input relation.
    // The input relation is NOT preserved in the output.
    // When the function returns 0 input for a input row,
    // the row is removed from the output.
    pub fn flatmap(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        func: LogicalRelExpr,
    ) -> LogicalRelExpr {
        if optimize && enabled_rules.is_enabled(&HeuristicRule::Decorrelate) {
            // Not correlated!
            if func.free().is_empty() {
                return self.join(
                    true,
                    enabled_rules,
                    col_id_gen,
                    JoinType::CrossJoin, // A cross join with an empty relation results in an empty relation.
                    func,
                    BTreeSet::new(),
                );
            }

            match func {
                LogicalRelExpr::Rename { .. }
                | LogicalRelExpr::Scan { .. }
                | LogicalRelExpr::FlatMap { .. } => {
                    // Do nothing
                    return LogicalRelExpr::FlatMap {
                        input: Box::new(self),
                        func: Box::new(func),
                    };
                }
                // Pull up Filters
                LogicalRelExpr::Select { src, predicates } => {
                    return self.flatmap(true, enabled_rules, col_id_gen, *src).select(
                        true,
                        enabled_rules,
                        col_id_gen,
                        predicates,
                    );
                }
                // Pull up Projects
                LogicalRelExpr::Project { src, mut cols } => {
                    cols.extend(self.att());
                    return self
                        .flatmap(true, enabled_rules, col_id_gen, *src)
                        .u_project(true, enabled_rules, col_id_gen, cols.into_iter().collect());
                }
                // Pull up Maps
                LogicalRelExpr::Map { input, exprs } => {
                    return self.flatmap(true, enabled_rules, col_id_gen, *input).map(
                        true,
                        enabled_rules,
                        col_id_gen,
                        exprs,
                    );
                }
                // Pull up OrderBy
                LogicalRelExpr::OrderBy { src, cols } => {
                    return self
                        .flatmap(true, enabled_rules, col_id_gen, *src)
                        .order_by(true, enabled_rules, col_id_gen, cols);
                }
                // Pull up Joins
                LogicalRelExpr::Join {
                    left,
                    right,
                    join_type,
                    predicates,
                } => {
                    let att = self.att();
                    let left_free = left.free();
                    let right_free = right.free();
                    let left_intersect: HashSet<usize> =
                        att.intersection(&left_free).cloned().collect();
                    let right_intersect: HashSet<usize> =
                        att.intersection(&right_free).cloned().collect();
                    if right_intersect.is_empty() {
                        return self.flatmap(true, enabled_rules, col_id_gen, *left).join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            join_type,
                            *right,
                            predicates,
                        );
                    } else if left_intersect.is_empty() {
                        return self.flatmap(true, enabled_rules, col_id_gen, *right).join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            join_type,
                            *left,
                            predicates,
                        );
                    } else {
                        unimplemented!("Join with correlated subquery")
                    }
                }
                // Pull up Aggregates
                LogicalRelExpr::Aggregate {
                    src,
                    group_by,
                    aggrs,
                } => {
                    // Return result should be self.att() + func.att()
                    // func.att() is group_by + aggrs
                    let counts: Vec<usize> = aggrs
                        .iter()
                        .filter_map(|(id, (_src_id, op))| {
                            if let AggOp::Count = op {
                                Some(*id)
                            } else {
                                None
                            }
                        })
                        .collect();
                    if counts.is_empty() {
                        let att = self.att();
                        let group_by: HashSet<usize> = group_by
                            .iter()
                            .cloned()
                            .chain(att.iter().cloned())
                            .collect();
                        return self
                            .flatmap(true, enabled_rules, col_id_gen, *src)
                            .aggregate(
                                true,
                                enabled_rules,
                                col_id_gen,
                                group_by.into_iter().collect(),
                                aggrs,
                            );
                    } else {
                        // Deal with the COUNT BUG
                        let orig = self.clone();

                        // Create a copy of the original plan and rename it. Left join the copy with the src.
                        // Need to replace the free variables in the src with the new column ids.
                        let (mut copy, new_col_ids) = self.rename(enabled_rules, col_id_gen);
                        let copy_att = copy.att();
                        let src = src.replace_variables(&new_col_ids);
                        copy = copy
                            .flatmap(true, enabled_rules, col_id_gen, src)
                            .aggregate(
                                true,
                                enabled_rules,
                                col_id_gen,
                                group_by
                                    .into_iter()
                                    .chain(copy_att.iter().cloned())
                                    .collect(),
                                aggrs,
                            );
                        // Join the original plan with the copy with the shared columns.
                        let plan = orig.join(
                            true,
                            enabled_rules,
                            col_id_gen,
                            JoinType::LeftOuter,
                            copy,
                            new_col_ids
                                .iter()
                                .map(|(src, dest)| {
                                    Expression::col_ref(*src).eq(Expression::col_ref(*dest))
                                })
                                .collect(),
                        );
                        // plan.att() contains (duplicated) join cols, group_by and aggrs.
                        // 1. We replace the original count columns with the new column ids.
                        // 2. We remove the duplicated join cols and convert the aggrs to a new column id
                        // if it's a count column.
                        // 3. We remap the count columns to the original count columns.

                        // We need to replace the count columns with new column ids.
                        // The case expression will return the value with the original count column id.
                        // Original -> New
                        let replace_count_cols = counts
                            .iter()
                            .map(|id| (*id, col_id_gen.next()))
                            .collect::<BTreeMap<_, _>>();
                        // New -> Original
                        let replace_count_cols_rev = replace_count_cols
                            .iter()
                            .map(|(src, dest)| (*dest, *src))
                            .collect::<BTreeMap<_, _>>();
                        let new_plan = plan.replace_variables(&replace_count_cols);

                        // Get the projected columns
                        let project_att: Vec<_> =
                            new_plan.att().difference(&copy_att).cloned().collect(); // Remove the duplicated join cols
                        let new_project_att = project_att
                            .iter()
                            .map(|id| {
                                // If the column is a count column, replace it with the new column id
                                *replace_count_cols_rev.get(id).unwrap_or(id)
                            })
                            .collect::<Vec<_>>(); // Convert the aggrs to a new column id if it's a count column

                        return new_plan
                            .map(
                                true,
                                enabled_rules,
                                col_id_gen,
                                replace_count_cols_rev
                                    .into_iter()
                                    .map(|(new_id, original_id)| {
                                        (
                                            // Set the count to 0 if it's NULL. Create a case expression for each count column.
                                            original_id,
                                            Expression::Case {
                                                expr: Some(Box::new(Expression::col_ref(new_id))),
                                                whens: [(
                                                    Expression::Field {
                                                        val: Field::Int(None),
                                                    },
                                                    Expression::int(0),
                                                )]
                                                .to_vec(),
                                                else_expr: Some(Box::new(Expression::col_ref(
                                                    new_id,
                                                ))),
                                            },
                                        )
                                    })
                                    .collect::<Vec<_>>(),
                            )
                            .u_project(
                                true,
                                enabled_rules,
                                col_id_gen,
                                new_project_att.into_iter().collect(),
                            );
                    }
                }
            }
        }
        LogicalRelExpr::FlatMap {
            input: Box::new(self),
            func: Box::new(func),
        }
    }
}

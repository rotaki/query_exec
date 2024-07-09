// Reference: https://github.com/rotaki/decorrelator

use super::prelude::*;
use crate::catalog::ColIdGenRef;
use std::collections::{BTreeSet, HashSet};

impl LogicalRelExpr {
    /// Apply on(join) to the current logical relational expression.
    pub fn on(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        join_type: JoinType,
        other: LogicalRelExpr,
        on_conditions: BTreeSet<Expression<LogicalRelExpr>>,
    ) -> LogicalRelExpr {
        let on_conditions: BTreeSet<_> = on_conditions
            .into_iter()
            .flat_map(|expr| expr.split_conjunction())
            .collect();

        if on_conditions.is_empty() {
            return LogicalRelExpr::Join {
                join_type,
                left: Box::new(self),
                right: Box::new(other),
                predicates: on_conditions,
            };
        }

        if optimize {
            let (left_pushdown, keep): (BTreeSet<_>, BTreeSet<_>) = on_conditions
                .into_iter()
                .partition(|pred| pred.bound_by(&self));
            let (right_pushdown, keep): (BTreeSet<_>, BTreeSet<_>) =
                keep.into_iter().partition(|pred| pred.bound_by(&other));
            let left_additional_pushdown = keep
                .iter()
                .flat_map(|pred| pred.extract_bounded_predicates(&self.att()))
                .collect::<BTreeSet<_>>();
            let right_additional_pushdown = keep
                .iter()
                .flat_map(|pred| pred.extract_bounded_predicates(&other.att()))
                .collect::<BTreeSet<_>>();
            let left = self.select(
                true,
                enabled_rules,
                col_id_gen,
                left_pushdown
                    .into_iter()
                    .chain(left_additional_pushdown)
                    .collect(),
            );
            let right = other.select(
                true,
                enabled_rules,
                col_id_gen,
                right_pushdown
                    .into_iter()
                    .chain(right_additional_pushdown)
                    .collect(),
            );
            match join_type {
                JoinType::Inner
                | JoinType::CrossJoin
                | JoinType::RightAnti
                | JoinType::RightOuter
                | JoinType::RightSemi
                | JoinType::RightMarkJoin(_) => LogicalRelExpr::Join {
                    join_type,
                    left: Box::new(left),
                    right: Box::new(right),
                    predicates: keep.into_iter().collect(),
                },
                JoinType::LeftOuter => LogicalRelExpr::Join {
                    join_type: JoinType::RightOuter,
                    left: Box::new(right),
                    right: Box::new(left),
                    predicates: keep.into_iter().collect(),
                },
                JoinType::LeftSemi => LogicalRelExpr::Join {
                    join_type: JoinType::RightSemi,
                    left: Box::new(right),
                    right: Box::new(left),
                    predicates: keep.into_iter().collect(),
                },
                JoinType::LeftAnti => LogicalRelExpr::Join {
                    join_type: JoinType::RightAnti,
                    left: Box::new(right),
                    right: Box::new(left),
                    predicates: keep.into_iter().collect(),
                },
                JoinType::LeftMarkJoin(col_id) => LogicalRelExpr::Join {
                    join_type: JoinType::RightMarkJoin(col_id),
                    left: Box::new(right),
                    right: Box::new(left),
                    predicates: keep.into_iter().collect(),
                },
                JoinType::FullOuter => {
                    // Not covered
                    unimplemented!("FullOuter join is not supported");
                }
            }
        } else {
            LogicalRelExpr::Join {
                join_type,
                left: Box::new(self),
                right: Box::new(other),
                predicates: on_conditions,
            }
        }
    }

    /// Apply join to the current and the other logical relational expressions.
    pub fn join(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        join_type: JoinType,
        other: LogicalRelExpr,
        predicates: BTreeSet<Expression<LogicalRelExpr>>,
    ) -> LogicalRelExpr {
        let predicates: BTreeSet<_> = predicates
            .into_iter()
            .flat_map(|expr| expr.split_conjunction())
            .collect();

        if predicates.is_empty() {
            return LogicalRelExpr::Join {
                join_type,
                left: Box::new(self),
                right: Box::new(other),
                predicates,
            };
        }

        if optimize {
            match join_type {
                JoinType::Inner => {
                    let (left_pushdown, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&self));
                    let (right_pushdown, keep): (BTreeSet<_>, BTreeSet<_>) =
                        keep.into_iter().partition(|pred| pred.bound_by(&other));
                    let left_additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&self.att()))
                        .collect::<BTreeSet<_>>();
                    let right_additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&other.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type,
                        left: Box::new(
                            self.select(
                                true,
                                enabled_rules,
                                col_id_gen,
                                left_pushdown
                                    .into_iter()
                                    .chain(left_additional_pushdown)
                                    .collect(),
                            ),
                        ),
                        right: Box::new(
                            other.select(
                                true,
                                enabled_rules,
                                col_id_gen,
                                right_pushdown
                                    .into_iter()
                                    .chain(right_additional_pushdown)
                                    .collect(),
                            ),
                        ),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::LeftOuter => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&self));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&self.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightOuter,
                        left: Box::new(other),
                        right: Box::new(self.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::RightOuter => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&other));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&other.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightOuter,
                        left: Box::new(self),
                        right: Box::new(other.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::CrossJoin => {
                    let (left_pushdown, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&self));
                    let (right_pushdown, keep): (BTreeSet<_>, BTreeSet<_>) =
                        keep.into_iter().partition(|pred| pred.bound_by(&other));
                    let left_additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&self.att()))
                        .collect::<BTreeSet<_>>();
                    let right_additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&other.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type,
                        left: Box::new(
                            self.select(
                                true,
                                enabled_rules,
                                col_id_gen,
                                left_pushdown
                                    .into_iter()
                                    .chain(left_additional_pushdown)
                                    .collect(),
                            ),
                        ),
                        right: Box::new(
                            other.select(
                                true,
                                enabled_rules,
                                col_id_gen,
                                right_pushdown
                                    .into_iter()
                                    .chain(right_additional_pushdown)
                                    .collect(),
                            ),
                        ),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::LeftSemi => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&self));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&self.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightSemi,
                        left: Box::new(other),
                        right: Box::new(self.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::RightSemi => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&other));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&other.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightSemi,
                        left: Box::new(self),
                        right: Box::new(other.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::LeftAnti => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&self));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&self.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightAnti,
                        left: Box::new(other),
                        right: Box::new(self.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::RightAnti => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&other));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&other.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightAnti,
                        left: Box::new(self),
                        right: Box::new(other.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::LeftMarkJoin(col_id) => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&self));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&self.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightMarkJoin(col_id),
                        left: Box::new(other),
                        right: Box::new(self.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::RightMarkJoin(col_id) => {
                    let (push_down, keep): (BTreeSet<_>, BTreeSet<_>) = predicates
                        .into_iter()
                        .partition(|pred| pred.bound_by(&other));
                    let additional_pushdown = keep
                        .iter()
                        .flat_map(|pred| pred.extract_bounded_predicates(&other.att()))
                        .collect::<BTreeSet<_>>();
                    LogicalRelExpr::Join {
                        join_type: JoinType::RightMarkJoin(col_id),
                        left: Box::new(self),
                        right: Box::new(other.select(
                            true,
                            enabled_rules,
                            col_id_gen,
                            push_down.into_iter().chain(additional_pushdown).collect(),
                        )),
                        predicates: keep.into_iter().collect(),
                    }
                }
                JoinType::FullOuter => {
                    // Not covered
                    unimplemented!("FullOuter join is not supported");
                }
            }
        } else {
            LogicalRelExpr::Join {
                join_type,
                left: Box::new(self),
                right: Box::new(other),
                predicates,
            }
        }
    }
}

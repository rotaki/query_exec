// Reference: https://github.com/rotaki/decorrelator

use super::prelude::*;
use crate::catalog::ColIdGenRef;
use std::collections::HashSet;

impl LogicalRelExpr {
    /// Apply on(join) to the current logical relational expression.
    pub fn on(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        join_type: JoinType,
        other: LogicalRelExpr,
        mut on_conditions: Vec<Expression<LogicalRelExpr>>, // On condition. On conditions can be pushed down to the source independently of the join type.
    ) -> LogicalRelExpr {
        if on_conditions.is_empty() {
            return LogicalRelExpr::Join {
                join_type,
                left: Box::new(self),
                right: Box::new(other),
                predicates: on_conditions,
            };
        }

        on_conditions = on_conditions
            .into_iter()
            .flat_map(|expr| expr.split_conjunction())
            .collect();

        if optimize {
            let (push_down, keep): (Vec<_>, Vec<_>) =
                on_conditions.iter().partition(|pred| pred.bound_by(&self));
            if !push_down.is_empty() {
                // This condition is necessary to avoid infinite recursion
                let push_down = push_down.into_iter().map(|expr| expr.clone()).collect();
                let keep = keep.into_iter().map(|expr| expr.clone()).collect();
                return self.select(true, enabled_rules, col_id_gen, push_down).on(
                    true,
                    enabled_rules,
                    col_id_gen,
                    join_type,
                    other,
                    keep,
                );
            }

            let (push_down, keep): (Vec<_>, Vec<_>) =
                on_conditions.iter().partition(|pred| pred.bound_by(&other));
            if !push_down.is_empty() {
                // This condition is necessary to avoid infinite recursion
                let push_down = push_down.into_iter().map(|expr| expr.clone()).collect();
                let keep = keep.into_iter().map(|expr| expr.clone()).collect();
                return self.on(
                    true,
                    enabled_rules,
                    col_id_gen,
                    join_type,
                    other.select(true, enabled_rules, col_id_gen, push_down),
                    keep,
                );
            }

            // We pushed down all the on_conditions to the source.
            return self.join(
                true,
                enabled_rules,
                col_id_gen,
                join_type,
                other,
                on_conditions,
            );
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
        mut predicates: Vec<Expression<LogicalRelExpr>>, // Pushed down predicates. This can only be applied to the source if join_type matches the condition.
    ) -> LogicalRelExpr {
        if predicates.is_empty() {
            return LogicalRelExpr::Join {
                join_type,
                left: Box::new(self),
                right: Box::new(other),
                predicates,
            };
        }

        predicates = predicates
            .into_iter()
            .flat_map(|expr| expr.split_conjunction())
            .collect();

        if optimize {
            if matches!(
                join_type,
                JoinType::Inner | JoinType::LeftOuter | JoinType::CrossJoin
            ) {
                let (push_down, keep): (Vec<_>, Vec<_>) =
                    predicates.iter().partition(|pred| pred.bound_by(&self));
                if !push_down.is_empty() {
                    // This condition is necessary to avoid infinite recursion
                    let push_down = push_down.into_iter().map(|expr| expr.clone()).collect();
                    let keep = keep.into_iter().map(|expr| expr.clone()).collect();
                    return self
                        .select(true, enabled_rules, col_id_gen, push_down)
                        .join(true, enabled_rules, col_id_gen, join_type, other, keep);
                }
            }

            if matches!(
                join_type,
                JoinType::Inner | JoinType::RightOuter | JoinType::CrossJoin
            ) {
                let (push_down, keep): (Vec<_>, Vec<_>) =
                    predicates.iter().partition(|pred| pred.bound_by(&other));
                if !push_down.is_empty() {
                    // This condition is necessary to avoid infinite recursion
                    let push_down = push_down.into_iter().map(|expr| expr.clone()).collect();
                    let keep = keep.into_iter().map(|expr| expr.clone()).collect();
                    return self.join(
                        true,
                        enabled_rules,
                        col_id_gen,
                        join_type,
                        other.select(true, enabled_rules, col_id_gen, push_down),
                        keep,
                    );
                }
            }

            // If the remaining predicates are bound by the left and right sides
            if matches!(join_type, JoinType::CrossJoin) {
                #[cfg(debug_assertions)]
                {
                    // The remaining predicates should not contain any free vaiables.
                    // Need to use flatmap or map to reference a free variable.
                    let free = predicates
                        .iter()
                        .flat_map(|expr| expr.free())
                        .collect::<HashSet<_>>();
                    let atts = self.att().union(&other.att()).cloned().collect();
                    assert!(free.is_subset(&atts));
                }

                return self.join(
                    false,
                    enabled_rules,
                    col_id_gen,
                    JoinType::Inner,
                    other,
                    predicates,
                );
            }

            if matches!(join_type, JoinType::LeftOuter) {
                // Always convert to right outer join because
                // we assume that we build the hash table on the left side
                // and probe the hash table on the right side, thus
                // right outer join is more efficient.
                return other.join(
                    false,
                    enabled_rules,
                    col_id_gen,
                    JoinType::RightOuter,
                    self,
                    predicates,
                );
            }
        }

        LogicalRelExpr::Join {
            join_type,
            left: Box::new(self),
            right: Box::new(other),
            predicates,
        }
    }
}

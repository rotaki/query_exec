// Reference: https://github.com/rotaki/decorrelator

use super::prelude::*;
use crate::catalog::ColIdGenRef;

impl LogicalRelExpr {
    // Make subquery into a FlatMap
    // FlatMap is sometimes called "Apply", "Dependent Join", or "Lateral Join"
    //
    // SQL Query:
    // Table x: a, b
    // Table y: c
    //
    // SELECT x.a, x.b, 4 + (SELECT x.a + y.c FROM y) FROM x
    //
    // Before:
    // ---------------------------------------
    // |  Map to @4                          |
    // |            ------------------------ |
    // |            |  Subquery: @3 + @1   | |
    // |    4 +     |  Scan @3             | |
    // |            ------------------------ |
    // ---------------------------------------
    //                 |
    // ---------------------------------------
    // |  Scan  @1, @2                       |
    // ---------------------------------------

    // After:
    // -------------------------------------------
    // |  Project @1, @2, @4                     |
    // -------------------------------------------
    //                  |
    // -------------------------------------------
    // |  Map to @4                              |
    // |     @lhs_id + @rhs_id                   |
    // -------------------------------------------
    //                  |
    // -------------------------------------------
    // |  FlatMap (@rhs_id <- @3 + @1)           |
    // -------------------------------------------
    //              /                   \
    // ---------------------------     -----------
    // |  Join (@lhs_id <- 4)    |     | @3 + @1 |
    // ---------------------------     -----------
    //          /         \
    // ----------------
    // |  Scan @1, @2 |     4
    // ----------------

    /// Try to make a subquery into a flatmap
    pub(crate) fn hoist(
        self,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        id: usize,
        expr: Expression<LogicalRelExpr>,
    ) -> LogicalRelExpr {
        match expr {
            Expression::Subquery { expr } => {
                let att = expr.att();
                assert!(att.len() == 1);
                let input_col_id = att.iter().next().unwrap();
                if att.len() != 1 {
                    panic!("Subquery has more than one column");
                }
                // Give the column the name that's expected
                let rhs: LogicalRelExpr = expr.map(
                    true,
                    enabled_rules,
                    col_id_gen,
                    vec![(id, Expression::col_ref(*input_col_id))],
                );
                self.flatmap(true, enabled_rules, col_id_gen, rhs)
            }
            Expression::Binary { op, left, right } => {
                // Hoist the left, hoist the right, then perform the binary operation
                let lhs_id = col_id_gen.next();
                let rhs_id = col_id_gen.next();
                let att = self.att();
                self.hoist(enabled_rules, col_id_gen, lhs_id, *left)
                    .hoist(enabled_rules, col_id_gen, rhs_id, *right)
                    .map(
                        true,
                        enabled_rules,
                        col_id_gen,
                        [(
                            id,
                            Expression::Binary {
                                op,
                                left: Box::new(Expression::col_ref(lhs_id)),
                                right: Box::new(Expression::col_ref(rhs_id)),
                            },
                        )],
                    )
                    .project(
                        true,
                        enabled_rules,
                        col_id_gen,
                        att.into_iter().chain([id].into_iter()).collect(),
                    )
            }
            Expression::Field { .. } | Expression::ColRef { .. } => {
                self.map(true, enabled_rules, col_id_gen, vec![(id, expr)])
            }
            Expression::IsNull { expr } => {
                let att = self.att();
                let expr_id = col_id_gen.next();
                self.hoist(enabled_rules, col_id_gen, expr_id, *expr)
                    .map(
                        true,
                        enabled_rules,
                        col_id_gen,
                        [(
                            id,
                            Expression::IsNull {
                                expr: Box::new(Expression::col_ref(expr_id)),
                            },
                        )],
                    )
                    .project(
                        true,
                        enabled_rules,
                        col_id_gen,
                        att.into_iter().chain([id].into_iter()).collect(),
                    )
            }
            Expression::Case { .. } => {
                panic!("Case expression is not supported in hoist")
            }
            Expression::Between { expr, lower, upper } => {
                let att = self.att();
                let expr_id = col_id_gen.next();
                let lower_id = col_id_gen.next();
                let upper_id = col_id_gen.next();
                self.hoist(enabled_rules, col_id_gen, expr_id, *expr)
                    .hoist(enabled_rules, col_id_gen, lower_id, *lower)
                    .hoist(enabled_rules, col_id_gen, upper_id, *upper)
                    .map(
                        true,
                        enabled_rules,
                        col_id_gen,
                        [(
                            id,
                            Expression::Between {
                                expr: Box::new(Expression::col_ref(expr_id)),
                                lower: Box::new(Expression::col_ref(lower_id)),
                                upper: Box::new(Expression::col_ref(upper_id)),
                            },
                        )],
                    )
                    .project(
                        true,
                        enabled_rules,
                        col_id_gen,
                        att.into_iter().chain([id].into_iter()).collect(),
                    )
            }
            Expression::Extract { field, expr } => {
                let att = self.att();
                let expr_id = col_id_gen.next();
                self.hoist(enabled_rules, col_id_gen, expr_id, *expr)
                    .map(
                        true,
                        enabled_rules,
                        col_id_gen,
                        [(
                            id,
                            Expression::Extract {
                                field,
                                expr: Box::new(Expression::col_ref(expr_id)),
                            },
                        )],
                    )
                    .project(
                        true,
                        enabled_rules,
                        col_id_gen,
                        att.into_iter().chain([id].into_iter()).collect(),
                    )
            }
            Expression::Like {
                expr,
                pattern,
                escape,
            } => {
                let att = self.att();
                let expr_id = col_id_gen.next();
                self.hoist(enabled_rules, col_id_gen, expr_id, *expr)
                    .map(
                        true,
                        enabled_rules,
                        col_id_gen,
                        [(
                            id,
                            Expression::Like {
                                expr: Box::new(Expression::col_ref(expr_id)),
                                pattern,
                                escape,
                            },
                        )],
                    )
                    .project(
                        true,
                        enabled_rules,
                        col_id_gen,
                        att.into_iter().chain([id].into_iter()).collect(),
                    )
            }
        }
    }
}

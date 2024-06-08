// Reference: https://github.com/rotaki/decorrelator

use crate::prelude::ColIdGenRef;

use super::prelude::*;

impl LogicalRelExpr {
    /// Apply aggregation to the current logical relational expression.
    /// aggrs: (dest_column_id, (src_column_id, agg_op))
    pub fn aggregate(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        group_by: Vec<ColumnId>,
        aggrs: Vec<(ColumnId, (ColumnId, AggOp))>,
    ) -> LogicalRelExpr {
        if optimize {
            // Add projection for group by and aggregation
            let mut src_cols = Vec::with_capacity(group_by.len() + aggrs.len());
            for g in &group_by {
                src_cols.push(*g);
            }
            for (_, (src, _)) in &aggrs {
                src_cols.push(*src);
            }
            let plan = self.project(true, enabled_rules, col_id_gen, src_cols);
            LogicalRelExpr::Aggregate {
                src: Box::new(plan),
                group_by,
                aggrs,
            }
        } else {
            LogicalRelExpr::Aggregate {
                src: Box::new(self),
                group_by,
                aggrs,
            }
        }
    }
}

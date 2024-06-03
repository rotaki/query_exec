use crate::{prelude::ColIdGenRef, ColumnId};

use super::{HeuristicRulesRef, LogicalRelExpr};

impl LogicalRelExpr {
    pub fn order_by(
        self,
        optimize: bool,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        cols: Vec<(ColumnId, bool, bool)>,
    ) -> Self {
        LogicalRelExpr::OrderBy {
            src: Box::new(self),
            cols: cols,
        }
    }
}

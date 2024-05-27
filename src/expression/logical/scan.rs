// Reference: https://github.com/rotaki/decorrelator

use super::LogicalRelExpr;
use crate::prelude::{ColumnId, ContainerId};

impl LogicalRelExpr {
    /// Create a new scan node
    pub fn scan(
        cid: ContainerId,
        table_name: String, // Redundant Info
        column_names: Vec<ColumnId>,
    ) -> LogicalRelExpr {
        LogicalRelExpr::Scan {
            cid,
            table_name,
            column_names,
        }
    }
}

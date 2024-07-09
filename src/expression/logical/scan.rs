// Reference: https://github.com/rotaki/decorrelator

use fbtree::prelude::DatabaseId;

use super::LogicalRelExpr;
use crate::prelude::{ColumnId, ContainerId};

impl LogicalRelExpr {
    /// Create a new scan node
    pub fn scan(
        db_id: DatabaseId,
        c_id: ContainerId,
        table_name: String, // Redundant Info
        column_names: Vec<ColumnId>,
    ) -> LogicalRelExpr {
        LogicalRelExpr::Scan {
            db_id,
            c_id,
            table_name,
            column_indices: column_names,
        }
    }
}

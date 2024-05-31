pub use crate::expression::prelude::{LogicalRelExpr, PhysicalRelExpr};

pub trait Optimizer {
    fn optimize(&self, expr: LogicalRelExpr) -> PhysicalRelExpr;
}

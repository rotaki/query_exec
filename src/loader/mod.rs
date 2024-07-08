mod simple_csv_loader;

use crate::catalog::SchemaRef;
use txn_storage::prelude::*;

pub mod prelude {
    pub use super::simple_csv_loader::SimpleCsvLoader;
}

pub trait DataLoader {
    fn load_data(
        &mut self,
        schema_ref: SchemaRef,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), String>;
}

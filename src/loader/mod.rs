use txn_storage::prelude::*;

use crate::{
    schema::{DataType, SchemaRef},
    tuple::{Field, Tuple},
};

pub trait DataLoader {
    fn load_data(
        &mut self,
        schema_ref: SchemaRef,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), String>;
}

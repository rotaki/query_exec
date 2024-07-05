use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use txn_storage::{DBOptions, DatabaseId, TxnStorageTrait};

use crate::{
    prelude::{Catalog, CatalogRef},
    query_executor::QueryExecutor,
};

pub struct DatabaseEngine<T: TxnStorageTrait> {
    pub storage: Arc<T>,
    pub catalogs: Mutex<HashMap<DatabaseId, CatalogRef>>,
}

impl<T: TxnStorageTrait> DatabaseEngine<T> {
    pub fn new(storage: Arc<T>) -> Self {
        Self {
            storage,
            catalogs: Mutex::new(HashMap::new()),
        }
    }

    pub fn create_db(&self, db_name: &str) -> DatabaseId {
        let db_id = self.storage.open_db(DBOptions::new(db_name)).unwrap();
        self.catalogs
            .lock()
            .unwrap()
            .insert(db_id, Catalog::new().into());
        db_id
    }

    pub fn get_executor(&self, db_id: DatabaseId) -> QueryExecutor<T> {
        let catalog = self.catalogs.lock().unwrap()[&db_id].clone();
        QueryExecutor::new(db_id, catalog, self.storage.clone())
    }
}

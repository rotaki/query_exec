use std::{marker::PhantomData, sync::Arc};

use fbtree::prelude::*;

use super::DataLoader;

use crate::{
    catalog::SchemaRef,
    tuple::{Field, Tuple},
};

pub struct SimpleCsvLoader<R: std::io::Read, T: TxnStorageTrait> {
    rdr: csv::Reader<R>,
    storage: Arc<T>,
    phantom: PhantomData<T>,
}

impl<R: std::io::Read, T: TxnStorageTrait> SimpleCsvLoader<R, T> {
    pub fn new(rdr: csv::Reader<R>, storage: Arc<T>) -> Self {
        Self {
            rdr,
            storage,
            phantom: PhantomData,
        }
    }
}

impl<R: std::io::Read, T: TxnStorageTrait> DataLoader for SimpleCsvLoader<R, T> {
    fn load_data(
        &mut self,
        schema_ref: SchemaRef,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), String> {
        println!("Loading data into container: {}", c_id);
        let mut count = 0;
        let num_tuples = 1000000; // Insert 1000 tuples at a time
        let txn = self.storage.begin_txn(&db_id, TxnOptions::default())?;
        let primary_key_indices = schema_ref.primary_key_indices();
        loop {
            let mut tuples = Vec::new();
            for result in self.rdr.records() {
                match result {
                    Ok(rec) => {
                        let mut tuple = Tuple::with_capacity(schema_ref.columns().len());
                        for (i, field) in rec.iter().enumerate() {
                            let col_def = schema_ref.get_column(i);
                            let field = Field::from_str(col_def, field).map_err(|e| {
                                format!(
                                    "Error: {} parsing field: {:?} (col: {:?}) at index: {}",
                                    e, field, col_def, i
                                )
                            })?;
                            tuple.push(field);
                        }
                        tuples.push((
                            tuple.to_primary_key_bytes(primary_key_indices),
                            tuple.to_bytes(),
                        ));

                        if tuples.len() == num_tuples {
                            break;
                        }
                    }
                    Err(e) => {
                        return Err(e.to_string());
                    }
                }
            }
            count += tuples.len();
            if tuples.is_empty() {
                break;
            } else {
                self.storage.insert_values(&txn, &c_id, tuples)?;
                println!("Inserted {} tuples", count);
            }
        }
        self.storage.commit_txn(&txn, false)?;
        println!("Inserted {} tuples in total", count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        catalog::{ColumnDef, DataType, Schema, SchemaRef},
        tuple::Tuple,
    };
    use csv::ReaderBuilder;
    use fbtree::prelude::*;

    use super::{DataLoader, SimpleCsvLoader};
    use fbtree::prelude::InMemStorage;

    fn get_in_mem_storage() -> Arc<InMemStorage> {
        Arc::new(InMemStorage::new())
    }

    fn setup_table_and_schema<T: TxnStorageTrait>(
        storage: impl AsRef<T>,
    ) -> (DatabaseId, ContainerId, SchemaRef) {
        let storage = storage.as_ref();
        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let c_id = storage
            .create_container(
                &txn,
                &db_id,
                ContainerOptions::new("test_table", ContainerType::BTree),
            )
            .unwrap();
        storage.commit_txn(&txn, false).unwrap();

        let schema = Schema::new(
            vec![
                ColumnDef::new("name", DataType::String, false),
                ColumnDef::new("age", DataType::Int, false),
                ColumnDef::new("city", DataType::String, false),
            ],
            vec![0, 1],
        ); // Primary key is name, age
        let schema_ref = Arc::new(schema);

        (db_id, c_id, schema_ref)
    }

    #[test]
    fn test_csv_load() {
        let storage = get_in_mem_storage();
        let (db_id, c_id, schema_ref) = setup_table_and_schema(&storage);

        let data = "name,age,city\nAlice,23,New York\nBob,25,Los Angeles\nAlice,18,San Francisco\n";
        let rdr = ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(true)
            .from_reader(data.as_bytes());
        let mut loader = SimpleCsvLoader::new(rdr, storage.clone());
        loader.load_data(schema_ref, db_id, c_id).unwrap();

        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let scan_res = storage.scan_range(&txn, &c_id, ScanOptions::new()).unwrap();
        let mut count = 0;
        while let Ok(Some((_k, v))) = storage.iter_next(&scan_res) {
            let tuple = Tuple::from_bytes(&v);
            let name = tuple.get(0).as_string().as_ref().unwrap();
            let age = tuple.get(1).as_int().unwrap();
            let city = tuple.get(2).as_string().as_ref().unwrap();
            match count {
                0 => {
                    assert_eq!(name, "Alice");
                    assert_eq!(age, 18);
                    assert_eq!(city, "San Francisco");
                }
                1 => {
                    assert_eq!(name, "Alice");
                    assert_eq!(age, 23);
                    assert_eq!(city, "New York");
                }
                2 => {
                    assert_eq!(name, "Bob");
                    assert_eq!(age, 25);
                    assert_eq!(city, "Los Angeles");
                }
                _ => {
                    panic!("Unexpected tuple");
                }
            }
            count += 1;
            // println!("{}", tuple);
        }

        assert_eq!(count, 3);
        storage.commit_txn(&txn, false).unwrap();
    }
}

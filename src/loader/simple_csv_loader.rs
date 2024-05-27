use std::sync::Arc;

use txn_storage::prelude::*;

use crate::{
    schema::{DataType, SchemaRef},
    tuple::{Field, Tuple},
};

pub struct SimpleCsvDataLoader<R: std::io::Read, T: TxnStorageTrait> {
    rdr: csv::Reader<R>,
    storage: Arc<T>,
}

impl<R: std::io::Read, T: TxnStorageTrait> SimpleCsvDataLoader<R, T> {
    pub fn new(rdr: csv::Reader<R>, storage: Arc<T>) -> Self {
        Self { rdr, storage }
    }
}

impl<R: std::io::Read, T: TxnStorageTrait> DataLoader for SimpleCsvDataLoader<R, T> {
    fn load_data(
        &mut self,
        schema_ref: SchemaRef,
        db_id: DatabaseId,
        c_id: ContainerId,
    ) -> Result<(), String> {
        let num_tuples = 1000; // Insert 1000 tuples at a time
        let txn = self.storage.begin_txn(&db_id, TxnOptions::default())?;
        let primary_key_indices = schema_ref.primary_key_indices();
        loop {
            let mut tuples = Vec::with_capacity(num_tuples);
            for result in self.rdr.records() {
                match result {
                    Ok(rec) => {
                        let mut tuple = Tuple::with_capacity(schema_ref.columns().len());
                        for (i, field) in rec.iter().enumerate() {
                            let field = Field::from_str(schema_ref.get_column(i), field)?;
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
            if tuples.is_empty() {
                break;
            } else {
                self.storage.insert_values(&txn, &c_id, tuples)?;
            }
        }
        self.storage.commit_txn(&txn, false)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        schema::{ColumnDef, DataType, Schema, SchemaRef},
        tuple::Tuple,
    };
    use csv::ReaderBuilder;
    use txn_storage::prelude::*;

    use super::{DataLoader, SimpleCsvDataLoader};

    fn get_in_mem_storage() -> Arc<txn_storage::InMemStorage> {
        Arc::new(txn_storage::InMemStorage::new())
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
        let mut loader = SimpleCsvDataLoader::new(rdr, storage.clone());
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

use std::sync::Arc;

use sqlparser::parser::ParserError;
use sqlparser::{dialect::GenericDialect, parser::Parser};
use txn_storage::{DatabaseId, TxnStorageTrait};

use crate::catalog::CatalogRef;
use crate::executor::Executor;
use crate::expression::prelude::{
    HeuristicRulesRef, LogicalRelExpr, LogicalToPhysicalRelExpr, PhysicalRelExpr,
};
use crate::parser::{Translator, TranslatorError};

#[derive(Debug)]
pub enum ConductorError {
    InvalidSqlString(ParserError),
    InvalidAst(TranslatorError),
    InvalidLogicalPlan,
    InvalidPhysicalPlan,
    InvalidExecutable,
}

impl From<ParserError> for ConductorError {
    fn from(error: ParserError) -> Self {
        ConductorError::InvalidSqlString(error)
    }
}

impl From<TranslatorError> for ConductorError {
    fn from(error: TranslatorError) -> Self {
        ConductorError::InvalidAst(error)
    }
}

pub struct Conductor<T: TxnStorageTrait> {
    pub db_id: DatabaseId,
    pub catalog_ref: CatalogRef,
    pub storage: Arc<T>,
    pub logical_rules: HeuristicRulesRef,
}

impl<T: TxnStorageTrait> Conductor<T> {
    pub fn new(db_id: DatabaseId, catalog_ref: CatalogRef, storage: Arc<T>) -> Self {
        let logical_rules = HeuristicRulesRef::default();
        Conductor {
            db_id,
            catalog_ref,
            storage,
            logical_rules,
        }
    }

    pub fn parse_sql(sql: &str) -> Result<sqlparser::ast::Query, ConductorError> {
        let dialect = GenericDialect {};
        let statements = Parser::new(&dialect)
            .try_with_sql(&sql)?
            .parse_statements()?;
        let query = {
            let statement = statements.into_iter().next().unwrap();
            match statement {
                sqlparser::ast::Statement::Query(query) => query,
                other => {
                    return Err(ConductorError::InvalidSqlString(ParserError::ParserError(
                        format!("Expected a query, got {:?}", other),
                    )))
                }
            }
        };
        Ok(*query)
    }

    pub fn to_logical(&self, sql_string: &str) -> Result<LogicalRelExpr, ConductorError> {
        let query = Conductor::<T>::parse_sql(sql_string)?;
        let mut translator = Translator::new(self.db_id, &self.catalog_ref, &self.logical_rules);
        let result = translator.process_query(&query)?;
        Ok(result.plan)
    }

    pub fn to_physical(&self, logical_plan: LogicalRelExpr) -> PhysicalRelExpr {
        let physical_plan = LogicalToPhysicalRelExpr.to_physical(logical_plan);
        physical_plan
    }

    pub fn to_executable<E: Executor<T>>(&self, physical_plan: PhysicalRelExpr) -> E {
        E::new(
            self.catalog_ref.clone(),
            self.storage.clone(),
            physical_plan,
        )
    }
}

#[cfg(test)]
mod tests {
    use txn_storage::{
        ContainerId, ContainerOptions, ContainerType, DBOptions, InMemStorage, TxnOptions,
    };

    use crate::{
        catalog::{self, Catalog, ColumnDef, DataType, Schema, SchemaRef, Table},
        executor::prelude::VolcanoIterator,
        tuple::Tuple,
        Field,
    };

    use super::*;

    fn get_in_mem_storage() -> Arc<InMemStorage> {
        Arc::new(InMemStorage::new())
    }

    fn setup_employees_table<T: TxnStorageTrait>(
        storage: impl AsRef<T>,
        db_id: DatabaseId,
        catalog: &Catalog,
    ) -> ContainerId {
        let storage = storage.as_ref();
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        // Create Employees table
        // Schema: id, name, age, department_id
        // 5 tuples

        // Create Employees table
        // Schema: id, name, age, department_id
        // 5 tuples
        /*
        id,name,age,department_id
        1,Alice,30,1
        2,Bob,22,2
        3,Charlie,35,1
        4,David,28,2
        5,Eva,40,NULL
         */
        let c_id = storage
            .create_container(
                &txn,
                &db_id,
                ContainerOptions::new("Employees", ContainerType::BTree),
            )
            .unwrap();

        let schema = Arc::new(Schema::new(
            vec![
                ColumnDef::new("id", DataType::Int, false),
                ColumnDef::new("name", DataType::String, false),
                ColumnDef::new("age", DataType::Int, false),
                ColumnDef::new("department_id", DataType::Int, true),
            ],
            vec![0],
        ));
        catalog.add_table(c_id, Arc::new(Table::new("Employees", schema.clone())));

        let data = vec![
            Tuple::from_fields(vec![1.into(), "Alice".into(), 30.into(), 1.into()]),
            Tuple::from_fields(vec![2.into(), "Bob".into(), 22.into(), 2.into()]),
            Tuple::from_fields(vec![3.into(), "Charlie".into(), 35.into(), 1.into()]),
            Tuple::from_fields(vec![4.into(), "David".into(), 28.into(), 2.into()]),
            Tuple::from_fields(vec![5.into(), "Eva".into(), 40.into(), Field::Int(None)]),
        ];

        storage
            .insert_values(
                &txn,
                &c_id,
                data.into_iter()
                    .map(|t| {
                        (
                            t.to_primary_key_bytes(schema.primary_key_indices()),
                            t.to_bytes(),
                        )
                    })
                    .collect(),
            )
            .unwrap();

        storage.commit_txn(&txn, false).unwrap();

        c_id
    }

    fn setup_departments_table<T: TxnStorageTrait>(
        storage: impl AsRef<T>,
        db_id: DatabaseId,
        catalog: &Catalog,
    ) -> ContainerId {
        let storage = storage.as_ref();
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        // Create Departments table
        // Schema: id, name
        // 3 tuples
        let c_id = storage
            .create_container(
                &txn,
                &db_id,
                ContainerOptions::new("Departments", ContainerType::BTree),
            )
            .unwrap();

        let schema = Arc::new(Schema::new(
            vec![
                ColumnDef::new("id", DataType::Int, false),
                ColumnDef::new("name", DataType::String, false),
            ],
            vec![0],
        ));
        catalog.add_table(c_id, Arc::new(Table::new("Departments", schema.clone())));

        /*
        id,name
        1,HR
        2,Engineering
        3,Marketing
        */

        let data = vec![
            Tuple::from_fields(vec![1.into(), "HR".into()]),
            Tuple::from_fields(vec![2.into(), "Engineering".into()]),
            Tuple::from_fields(vec![3.into(), "Marketing".into()]),
        ];

        storage
            .insert_values(
                &txn,
                &c_id,
                data.into_iter()
                    .map(|t| {
                        (
                            t.to_primary_key_bytes(schema.primary_key_indices()),
                            t.to_bytes(),
                        )
                    })
                    .collect(),
            )
            .unwrap();

        storage.commit_txn(&txn, false).unwrap();

        c_id
    }

    fn check_result(result: &[Tuple], expected: &[Tuple], verbose: bool) {
        let result_string = result
            .iter()
            .map(|t| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        let expected_string = expected
            .iter()
            .map(|t| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        if verbose {
            println!("--- Result ---\n{}", result_string);
            println!("--- Expected ---\n{}", expected_string);
        }
        assert_eq!(
            result, expected,
            "\n--- Result ---\n{}\n--- Expected ---\n{}\n",
            result_string, expected_string
        );
    }

    fn setup_conductor<T: TxnStorageTrait>(storage: Arc<T>) -> Conductor<T> {
        let catalog = Catalog::new();
        let db_id = storage.as_ref().open_db(DBOptions::new("test_db")).unwrap();

        let employees_c_id = setup_employees_table(&storage, db_id, &catalog);
        assert_eq!(employees_c_id, 0);
        let departments_c_id = setup_departments_table(&storage, db_id, &catalog);
        assert_eq!(departments_c_id, 1);

        Conductor::new(db_id, Arc::new(catalog), storage)
    }

    fn run_query<T: TxnStorageTrait>(
        conductor: &Conductor<T>,
        sql_string: &str,
        verbose: bool,
    ) -> Vec<Tuple> {
        let logical_plan = conductor.to_logical(sql_string).unwrap();
        if verbose {
            println!("=== Logical Plan ===");
            logical_plan.pretty_print();
        }
        let physical_plan = conductor.to_physical(logical_plan);
        if verbose {
            println!("=== Physical Plan ===");
            physical_plan.pretty_print();
        }
        let mut executor = conductor.to_executable::<VolcanoIterator<T>>(physical_plan);
        if verbose {
            println!("=== Executor ===");
            println!("{}", executor.to_pretty_string());
        }
        // todo!("Implement pretty print for executor");
        let txn = conductor
            .storage
            .begin_txn(&conductor.db_id, TxnOptions::default())
            .unwrap();
        let result = executor.execute(&txn).unwrap();
        conductor.storage.commit_txn(&txn, false).unwrap();
        result
    }

    #[test]
    fn test_projection() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT name, age FROM Employees";
        let result = run_query(&conductor, sql_string, false);
        let expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["David".into(), 28.into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
        ];
        assert_eq!(
            result,
            expected,
            "Result: \n{}\n",
            result
                .iter()
                .map(|t| t.to_pretty_string())
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    #[test]
    fn test_projection_with_expression() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT name, age + 1 FROM Employees";
        let result = run_query(&conductor, sql_string, false);
        let expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 31.into()]),
            Tuple::from_fields(vec!["Bob".into(), 23.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 36.into()]),
            Tuple::from_fields(vec!["David".into(), 29.into()]),
            Tuple::from_fields(vec!["Eva".into(), 41.into()]),
        ];
        check_result(&result, &expected, false);
    }

    #[test]
    fn test_filter() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT name, age FROM Employees WHERE age > 30";
        let result = run_query(&conductor, sql_string, false);
        let expected = vec![
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
        ];
        check_result(&result, &expected, false);
    }

    #[test]
    fn test_join() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT e.name, e.age, d.name AS department FROM Employees e JOIN Departments d ON e.department_id = d.id";
        let mut result = run_query(&conductor, sql_string, false);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "HR".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "HR".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "Engineering".into()]),
        ];
        result.sort();
        expected.sort();
        check_result(&result, &expected, false);
    }

    #[test]
    fn test_left_join() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT d.name, sum(e.age) FROM Departments d LEFT JOIN Employees e ON e.department_id = d.id GROUP BY d.name";
        let mut result = run_query(&conductor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), Field::Int(None)]),
        ];
        result.sort();
        expected.sort();
        check_result(&result, &expected, true);
    }

    #[test]
    fn test_right_join() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string =  "SELECT d.name, sum(e.age) FROM Employees e RIGHT JOIN Departments d ON e.department_id = d.id GROUP BY d.name";
        let mut result = run_query(&conductor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), Field::Int(None)]),
        ];
        result.sort();
        expected.sort();
        check_result(&result, &expected, true);
    }

    #[test]
    fn test_cross_join() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string =
            "SELECT e.name, e.age, d.name AS department FROM Employees e, Departments d";
        let mut result = run_query(&conductor, sql_string, false);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "HR".into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "HR".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "HR".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "HR".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into(), "HR".into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into(), "Marketing".into()]),
        ];
        result.sort();
        expected.sort();
        check_result(&result, &expected, false);
    }

    #[test]
    fn test_aggregate() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT COUNT(*), AVG(age) FROM Employees";
        let result = run_query(&conductor, sql_string, false);
        let expected = vec![Tuple::from_fields(vec![5.into(), 31.0.into()])];
        check_result(&result, &expected, false);
    }

    #[test]
    fn test_groupby_aggregate() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT d.name, AVG(e.age) AS average_age FROM Employees e JOIN Departments d ON e.department_id = d.id GROUP BY d.name;";
        let mut result = run_query(&conductor, sql_string, false);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 32.5.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 25.0.into()]),
        ];
        result.sort();
        expected.sort();
        check_result(&result, &expected, false);
    }

    #[test]
    fn test_subquery() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        // For each department, count the number of employees and sum of their ages
        let sql_string = "SELECT d.name, cnt, sum_age FROM Departments d, (SELECT department_id, COUNT(*) AS cnt, SUM(age) AS sum_age FROM Employees e WHERE e.department_id = d.id)";
        let mut result = run_query(&conductor, sql_string, false);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 2.into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 2.into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), 0.into(), Field::Int(None)]),
        ];
        result.sort();
        expected.sort();
        check_result(&result, &expected, false);
    }

    #[test]
    fn test_where_exists() {
        let storage = get_in_mem_storage();
        let conductor = setup_conductor(storage.clone());
        let sql_string = "SELECT d.name FROM Departments d WHERE EXISTS ( SELECT 1 FROM Employees e WHERE e.department_id = d.id ); ";
        let mut result = run_query(&conductor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        result.sort();
        expected.sort();
        check_result(&result, &expected, false);
    }
}

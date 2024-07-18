use std::path::Path;
use std::sync::Arc;

use fbtree::prelude::{
    ContainerId, ContainerOptions, ContainerType, DatabaseId, TxnStorageStatus, TxnStorageTrait,
};
use sqlparser::parser::ParserError;
use sqlparser::{dialect::GenericDialect, parser::Parser};

use crate::catalog::CatalogRef;
use crate::error::ExecError;
use crate::executor::{Executor, TupleBuffer, TupleBufferIter};
use crate::expression::prelude::{
    HeuristicRule, HeuristicRules, HeuristicRulesRef, LogicalRelExpr, LogicalToPhysicalRelExpr,
    PhysicalRelExpr,
};
use crate::loader::prelude::SimpleCsvLoader;
use crate::loader::DataLoader;
use crate::parser::{Translator, TranslatorError};
use crate::prelude::{ColumnDef, DataType, Schema, SchemaRef, Table};

pub fn print_tuples(tuples: Arc<impl TupleBuffer>) {
    let mut count = 0;
    let tuples = tuples.iter();
    while let Some(t) = tuples.next().unwrap() {
        count += 1;
        println!("{}", t.to_pretty_string());
    }
    println!("Total tuples: {}", count);
}

#[derive(Debug)]
pub enum QueryExecutorError {
    InvalidCSV(String),
    InvalidTable(String),
    InvalidSqlString(ParserError),
    InvalidAst(TranslatorError),
    InvalidLogicalPlan,
    InvalidPhysicalPlan,
    InvalidExecutable,
    ExecutionError(ExecError),
}

impl From<ParserError> for QueryExecutorError {
    fn from(error: ParserError) -> Self {
        QueryExecutorError::InvalidSqlString(error)
    }
}

impl From<TranslatorError> for QueryExecutorError {
    fn from(error: TranslatorError) -> Self {
        QueryExecutorError::InvalidAst(error)
    }
}

impl From<ExecError> for QueryExecutorError {
    fn from(error: ExecError) -> Self {
        QueryExecutorError::ExecutionError(error)
    }
}

impl From<TxnStorageStatus> for QueryExecutorError {
    fn from(status: TxnStorageStatus) -> Self {
        QueryExecutorError::ExecutionError(ExecError::Storage(status.into()))
    }
}

pub struct QueryExecutor<T: TxnStorageTrait> {
    pub db_id: DatabaseId,
    pub catalog_ref: CatalogRef,
    pub storage: Arc<T>,
    pub logical_rules: HeuristicRulesRef,
}

impl<T: TxnStorageTrait> QueryExecutor<T> {
    pub fn new(db_id: DatabaseId, catalog_ref: CatalogRef, storage: Arc<T>) -> Self {
        let logical_rules = HeuristicRules::new();
        logical_rules.enable(HeuristicRule::Hoist);
        logical_rules.enable(HeuristicRule::Decorrelate);
        logical_rules.enable(HeuristicRule::SelectionPushdown);
        logical_rules.enable(HeuristicRule::ProjectionPushdown);

        QueryExecutor {
            db_id,
            catalog_ref,
            storage,
            logical_rules: Arc::new(logical_rules),
        }
    }

    pub fn parse_sql(sql: &str) -> Result<sqlparser::ast::Query, QueryExecutorError> {
        let dialect = GenericDialect {};
        let statements = Parser::new(&dialect)
            .try_with_sql(sql)?
            .parse_statements()?;
        let query = {
            let statement = statements.into_iter().next().unwrap();
            match statement {
                sqlparser::ast::Statement::Query(query) => query,
                other => {
                    return Err(QueryExecutorError::InvalidSqlString(
                        ParserError::ParserError(format!("Expected a query, got {:?}", other)),
                    ))
                }
            }
        };
        Ok(*query)
    }

    pub fn to_logical(&self, sql_string: &str) -> Result<LogicalRelExpr, QueryExecutorError> {
        pub fn parse_query(sql: &str) -> Result<sqlparser::ast::Query, QueryExecutorError> {
            let dialect = GenericDialect {};
            let statements = Parser::new(&dialect)
                .try_with_sql(sql)?
                .parse_statements()?;
            let query = {
                let statement = statements.into_iter().next().unwrap();
                match statement {
                    sqlparser::ast::Statement::Query(query) => query,
                    other => {
                        return Err(QueryExecutorError::InvalidSqlString(
                            ParserError::ParserError(format!("Expected a query, got {:?}", other)),
                        ))
                    }
                }
            };
            Ok(*query)
        }
        let query = parse_query(sql_string)?;
        let mut translator = Translator::new(self.db_id, &self.catalog_ref, &self.logical_rules);
        let result = translator.process_query(&query)?;
        Ok(result.plan)
    }

    pub fn to_physical(&self, logical_plan: LogicalRelExpr) -> PhysicalRelExpr {
        LogicalToPhysicalRelExpr.to_physical(logical_plan)
    }

    // Specifies a specific lifetime for the executor.
    // The storage ensures that the iterator lives as long as the executor.
    pub fn to_executable<E: Executor<T>>(&self, physical_plan: PhysicalRelExpr) -> E {
        E::new(
            self.catalog_ref.clone(),
            self.storage.clone(),
            physical_plan,
        )
    }

    // Executes the query and returns the result.
    pub fn execute<E: Executor<T>>(&self, exec: E) -> Result<Arc<E::Buffer>, QueryExecutorError> {
        let txn = self.storage.begin_txn(&self.db_id, Default::default())?;
        let result = exec.execute(&txn)?;
        self.storage.commit_txn(&txn, false)?;
        Ok(result)
    }

    pub fn create_table_from_sql(
        &self,
        sql_string: &str,
        container_type: ContainerType,
    ) -> Result<ContainerId, QueryExecutorError> {
        fn parse_create_table(sql: &str) -> Result<(String, SchemaRef), QueryExecutorError> {
            let dialect = GenericDialect {};
            let statements = Parser::new(&dialect)
                .try_with_sql(sql)?
                .parse_statements()?;
            let statement = statements.into_iter().next().unwrap();
            match statement {
                sqlparser::ast::Statement::CreateTable {
                    name,
                    columns,
                    constraints,
                    ..
                } => {
                    // Create a schema
                    let col_defs = columns
                        .iter()
                        .map(|c| {
                            let data_type = match &c.data_type {
                                sqlparser::ast::DataType::Int(_)
                                | sqlparser::ast::DataType::Integer(_) => DataType::Int,
                                sqlparser::ast::DataType::Text => DataType::String,
                                sqlparser::ast::DataType::Boolean => DataType::Boolean,
                                sqlparser::ast::DataType::Float(_)
                                | sqlparser::ast::DataType::Double
                                | sqlparser::ast::DataType::Decimal(_) => DataType::Float,
                                sqlparser::ast::DataType::Char(_) => DataType::String,
                                sqlparser::ast::DataType::Varchar(_) => DataType::String,
                                sqlparser::ast::DataType::Date => DataType::Date,
                                other => {
                                    return Err(QueryExecutorError::InvalidSqlString(
                                        ParserError::ParserError(format!(
                                            "Unsupported data type: {:?}",
                                            other
                                        )),
                                    ))
                                }
                            };
                            let not_null = c
                                .options
                                .iter()
                                .any(|o| matches!(o.option, sqlparser::ast::ColumnOption::NotNull));
                            Ok(ColumnDef::new(&c.name.value, data_type, !not_null))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // Find the column index of the primary key.
                    let inline_pk = columns
                        .iter()
                        .enumerate()
                        .filter_map(|(i, c)| {
                            c.options
                                .iter()
                                .find(|o| {
                                    matches!(
                                        o.option,
                                        sqlparser::ast::ColumnOption::Unique {
                                            is_primary: true,
                                            ..
                                        }
                                    )
                                })
                                .map(|_| i)
                        })
                        .collect::<Vec<_>>();
                    let external_pk = constraints.iter().find_map(|c| match c {
                        sqlparser::ast::TableConstraint::PrimaryKey { columns: pks, .. } => Some(
                            pks.iter()
                                .map(|pk| {
                                    columns
                                        .iter()
                                        .position(|c| c.name.value == pk.value)
                                        .unwrap()
                                })
                                .collect::<Vec<_>>(),
                        ),
                        _ => None,
                    });
                    let primary_key = match (inline_pk.len(), external_pk) {
                        (0, None) => {
                            return Err(QueryExecutorError::InvalidSqlString(
                                ParserError::ParserError("Primary key not found".to_string()),
                            ));
                        }
                        (1, None) => inline_pk,
                        (0, Some(pk)) => pk,
                        _ => {
                            return Err(QueryExecutorError::InvalidSqlString(
                                ParserError::ParserError(
                                    "Multiple primary keys are not supported".to_string(),
                                ),
                            ))
                        }
                    };
                    let schema = Arc::new(Schema::new(col_defs, primary_key));
                    // name joined with a .
                    let table_name = name.to_string();
                    Ok((table_name, schema))
                }
                other => Err(QueryExecutorError::InvalidSqlString(
                    ParserError::ParserError(format!(
                        "Expected a CREATE TABLE statement, got {:?}",
                        other
                    )),
                )),
            }
        }

        let (table_name, schema) = parse_create_table(sql_string)?;
        self.create_table(&table_name, schema, container_type)
    }

    pub fn create_table(
        &self,
        table_name: &str,
        schema: SchemaRef,
        container_type: ContainerType,
    ) -> Result<ContainerId, QueryExecutorError> {
        let txn = self.storage.begin_txn(&self.db_id, Default::default())?;
        let c_id = self.storage.create_container(
            &txn,
            &self.db_id,
            ContainerOptions::new(table_name, container_type),
        )?;
        self.catalog_ref
            .add_table(c_id, Arc::new(Table::new(table_name, schema)));
        self.storage.commit_txn(&txn, false)?;
        Ok(c_id)
    }

    pub fn import_csv<P: AsRef<Path>>(
        &self,
        c_id: ContainerId,
        csv_path: P,
        has_header: bool,
        delimiter: u8,
    ) -> Result<(), QueryExecutorError> {
        let rdr = csv::ReaderBuilder::new()
            .has_headers(has_header)
            .delimiter(delimiter)
            .from_path(csv_path.as_ref())
            .map_err(|e| QueryExecutorError::InvalidCSV(e.to_string()))?;
        let mut loader = SimpleCsvLoader::new(rdr, self.storage.clone());
        loader
            .load_data(
                self.catalog_ref
                    .get_schema(c_id)
                    .ok_or(QueryExecutorError::InvalidTable(
                        "Table not found".to_string(),
                    ))?,
                self.db_id,
                c_id,
            )
            .map_err(|e| QueryExecutorError::InvalidCSV(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use fbtree::prelude::{
        ContainerId, ContainerOptions, ContainerType, DBOptions, InMemStorage, TxnOptions,
    };

    use crate::{
        catalog::{Catalog, ColumnDef, DataType, Schema, Table},
        executor::prelude::PipelineQueue,
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

    fn check_result(
        result: Arc<impl TupleBuffer>,
        expected: &mut [Tuple],
        sorted: bool,
        verbose: bool,
    ) {
        let mut vec = Vec::new();
        let result = result.iter();
        while let Some(t) = result.next().unwrap() {
            vec.push(t);
        }
        let mut result = vec;
        if sorted {
            result.sort();
            expected.sort();
        }
        let result_string = result
            .iter()
            .map(|t| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        let expected_string = expected
            .iter()
            .map(|t: &Tuple| t.to_pretty_string())
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

    fn setup_executor<T: TxnStorageTrait>(storage: Arc<T>) -> QueryExecutor<T> {
        let catalog = Catalog::new();
        let db_id = storage.as_ref().open_db(DBOptions::new("test_db")).unwrap();

        let employees_c_id = setup_employees_table(&storage, db_id, &catalog);
        assert_eq!(employees_c_id, 0);
        let departments_c_id = setup_departments_table(&storage, db_id, &catalog);
        assert_eq!(departments_c_id, 1);

        QueryExecutor::new(db_id, Arc::new(catalog), storage)
    }

    fn run_query<T: TxnStorageTrait>(
        executor: &QueryExecutor<T>,
        sql_string: &str,
        verbose: bool,
    ) -> Arc<impl TupleBuffer> {
        let logical_plan = executor.to_logical(sql_string).unwrap();
        if verbose {
            println!("=== Logical Plan ===");
            logical_plan.pretty_print();
        }
        let physical_plan = executor.to_physical(logical_plan);
        if verbose {
            println!("=== Physical Plan ===");
            physical_plan.pretty_print();
        }
        let exec = executor.to_executable::<PipelineQueue<T>>(physical_plan);
        if verbose {
            println!("=== Executor ===");
            println!("{}", exec.to_pretty_string());
        }
        // todo!("Implement pretty print for executor");
        let txn = executor
            .storage
            .begin_txn(&executor.db_id, TxnOptions::default())
            .unwrap();
        let result = exec.execute(&txn).unwrap();
        executor.storage.commit_txn(&txn, false).unwrap();
        result
    }

    #[test]
    fn test_projection() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT name, age FROM Employees";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["David".into(), 28.into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[test]
    fn test_projection_with_expression() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT name, age + 1 FROM Employees";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 31.into()]),
            Tuple::from_fields(vec!["Bob".into(), 23.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 36.into()]),
            Tuple::from_fields(vec!["David".into(), 29.into()]),
            Tuple::from_fields(vec!["Eva".into(), 41.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[test]
    fn test_filter() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT name, age FROM Employees WHERE age > 30";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[test]
    fn test_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT e.name, e.age, d.name AS department FROM Employees e JOIN Departments d ON e.department_id = d.id";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "HR".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "HR".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_left_outer_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT d.name, sum(e.age) FROM Departments d LEFT OUTER JOIN Employees e ON e.department_id = d.id GROUP BY d.name";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), Field::Int(None)]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_right_outer_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string =  "SELECT d.name, sum(e.age) FROM Employees e RIGHT OUTER JOIN Departments d ON e.department_id = d.id GROUP BY d.name";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), Field::Int(None)]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_left_semi_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string =
            "SELECT d.name FROM Departments d LEFT SEMI JOIN Employees e ON e.department_id = d.id";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_right_semi_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT d.name FROM Employees e RIGHT SEMI JOIN Departments d ON e.department_id = d.id";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_left_anti_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string =
            "SELECT d.name FROM Departments d LEFT ANTI JOIN Employees e ON e.department_id = d.id";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![Tuple::from_fields(vec!["Marketing".into()])];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_right_anti_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT d.name FROM Employees e RIGHT ANTI JOIN Departments d ON e.department_id = d.id";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![Tuple::from_fields(vec!["Marketing".into()])];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_cross_join() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string =
            "SELECT e.name, e.age, d.name AS department FROM Employees e, Departments d";
        let result = run_query(&executor, sql_string, false);
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
        check_result(result, &mut expected, true, false);
    }

    #[test]
    fn test_aggregate() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT COUNT(*), AVG(age) FROM Employees";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![Tuple::from_fields(vec![5.into(), 31.0.into()])];
        check_result(result, &mut expected, true, false);
    }

    #[test]
    fn test_groupby_aggregate() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT d.name, AVG(e.age) AS average_age FROM Employees e JOIN Departments d ON e.department_id = d.id GROUP BY d.name;";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 32.5.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 25.0.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[test]
    fn test_groupby_aggregate_having() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT d.name, AVG(e.age) AS average_age FROM Employees e JOIN Departments d ON e.department_id = d.id GROUP BY d.name HAVING AVG(e.age) > 30";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![Tuple::from_fields(vec!["HR".into(), 32.5.into()])];
        check_result(result, &mut expected, true, false);
    }

    #[test]
    fn test_subquery() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        // For each department, count the number of employees and sum of their ages
        let sql_string = "SELECT d.name, cnt, sum_age FROM Departments d, (SELECT department_id, COUNT(*) AS cnt, SUM(age) AS sum_age FROM Employees e WHERE e.department_id = d.id)";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 2.into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 2.into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), 0.into(), Field::Int(None)]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_correlated_exists() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT d.name FROM Departments d WHERE EXISTS ( SELECT 1 FROM Employees e WHERE e.department_id = d.id ); ";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_uncorrelated_exists() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string =
            "SELECT d.name FROM Departments d WHERE EXISTS ( SELECT 1 FROM Employees ); ";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
            Tuple::from_fields(vec!["Marketing".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[test]
    fn test_orderby() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT name, age FROM Employees ORDER BY age DESC, name ASC";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into()]),
            Tuple::from_fields(vec!["David".into(), 28.into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into()]),
        ];
        check_result(result, &mut expected, false, false);
    }

    #[test]
    fn test_orderby_expression() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT name, age FROM Employees ORDER BY age + 1 DESC";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into()]),
            Tuple::from_fields(vec!["David".into(), 28.into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into()]),
        ];
        check_result(result, &mut expected, false, false);
    }

    #[test]
    fn test_groupby_orderby() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT d.name, AVG(e.age) AS average_age FROM Employees e JOIN Departments d ON e.department_id = d.id GROUP BY d.name ORDER BY average_age DESC";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 32.5.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 25.0.into()]),
        ];
        check_result(result, &mut expected, false, false);
    }

    #[test]
    fn test_in_subquery() {
        let storage = get_in_mem_storage();
        let executor = setup_executor(storage.clone());
        let sql_string = "SELECT name FROM Employees WHERE department_id IN (SELECT id FROM Departments WHERE name = 'HR')";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into()]),
            Tuple::from_fields(vec!["Charlie".into()]),
        ];
        check_result(result, &mut expected, true, false);

        let sql_string = "SELECT name FROM Employees WHERE department_id IN (SELECT id FROM Departments WHERE name = 'Marketing')";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![];
        check_result(result, &mut expected, true, false);

        let sql_string =
            "SELECT name FROM Departments WHERE id IN (SELECT department_id FROM Employees)";
        let result = run_query(&executor, sql_string, true);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, false);
    }
}

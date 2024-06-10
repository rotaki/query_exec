use std::{
    cell::{Ref, RefCell},
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use chrono::{Datelike, Month};
use txn_storage::DatabaseId;

use crate::{
    catalog::{self, Catalog, CatalogRef, ColIdGen, ColIdGenRef},
    expression::{
        prelude::{
            BinaryOp, HeuristicRule, HeuristicRulesRef, JoinType, LogicalRelExpr, PlanTrait,
        },
        AggOp, DateField, Expression,
    },
    prelude::DataType,
    tuple::Field,
};

type EnvironmentRef = Rc<Environment>;

#[derive(Debug, Clone)]
struct Environment {
    outer: Option<EnvironmentRef>,
    columns: RefCell<HashMap<String, usize>>,
}

impl Environment {
    fn new() -> Environment {
        Environment {
            outer: None,
            columns: RefCell::new(HashMap::new()),
        }
    }

    fn new_with_outer(outer: EnvironmentRef) -> Environment {
        Environment {
            outer: Some(outer),
            columns: RefCell::new(HashMap::new()),
        }
    }

    fn get(&self, name: &str) -> Option<usize> {
        if let Some(index) = self.columns.borrow().get(name) {
            return Some(*index);
        }

        if let Some(outer) = &self.outer {
            return outer.get(name);
        }

        None
    }

    fn get_at(&self, distance: usize, name: &str) -> Option<usize> {
        if distance == 0 {
            if let Some(index) = self.columns.borrow().get(name) {
                return Some(*index);
            } else {
                return None;
            }
        }

        if let Some(outer) = &self.outer {
            return outer.get_at(distance - 1, name);
        }

        None
    }

    fn set(&self, name: &str, index: usize) {
        self.columns.borrow_mut().insert(name.to_string(), index);
    }

    fn get_names(&self, col_id: usize) -> Vec<String> {
        let mut names = Vec::new();
        for (name, index) in self.columns.borrow().iter() {
            if *index == col_id {
                names.push(name.clone());
            }
        }
        names
    }
}

pub struct Translator {
    db_id: DatabaseId,
    catalog_ref: CatalogRef,
    enabled_rules: HeuristicRulesRef,
    col_id_gen: ColIdGenRef,
    env: EnvironmentRef, // Variables in the current scope
}

pub struct Query {
    pub env: EnvironmentRef,
    pub plan: LogicalRelExpr,
}

#[derive(Debug)]
pub enum TranslatorError {
    ColumnNotFound(String),
    TableNotFound(String),
    InvalidSQL(String),
    UnsupportedSQL(String),
    FailedToTranslate(String),
}

macro_rules! translation_err {
    (ColumnNotFound, $($arg:tt)*) => {
        TranslatorError::ColumnNotFound(format!($($arg)*))
    };
    (TableNotFound, $($arg:tt)*) => {
        TranslatorError::TableNotFound(format!($($arg)*))
    };
    (InvalidSQL, $($arg:tt)*) => {
        TranslatorError::InvalidSQL(format!($($arg)*))
    };
    (UnsupportedSQL, $($arg:tt)*) => {
        TranslatorError::UnsupportedSQL(format!($($arg)*))
    };
    (FailedToTranslate, $($arg:tt)*) => {
        TranslatorError::FailedToTranslate(format!($($arg)*))
    };
}

impl Translator {
    pub fn new(
        db_id: DatabaseId,
        catalog: &CatalogRef, // Per DB catalog
        enabled_rules: &HeuristicRulesRef,
    ) -> Translator {
        Translator {
            db_id,
            catalog_ref: catalog.clone(),
            enabled_rules: enabled_rules.clone(),
            col_id_gen: ColIdGen::new(),
            env: Rc::new(Environment::new()),
        }
    }

    fn new_with_outer(
        db_id: DatabaseId,
        catalog: &CatalogRef,
        enabled_rules: &HeuristicRulesRef,
        col_id_gen: &ColIdGenRef,
        outer: &EnvironmentRef,
    ) -> Translator {
        Translator {
            db_id,
            col_id_gen: col_id_gen.clone(),
            enabled_rules: enabled_rules.clone(),
            catalog_ref: catalog.clone(),
            env: Rc::new(Environment::new_with_outer(outer.clone())),
        }
    }

    pub fn process_query(
        &mut self,
        query: &sqlparser::ast::Query,
    ) -> Result<Query, TranslatorError> {
        let select = match query.body.as_ref() {
            sqlparser::ast::SetExpr::Select(select) => select,
            _ => {
                return Err(translation_err!(
                    UnsupportedSQL,
                    "Only SELECT queries are supported"
                ))
            }
        };

        let plan = self.process_from(&select.from)?;
        let plan = self.process_where(plan, &select.selection)?;
        let plan = self.process_projection(
            plan,
            &select.projection,
            &select.from,
            &query.order_by,
            &query.limit,
            &select.group_by,
            &select.having,
            &select.distinct,
        )?;

        Ok(Query {
            env: self.env.clone(),
            plan,
        })
    }

    fn process_from(
        &mut self,
        from: &[sqlparser::ast::TableWithJoins],
    ) -> Result<LogicalRelExpr, TranslatorError> {
        if from.is_empty() {
            return Err(translation_err!(InvalidSQL, "FROM clause is empty"));
        }

        let mut join_exprs = Vec::with_capacity(from.len());
        for table_with_joins in from {
            let join_expr = self.process_table_with_joins(table_with_joins)?;
            join_exprs.push(join_expr);
        }
        let (mut plan, _) = join_exprs.remove(0);
        for (join_expr, is_subquery) in join_exprs.into_iter() {
            plan = if is_subquery {
                plan.flatmap(true, &self.enabled_rules, &self.col_id_gen, join_expr)
            } else {
                plan.join(
                    true,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    JoinType::CrossJoin,
                    join_expr,
                    vec![],
                )
            }
        }
        Ok(plan)
    }

    fn process_table_with_joins(
        &mut self,
        table_with_joins: &sqlparser::ast::TableWithJoins,
    ) -> Result<(LogicalRelExpr, bool), TranslatorError> {
        let (mut plan, is_sbqry) = self.process_table_factor(&table_with_joins.relation)?;
        for join in &table_with_joins.joins {
            let (right, is_subquery) = self.process_table_factor(&join.relation)?;
            // If it is a subquery, we use flat_map + condition
            // Other wise we use a join
            let (join_type, condition) = self.process_join_operator(&join.join_operator)?;
            plan = if is_subquery {
                if matches!(
                    join_type,
                    JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter
                ) {
                    return Err(translation_err!(
                        UnsupportedSQL,
                        "Unsupported join type with subquery"
                    ));
                }
                plan.flatmap(true, &self.enabled_rules, &self.col_id_gen, right)
                    .select(
                        true,
                        &self.enabled_rules,
                        &self.col_id_gen,
                        condition.into_iter().collect(),
                    )
            } else {
                plan.on(
                    true,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    join_type,
                    right,
                    condition.into_iter().collect(),
                )
            }
        }
        Ok((plan, is_sbqry))
    }

    fn process_join_operator(
        &self,
        join_operator: &sqlparser::ast::JoinOperator,
    ) -> Result<(JoinType, Option<Expression<LogicalRelExpr>>), TranslatorError> {
        use sqlparser::ast::{JoinConstraint, JoinOperator::*};
        match join_operator {
            Inner(JoinConstraint::On(cond)) => {
                Ok((JoinType::Inner, Some(self.process_expr(cond, None)?)))
            }
            LeftOuter(JoinConstraint::On(cond)) => {
                Ok((JoinType::LeftOuter, Some(self.process_expr(cond, None)?)))
            }
            RightOuter(JoinConstraint::On(cond)) => {
                Ok((JoinType::RightOuter, Some(self.process_expr(cond, None)?)))
            }
            FullOuter(JoinConstraint::On(cond)) => {
                Ok((JoinType::FullOuter, Some(self.process_expr(cond, None)?)))
            }
            CrossJoin => Ok((JoinType::CrossJoin, None)),
            LeftSemi(JoinConstraint::On(cond)) => {
                Ok((JoinType::LeftSemi, Some(self.process_expr(cond, None)?)))
            }
            RightSemi(JoinConstraint::On(cond)) => {
                Ok((JoinType::RightSemi, Some(self.process_expr(cond, None)?)))
            }
            _ => Err(translation_err!(
                UnsupportedSQL,
                "Unsupported join operator: {:?}",
                join_operator
            )),
        }
    }

    // Out: (LogicalRelExpr, is_subquery: bool)
    fn process_table_factor(
        &mut self,
        table_factor: &sqlparser::ast::TableFactor,
    ) -> Result<(LogicalRelExpr, bool), TranslatorError> {
        match table_factor {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                // Find the actual name from the catalog
                // If name exists in the catalog, then add the columns to the environment
                // Otherwise return an error
                let table_name = get_name(name);
                if self.catalog_ref.is_valid_table(&table_name) {
                    let (c_id, table) = self.catalog_ref.get_table(&table_name).unwrap();
                    let schema = table.schema();
                    let cols = schema.columns();
                    let plan = LogicalRelExpr::scan(
                        self.db_id,
                        c_id,
                        table_name.clone(),
                        (0..cols.len()).collect(),
                    );
                    let (plan, mut new_col_ids) =
                        plan.rename(&self.enabled_rules, &self.col_id_gen);

                    // Add the
                    for (old_col_id, new_col_id) in new_col_ids.drain() {
                        // get the name of the column
                        let col_name = cols.get(old_col_id).unwrap().name();
                        self.env.set(&col_name, new_col_id);
                        self.env
                            .set(&format!("{}.{}", table_name, col_name), new_col_id);

                        // If there is an alias, set the alias in the current environment
                        if let Some(alias) = alias {
                            if is_valid_alias(&alias.name.value) {
                                self.env.set(&format!("{}.{}", alias, col_name), new_col_id);
                            } else {
                                return Err(translation_err!(
                                    InvalidSQL,
                                    "Invalid alias name: {}",
                                    alias.name.value
                                ));
                            }
                        }
                    }

                    Ok((plan, false))
                } else {
                    Err(translation_err!(TableNotFound, "{}", table_name))
                }
            }
            sqlparser::ast::TableFactor::Derived {
                subquery, alias, ..
            } => {
                let mut translator = Translator::new_with_outer(
                    self.db_id,
                    &self.catalog_ref,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    &self.env,
                );
                let subquery = translator.process_query(subquery)?;
                let plan = subquery.plan;
                if let Some(alias) = alias {
                    if !is_valid_alias(&alias.name.value) {
                        return Err(translation_err!(
                            InvalidSQL,
                            "Invalid table alias name: {}",
                            alias.name.value
                        ));
                    }
                    if alias.columns.is_empty() {
                        // Just add alias.name to the environment
                        let att = plan.att();
                        for i in att {
                            // get the name of the column from env
                            let names = subquery.env.get_names(i);
                            for name in &names {
                                self.env.set(&name, i);
                                self.env.set(&format!("{}.{}", alias.name.value, name), i);
                            }
                        }
                    } else {
                        // columns correspond to the subquery's output columns
                        if let LogicalRelExpr::Project { src: _, cols } = &plan {
                            let table_alias = alias.name.value.clone();
                            for (col_alias, col_id) in alias.columns.iter().zip(cols.iter()) {
                                if !is_valid_alias(&col_alias.value) {
                                    return Err(translation_err!(
                                        InvalidSQL,
                                        "Invalid column alias name: {}",
                                        col_alias.value
                                    ));
                                }
                                let names = subquery.env.get_names(*col_id);
                                for name in &names {
                                    self.env.set(&name, *col_id);
                                }
                                self.env.set(&col_alias.value, *col_id);
                                self.env
                                    .set(&format!("{}.{}", table_alias, col_alias.value), *col_id);
                            }
                        } else {
                            return Err(translation_err!(
                                FailedToTranslate,
                                "Top level operation of a query should be a projection: {:?}",
                                plan
                            ));
                        }
                    }
                } else {
                    // No alias. Just add the columns to the environment.
                    let att = plan.att();
                    for i in att {
                        let names = subquery.env.get_names(i);
                        for name in &names {
                            self.env.set(&name, i);
                        }
                    }
                }
                Ok((plan, true))
            }
            _ => Err(translation_err!(UnsupportedSQL, "Unsupported table factor")),
        }
    }

    fn process_where(
        &mut self,
        plan: LogicalRelExpr,
        where_clause: &Option<sqlparser::ast::Expr>,
    ) -> Result<LogicalRelExpr, TranslatorError> {
        if let Some(expr) = where_clause {
            match self.process_expr(expr, Some(0)) {
                Ok(expr) => {
                    if expr.has_subquery() {
                        let col_id = self.col_id_gen.next();
                        let plan = plan.map(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            [(col_id, expr)],
                        );
                        Ok(plan.select(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            vec![Expression::col_ref(col_id)],
                        ))
                    } else {
                        Ok(plan.select(true, &self.enabled_rules, &self.col_id_gen, vec![expr]))
                    }
                }
                Err(TranslatorError::ColumnNotFound(_)) => {
                    // Search globally.
                    let expr = self.process_expr(expr, None)?;
                    let col_id = self.col_id_gen.next();
                    Ok(plan
                        .map(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            [(col_id, expr)],
                        )
                        .select(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            vec![Expression::col_ref(col_id)],
                        ))
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(plan)
        }
    }

    fn process_projection_col(
        &mut self,
        mut plan: LogicalRelExpr,
        expr: &sqlparser::ast::Expr,
        projected_cols: &mut Vec<usize>,
        agg_ops: &mut Vec<(usize, (usize, AggOp))>,
        agg_maps: &mut Vec<(usize, Expression<LogicalRelExpr>)>,
    ) -> Result<(LogicalRelExpr, usize), TranslatorError> {
        // create a new col_id for the expression
        let col_id = if !has_agg(expr) {
            let col_id = match self.process_expr(expr, Some(0)) {
                Ok(expr) => {
                    if let Expression::ColRef { id } = expr {
                        id
                    } else {
                        let col_id = self.col_id_gen.next();
                        plan = plan.map(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            [(col_id, expr)],
                        );
                        col_id
                    }
                }
                Err(TranslatorError::ColumnNotFound(_)) => {
                    // Search globally.
                    let expr = self.process_expr(expr, None)?;
                    let col_id = self.col_id_gen.next();
                    plan = plan.map(
                        true,
                        &self.enabled_rules,
                        &self.col_id_gen,
                        [(col_id, expr)],
                    );
                    col_id
                }
                Err(e) => return Err(e),
            };
            projected_cols.push(col_id);
            col_id
        } else {
            // The most complicated case will be:
            // Agg(a + b) + Agg(c + d) + 4
            // if we ignore nested aggregation.
            //
            // In this case,
            // Level1: | map a + b to col_id1
            //         | map c + d to col_id2
            // Level2: |Agg(col_id1) to col_id3
            //         |Agg(col_id2) to col_id4
            // Level3: |map col_id3 + col_id4 + 4 to col_id5

            let mut aggs = Vec::new();
            let res = self.process_aggregation_arguments(plan, expr, &mut aggs);
            plan = res.0;
            let expr = res.1;
            let col_id = if let Expression::ColRef { id } = expr {
                id
            } else {
                // create a new col_id for the expression
                let col_id = self.col_id_gen.next();
                agg_maps.push((col_id, expr));
                col_id
            };
            agg_ops.append(&mut aggs);
            projected_cols.push(col_id);
            col_id
        };
        Ok((plan, col_id))
    }

    fn process_having(
        &mut self,
        mut plan: LogicalRelExpr,
        having: &sqlparser::ast::Expr,
        agg_ops: &mut Vec<(usize, (usize, AggOp))>,
        agg_maps: &mut Vec<(usize, Expression<LogicalRelExpr>)>,
    ) -> Result<(LogicalRelExpr, usize), TranslatorError> {
        let mut aggs = Vec::new();
        let res = self.process_aggregation_arguments(plan, having, &mut aggs);
        plan = res.0;
        let expr = res.1;
        let col_id = if let Expression::ColRef { id } = expr {
            id
        } else {
            // create a new col_id for the expression
            let col_id = self.col_id_gen.next();
            agg_maps.push((col_id, expr));
            col_id
        };
        agg_ops.append(&mut aggs);
        Ok((plan, col_id))
    }

    fn process_projection(
        &mut self,
        mut plan: LogicalRelExpr,
        projection: &Vec<sqlparser::ast::SelectItem>,
        from: &Vec<sqlparser::ast::TableWithJoins>,
        order_by: &Vec<sqlparser::ast::OrderByExpr>,
        limit: &Option<sqlparser::ast::Expr>,
        group_by: &sqlparser::ast::GroupByExpr,
        having: &Option<sqlparser::ast::Expr>,
        distinct: &Option<sqlparser::ast::Distinct>,
    ) -> Result<LogicalRelExpr, TranslatorError> {
        let mut projected_cols = Vec::new();
        let mut agg_ops = Vec::new();
        let mut agg_maps = Vec::new();

        for item in projection {
            match item {
                sqlparser::ast::SelectItem::Wildcard(_) => {
                    let mut all_cols = HashSet::new();
                    // Add all the environment variables to the projected columns
                    for (_, col_id) in self.env.columns.borrow().iter() {
                        all_cols.insert(*col_id);
                    }
                    projected_cols.extend(all_cols);
                }
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                    let res = self.process_projection_col(
                        plan,
                        expr,
                        &mut projected_cols,
                        &mut agg_ops,
                        &mut agg_maps,
                    )?;
                    plan = res.0;
                }
                sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let res = self.process_projection_col(
                        plan,
                        expr,
                        &mut projected_cols,
                        &mut agg_ops,
                        &mut agg_maps,
                    )?;
                    plan = res.0;
                    let col_id = res.1;
                    // Add the alias to the aliases map
                    let alias_name = alias.value.clone();
                    if is_valid_alias(&alias_name) {
                        self.env.set(&alias_name, col_id);
                    } else {
                        return Err(translation_err!(
                            InvalidSQL,
                            "Invalid alias name: {}",
                            alias_name
                        ));
                    }
                }
                _ => {
                    return Err(translation_err!(
                        UnsupportedSQL,
                        "Unsupported select item: {:?}",
                        item
                    ))
                }
            }
        }

        // Process aggregations
        let mut having_predicates = Vec::new();
        if let Some(h) = having {
            let res = self.process_having(plan, h, &mut agg_ops, &mut agg_maps)?;
            plan = res.0;
            having_predicates.push(Expression::col_ref(res.1));
        }
        let group_by = match group_by {
            sqlparser::ast::GroupByExpr::All => Err(translation_err!(
                UnsupportedSQL,
                "GROUP BY ALL is not supported"
            ))?,
            sqlparser::ast::GroupByExpr::Expressions(exprs) => {
                let mut group_by = Vec::new();
                for expr in exprs {
                    let expr = self.process_expr(expr, None)?;
                    let col_id = if let Expression::ColRef { id } = expr {
                        id
                    } else {
                        // create a new col_id for the expression
                        let col_id = self.col_id_gen.next();
                        plan = plan.map(
                            true,
                            &self.enabled_rules,
                            &self.col_id_gen,
                            [(col_id, expr)],
                        );
                        col_id
                    };
                    group_by.push(col_id);
                }
                group_by
            }
        };
        if !group_by.is_empty() || !agg_ops.is_empty() {
            plan = plan.aggregate(
                true,
                &self.enabled_rules,
                &self.col_id_gen,
                group_by,
                agg_ops,
            );
            plan = plan.map(true, &self.enabled_rules, &self.col_id_gen, agg_maps);
        }
        if !having_predicates.is_empty() {
            plan = plan.select(
                true,
                &self.enabled_rules,
                &self.col_id_gen,
                having_predicates,
            );
        }

        let mut order_by_fields = Vec::new();
        for order_by_expr in order_by {
            let expr = self.process_expr(&order_by_expr.expr, Some(0))?;
            let col_id = if let Expression::ColRef { id } = expr {
                id
            } else {
                let col_id = self.col_id_gen.next();
                plan = plan.map(
                    true,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    [(col_id, expr)],
                );
                col_id
            };
            let asc = order_by_expr.asc.unwrap_or(true);
            let nulls_first = order_by_expr.nulls_first.unwrap_or(false);
            order_by_fields.push((col_id, asc, nulls_first));
        }

        if !order_by_fields.is_empty() {
            plan = plan.order_by(true, &self.enabled_rules, &self.col_id_gen, order_by_fields);
        }

        plan = plan.o_project(true, &self.enabled_rules, &self.col_id_gen, projected_cols);
        Ok(plan)
    }

    // DFS until we find an aggregation function
    // If we find an aggregation function, then add the aggregation argument to the plan
    // and put the aggregation function in the aggregation list, return the modified plan with the expression.
    // For example, if SUM(a+b) + AVG(c+d) + 4, then
    // a+b -> col_id1, c+d -> col_id2 will be added to the plan
    // SUM(col_id1) -> col_id3, AVG(col_id2) -> col_id4 will be added to the aggregation list
    // col_id3 + col_id4 + 4 -> col_id5 will be returned with the plan
    fn process_aggregation_arguments(
        &self,
        mut plan: LogicalRelExpr,
        expr: &sqlparser::ast::Expr,
        aggs: &mut Vec<(usize, (usize, AggOp))>,
    ) -> (LogicalRelExpr, Expression<LogicalRelExpr>) {
        match expr {
            sqlparser::ast::Expr::Identifier(_) | sqlparser::ast::Expr::CompoundIdentifier(_) => {
                unreachable!(
                    "Identifier and compound identifier should be processed in the Function branch"
                )
            }
            sqlparser::ast::Expr::Subquery(query) => {
                let mut translator = Translator::new_with_outer(
                    self.db_id,
                    &self.catalog_ref,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    &self.env,
                );
                let subquery = translator.process_query(query).unwrap();
                (plan, Expression::subquery(subquery.plan))
            }
            sqlparser::ast::Expr::Value(_) | sqlparser::ast::Expr::TypedString { .. } => {
                let expr = self.process_expr(expr, Some(0)).unwrap();
                (plan, expr)
            }
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                let (plan, left) = self.process_aggregation_arguments(plan, left, aggs);
                let (plan, right) = self.process_aggregation_arguments(plan, right, aggs);
                let bin_op = match op {
                    sqlparser::ast::BinaryOperator::And => BinaryOp::And,
                    sqlparser::ast::BinaryOperator::Or => BinaryOp::Or,
                    sqlparser::ast::BinaryOperator::Plus => BinaryOp::Add,
                    sqlparser::ast::BinaryOperator::Minus => BinaryOp::Sub,
                    sqlparser::ast::BinaryOperator::Multiply => BinaryOp::Mul,
                    sqlparser::ast::BinaryOperator::Divide => BinaryOp::Div,
                    sqlparser::ast::BinaryOperator::Eq => BinaryOp::Eq,
                    sqlparser::ast::BinaryOperator::NotEq => BinaryOp::Neq,
                    sqlparser::ast::BinaryOperator::Lt => BinaryOp::Lt,
                    sqlparser::ast::BinaryOperator::Gt => BinaryOp::Gt,
                    sqlparser::ast::BinaryOperator::LtEq => BinaryOp::Le,
                    sqlparser::ast::BinaryOperator::GtEq => BinaryOp::Ge,
                    _ => unimplemented!("Unsupported binary operator: {:?}", op),
                };
                (plan, Expression::binary(bin_op, left, right))
            }
            sqlparser::ast::Expr::Cast {
                kind: _,
                expr,
                data_type,
                format: _,
            } => {
                let (plan, expr) = self.process_aggregation_arguments(plan, expr, aggs);
                let data_type = get_type(data_type);
                (plan, Expression::cast(expr, data_type))
            }
            sqlparser::ast::Expr::Function(function) => {
                let name = get_name(&function.name).to_uppercase();
                let agg_op = match name.as_str() {
                    "COUNT" => AggOp::Count,
                    "SUM" => AggOp::Sum,
                    "AVG" => AggOp::Avg,
                    "MIN" => AggOp::Min,
                    "MAX" => AggOp::Max,
                    _ => unimplemented!("Unsupported aggregation function: {:?}", function),
                };
                let args = match &function.args {
                    sqlparser::ast::FunctionArguments::List(args) => &args.args,
                    _ => unimplemented!("Unsupported aggregation function: {:?}", function),
                };
                if args.len() != 1 {
                    unimplemented!("Unsupported aggregation function: {:?}", function);
                }
                let function_arg_expr = match &args[0] {
                    sqlparser::ast::FunctionArg::Named { arg, .. } => arg,
                    sqlparser::ast::FunctionArg::Unnamed(arg) => arg,
                };

                let agg_col_id = self.col_id_gen.next();
                match function_arg_expr {
                    sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                        match self.process_expr(&expr, Some(0)) {
                            Ok(expr) => {
                                if let Expression::ColRef { id } = expr {
                                    aggs.push((agg_col_id, (id, agg_op)));
                                    (plan, Expression::col_ref(agg_col_id))
                                } else {
                                    plan = plan.map(
                                        true,
                                        &self.enabled_rules,
                                        &self.col_id_gen,
                                        [(agg_col_id, expr)],
                                    );
                                    aggs.push((agg_col_id, (agg_col_id, agg_op)));
                                    (plan, Expression::col_ref(agg_col_id))
                                }
                            }
                            Err(TranslatorError::ColumnNotFound(_)) => {
                                // Search globally.
                                let expr = self.process_expr(&expr, None).unwrap();
                                let col_id = self.col_id_gen.next();
                                plan = plan.map(
                                    true,
                                    &self.enabled_rules,
                                    &self.col_id_gen,
                                    [(col_id, expr)],
                                );
                                aggs.push((agg_col_id, (col_id, agg_op)));
                                (plan, Expression::col_ref(agg_col_id))
                            }
                            other => {
                                unimplemented!("Unsupported expression: {:?}", other)
                            }
                        }
                    }
                    sqlparser::ast::FunctionArgExpr::QualifiedWildcard(_) => {
                        unimplemented!("QualifiedWildcard is not supported yet")
                    }
                    sqlparser::ast::FunctionArgExpr::Wildcard => {
                        // Wildcard is only supported for COUNT
                        // If wildcard, just need to return Int(1) as it returns the count of rows
                        if matches!(agg_op, AggOp::Count) {
                            let col_id = self.col_id_gen.next();
                            plan = plan.map(
                                true,
                                &self.enabled_rules,
                                &self.col_id_gen,
                                [(col_id, Expression::int(1))],
                            );
                            aggs.push((agg_col_id, (col_id, agg_op)));
                            (plan, Expression::col_ref(agg_col_id))
                        } else {
                            panic!("Wildcard is only supported for COUNT");
                        }
                    }
                }
            }
            sqlparser::ast::Expr::Nested(expr) => {
                self.process_aggregation_arguments(plan, expr, aggs)
            }
            _ => unimplemented!("Unsupported expression: {:?}", expr),
        }
    }

    fn process_expr(
        &self,
        expr: &sqlparser::ast::Expr,
        distance: Option<usize>,
    ) -> Result<Expression<LogicalRelExpr>, TranslatorError> {
        match expr {
            sqlparser::ast::Expr::Identifier(ident) => {
                let id = if let Some(distance) = distance {
                    self.env.get_at(distance, &ident.value)
                } else {
                    self.env.get(&ident.value)
                };
                let id = id.ok_or(translation_err!(
                    ColumnNotFound,
                    "{}, env: {}",
                    ident.value,
                    self.env
                        .columns
                        .borrow()
                        .iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))?;
                Ok(Expression::col_ref(id))
            }
            sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                let name = idents
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let id = if let Some(distance) = distance {
                    self.env.get_at(distance, &name)
                } else {
                    self.env.get(&name)
                };
                let id = id.ok_or(translation_err!(
                    ColumnNotFound,
                    "{}, env: {}",
                    name,
                    self.env
                        .columns
                        .borrow()
                        .iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))?;
                Ok(Expression::col_ref(id))
            }
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator::*;
                let left = self.process_expr(left, distance)?;
                let right = self.process_expr(right, distance)?;
                let bin_op = match op {
                    And => BinaryOp::And,
                    Or => BinaryOp::Or,
                    Plus => BinaryOp::Add,
                    Minus => BinaryOp::Sub,
                    Multiply => BinaryOp::Mul,
                    Divide => BinaryOp::Div,
                    Eq => BinaryOp::Eq,
                    NotEq => BinaryOp::Neq,
                    Lt => BinaryOp::Lt,
                    Gt => BinaryOp::Gt,
                    LtEq => BinaryOp::Le,
                    GtEq => BinaryOp::Ge,
                    _ => {
                        return Err(translation_err!(
                            UnsupportedSQL,
                            "Unsupported binary operator: {:?}",
                            op
                        ));
                    }
                };
                Ok(Expression::binary(bin_op, left, right))
            }
            sqlparser::ast::Expr::TypedString { data_type, value } => match data_type {
                sqlparser::ast::DataType::Date => {
                    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                        .map_err(|e| translation_err!(InvalidSQL, "{}", e))?;
                    Ok(Expression::date(date))
                }
                _ => Err(translation_err!(
                    UnsupportedSQL,
                    "Unsupported data type: {:?}",
                    data_type
                )),
            },
            sqlparser::ast::Expr::Value(value) => match value {
                sqlparser::ast::Value::Number(num, _) => {
                    // Try to parse as integer first
                    match num.parse::<i64>() {
                        Ok(num) => Ok(Expression::int(num)),
                        Err(_) => Ok(Expression::float(num.parse().unwrap())),
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s) => Ok(Expression::string(s.clone())),
                _ => Err(translation_err!(
                    UnsupportedSQL,
                    "Unsupported value: {:?}",
                    value
                )),
            },
            sqlparser::ast::Expr::Exists { subquery, negated } => {
                let mut translator = Translator::new_with_outer(
                    self.db_id,
                    &self.catalog_ref,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    &self.env,
                );
                let subquery = translator.process_query(subquery)?;
                let mut plan = subquery.plan;
                // Add count(*) to the subquery
                let col_id1 = translator.col_id_gen.next();
                plan = plan.map(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [(col_id1, Expression::int(1))],
                );
                let col_id2 = translator.col_id_gen.next();
                plan = plan.aggregate(
                    true,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    vec![],
                    vec![(col_id2, (col_id1, AggOp::Count))],
                );
                // Add count(*) > 0  to the subquery
                let exists_expr = if *negated {
                    Expression::binary(
                        BinaryOp::Le,
                        Expression::col_ref(col_id2),
                        Expression::int(0),
                    )
                } else {
                    Expression::binary(
                        BinaryOp::Gt,
                        Expression::col_ref(col_id2),
                        Expression::int(0),
                    )
                };
                let col_id3 = self.col_id_gen.next();
                plan = plan.map(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [(col_id3, exists_expr)],
                );
                // Add project col 'count(*) > 0' to the subquery
                plan = plan.u_project(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [col_id3].into_iter().collect(),
                );
                Ok(Expression::subquery(plan))
            }
            sqlparser::ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                // X NOT IN Y is equivalent to NOT (ANY (X = Y))
                let left = expr;
                let right = subquery;
                let any = sqlparser::ast::Expr::AnyOp {
                    left: Box::new(*left.clone()),
                    compare_op: sqlparser::ast::BinaryOperator::Eq,
                    right: Box::new(sqlparser::ast::Expr::Subquery(right.clone())),
                };
                let expr = self.process_expr(&any, distance)?;
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            sqlparser::ast::Expr::AnyOp {
                left,
                compare_op,
                right,
            } => process_any(self, left, compare_op, right, distance),
            sqlparser::ast::Expr::Subquery(query) => {
                let mut translator = Translator::new_with_outer(
                    self.db_id,
                    &self.catalog_ref,
                    &self.enabled_rules,
                    &self.col_id_gen,
                    &self.env,
                );
                let subquery = translator.process_query(query)?;
                let plan = subquery.plan;
                let att = plan.att();
                if att.len() != 1 {
                    panic!("Subquery returns more than one column")
                }
                Ok(Expression::subquery(plan))
            }
            sqlparser::ast::Expr::Nested(expr) => self.process_expr(expr, distance),
            sqlparser::ast::Expr::Interval(interval) => match interval.leading_field {
                Some(sqlparser::ast::DateTimeField::Month) => {
                    let months = match &*interval.value {
                        sqlparser::ast::Expr::Value(val) => match val {
                            sqlparser::ast::Value::Number(s, _) => s.parse::<i32>().map_err(|e| {
                                translation_err!(
                                    InvalidSQL,
                                    "Failed to parse number from string {}, error: {}",
                                    s,
                                    e
                                )
                            }),
                            sqlparser::ast::Value::SingleQuotedString(s) => {
                                s.parse::<i32>().map_err(|e| {
                                    translation_err!(
                                        InvalidSQL,
                                        "Failed to parse number from string {}, error: {}",
                                        s,
                                        e
                                    )
                                })
                            }
                            _ => Err(translation_err!(
                                InvalidSQL,
                                "Unsupported value: {:?} for interval",
                                val
                            )),
                        },
                        other => Err(translation_err!(
                            InvalidSQL,
                            "Unsupported interval value: {:?}",
                            other
                        )),
                    }?;
                    Ok(Expression::months(months as u32))
                }
                _ => Err(translation_err!(
                    InvalidSQL,
                    "Unsupported interval: {:?}",
                    interval
                )),
            },
            sqlparser::ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let operand = match operand {
                    Some(operand) => Some(self.process_expr(operand, distance)?),
                    None => None,
                };
                let mut whens = Vec::with_capacity(conditions.len());
                for (condition, result) in conditions.iter().zip(results.iter()) {
                    let condition = self.process_expr(condition, distance)?;
                    let result = self.process_expr(result, distance)?;
                    whens.push((condition, result));
                }
                let else_result = match else_result {
                    Some(expr) => Some(self.process_expr(expr, distance)?),
                    None => None,
                };
                Ok(Expression::case(operand, whens, else_result))
            }
            sqlparser::ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.process_expr(expr, distance)?;
                let low = self.process_expr(low, distance)?;
                let high = self.process_expr(high, distance)?;
                let between = expr.between(low, high);
                if *negated {
                    Ok(between.not())
                } else {
                    Ok(between)
                }
            }
            sqlparser::ast::Expr::Extract { field, expr } => {
                let expr = self.process_expr(expr, distance)?;
                let field = match field {
                    sqlparser::ast::DateTimeField::Year => DateField::Year,
                    sqlparser::ast::DateTimeField::Month => DateField::Month,
                    sqlparser::ast::DateTimeField::Day => DateField::Day,
                    _ => {
                        return Err(translation_err!(
                            UnsupportedSQL,
                            "Unsupported extract field: {:?}",
                            field
                        ));
                    }
                };
                Ok(expr.extract(field))
            }
            sqlparser::ast::Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                let expr = self.process_expr(expr, distance)?;
                let pattern = self.process_expr(pattern, distance)?;
                let pattern = match pattern {
                    Expression::Field {
                        val: Field::String(Some(s)),
                    } => Ok(s),
                    _ => {
                        return Err(translation_err!(
                            UnsupportedSQL,
                            "Unsupported pattern: {:?}",
                            pattern
                        ));
                    }
                }?;
                let expr = expr.like(pattern, escape_char.clone());
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            sqlparser::ast::Expr::Cast {
                kind: _,
                expr,
                data_type,
                format: _,
            } => {
                let expr = self.process_expr(expr, distance)?;
                let data_type = get_type(data_type);
                Ok(expr.cast(data_type))
            }
            sqlparser::ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.process_expr(expr, distance)?;
                let list = list
                    .iter()
                    .map(|expr| self.process_expr(expr, distance))
                    .collect::<Result<Vec<_>, _>>()?;
                let expr = expr.in_list(list);
                if *negated {
                    Ok(expr.not())
                } else {
                    Ok(expr)
                }
            }
            other => Err(translation_err!(
                UnsupportedSQL,
                "Unsupported expression: {:?} matched {:?}",
                expr,
                other
            )),
        }
    }
}

// Helper functions
fn get_name(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn is_valid_alias(alias: &str) -> bool {
    alias.chars().all(|c| c.is_alphanumeric() || c == '_')
}

fn has_agg(expr: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr::*;
    match expr {
        Identifier(_) => false,
        CompoundIdentifier(_) => false,
        Value(_) => false,
        TypedString { .. } => false,

        BinaryOp { left, op: _, right } => has_agg(left) || has_agg(right),
        Function(function) => match get_name(&function.name).to_uppercase().as_str() {
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => true,
            _ => false,
        },
        Nested(expr) => has_agg(expr),
        Extract { .. } => false,
        Cast { expr, .. } => has_agg(expr),
        _ => unimplemented!("Unsupported expression: {:?}", expr),
    }
}

fn get_type(d_type: &sqlparser::ast::DataType) -> DataType {
    match d_type {
        sqlparser::ast::DataType::Int(_)
        | sqlparser::ast::DataType::SmallInt(_)
        | sqlparser::ast::DataType::BigInt(_) => DataType::Int,
        sqlparser::ast::DataType::Double | sqlparser::ast::DataType::Decimal(_) => DataType::Float,
        sqlparser::ast::DataType::Char(_)
        | sqlparser::ast::DataType::Varchar(_)
        | sqlparser::ast::DataType::Text => DataType::String,
        sqlparser::ast::DataType::Date => DataType::Date,
        sqlparser::ast::DataType::Boolean => DataType::Boolean,
        _ => unimplemented!("Unsupported data type: {:?}", d_type),
    }
}

fn process_any(
    translator: &Translator,
    left: &sqlparser::ast::Expr,
    compare_op: &sqlparser::ast::BinaryOperator,
    right: &sqlparser::ast::Expr,
    distance: Option<usize>,
) -> Result<Expression<LogicalRelExpr>, TranslatorError> {
    let left = translator.process_expr(left, distance)?;
    let right = translator.process_expr(right, distance)?;
    let bin_op = match compare_op {
        sqlparser::ast::BinaryOperator::Eq => BinaryOp::Eq,
        sqlparser::ast::BinaryOperator::NotEq => BinaryOp::Neq,
        sqlparser::ast::BinaryOperator::Lt => BinaryOp::Lt,
        sqlparser::ast::BinaryOperator::Gt => BinaryOp::Gt,
        sqlparser::ast::BinaryOperator::LtEq => BinaryOp::Le,
        sqlparser::ast::BinaryOperator::GtEq => BinaryOp::Ge,
        _ => {
            unreachable!()
        }
    };
    match right {
        Expression::Subquery { expr } => {
            let mut plan = *expr;
            let att = plan.att();
            if att.len() != 1 {
                panic!("Subquery in ANY should return only one column")
            }
            let col_id = att.iter().next().unwrap();
            // left bin_op ANY (right) is translated to:
            // 1. If right contains NULL, then match TRUE else NULL
            // 2. If right does not contain NULL, then match TRUE else FALSE
            // 3. If right returns 0 rows, then FALSE even if left row is NULL
            //
            // We compute this by taking the aggregate of the subquery to compute
            // the number of rows and if there is any NULL in left or right rows.
            //
            // Super dirty hack to deal with ANY subquery without introducing a new join rule (mark join)
            // First, we append two columns to the result of the subquery
            // Expression::int(1) and the result of the case when operation
            //
            let col_id0 = translator.col_id_gen.next();
            let col_id1 = translator.col_id_gen.next();
            // Case when col is NULL then MIN_INT/2 (Large negative number)
            //      when left is NULL then MIN_INT/4 (Second large negative number)
            //      when (left bin_op col) then 1
            //      else 0
            let case_expr1 = Expression::Case {
                expr: None,
                whens: vec![
                    (
                        Expression::col_ref(*col_id).is_null(),
                        Expression::int(i64::MIN / 2),
                    ),
                    (left.clone().is_null(), Expression::int(i64::MIN / 4)),
                    (
                        Expression::binary(bin_op, left, Expression::col_ref(*col_id)),
                        Expression::int(1),
                    ),
                ],
                else_expr: Some(Box::new(Expression::int(0))),
            };
            plan = plan.map(
                true,
                &translator.enabled_rules,
                &translator.col_id_gen,
                [(col_id0, Expression::int(1)), (col_id1, case_expr1)],
            );
            // Take the count, max, min of the columns
            let col_id2 = translator.col_id_gen.next();
            let col_id3 = translator.col_id_gen.next();
            let col_id4 = translator.col_id_gen.next();
            plan = plan.aggregate(
                true,
                &translator.enabled_rules,
                &translator.col_id_gen,
                vec![],
                vec![
                    (col_id2, (col_id0, AggOp::Count)),
                    (col_id3, (col_id1, AggOp::Max)),
                    (col_id4, (col_id1, AggOp::Min)),
                ],
            );
            // Case when count(1) == 0 then FALSE
            //      when max == 1 then TRUE
            //      when min == MIN_INT/2 then NULL
            //      when min == MIN_INT/4 then NULL
            //      else FALSE
            let case_expr2 = Expression::Case {
                expr: None,
                whens: vec![
                    (
                        Expression::col_ref(col_id2).eq(Expression::int(0)),
                        Expression::bool(false),
                    ),
                    (
                        Expression::col_ref(col_id3).eq(Expression::int(1)),
                        Expression::bool(true),
                    ),
                    (
                        Expression::col_ref(col_id4).eq(Expression::int(i64::MIN / 2)),
                        Expression::Field {
                            val: Field::Boolean(None),
                        },
                    ),
                    (
                        Expression::col_ref(col_id4).eq(Expression::int(i64::MIN / 4)),
                        Expression::Field {
                            val: Field::Boolean(None),
                        },
                    ),
                ],
                else_expr: Some(Box::new(Expression::bool(false))),
            };
            let col_id5 = translator.col_id_gen.next();
            plan = plan
                .map(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [(col_id5, case_expr2)],
                )
                .u_project(
                    true,
                    &translator.enabled_rules,
                    &translator.col_id_gen,
                    [col_id5].into_iter().collect(),
                );
            Ok(Expression::subquery(plan))
        }
        _ => {
            unimplemented!("AnyOp should have a subquery on the right side")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Translator;
    use crate::{
        catalog::{Catalog, ColIdGen, ColIdGenRef, ColumnDef, DataType, Schema, Table},
        expression::prelude::{HeuristicRule, HeuristicRules},
    };
    use sqlparser::dialect::{DuckDbDialect, PostgreSqlDialect};
    use std::sync::Arc;

    fn get_test_catalog() -> Catalog {
        let catalog = Catalog::new();
        catalog.add_table(
            0,
            Arc::new(Table::new(
                "t1",
                Arc::new(Schema::new(
                    vec![
                        ColumnDef::new("a", DataType::Int, true),
                        ColumnDef::new("b", DataType::Int, true),
                        ColumnDef::new("p", DataType::Int, true),
                        ColumnDef::new("q", DataType::Int, true),
                        ColumnDef::new("r", DataType::Int, true),
                    ],
                    vec![0],
                )),
            )),
        );
        catalog.add_table(
            1,
            Arc::new(Table::new(
                "t2",
                Arc::new(Schema::new(
                    vec![
                        ColumnDef::new("c", DataType::Int, true),
                        ColumnDef::new("d", DataType::Int, true),
                    ],
                    vec![0],
                )),
            )),
        );

        catalog.add_table(
            2,
            Arc::new(Table::new(
                "t3",
                Arc::new(Schema::new(
                    vec![
                        ColumnDef::new("e", DataType::Int, true),
                        ColumnDef::new("f", DataType::Int, true),
                    ],
                    vec![0],
                )),
            )),
        );

        catalog
    }

    fn parse_sql(sql: &str) -> sqlparser::ast::Query {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = DuckDbDialect {};
        let statements = Parser::new(&dialect)
            .try_with_sql(&sql)
            .unwrap()
            .parse_statements()
            .unwrap();
        let query = {
            let statement = statements.into_iter().next().unwrap();
            if let sqlparser::ast::Statement::Query(query) = statement {
                query
            } else {
                panic!("Expected a query");
            }
        };
        *query
    }

    fn get_translator() -> Translator {
        let db_id = 0;
        let catalog = Arc::new(get_test_catalog());
        let enabled_rules = Arc::new(HeuristicRules::default());
        // enabled_rules.disable(HeuristicRule::Decorrelate);
        // enabled_rules.disable(HeuristicRule::Hoist);
        // enabled_rules.disable(HeuristicRule::ProjectionPushdown);
        Translator::new(db_id, &catalog, &enabled_rules)
    }

    fn get_plan(sql: &str) -> String {
        let query = parse_sql(sql);
        let mut translator = get_translator();
        let query = translator.process_query(&query).unwrap();
        query.plan.pretty_string()
    }

    #[test]
    fn parse_from_clause() {
        let sql = "SELECT a FROM t1 WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    #[should_panic]
    fn parse_from_with_subquery() {
        let sql = "SELECT a FROM (SELECT a FROM t1) WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_subquery_2() {
        let sql = "SELECT a FROM (SELECT a, b FROM t1) AS t WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    #[should_panic]
    fn parse_from_with_subquery_and_alias() {
        let sql = "SELECT a FROM (SELECT a FROM t1) AS t WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_join() {
        let sql = "SELECT a FROM t1 JOIN t2 ON t1.a = t2.c WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_multiple_joins() {
        let sql =
            "SELECT a FROM t1 JOIN t2 ON t1.a = t2.c JOIN t3 ON t2.d = t3.e WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    #[should_panic]
    fn parse_from_with_subquery_joins() {
        let sql = "SELECT a FROM (SELECT a FROM t1) AS t1 JOIN (SELECT c FROM t2) AS t2 ON t1.a = t2.c WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_from_with_subquery_joins_2() {
        let sql = "SELECT a FROM (SELECT a, b FROM t1) AS t1 JOIN (SELECT c, d FROM t2) AS t2 ON t1.a = t2.c WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_where_clause() {
        let sql = "SELECT a FROM t1 WHERE a = 1 AND b = 2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery() {
        let sql = "SELECT a, x, y FROM t1, (SELECT COUNT(*) AS x, SUM(c) as y FROM t2 WHERE c = a)";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_2() {
        // This actually makes sense if we consider AVG(b) as AVG(0+b)
        let sql = "SELECT a, x, y FROM t1, (SELECT AVG(b) AS x, SUM(d) as y FROM t2 WHERE c = a)";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_3() {
        let sql = "SELECT a, k, x, y FROM t1, (SELECT b as k, c as x, d as y FROM t2 WHERE c = a)";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_with_alias() {
        let sql =
            "SELECT a, x, y FROM t1, (SELECT COUNT(a) AS x, SUM(b) as y FROM t2 WHERE c = a) AS t2";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_with_same_tables() {
        let sql = "SELECT x, y FROM (SELECT a as x FROM t1), (SELECT a as y FROM t1) WHERE x = y";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_joins_with_same_name() {
        let sql = "SELECT * FROM t1 as a, t1 as b";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parser_aggregate() {
        let sql = "SELECT COUNT(a), SUM(b) FROM t1";
        println!("{}", get_plan(sql));
    }

    #[test]
    fn parse_subquery_where() {
        let sql = "SELECT a FROM t1 WHERE exists (SELECT * FROM t2 WHERE c = a)";
        println!("{}", get_plan(sql));
    }

    // #[test]
    // fn parse_subquery_where_any() {
    //     let sql = "SELECT * FROM t1 WHERE a IN (SELECT * FROM t2 WHERE c = a)";
    //     println!("{}", get_plan(sql));
    // }
}

// Subquery types
// 1. Select clause
//   a. Scalar subquery. A subquery that returns a single row.
//   b. EXISTS subquery. Subquery can return multiple rows.
//   c. ANY subquery. Subquery can return multiple rows.
// 2. From clause
//   a. Subquery can return multiple rows.
//

use std::collections::{HashMap, HashSet};

use duckdb::Connection;
use regex::Regex;
use sqlparser::{
    ast::{Cte, Query, Statement},
    dialect::DuckDbDialect,
};
use sqlparser::{
    ast::{SetExpr, TableFactor, TableWithJoins},
    parser::Parser,
};

use crate::errors::ArnabError;

#[derive(Clone)]
pub enum NodeType {
    Sql,
    // Python, --> need to figure out how to pass data to-from python
    // Shell,
    // Unknown,
}

pub enum NodeExecutionResult {
    Sql { n_rows: usize },
}

#[derive(Clone)]
pub struct Node {
    pub(crate) path: String,
    pub(crate) id: String,
    pub(crate) raw_src: String,
    pub(crate) rendered_src: String,
    pub(crate) nexts: HashSet<String>,
    pub(crate) prevs: HashSet<String>,
    pub(crate) node_type: NodeType,
    pub(crate) materialize: Option<String>,
}

impl Node {
    pub fn new(node_type: NodeType, path: &str, id: &str, raw_src: &str) -> Self {
        Self {
            path: path.into(),
            id: id.into(),
            raw_src: raw_src.into(),
            rendered_src: Default::default(),
            nexts: Default::default(),
            prevs: Default::default(),
            materialize: None,
            node_type,
        }
    }

    pub fn execute(&self, conn: &Connection) -> Result<NodeExecutionResult, ArnabError> {
        let res = match &self.node_type {
            NodeType::Sql => self.execute_sql(conn)?,
        };
        Ok(res)
    }

    pub(crate) fn render_and_populate_refs(
        &mut self,
        macros: &HashMap<String, String>,
        all_model_names: &[String],
    ) {
        // strip one-line comments
        let mut raw_no_comment = self
            .raw_src
            .split('\n')
            .filter(|line| !line.trim().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        // strip block comments
        let re = Regex::new(r"/\*[\s\S]*?\*/").unwrap();
        raw_no_comment = re.replace_all(&raw_no_comment, "").to_string();

        let mut env = minijinja::Environment::new();

        // Append macros to the raw source
        let mut macro_src_concat = macros
            .values()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        macro_src_concat.push('\n');
        macro_src_concat.push_str(&raw_no_comment);

        // get all dependency candidates from a SQL statement. Then
        // filter out those who don't belong to the found models, because
        // they could be a reference to CTE, alias, etc. So we will just ignore
        // them from graph creation.
        let prevs = get_sql_references(&macro_src_concat)
            .into_iter()
            .filter(|v| all_model_names.contains(v))
            .collect::<HashSet<String>>();
        self.prevs = prevs;

        env.add_template(&self.id, &macro_src_concat).unwrap();
        let rendered = env
            .get_template(&self.id)
            .unwrap()
            .render(minijinja::context! {})
            .unwrap();
        self.rendered_src = rendered.to_string();
    }
}

impl Node {
    fn will_produce_records(&self, statement: &str) -> bool {
        let starting_words = vec!["SELECT", "WITH"];
        for sw in starting_words {
            if statement[..50.min(statement.len())]
                .to_uppercase()
                .starts_with(sw)
            {
                return true;
            }
        }
        false
    }

    fn execute_sql(&self, conn: &Connection) -> Result<NodeExecutionResult, ArnabError> {
        let statements: Vec<String> = self
            .rendered_src
            .split(';')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Statement batch validation will check if a model has exactle one
        // SELECT statement.
        let statements_returning_records = statements
            .iter()
            .filter(|s| self.will_produce_records(s))
            .collect::<Vec<_>>();

        if statements_returning_records.len() != 1 {
            return Err(
                ArnabError::Error(format!("Models must have exactly one `SELECT` statement (or equivalent statements returning recods), but {} has {}", self.id, statements_returning_records.len())),
            );
        }

        // We are not going to bulk-execute statements, so the source code is split
        // by semicolon. A single statement containing SELECT, WITH, etc., will
        // be treated differently to create VIEW or TABLE.
        let mut n_rows: usize = 0;
        for statement in &statements {
            let mut adjusted_statement = statement.to_string();

            // Only process non-empty statements
            // We shall process SQL statement that returns record
            if self.will_produce_records(&adjusted_statement) {
                let create_view_statement =
                    format!("CREATE OR REPLACE VIEW {} AS ({})", self.id, statement);
                adjusted_statement = match &self.materialize {
                    Some(materialize) => match materialize.to_lowercase().as_str() {
                        "table" => {
                            format!("CREATE OR REPLACE TABLE {} AS ({})", self.id, statement)
                        }
                        "view" => create_view_statement,
                        _ => {
                            return Err(ArnabError::Error(format!(
                                "Unknown materialization type `{}`",
                                materialize
                            )))
                        }
                    },
                    None => create_view_statement,
                };
            }

            let res = conn.execute(&adjusted_statement, []);
            match res {
                Ok(_) => {
                    if self.materialize == Some("table".into()) {
                        // Count the number of rows
                        let count_sql = format!("SELECT COUNT(*) FROM {}", self.id);
                        if let Ok(stmt) = &mut conn.prepare(&count_sql) {
                            let mut rows = stmt.query([]).unwrap();
                            rows.next().into_iter().for_each(|r| {
                                let n: usize = r.unwrap().get(0).unwrap();
                                n_rows = n;
                            });
                        }
                    }
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    // TODO: fix this brittle way to check empty statement
                    if err_msg.contains("No statement to prepare") {
                        continue;
                    }

                    return Err(ArnabError::StatementExecutionError {
                        msg: e.to_string(),
                        path: self.path.clone(),
                        sql: statement.to_string(),
                    });
                }
            };
        }
        Ok(NodeExecutionResult::Sql { n_rows })
    }
}

/// Get references from a SINGLE sql statement
pub fn get_sql_references(stmt: &str) -> HashSet<String> {
    let dialect = DuckDbDialect {};
    let ast = Parser::parse_sql(&dialect, stmt).unwrap();

    let mut tables = HashSet::new();
    for statement in ast {
        if let Statement::Query(query) = statement {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    extract_from_cte(&cte, &mut tables);
                }
            }
            if let SetExpr::Select(select) = &*query.body {
                for tables_with_joins in &select.from {
                    extract_dependency_names(tables_with_joins, &mut tables);
                }
            }
        }
    }

    tables
}

fn extract_dependency_names(table_with_joins: &TableWithJoins, tables: &mut HashSet<String>) {
    match &table_with_joins.relation {
        TableFactor::Table { name, .. } => {
            tables.insert(name.to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            extract_from_subquery(subquery, tables);
        }
        _ => {}
    }

    for join in &table_with_joins.joins {
        match &join.relation {
            TableFactor::Table { name, .. } => {
                tables.insert(name.to_string());
            }
            TableFactor::Derived { subquery, .. } => {
                extract_from_subquery(subquery, tables);
            }
            _ => {}
        }
    }
}

fn extract_from_cte(cte: &Cte, tables: &mut HashSet<String>) {
    if let SetExpr::Select(select) = &*cte.query.body {
        for table_with_joins in &select.from {
            extract_dependency_names(table_with_joins, tables);
        }
    }
}

fn extract_from_subquery(query: &Query, tables: &mut HashSet<String>) {
    if let SetExpr::Select(select) = &*query.body {
        for table_with_joins in &select.from {
            extract_dependency_names(table_with_joins, tables);
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::get_sql_references;

    #[test]
    fn get_ref() {
        let refs = get_sql_references("SELECT * FROM abc");
        assert_eq!(refs, HashSet::from(["abc".to_string()]))
    }

    #[test]
    fn get_ref_subtable() {
        let sql = "SELECT * FROM (SELECT * FROM my_sub_table) AS sub_query, my_table WHERE id = 1";
        let refs = get_sql_references(sql);
        assert_eq!(
            refs,
            HashSet::from(["my_sub_table".to_string(), "my_table".to_string()])
        );
    }
}

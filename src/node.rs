use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use duckdb::Connection;
use regex::Regex;

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
            // NodeType::Shell => todo!(),
            // NodeType::Unknown => todo!(),
        };
        Ok(res)
    }

    pub(crate) fn render_and_populate_refs(&mut self, macros: &HashMap<String, String>) {
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

        let deps: Arc<RwLock<HashSet<String>>> = Default::default();
        let deps_clone = deps.clone();

        let mut env = minijinja::Environment::new();

        let mut macro_src_concat = macros
            .values()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        macro_src_concat.push('\n');
        macro_src_concat.push_str(&raw_no_comment);

        let the_fn = move |name: String| {
            let deps = &deps_clone;
            deps.write().unwrap().insert(name.clone());
            Ok(name)
        };
        env.add_template(&self.id, &macro_src_concat).unwrap();
        env.add_function("ref", the_fn);

        let rendered = env
            .get_template(&self.id)
            .unwrap()
            .render(minijinja::context! {})
            .unwrap();
        self.prevs = deps.read().unwrap().clone();
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

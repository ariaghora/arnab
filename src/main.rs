mod session;

#[allow(unused_imports)]
use clap::{Command, Parser, Subcommand};
use glob;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
};

use duckdb::Connection;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run sql script file
    RunFile(RunScriptArgs),
    Run(RunArgs),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct RunScriptArgs {
    script_path: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct RunArgs {
    models: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Config {
    db_path: Option<String>,
    model_path: Option<String>,
}

#[derive(Debug)]
enum ArnabError {
    Error(String),
    StatementExecutionError {
        msg: String,
        sql: String,
        path: String,
    },
    UnknownModelType(String),
}

impl std::fmt::Display for ArnabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArnabError::Error(msg) => write!(f, "{}", msg),
            ArnabError::StatementExecutionError { .. } => write!(f, "{:#?}", self),
            ArnabError::UnknownModelType(model_type) => {
                write!(f, "Unknown model type: {}", model_type)
            }
        }
    }
}

#[derive(Clone)]
enum NodeType {
    Sql,
    // Shell,
    // Unknown,
}

struct Session {
    config: Config,
    db_conn: Connection,
}

impl Session {
    pub fn new(config: Config, connection: Connection) -> Self {
        Self {
            config,
            db_conn: connection,
        }
    }

    pub fn run(&mut self) -> Result<(), ArnabError> {
        let glob_pattern =
            std::path::Path::new(&self.config.model_path.as_ref().unwrap()).join("*.*");
        let model_paths = glob::glob(glob_pattern.to_str().unwrap())
            .unwrap()
            .map(|v| v.unwrap())
            .collect::<Vec<_>>();

        let mut node_map = HashMap::new();
        let mut n_source = 0;
        for p in model_paths.into_iter() {
            let path_string = p.to_string_lossy().to_string();
            let node_id = {
                let path = std::path::Path::new(&path_string);
                let path_no_ext = path.with_extension("");
                let file_name = path_no_ext.file_name().unwrap();
                file_name.to_string_lossy().to_string()
            };

            println!("Found model source: {}", path_string);
            n_source += 1;

            let raw_src = std::fs::read_to_string(&path_string).unwrap();
            let node_type = {
                let extension = p.extension().unwrap().to_str().unwrap();
                match extension {
                    "sql" => NodeType::Sql,
                    _ => return Err(ArnabError::UnknownModelType(extension.into())),
                }
            };
            let mut node = Node::new(node_type, &path_string, &node_id, &raw_src);
            node.populate_refs();
            node_map.insert(node_id, node);
        }
        println!(
            "Found {} model source{}",
            n_source,
            if n_source > 1 { "s" } else { "" }
        );

        // Populate outgoing nodes
        let ids: Vec<String> = node_map.keys().map(|v| v.to_string()).collect();
        let mut invalid_node_ids = HashSet::new();
        for id in ids {
            let node = &node_map.get(&id).unwrap();
            let prevs = node.prevs.clone();
            for prev_id in prevs {
                match node_map.get_mut(&prev_id) {
                    Some(prev_node) => {
                        prev_node.nexts.push(id.to_string());
                    }
                    None => {
                        invalid_node_ids.insert(prev_id.clone());
                        println!(
                            "WARNING: Model `{}` required by `{}` not found",
                            prev_id, id
                        )
                    }
                };
            }
        }

        let terminal_ids: Vec<String> = node_map
            .values()
            .filter(|v| v.nexts.len() == 0)
            .map(|v| v.id.clone())
            .collect();

        let mut sorted_ids = vec![];
        for t_id in terminal_ids {
            self.topo(&node_map[&t_id], &mut sorted_ids);
        }

        let sorted_valid_ids = sorted_ids
            .iter()
            .filter(|v| !invalid_node_ids.contains(&v.to_string()))
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        let mut n_execution_success = 0;
        let mut execution_errors = Vec::new();
        for s_id in sorted_valid_ids {
            let node = &node_map[&s_id];
            match node.execute(&self.db_conn) {
                Ok(_) => {
                    n_execution_success += 1;
                    println!("Model {}: OK", s_id);
                }
                Err(e) => {
                    println!("Model {}: ERROR", s_id);
                    execution_errors.push(e);
                }
            };
        }

        for err in &execution_errors {
            println!("{}", err.to_string())
        }

        println!(
            "OK={}, ERROR={}",
            n_execution_success,
            execution_errors.len()
        );

        Ok(())
    }

    fn topo(&self, root: &Node, out: &mut Vec<String>) {
        if out.contains(&root.id) {
            return;
        }

        for prev_id in &root.prevs {
            out.push(prev_id.clone())
        }
        out.push(root.id.clone())
    }
}

#[derive(Clone)]
struct Node {
    path: String,
    id: String,
    raw_src: String,
    rendered_src: String,
    nexts: Vec<String>,
    prevs: Vec<String>,
    node_type: NodeType,
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
            node_type,
        }
    }

    pub fn execute(&self, conn: &Connection) -> Result<(), ArnabError> {
        match &self.node_type {
            NodeType::Sql => self.execute_sql(conn)?,
            // NodeType::Shell => todo!(),
            // NodeType::Unknown => todo!(),
        }
        Ok(())
    }

    fn populate_refs(&mut self) {
        let re = regex::Regex::new(r"\{\{([^}]*)\}\}").unwrap();
        for cap in re.captures_iter(&self.raw_src) {
            self.prevs.push(cap[1].to_string());
        }

        let rendered = re.replace_all(&self.raw_src, "$1");
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

    fn execute_sql(&self, conn: &Connection) -> Result<(), ArnabError> {
        let statements: Vec<String> = self
            .rendered_src
            .split(";")
            .map(|s| s.trim().to_string())
            .filter(|s| s.len() > 0)
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
        for statement in statements {
            // Only process non-empty statements
            // We shall process SQL statement that returns record
            if self.will_produce_records(&statement) {
                // TODO
            }

            let res = conn.execute(&statement, []);
            match res {
                Ok(_) => {
                    //TODO
                    // println!(
                    //     "EXECUTED: {}...\nAFFECTED: {}\n\n",
                    //     &statement[0..50.min(statement.len())],
                    //     n_affected
                    // );
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
        Ok(())
    }
}

fn run_session_with_args(_args: RunArgs, conn: Connection, config: Config) {
    let mut session = Session::new(config, conn);
    match session.run() {
        Ok(_) => {
            println!("Finished")
        }
        Err(e) => {
            match e {
                ArnabError::Error(msg) => println!("Error: {}", msg),
                ArnabError::StatementExecutionError { msg, sql, path } => {
                    println!("Failed to execute SQL statement.");
                    println!("Error      : {}", msg);
                    println!("Source path: {}", path);
                    println!("SQL:\n{}", sql);
                }
                _ => {
                    println!("{:#?}", e)
                }
            }
            std::process::exit(1)
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let config_name = "config.yaml";
    let config_exists = std::path::Path::new(config_name).exists();
    if !config_exists {
        println!("Config file (config.yaml) not found on project root");
        std::process::exit(1);
    }

    let config_str = std::fs::read_to_string(config_name)?;
    let config: Config = serde_yaml::from_str(&config_str)?;

    let conn = match &config.db_path {
        Some(db_path) => Connection::open(&db_path)?,
        None => {
            println!("db_path must be configured in the configuration");
            std::process::exit(1);
        }
    };

    let cli = Cli::parse();
    match cli.command {
        Commands::RunFile(arg) => match std::fs::read_to_string(&arg.script_path) {
            Ok(content) => {
                let script_exec_result = conn.execute_batch(&content);
                match script_exec_result {
                    Ok(_) => {
                        println!("Finished executing {}", arg.script_path);
                    }
                    Err(e) => {
                        println!("{}", e.to_string());
                        std::process::exit(1);
                    }
                }
            }
            Err(_) => todo!(),
        },
        Commands::Run(args) => {
            run_session_with_args(args, conn, config);
        }
    }

    Ok(())
}

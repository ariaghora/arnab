mod session;

#[allow(unused_imports)]
use clap::{Command, Parser, Subcommand};
use colored::Colorize;
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    io::Write,
    sync::{Arc, RwLock},
    usize,
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
    /// Run pipelines
    Run(RunArgs),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct RunScriptArgs {
    /// Paths to script or pattern
    script_paths: Vec<String>,
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
    models: Option<HashMap<String, ModelInfo>>,
}

#[derive(Debug, Deserialize)]
struct ModelInfo {
    materialize: Option<String>,
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

fn format_elapsed(elapsed: std::time::Duration) -> String {
    let hours = elapsed.as_secs() / 3600;
    let minutes = (elapsed.as_secs() % 3600) / 60;
    let seconds = elapsed.as_secs() % 60;
    let milliseconds = elapsed.subsec_millis();

    let mut components = Vec::new();

    if hours > 0 {
        components.push(format!("{}h", hours));
    }
    if minutes > 0 {
        components.push(format!("{}m", minutes));
    }
    if seconds > 0 {
        components.push(format!("{}s", seconds));
    }

    // always show milliseconds
    components.push(format!("{}ms", milliseconds));

    let formatted_duration = components.join(" ");

    formatted_duration
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

            // set model's materialization mode
            if let Some(models) = &self.config.models {
                if let Some(model_info) = models.get(&node_id) {
                    node.materialize = model_info.materialize.clone();
                }
            }

            node.render_and_populate_refs();
            node_map.insert(node_id, node);
        }
        println!(
            "Found {} model source{}\n",
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
                        prev_node.nexts.insert(id.to_string());
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

        // Terminal ids are the id of nodes who have no outgoing edge,
        // i.e., node.nexts.len() == 0
        let terminal_ids: Vec<String> = node_map
            .values()
            .filter(|v| v.nexts.len() == 0)
            .map(|v| v.id.clone())
            .collect();

        // Obtain node ids in topological order
        let mut sorted_ids = vec![];
        for t_id in &terminal_ids {
            self.topo(t_id, &node_map, &mut sorted_ids);
        }

        // Filter out invalid node ids
        let sorted_valid_ids = sorted_ids
            .iter()
            .filter(|v| !invalid_node_ids.contains(&v.to_string()))
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        let now = chrono::Local::now();
        println!("Start pipeline execution on {}", now.format("%Y-%m-%d"));

        // Main pipeline execution
        let mut n_execution_success = 0;
        let mut execution_errors = Vec::new();
        let mut nth_processed = 1;
        let pipeline_start_time = std::time::Instant::now();
        for s_id in &sorted_valid_ids {
            let node = &node_map[s_id];

            let start_time = std::time::Instant::now();
            let mut status: String;

            let now_node = chrono::Local::now();

            let mut process_info = format!(
                "{}  {} of {}: creating {} {} model",
                now_node.format("%H:%M:%S"),
                nth_processed,
                sorted_valid_ids.len(),
                node.id.blue(),
                node.materialize
                    .as_ref()
                    .unwrap_or(&"view".to_string())
                    .to_lowercase(),
            );
            // pad with dots to fill terminal width nicely in `n_col` columns
            let n_col = 80;
            if process_info.len() < n_col {
                process_info.extend(std::iter::repeat('.').take(n_col - process_info.len()));
            }

            print!("{}", process_info);
            std::io::stdout().flush().unwrap();

            match node.execute(&self.db_conn) {
                Ok(execution_result) => {
                    n_execution_success += 1;
                    status = "CREATE VIEW".green().to_string();
                    match execution_result {
                        NodeExecutionResult::Sql { n_rows } => {
                            if let Some(materialize) = &node.materialize {
                                if materialize == "table" {
                                    status = format!("SELECT {}", n_rows).green().to_string();
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    status = "ERROR".red().to_string();
                    execution_errors.push(e);
                }
            };
            println!("[{} in {}]", status, format_elapsed(start_time.elapsed()));

            nth_processed += 1;
        }

        if execution_errors.len() > 0 {
            println!("\nErrors:");
            for err in &execution_errors {
                match err {
                    ArnabError::StatementExecutionError { msg, sql: _, path } => {
                        println!("Failed to execute SQL statement.");
                        println!("Source path : {}", path);
                        println!("Error       : {}\n", msg.red());
                    }
                    _ => println!("{}\n", err.to_string()),
                }
            }
        }

        println!(
            "\nPipeline execution completed in {} with {} success and {} errors",
            format_elapsed(pipeline_start_time.elapsed()),
            n_execution_success,
            execution_errors.len()
        );

        Ok(())
    }

    fn topo(&self, root_id: &String, nodes: &HashMap<String, Node>, out: &mut Vec<String>) {
        if out.contains(&root_id) {
            return;
        }

        let root_node = &nodes[root_id];
        for prev_id in &root_node.prevs {
            self.topo(prev_id, nodes, out);
        }
        out.push(root_id.clone())
    }
}

#[derive(Clone)]
struct Node {
    path: String,
    id: String,
    raw_src: String,
    rendered_src: String,
    nexts: HashSet<String>,
    prevs: HashSet<String>,
    node_type: NodeType,
    materialize: Option<String>,
}

enum NodeExecutionResult {
    Sql { n_rows: usize },
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

    fn render_and_populate_refs(&mut self) {
        // strip one-line comments
        let mut raw_no_comment = self
            .raw_src
            .split("\n")
            .filter(|line| !line.trim().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        // strip block comments
        let re = Regex::new(r"/\*[\s\S]*?\*/").unwrap();
        raw_no_comment = re.replace_all(&raw_no_comment, "").to_string();

        let deps: Arc<RwLock<HashSet<String>>> = Default::default();
        let deps_clone = deps.clone();
        let mut env = minijinja::Environment::new();
        let the_fn = move |name: String| {
            let foo = &deps_clone;
            foo.write().unwrap().insert(name.clone());
            Ok(name)
        };
        env.add_template(&self.id, &raw_no_comment).unwrap();
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

fn run_session_with_args(_args: RunArgs, conn: Connection, config: Config) {
    let mut session = Session::new(config, conn);
    match session.run() {
        Ok(_) => {
            // TODO: do something on session completed
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
        Commands::RunFile(arg) => {
            //let g = glob(&arg.script_paths).unwrap();
            for path in &arg.script_paths {
                match std::fs::read_to_string(path) {
                    Ok(content) => {
                        print!("Running {}... ", path);
                        std::io::stdout().flush().unwrap();

                        let script_exec_result = conn.execute_batch(&content);
                        match script_exec_result {
                            Ok(_) => {
                                println!("OK");
                            }
                            Err(e) => {
                                println!("ERROR: {}\nSkipping {}", e.to_string(), path);
                            }
                        }
                    }
                    Err(_) => println!("Cannot open {}, skipping", path),
                };
            }
        }
        Commands::Run(args) => {
            run_session_with_args(args, conn, config);
        }
    }

    Ok(())
}

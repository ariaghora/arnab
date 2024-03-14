use std::{
    collections::{HashMap, HashSet},
    io::Write,
};

use colored::Colorize;
use duckdb::Connection;
use serde::Deserialize;

use crate::{
    errors::ArnabError,
    node::{Node, NodeExecutionResult, NodeType},
};

#[derive(Debug, Deserialize)]
pub struct ModelInfo {
    pub(crate) materialize: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub(crate) db_path: Option<String>,
    pub(crate) macro_path: Option<String>,
    pub(crate) duckdb_settings: Option<HashMap<String, String>>,
    pub(crate) model_path: Option<String>,
    pub(crate) models: Option<HashMap<String, ModelInfo>>,
}

/// Representation of a single process of pipeline execution
pub struct Session {
    pub(crate) config: Config,
    pub(crate) db_conn: Connection,
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

        // load User-defined macros
        let mut macros = HashMap::new();
        if let Some(macro_path) = &self.config.macro_path {
            let macro_paths = glob::glob(
                std::path::Path::new(macro_path)
                    .join("*.*")
                    .to_str()
                    .unwrap(),
            )
            .unwrap();
            for path_opt in macro_paths {
                let path = path_opt.unwrap();
                let path_str = path.to_string_lossy().to_string();
                let macro_src = std::fs::read_to_string(&path_str).unwrap();
                macros.insert(path_str, macro_src);
            }
        }

        // Populate nodemap, a mapping from filename to Node struct
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

            // node.render_and_populate_refs(&macros);

            node_map.insert(node_id, node);
        }

        // Render SQL and populate incoming edges
        let found_model_names = node_map
            .keys()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();
        for (_, node) in node_map.iter_mut() {
            node.render_and_populate_refs(&macros, &found_model_names);
        }

        println!(
            "Found {} model source{}, {} macro{}\n",
            n_source,
            if n_source > 1 { "s" } else { "" },
            macros.len(),
            if macros.len() > 1 { "s" } else { "" },
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
            .filter(|v| v.nexts.is_empty())
            .map(|v| v.id.clone())
            .collect();

        // Obtain node ids in topological order
        let mut sorted_ids = vec![];
        for t_id in &terminal_ids {
            topo(t_id, &node_map, &mut sorted_ids);
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

        if !execution_errors.is_empty() {
            println!("\nErrors:");
            for err in &execution_errors {
                match err {
                    ArnabError::StatementExecutionError { msg, sql: _, path } => {
                        println!("Failed to execute SQL statement.");
                        println!("Source path : {}", path);
                        println!("Error       : {}\n", msg.red());
                    }
                    _ => println!("{}\n", err),
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
}

fn topo(root_id: &String, nodes: &HashMap<String, Node>, out: &mut Vec<String>) {
    if out.contains(root_id) {
        return;
    }

    let root_node = &nodes[root_id];
    for prev_id in &root_node.prevs {
        topo(prev_id, nodes, out);
    }
    out.push(root_id.clone())
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

    components.join(" ")
}

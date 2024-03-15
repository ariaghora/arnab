pub mod errors;
mod graphviz;
pub mod node;
mod session;

#[allow(unused_imports)]
use clap::{Command, Parser, Subcommand};
use duckdb::Connection;
use errors::ArnabError;
use session::{Config, Session};
use std::{error::Error, io::Write};

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
    /// Visualize pipelines
    Viz(VizArgs),
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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct VizArgs {
    svg_output_path: String,
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

fn visualize_with_args(_args: VizArgs, conn: Connection, config: Config) {
    let mut session = Session::new(config, conn);
    match session.save_visualization() {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

fn run_session_with_args(_args: RunArgs, conn: Connection, config: Config) {
    let mut session = Session::new(config, conn);
    match session.run_nodes() {
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
        Some(db_path) => Connection::open(db_path)?,
        None => {
            println!("db_path must be configured in the configuration");
            std::process::exit(1);
        }
    };

    // Override DuckDb's settings if specified in the configuration
    if let Some(duckdb_settings) = &config.duckdb_settings {
        for (k, v) in duckdb_settings.iter() {
            let mut stmt = match conn.prepare(&format!("SET {} = {:?};", k, v)) {
                Ok(stmt) => stmt,
                Err(e) => {
                    println!("FATAL ERROR: {}\nExiting", e);
                    std::process::exit(1);
                }
            };

            match stmt.execute([]) {
                Ok(_) => {}
                Err(e) => {
                    println!("FATAL ERROR: {}\nExiting", e);
                    std::process::exit(1);
                }
            };
        }

        println!("Overridden duckdb settings:\n{:?}", duckdb_settings);
    }

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
                                println!("ERROR: {}\nSkipping {}", e, path);
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
        Commands::Viz(args) => {
            todo!()
        }
    }

    Ok(())
}

mod session;
use glob;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};

use duckdb::Connection;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Config {
    db_path: Option<String>,
    model_path: Option<String>,
}

#[derive(Debug)]
enum ArnabError {}

#[derive(Clone)]
enum NodeType {
    Sql,
    Shell,
}

struct Session {
    config: Config,
    connection: Connection,
}

impl Session {
    pub fn new(config: Config, connection: Connection) -> Self {
        Self { config, connection }
    }

    pub fn run(&mut self) -> Result<(), ArnabError> {
        let glob_pattern =
            std::path::Path::new(&self.config.model_path.as_ref().unwrap()).join("*.*");
        let model_paths = glob::glob(glob_pattern.to_str().unwrap())
            .unwrap()
            .map(|v| v.unwrap())
            .collect::<Vec<_>>();

        let mut node_map = HashMap::new();
        for p in model_paths.into_iter() {
            let path_string = p.to_string_lossy().to_string();
            let node_id = {
                let path = std::path::Path::new(&path_string);
                let path_no_ext = path.with_extension("");
                let file_name = path_no_ext.file_name().unwrap();
                file_name.to_string_lossy().to_string()
            };

            let raw_src = std::fs::read_to_string(&path_string).unwrap();
            let mut node = Node::new(&path_string, &node_id, &raw_src);
            node.populate_refs(self);
            node_map.insert(node_id, node);
        }

        // populate outgoing nodes
        let ids: Vec<String> = node_map.keys().map(|v| v.to_string()).collect();
        for id in ids {
            let node = &node_map.get(&id).unwrap();
            let prevs = node.prevs.clone();
            for prev_id in prevs {
                let prev_node = node_map.get_mut(&prev_id).unwrap();
                prev_node.nexts.push(id.to_string());
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
        println!("{:?}", sorted_ids);

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
}

impl Node {
    pub fn new(path: &str, id: &str, raw_src: &str) -> Self {
        Self {
            path: path.into(),
            id: id.into(),
            raw_src: raw_src.into(),
            rendered_src: Default::default(),
            nexts: Default::default(),
            prevs: Default::default(),
        }
    }

    pub fn populate_refs(&mut self, session: &mut Session) {
        let re = regex::Regex::new(r"\{\{([^}]*)\}\}").unwrap();
        for cap in re.captures_iter(&self.raw_src) {
            self.prevs.push(cap[1].to_string());
        }

        let rendered = re.replace_all(&self.raw_src, "$1");
        self.rendered_src = rendered.to_string();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let config_name = "config.yaml";
    let config_exists = std::path::Path::new(config_name).exists();
    if !config_exists {
        panic!("Config not found")
    }

    let config_str = std::fs::read_to_string(config_name)?;
    let config: Config = serde_yaml::from_str(&config_str)?;

    let conn = match &config.db_path {
        Some(db_path) => Connection::open(&db_path)?,
        None => panic!("db_path must be configured"),
    };

    let mut session = Session::new(config, conn);
    session.run().unwrap();

    Ok(())
}

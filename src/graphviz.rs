use std::collections::HashMap;

use crate::node::Node;
use layout::{
    backends::svg::SVGWriter,
    gv::{self, GraphBuilder},
};

/// Given a list of node name and a map from node name to node object, render 
/// rendered graph in SVG format.
pub fn render_dot(node_names: &Vec<String>, node_map: &HashMap<String, Node>) -> String {
    let gv_nodes = node_names
        .iter()
        .map(|s| format!("\t{};", s))
        .collect::<Vec<String>>()
        .join("\n");

    let mut gv_edges = "".to_string();
    for name in node_names {
        let node = &node_map[name];
        let edges_str = node
            .nexts
            .iter()
            .map(|s| format!("\t{} -> {};", name, s))
            .collect::<Vec<String>>()
            .join("\n");

        gv_edges.push_str(&edges_str);
    }

    let dot_src = format!("digraph LR {{\n {} \n {} \n}}", gv_nodes, gv_edges);
    let mut parser = gv::DotParser::new(&dot_src);
    let graph = parser.process().unwrap();

    let mut gb = GraphBuilder::new();
    gb.visit_graph(&graph);
    let mut visual_graph = gb.get();
    let mut svg_writer = SVGWriter::new();
    visual_graph.do_it(false, false, false, &mut svg_writer);
    svg_writer.finalize()
}

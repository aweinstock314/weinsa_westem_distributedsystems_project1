extern crate argparse;

use argparse::{ArgumentParser, Store};

fn main() {
    let mut pid = 0;
    let mut tree_fname: String = "tree.txt".into();
    let mut nodes_fname: String = "nodes.txt".into();
    {
        let tree_descr = format!("File to load the tree topology from (default {})", tree_fname);
        let nodes_descr = format!("File to load the node hosts/ports from (default {})", nodes_fname);
        let mut ap = ArgumentParser::new();
        ap.set_description("Implementation of Raymond's distributed mutex algorithm by Avi Weinstock and Mark Westerhoff for Distributed Systems class");
        ap.refer(&mut pid).add_argument("pid", Store, "This node's process id").required();
        ap.refer(&mut tree_fname).add_option(&["-t", "--tree-file"], Store, &tree_descr);
        ap.refer(&mut nodes_fname).add_option(&["-n", "--nodes-file"], Store, &nodes_descr);
        ap.parse_args_or_exit();
    }
    println!("{}, {}, {}", pid, tree_fname, nodes_fname);
}

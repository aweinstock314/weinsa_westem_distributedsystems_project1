extern crate argparse;
extern crate futures;
#[macro_use] extern crate nom;
extern crate tokio_core;

use argparse::{ArgumentParser, Store};
use futures::Future;
use futures::stream::Stream;
use nom::IResult;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::net::{IpAddr, SocketAddr};
use std::str;
use std::str::FromStr;
use tokio_core::io::{read_to_end, write_all};
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Core;

type Pid = usize;
type Topology = Vec<(Pid, Pid)>;
type Nodes = HashMap<Pid, SocketAddr>;
type Peers = HashMap<Pid, TcpStream>;

named!(parse_usize<&[u8], usize>, map_res!(map_res!(nom::digit, str::from_utf8), str::FromStr::from_str));

fn parse_tree(input: &[u8]) -> IResult<&[u8], Topology> {
    named!(line<&[u8], (Pid, Pid)>, chain!(tag!("(") ~ src: parse_usize ~ tag!(",") ~ dst: parse_usize ~ tag!(")"), || (src, dst)));
    named!(tree<&[u8], Topology>, separated_list!(is_a!("\r\n"), line));
    tree(input)
}

fn parse_nodes(input: &[u8]) -> IResult<&[u8], Nodes> {
    named!(quoted_string<&[u8], &[u8]>, chain!(tag!("\"") ~ s: is_not!("\"\r\n") ~ tag!("\""), || s));
    named!(quoted_ip<&[u8], IpAddr>, map_res!(map_res!(quoted_string, str::from_utf8), IpAddr::from_str));
    named!(node<&[u8], (Pid, SocketAddr)>, chain!(tag!("(") ~
        pid: parse_usize ~ tag!(",") ~
        ip: quoted_ip ~ tag!(",") ~
        port: parse_usize ~ tag!(")"),
        || { (pid, SocketAddr::new(ip, port as u16)) }));
    named!(nodes<&[u8], Vec<(Pid, SocketAddr)> >, separated_list!(is_a!("\r\n"), node));

    nodes(input).map(|nodes: Vec<(Pid, SocketAddr)>| { nodes.into_iter().collect::<Nodes>() })
}

// Can't get the types to line up right, too general for now
/*fn run_parser<R: Read+Send, A: Send, E: Send, F: FnMut(&[u8]) -> IResult<&[u8], A, E>>(reader: R, parser: F) -> Box<Future<Item=A, Error=Box<Error+Send>>> {
    let buf_future: Box<Future<Item=A, Error=Box<Error+Send>>> = read_to_end(reader, Vec::new()).map_err(Box::new).boxed();
    buf_future.and_then(|(_, buf)| {
        match parser(&buf) {
            IResult::Done(_, o) => futures::finished(o).boxed(),
            IResult::Error(_) => futures::failed("Parse error".into()).boxed(),
            IResult::Incomplete(_) => futures::failed("Incomplete parse".into()).boxed(),
        }
    }).boxed()
}*/

fn run_parser_on_file<A, F: Fn(&[u8]) -> IResult<&[u8], A>>(filename: &str, parser: F) -> Result<A, Box<Error>> {
    let mut file = BufReader::new(try!(File::open(filename)));
    let mut buf = vec![];
    try!(file.read_to_end(&mut buf));
    match parser(&buf) {
        IResult::Done(_, o) => Ok(o),
        IResult::Error(_) => Err("Parse error".into()),
        IResult::Incomplete(_) => Err("Incomplete parse".into()),
    }
}

fn main() {
    let mut pid: Pid = 0;
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
    let topology = run_parser_on_file(&tree_fname, parse_tree).expect(&format!("Couldn't parse {}", tree_fname));
    println!("topology: {:?}", topology);
    let nodes = run_parser_on_file(&nodes_fname, parse_nodes).expect(&format!("Couldn't parse {}", nodes_fname));
    println!("nodes: {:?}", nodes);
    let own_addr = nodes.get(&pid).expect(&format!("Couldn't find an entry for pid {} in {} ({:?})", pid, nodes_fname, nodes));
    println!("own_addr: {:?}", own_addr);

    let mut core = Core::new().expect("Failed to initialize event loop.");
    let listener = TcpListener::bind(&own_addr, &core.handle()).expect("Failed to bind listener.");
    let server = {
        let handle = core.handle();
        listener.incoming().for_each(move |(sock, addr)| {
            handle.spawn(write_all(sock, b"Hello, world!\n").map(|_| ()).map_err(|_| ()));
            Ok(())
        })
    };
    core.run(server).expect("Failed to run event loop.");
}

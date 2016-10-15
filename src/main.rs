#![feature(proc_macro)]
pub extern crate argparse;
pub extern crate bincode;
pub extern crate byteorder;
#[macro_use] pub extern crate futures;
#[macro_use] pub extern crate lazy_static;
#[macro_use] pub extern crate nom;
#[macro_use] pub extern crate serde_derive;
pub extern crate serde_json;
pub extern crate tokio_core;

pub use argparse::{ArgumentParser, Store};
pub use bincode::SizeLimit;
pub use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
pub use futures::stream::Stream;
pub use futures::{Async, Future, Poll};
pub use nom::IResult;
pub use std::collections::{HashMap, HashSet, VecDeque};
pub use std::error::Error;
pub use std::fs::File;
pub use std::io::BufReader;
pub use std::io::prelude::*;
pub use std::net::{IpAddr, SocketAddr};
pub use std::str::FromStr;
pub use std::sync::Mutex;
pub use std::{fmt, io, mem, str};
pub use tokio_core::io::{FramedIo, Io, ReadHalf, WriteHalf};
pub use tokio_core::net::{TcpListener, TcpStream};
pub use tokio_core::reactor::Core;

pub type Pid = usize;
pub type Topology = Vec<(Pid, Pid)>;
pub type Nodes = HashMap<Pid, SocketAddr>;
pub type Peers = HashMap<Pid, TcpStream>;

pub mod framing_helpers;
pub mod mutexalgo;
pub mod parsers;
pub use framing_helpers::*;
pub use mutexalgo::*;
pub use parsers::*;

#[derive(Debug)]
struct ApplicationState {
    files: HashMap<String, RaymondState<String>>, // fname -> contents
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ApplicationMessage {
    fname: String,
    ty: ApplicationMessageType,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum ApplicationMessageType {
    CreateFile,
    Raymond(RaymondMessage<String>),
    DeleteFile, // should only be issued after a lock is held
}

// TODO: generalize to arbitrary serde types and move to framing_helpers
struct ApplicationMessageReader(LengthPrefixedReader<TcpStream>);
struct ApplicationMessageWriter(LengthPrefixedWriter<TcpStream>);

impl FramedIo for ApplicationMessageReader {
    type In = ApplicationMessage;
    type Out = ApplicationMessage;
    fn poll_read(&mut self) -> Async<()> {
        self.0.poll_read()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        let buf = try_ready!(self.0.read());
        if cfg!(feature="use_bincode") {
            bincode::serde::deserialize(&buf)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .map(|r| Async::Ready(r))
        } else {
            serde_json::from_str(&String::from_utf8_lossy(&buf))
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .map(|r| Async::Ready(r))
        }
    }
    fn poll_write(&mut self) -> Async<()> {
        panic!("poll_write on ApplicationMessageReader");
    }
    fn write(&mut self, _: Self::In) -> Poll<(), io::Error> {
        panic!("write on ApplicationMessageReader");
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        panic!("flush on ApplicationMessageReader");
    }
}

impl FramedIo for ApplicationMessageWriter {
    type In = ApplicationMessage;
    type Out = ApplicationMessage;
    fn poll_read(&mut self) -> Async<()> {
        panic!("poll_read on ApplicationMessageWriter");
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        panic!("poll_write on ApplicationMessageWriter");
    }
    fn poll_write(&mut self) -> Async<()> {
        self.0.poll_write()
    }
    fn write(&mut self, req: Self::In) -> Poll<(), io::Error> {
        if cfg!(feature="use_bincode") {
            let buf = try!(
                bincode::serde::serialize(&req, bincode::SizeLimit::Infinite)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
            self.0.write(buf)
        } else {
            let buf = try!(
                serde_json::to_string(&req)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
            self.0.write(buf.as_bytes().into())
        }
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        self.0.flush()
    }
}

fn get_neighbors(topology: &Topology, pid: Pid) -> HashSet<Pid> {
    topology.iter()
        .filter(|&&(p1,p2)| { p1 == pid || p2 == pid }) // find entries containing our own pid (symmetric since tree.txt is undirected)
        .map(|&(p1,p2)| { if p1 == pid { p2 } else { p1 } }) // in each edge, take the vertex which isn't us
        .collect()
}

lazy_static! {
    static ref APPSTATE: Mutex<ApplicationState> = Mutex::new(ApplicationState {
        files: HashMap::new(),
    });
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

    // raw pairs of (pid,pid) edges
    let topology = run_parser_on_file(&tree_fname, parse_tree).expect(&format!("Couldn't parse {}", tree_fname));
    println!("topology: {:?}", topology);
    // set of pids which are our neighbors
    let own_neighbors = get_neighbors(&topology, pid);
    println!("{}'s neighbors: {:?}", pid, own_neighbors);

    // (pid -> ip) mapping
    let nodes = run_parser_on_file(&nodes_fname, parse_nodes).expect(&format!("Couldn't parse {}", nodes_fname));
    println!("nodes: {:?}", nodes);
    // (ip -> pid) mapping
    let nodes_rev: HashMap<SocketAddr, Pid> = nodes.iter().map(|(&k,&v)| (v,k)).collect();
    println!("nodes_rev: {:?}", nodes_rev);

    let own_addr = nodes.get(&pid).expect(&format!("Couldn't find an entry for pid {} in {} ({:?})", pid, nodes_fname, nodes));
    println!("own_addr: {:?}", own_addr);

    let mut core = Core::new().expect("Failed to initialize event loop.");
    let listener = TcpListener::bind(&own_addr, &core.handle()).expect("Failed to bind listener.");
    let server = {
        let handle = core.handle();
        listener.incoming().for_each(move |(sock, peer_addr)| {
            println!("Incoming message from {:?}", peer_addr);
            if let Some(&peer_pid) = nodes_rev.get(&peer_addr) {
                println!("peer_pid: {}", peer_pid);
                if own_neighbors.contains(&peer_pid) {
                    println!("{} is our neighbor", peer_pid);
                    let rw = futures::lazy(move || {
                        futures::finished(sock.split())
                    });
                    let rw = rw.map(|(r, w)| (
                        ApplicationMessageReader(LengthPrefixedReader::new(r, SizeLimit::Bounded(0x10000))),
                        ApplicationMessageWriter(LengthPrefixedWriter::new(w))
                    ));
                    let neighbors = own_neighbors.clone();
                    let todo = rw.and_then(move |(r, w)| {
                        let reader = read_frame(r).and_then(move |(_, msg)| {
                            println!("Received {:?} from {}", msg, peer_pid);
                            let mut appstate = APPSTATE.lock().expect("Failed to acquire APPSTATE lock.");
                            println!("application state before: {:#?}", *appstate);
                            match msg.ty {
                                ApplicationMessageType::CreateFile => {
                                    // TODO: check for existence, report warnings
                                    appstate.files.insert(msg.fname, RaymondState::new(peer_pid));
                                },
                                ApplicationMessageType::Raymond(raymsg) => {
                                    if let Some(mut raystate) = appstate.files.get_mut(&msg.fname) {
                                        let pc = PeerContext { selfpid: pid, peerpid: peer_pid, neighbors: &neighbors };
                                        let tosend = raystate.handle_message(&pc, raymsg);
                                        println!("tosend: {:?}", tosend);
                                        // TODO: sending things
                                        // store an MPSC sender in the global appstate?
                                    } else {
                                        println!("warning: got a raymond message for a nonexistant file: {}", msg.fname);
                                    }
                                },
                                ApplicationMessageType::DeleteFile => {
                                    //TODO: implement
                                    ()
                                },
                            }
                            println!("application state after: {:#?}", *appstate);
                            Ok(())
                        });
                        reader
                    });
                    handle.spawn(todo.map(|_| ()).map_err(|e| { println!("an error occurred: {:?}", e); }));
                } else {
                    println!("warning: contacted by a non-neighbor: {}", peer_pid);
                }
            } else {
                println!("warning: contacted by a node outside the network: {:?}", peer_addr);
            }
            Ok(())
        })
    };
    core.run(server).expect("Failed to run event loop.");
}

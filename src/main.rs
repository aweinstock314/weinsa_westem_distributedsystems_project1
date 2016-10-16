#![feature(conservative_impl_trait)]
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
pub use std::sync::{Mutex, MutexGuard};
pub use std::{fmt, io, mem, net, str, thread};
pub use tokio_core::io::{FramedIo, Io, ReadHalf, WriteHalf};
pub use tokio_core::net::{TcpListener, TcpStream};
pub use tokio_core::reactor::{Core, Handle};

pub type Pid = usize;
pub type Topology = Vec<(Pid, Pid)>;
pub type Nodes = HashMap<Pid, (SocketAddr, u16)>;
pub type Peers = HashMap<Pid, TcpStream>;

pub mod framing_helpers;
pub mod mutexalgo;
pub mod parsers;
pub use framing_helpers::*;
pub use mutexalgo::*;
pub use parsers::*;

struct ApplicationState {
    files: HashMap<String, RaymondState<String>>, // fname -> contents
    cached_peers: HashMap<Pid, futures::stream::Sender<ApplicationMessage, io::Error>>,
    nodes: Nodes,
}

impl ApplicationState {
    fn cache_peer(&mut self, pid: Pid, sender: futures::stream::Sender<ApplicationMessage, io::Error>) {
        self.cached_peers.entry(pid).or_insert(sender);
    }
    /*fn send_message(&mut self, pid: Pid, msg: ApplicationMessage, h: &Handle) -> impl Future<Item=(), Error=io::Error> {
        println!("ApplicationState::send_message: trying to send {:?}", msg);
        if let None = self.cached_peers.get(&pid) {
            let addr = self.nodes.get(&pid).expect(&format!("Tried to contact pid {}, but they don't have a nodes.txt entry (nodes: {:?})", pid, self.nodes));
            println!("ApplicationState::send_message: trying to contact {:?}", addr);
            let sock = TcpStream::connect(&addr.0, h);
            let rw = sock.and_then(move |sock| { split_sock(sock) });
            let r_sender_writer = rw.and_then(|(r,w)| {
                let (sender, writer) = make_stream_writer(w);
                Ok((r,sender, writer))
            }).wait();
            if let Ok((r,sender,writer)) = r_sender_writer {
                println!("tcp connection happened");
            } else {
                println!("tcp connection didn't happen");
            }
        }
        // TODO: 1) address the case where we have a connection cached 2) maybe cache the reader? start a readloop? 3) actually send the message
        futures::lazy(|| Ok(()))
    }*/
    fn send_message_sync(&mut self, pid: Pid, msg: ApplicationMessage) -> Result<(), io::Error> {
        println!("ApplicationState::send_message_sync: trying to send {:?} to {}", msg, pid);
        let addr = self.nodes.get(&pid).expect(&format!("Tried to contact pid {}, but they don't have a nodes.txt entry (nodes: {:?})", pid, self.nodes));
        let mut sock = try!(net::TcpStream::connect(addr.0));
        let serialized_message = try!(serde_json::to_string(&msg).map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
        let mut buf = vec![];
        try!(buf.write_u64::<LittleEndian>(serialized_message.len() as u64));
        buf.extend_from_slice(serialized_message.as_bytes());
        try!(sock.write_all(&buf));
        Ok(())
    }
}

impl fmt::Debug for ApplicationState {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ApplicationState")
            .field("files", &self.files)
            .field("cached_peers", &self.cached_peers.iter().map(|(&k, _)| k).collect::<HashSet<Pid>>())
            .finish()
    }
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
        cached_peers: HashMap::new(),
        nodes: HashMap::new(),
    });
}

fn get_appstate<'a>() -> MutexGuard<'a, ApplicationState> {
    APPSTATE.lock().expect("Failed to acquire APPSTATE lock.")
}

// TODO: dream up better stream combinators?
struct StreamWriter<W: FramedIo> {
    writer: W,
    receiver: futures::stream::Receiver<W::In, io::Error>,
    queue: VecDeque<W::In>,
}
impl<W: FramedIo> Stream for StreamWriter<W> where W::In: fmt::Debug {
    type Item = ();
    type Error = io::Error;
    fn poll(&mut self) -> Poll<Option<()>, io::Error> {
        println!("In StreamWriter::poll");
        while let Async::Ready(Some(msg)) = try!(self.receiver.poll()) {
            println!("\tPushing {:?}", msg);
            self.queue.push_back(msg);
        }
        if !self.queue.is_empty() {
            println!("\tAbout to poll_write");
            if let Async::NotReady = self.writer.poll_write() {
                println!("\t\tpoll_write returned Async::NotReady");
            } else {
                println!("\t\tpoll_write returned Async::Ready");
                for msg in self.queue.drain(..) {
                    println!("About to write {:?}", msg);
                    try_ready!(self.writer.write(msg));
                }
                println!("Returning from StreamWriter::poll");
            }
        }
        Ok(Async::NotReady) // deliberate infinite loop/yield
    }
}
fn make_stream_writer<W: FramedIo>(w: W) -> (futures::stream::Sender<W::In, io::Error>, StreamWriter<W>) {
    let (send, recv) = futures::stream::channel::<W::In, io::Error>();
    let sw = StreamWriter {
        writer: w,
        receiver: recv,
        queue: VecDeque::new(),
    };
    (send, sw)
}

fn split_sock(sock: TcpStream) -> impl Future<Item=(ApplicationMessageReader, ApplicationMessageWriter), Error=io::Error> {
    let rw = futures::lazy(move || {
        futures::finished(sock.split())
    });
    let rw = rw.map(|(r, w)| (
        ApplicationMessageReader(LengthPrefixedReader::new(r, SizeLimit::Bounded(0x10000))),
        ApplicationMessageWriter(LengthPrefixedWriter::new(w))
    ));
    rw
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
    let nodes_rev: HashMap<SocketAddr, Pid> = nodes.iter().map(|(&k,&v)| (v.0,k)).collect();
    println!("nodes_rev: {:?}", nodes_rev);

    let own_addr = nodes.get(&pid).expect(&format!("Couldn't find an entry for pid {} in {} ({:?})", pid, nodes_fname, nodes));
    println!("own_addr: {:?}", own_addr);

    let mut core = Core::new().expect("Failed to initialize event loop.");

    {
        let mut appstate = get_appstate();
        appstate.nodes = nodes.clone();
    }

    handle_clis_in_seperate_thread(own_addr.1);

    let listener = TcpListener::bind(&own_addr.0, &core.handle()).expect("Failed to bind listener.");
    let server = {
        let handle = core.handle();
        listener.incoming().for_each(move |(sock, peer_addr)| {
            println!("Incoming message from {:?}", peer_addr);
            if let Some(&peer_pid) = nodes_rev.get(&peer_addr) {
                println!("peer_pid: {}", peer_pid);
                if own_neighbors.contains(&peer_pid) {
                    println!("{} is our neighbor", peer_pid);
                    let rw = split_sock(sock);
                    let neighbors = own_neighbors.clone();
                    let todo = rw.and_then(move |(r, w)| {
                        let (sender, writer) = make_stream_writer(w);
                        let writer = writer.for_each(|()| Ok(())).map_err(|e| {
                            println!("An error occurred during the exection of writer: {:?}", e);
                            io::Error::new(io::ErrorKind::Other, "writer failed")
                        });
                        let reader = read_frame(r).and_then(move |(_, msg)| {
                            println!("Received {:?} from {}", msg, peer_pid);
                            let mut appstate = get_appstate();
                            println!("application state before: {:#?}", *appstate);
                            appstate.cache_peer(peer_pid, sender);
                            match msg.ty {
                                ApplicationMessageType::CreateFile => {
                                    let newstate = move || { RaymondState::new(peer_pid, pid) };
                                    if let Some(oldstate) = appstate.files.insert(msg.fname.clone(), newstate()) {
                                        println!("warning: created a file that already existed (new: {:?}, old {:?})", newstate(), oldstate);
                                    }
                                    for neighbor in neighbors {
                                        if neighbor != peer_pid {
                                            if let Err(e) = appstate.send_message_sync(neighbor, msg.clone()) {
                                                println!("warning: error in CreateFile propagation: {:?}", e);
                                            }
                                        }
                                    }
                                },
                                ApplicationMessageType::Raymond(raymsg) => {
                                    let mut tosend = if let Some(mut raystate) = appstate.files.get_mut(&msg.fname) {
                                        let pc = PeerContext { selfpid: pid, peerpid: peer_pid, neighbors: &neighbors };
                                        raystate.handle_message(&pc, raymsg)
                                    } else {
                                        println!("warning: got a raymond message for a nonexistant file: {}", msg.fname);
                                        vec![]
                                    };
                                    println!("tosend: {:?}", tosend);
                                    for (neighbor, raymsg) in tosend.drain(..) {
                                        let appmsg = ApplicationMessage {
                                            fname: msg.fname.clone(),
                                            ty: ApplicationMessageType::Raymond(raymsg),
                                        };
                                        if let Err(e) = appstate.send_message_sync(neighbor, appmsg) {
                                            println!("error in Raymond sending: {:?}", e);
                                        }
                                    }
                                },
                                ApplicationMessageType::DeleteFile => {
                                    match appstate.files.remove(&msg.fname) {
                                        Some(x) => println!("Deleted {:?}", x),
                                        None => println!("warning: tried to delete a nonexistant file: {}", msg.fname),
                                    }
                                    // TODO: deduplicate w.r.t. CreateFile
                                    for neighbor in neighbors {
                                        if neighbor != peer_pid {
                                            if let Err(e) = appstate.send_message_sync(neighbor, msg.clone()) {
                                                println!("warning: error in DeleteFile propagation: {:?}", e);
                                            }
                                        }
                                    }
                                },
                            }
                            println!("application state after: {:#?}", *appstate);
                            Ok(())
                        });
                        reader.select(writer).map_err(|(e,_)| e)
                    });
                    handle.spawn(todo.map(|_| ()).map_err(|e| { println!("An error occurred: {:?}", e); }));
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

fn handle_clis_in_seperate_thread(port: u16) {
    thread::spawn(move || {
        for sock in net::TcpListener::bind(("0.0.0.0", port)).expect("Failed to bind CLI listener.").incoming() {
            if let Ok(sock) = sock {
                let addr = sock.peer_addr();
                println!("Got a CLI client: {:?}", addr);
                thread::spawn(move || {
                    let mut reader = BufReader::new(&sock);
                    loop {
                        let mut line = "".into();
                        if let Ok(_) = reader.read_line(&mut line) {
                            println!("Got line from {:?}: {}", addr, line);
                            if let Err(_) = reader.get_mut().write_all(format!("echoing: {}", line).as_bytes()) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                });
            }
        }
    });
}

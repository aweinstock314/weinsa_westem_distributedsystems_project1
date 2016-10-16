#![feature(conservative_impl_trait)]
#![feature(proc_macro)]

/// The main program
/// This is the file that runs everything
/// Currently it does the following:
///     1. Read in config files (tree.txt, nodes.txt)
///     2. Boots up a CLI interface on a socket
///     3. Boots up a tcp server to communicate between nodes

//external imports
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
pub use std::sync::{mpsc, Mutex, MutexGuard};
pub use std::{fmt, io, mem, net, str, thread};
pub use std::iter::Iterator;
pub use tokio_core::io::{FramedIo, Io, ReadHalf, WriteHalf};
pub use tokio_core::net::{TcpListener, TcpStream};
pub use tokio_core::reactor::{Core, Handle};

//typedefs
pub type Pid = usize;
pub type Topology = Vec<(Pid, Pid)>;
pub type Nodes = HashMap<Pid, (SocketAddr, u16)>;
pub type Peers = HashMap<Pid, TcpStream>;

//local imports
pub mod framing_helpers;
pub mod mutexalgo;
pub mod parsers;
pub use framing_helpers::*;
pub use mutexalgo::*;
pub use parsers::*;

// ApplicationState
// Global state of the program. Mutexed
// files: Map of name => RaymondState of all created Resources
// cached_peers: Currently unused
// nodes: A mapping of all pids -> (comm_socket, cli_port)
struct ApplicationState {
    files: HashMap<String, RaymondState<String>>,
    cached_peers: HashMap<Pid, futures::stream::Sender<ApplicationMessage, io::Error>>,
    nodes: Nodes,
    neighbors: HashSet<Pid>,
    ourpid: Pid,
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
    fn send_message_sync(&self, pid: Pid, msg: ApplicationMessage) -> Result<(), io::Error> {
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
    sender: Pid,
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
        neighbors: HashSet::new(),
        ourpid: 0,
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
        appstate.neighbors = own_neighbors.clone();
        appstate.ourpid = pid;
    }

    handle_clis_in_seperate_thread(pid, own_addr.1);

    let listener = TcpListener::bind(&own_addr.0, &core.handle()).expect("Failed to bind listener.");
    let server = {
        let handle = core.handle();
        listener.incoming().for_each(move |(sock, peer_addr)| {
            println!("Incoming message from {:?}", peer_addr);
            let rw = split_sock(sock);
            let neighbors = own_neighbors.clone();
            let todo = rw.and_then(move |(r, w)| {
                let (sender, writer) = make_stream_writer(w);
                let writer = writer.for_each(|()| Ok(())).map_err(|e| {
                    println!("An error occurred during the exection of writer: {:?}", e);
                    io::Error::new(io::ErrorKind::Other, "writer failed")
                });
                let reader = read_frame(r).and_then(move |(_, msg)| {
                    let peer_pid = msg.sender;
                    println!("Received {:?} from {}", msg, peer_pid);
                    let mut appstate = get_appstate();
                    println!("application state before: {:#?}", *appstate);
                    appstate.cache_peer(peer_pid, sender);
                    match msg.ty {
                        ApplicationMessageType::CreateFile => {
                            let newstate = move || {
                                let mut st = RaymondState::new(peer_pid, pid);
                                st.resource = Some("".into());
                                st
                            };
                            if let Some(oldstate) = appstate.files.insert(msg.fname.clone(), newstate()) {
                                println!("warning: created a file that already existed (new: {:?}, old {:?})", newstate(), oldstate);
                            } else {
                                for neighbor in neighbors {
                                    if neighbor != peer_pid {
                                        let mut newmsg = msg.clone();
                                        newmsg.sender = pid;
                                        if let Err(e) = appstate.send_message_sync(neighbor, newmsg) {
                                            println!("warning: error in CreateFile propagation: {:?}", e);
                                        }
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
                                    sender: pid,
                                    ty: ApplicationMessageType::Raymond(raymsg),
                                };
                                if let Err(e) = appstate.send_message_sync(neighbor, appmsg) {
                                    println!("error in Raymond sending: {:?}", e);
                                }
                            }
                        },
                        ApplicationMessageType::DeleteFile => {
                            if let Some(x) = appstate.files.remove(&msg.fname) {
                                println!("Deleted {:?}", x);
                                for neighbor in neighbors {
                                    if neighbor != peer_pid {
                                        let mut newmsg = msg.clone();
                                        newmsg.sender = pid;
                                        if let Err(e) = appstate.send_message_sync(neighbor, newmsg) {
                                            println!("warning: error in DeleteFile propagation: {:?}", e);
                                        }
                                    }
                                }
                            } else {
                                println!("warning: tried to delete a nonexistant file: {}", msg.fname);
                            }
                        },
                    }
                    println!("application state after: {:#?}", *appstate);
                    Ok(())
                });
                reader.select(writer).map_err(|(e,_)| e)
            });
            handle.spawn(todo.map(|_| ()).map_err(|e| { println!("An error occurred: {:?}", e); }));
            Ok(())
        })
    };
    core.run(server).expect("Failed to run event loop.");
}

fn create(args: Vec<&str>, cli_out: &mut net::TcpStream) {
    cli_out.write_all(format!("Called create function.\n").as_bytes()).unwrap();
    if args.len() != 1 {
        cli_out.write_all(format!("Incorrect number of args: create res_name\n").as_bytes()).unwrap();
        return;
    }
    let res_name: &str = args[0];
    let appstate = get_appstate();
    let ourpid = appstate.ourpid;
    if let Some(_) = appstate.files.get(res_name) {
        cli_out.write_all(format!("Cannot create resource {}; already exists!\n", res_name).as_bytes()).unwrap();
    } else {
        cli_out.write_all(format!("Can create!\n").as_bytes()).unwrap();
        let appmsg = ApplicationMessage {
            fname: res_name.into(),
            sender: ourpid,
            ty: ApplicationMessageType::CreateFile,
        };
        appstate.send_message_sync(ourpid, appmsg).unwrap();
    }
}

fn delete(args: Vec<&str>, cli_out: &mut net::TcpStream) {
    cli_out.write_all(format!("Called delete function.\n").as_bytes()).unwrap();
    if args.len() != 1 {
        cli_out.write_all(format!("Incorrect number of args: delete res_name\n").as_bytes()).unwrap();
        return;
    }
    let res_name: &str = args[0];
    let appstate = get_appstate();
    let ourpid = appstate.ourpid;
    if let Some(_) = appstate.files.get(res_name) {
        cli_out.write_all(format!("Can delete!\n").as_bytes()).unwrap();
        // TODO: maybe acquire a lock first?
        let appmsg = ApplicationMessage {
            fname: res_name.into(),
            sender: ourpid,
            ty: ApplicationMessageType::DeleteFile,
        };
        appstate.send_message_sync(ourpid, appmsg).unwrap();
    } else {
        cli_out.write_all(format!("Cannot delete resource {}; doesn't exist!\n", res_name).as_bytes()).unwrap();
    }
}

fn read(args: Vec<&str>, cli_out: &mut net::TcpStream) {
    cli_out.write_all(format!("Called read function.\n").as_bytes()).unwrap();
    if args.len() != 1 {
        cli_out.write_all(format!("Incorrect number of args: read res_name\n").as_bytes()).unwrap();
        return;
    }
    let res_name: &str = args[0];
    let mut appstate = get_appstate();
    let ourpid = appstate.ourpid;
    if let Some((rchan, mut tosend)) = if let Some(raystate) = appstate.files.get_mut(res_name) {
        cli_out.write_all(format!("Attempting to read the resource:\n").as_bytes()).unwrap();
        Some(raystate.request())
    } else {
        cli_out.write_all(format!("Cannot read resource {}; doesn't exist!\n", res_name).as_bytes()).unwrap();
        None
    } {
        for (pid, raymsg) in tosend.drain(..) {
            let appmsg = ApplicationMessage {
                fname: res_name.into(),
                sender: ourpid,
                ty: ApplicationMessageType::Raymond(raymsg),
            };
            appstate.send_message_sync(pid, appmsg).unwrap();
        }
        let resource = rchan.recv().unwrap();
        println!("got resource: {}", resource);
        cli_out.write_all(format!("Contents of resource {:?}: {}\n", res_name, resource).as_bytes()).unwrap();
        let mut tosend = appstate.files.get_mut(res_name).unwrap().release();
        for (pid, raymsg) in tosend.drain(..) {
            let appmsg = ApplicationMessage {
                fname: res_name.into(),
                sender: ourpid,
                ty: ApplicationMessageType::Raymond(raymsg),
            };
            appstate.send_message_sync(pid, appmsg).unwrap();
        }
    }
    println!("Done with read lock");
}


fn append(args: Vec<&str>, cli_out: &mut net::TcpStream) {
    cli_out.write_all(format!("Called append function.\n").as_bytes()).unwrap();
    if args.len() != 1 {
        cli_out.write_all(format!("Incorrect number of args: append res_name data\n").as_bytes()).unwrap();
        return;
    }
    let res_name: &str = args[0];
    let mut appstate = get_appstate();
    if let Some((rchan, mut tosend)) = if let Some(raystate) = appstate.files.get_mut(res_name) {
        cli_out.write_all(format!("Can append!\n").as_bytes()).unwrap();
        Some(raystate.request())
    } else {
        cli_out.write_all(format!("Cannot append resource {}; doesn't exist!\n", res_name).as_bytes()).unwrap();
        None
    } {
        for (pid, raymsg) in tosend.drain(..) {
            let appmsg = ApplicationMessage {
                fname: res_name.into(),
                sender: pid,
                ty: ApplicationMessageType::Raymond(raymsg),
            };
            appstate.send_message_sync(pid, appmsg).unwrap();
        }
        let resource = rchan.recv().unwrap();
    }
}

fn ls(args: Vec<&str>, cli_out: &mut net::TcpStream) {
    if args.len() != 0 {
        cli_out.write_all(format!("Incorrect number of args: ls\n").as_bytes()).unwrap();
        return;
    }
    let appstate = get_appstate();
    if appstate.files.len() == 0 {
        cli_out.write_all(format!("No Active Resources Found.\n").as_bytes()).unwrap();
        return;
    }
    cli_out.write_all(format!("Listing of Active Resources:\n").as_bytes()).unwrap();
    cli_out.write_all(format!("{:<8} {:<6}\n", "Name", "Holder\n").as_bytes()).unwrap();
    for (name, filestate) in &appstate.files {
        cli_out.write_all(format!("{:<8} {:<6}\n", name, filestate.holder).as_bytes()).unwrap();
    }
}

fn handle_clis_in_seperate_thread(ourpid: Pid, port: u16) {
    thread::spawn(move || {
        let listener = net::TcpListener::bind(("0.0.0.0", port)).expect("Failed to bind CLI listener.");
        println!("Management CLI bound to port {}", port);
        for sock in listener.incoming() {
            if let Ok(mut sock) = sock {
                let addr = sock.peer_addr();
                println!("Got a CLI client: {:?}", addr);
                let options = concat!("Available commands:\n",
                    "\tcreate res_name\n",
                    "\tdelete res_name\n",
                    "\tread res_name\n",
                    "\tappend res_name data\n",
                    "\tls\n"
                );
                if let Err(_) = sock.write_all(format!("Welcome to node {}'s management CLI\n{}", ourpid, options).as_bytes()) {
                    return;
                }
                thread::spawn(move || {
                    let mut reader = BufReader::new(sock);
                    loop {
                        reader.get_mut().write_all(("--> ").as_bytes()).unwrap();
                        let mut line = "".into();
                        if let Ok(_) = reader.read_line(&mut line) {
                            println!("Got line from {:?}: {}", addr, line);
                            if let Err(_) = reader.get_mut().write_all(format!("echoing: {}\n", line).as_bytes()) {
                                break;
                            }
                            let mut iter = line.split_whitespace();
                            match iter.next() {
                                Some("create") => create(iter.collect(), reader.get_mut()),
                                Some("delete") => delete(iter.collect(), reader.get_mut()),
                                Some("read") => read(iter.collect(), reader.get_mut()),
                                Some("append") => append(iter.collect(), reader.get_mut()),
                                Some("ls") => ls(iter.collect(), reader.get_mut()),
                                Some(x) => {
                                    reader.get_mut().write_all(format!("Invalid command {}\n", x).as_bytes()).unwrap();
                                },
                                None => continue,
                            };
                        } else {
                            break;
                        }
                    }
                });
            }
        }
    });
}

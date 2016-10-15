#![feature(rustc_macro)]
extern crate argparse;
extern crate bincode;
extern crate byteorder;
#[macro_use] extern crate futures;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate nom;
#[macro_use] extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;

use argparse::{ArgumentParser, Store};
use bincode::SizeLimit;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use futures::stream::Stream;
use futures::{Async, Future, Poll};
use nom::IResult;
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Mutex;
use std::{fmt, io, mem, str};
use tokio_core::io::{FramedIo, Io, ReadHalf, WriteHalf};
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

trait MutexAlgorithm<Resource, Message, E> {
    fn request(&mut self) -> (Box<Future<Item=Resource, Error=E>>, Vec<(Pid, Message)>);
    fn release(&mut self) -> Vec<(Pid, Message)>;
    fn handle_message(&mut self, &PeerContext, Message) -> Vec<(Pid, Message)>;
}

struct RaymondState<Resource> {
    using_resource: bool,
    holder: Pid,
    requests: VecDeque<Pid>, // enqueue with push_back, dequeue with pop_front
    asked: bool,
    resolvers: Vec<futures::Complete<Resource>>
}

impl<Resource> RaymondState<Resource> {
    fn new(holder: Pid) -> RaymondState<Resource> {
        RaymondState {
            using_resource: false,
            holder: holder,
            requests: VecDeque::new(),
            asked: false,
            resolvers: Vec::new(),
        }
    }
}

// https://github.com/rust-lang/rfcs/blob/master/text/0640-debug-improvements.md#helper-types
impl<Resource: fmt::Debug> fmt::Debug for RaymondState<Resource> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RaymondState")
            .field("using_resource", &self.using_resource)
            .field("holder", &self.holder)
            .field("requests", &self.requests)
            .field("asked", &self.asked)
            .field("resolvers", &self.resolvers.iter().map(|_| ()).collect::<Vec<()>>())
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum RaymondMessage<Resource> { GrantToken(Resource), Request }

#[derive(Debug)]
struct PeerContext<'a> {
    selfpid: Pid,
    peerpid: Pid,
    neighbors: &'a HashSet<Pid>,
}

impl<Resource> MutexAlgorithm<Resource, RaymondMessage<Resource>, ()> for RaymondState<Resource> {
    fn request(&mut self) -> (Box<Future<Item=Resource, Error=()>>, Vec<(Pid, RaymondMessage<Resource>)>) {
        unimplemented!();
    }
    fn release(&mut self) -> Vec<(Pid, RaymondMessage<Resource>)> {
        unimplemented!();
    }
    fn handle_message(&mut self, pc: &PeerContext, msg: RaymondMessage<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
        unimplemented!();
    }
}

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

struct LengthPrefixedFramerState {
    sofar: usize,
    buf: Vec<u8>,
}
struct LengthPrefixedFramer<I> {
    reader: LengthPrefixedReader<I>,
    writer: LengthPrefixedWriter<I>,
}

struct LengthPrefixedReader<I> {
    underlying: ReadHalf<I>,
    readstate: Option<LengthPrefixedFramerState>,
    sizebound: SizeLimit,
}
impl<I> LengthPrefixedReader<I> {
    fn new(i: ReadHalf<I>, bound: SizeLimit) -> Self {
        LengthPrefixedReader {
            underlying: i,
            readstate: None,
            sizebound: bound,
        }
    }
}

struct LengthPrefixedWriter<I> {
    underlying: WriteHalf<I>,
    writestate: Option<LengthPrefixedFramerState>,
}
impl<I> LengthPrefixedWriter<I> {
    fn new(i: WriteHalf<I>) -> Self {
        LengthPrefixedWriter {
            underlying: i,
            writestate: None,
        }
    }
}

impl<I: Io> FramedIo for LengthPrefixedReader<I> {
    type In = Vec<u8>;
    type Out = Vec<u8>;
    fn poll_read(&mut self) -> Async<()> {
        self.underlying.poll_read()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        let mut tmp = mem::replace(&mut self.readstate, None);
        let (restore, result); // borrow checker workaround
        if let Some(ref mut st) = tmp {
            let newbytes = try!(self.underlying.read(&mut st.buf[st.sofar..]));
            st.sofar += newbytes;
            if st.sofar == st.buf.len() {
                restore = false;
                result = Ok(Async::Ready(mem::replace(&mut st.buf, Vec::new())));
            } else {
                restore = true;
                result = Ok(Async::NotReady);
            }
        } else {
            let size = try!(self.underlying.read_u64::<LittleEndian>()) as usize;
            if let SizeLimit::Bounded(maxsize) = self.sizebound {
                if size >= maxsize as usize {
                    println!("warning: in LengthPrefixedReader, received an input of size {} (bound is {:?})", size, self.sizebound);
                    return Err(io::Error::new(io::ErrorKind::Other, "LengthPrefixedReader: bound exceeded"));
                }
            }
            let mut st = LengthPrefixedFramerState { sofar: 0, buf: vec![0u8; size], };
            let newbytes = try!(self.underlying.read(&mut st.buf[st.sofar..]));
            st.sofar += newbytes;
            if st.sofar == st.buf.len() {
                result = Ok(Async::Ready(mem::replace(&mut st.buf, Vec::new())));
            } else {
                result = Ok(Async::NotReady);
                self.readstate = Some(st);
            }
            restore = false;
        }
        if restore {
            self.readstate = tmp;
        }
        result
    }
    fn poll_write(&mut self) -> Async<()> {
        panic!("poll_write on LengthPrefixedReader");
    }
    fn write(&mut self, _: Self::In) -> Poll<(), io::Error> {
        panic!("write on LengthPrefixedReader");
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        panic!("flush on LengthPrefixedReader");
    }
}


impl<I: Io> FramedIo for LengthPrefixedWriter<I> {
    type In = Vec<u8>;
    type Out = Vec<u8>;
    fn poll_read(&mut self) -> Async<()> {
        panic!("poll_read on LengthPrefixedWriter");
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        panic!("read on LengthPrefixedWriter");
    }
    fn poll_write(&mut self) -> Async<()> {
        self.underlying.poll_write()
    }
    fn write(&mut self, req: Self::In) -> Poll<(), io::Error> {
        let mut tmp = mem::replace(&mut self.writestate, None);
        let (restore, result); // borrow checker workaround
        if let Some(ref mut st) = tmp {
            // if we're in the middle of a write, do that before starting the new request
            let newbytes = try!(self.underlying.write(&st.buf[st.sofar..]));
            st.sofar += newbytes;
            restore = if st.sofar == st.buf.len() { false } else { true };
            result = Ok(Async::NotReady);
        } else {
            // if we aren't busy, start the write
            try!(self.underlying.write_u64::<LittleEndian>(req.len() as u64));
            let mut st = LengthPrefixedFramerState { sofar: 0, buf: req, };
            let newbytes = try!(self.underlying.write(&st.buf[st.sofar..]));
            st.sofar += newbytes;
            if st.sofar == st.buf.len() {
                result = Ok(Async::Ready(()));
            } else {
                // if we didn't finish the write in one shot, stash the state
                self.writestate = Some(st);
                result = Ok(Async::NotReady);
            }
            restore = false;
        }
        if restore {
            self.writestate = tmp;
        }
        result
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        self.underlying.flush().map(Async::Ready)
    }
}

impl<I: Io> FramedIo for LengthPrefixedFramer<I> {
    type In = Vec<u8>;
    type Out = Vec<u8>;
    fn poll_read(&mut self) -> Async<()> {
        self.reader.poll_read()
    }
    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        self.reader.read()
    }
    fn poll_write(&mut self) -> Async<()> {
        self.writer.poll_write()
    }
    fn write(&mut self, req: Self::In) -> Poll<(), io::Error> {
        self.writer.write(req)
    }
    fn flush(&mut self) -> Poll<(), io::Error> {
        self.writer.flush()
    }
}

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

struct ReadFrame<I>(Option<I>);
impl<I: FramedIo<In=In, Out=Out>, In, Out> Future for ReadFrame<I> {
    type Item = (I, Out);
    type Error = io::Error;
    fn poll(&mut self) -> Poll<(I, Out), io::Error> {
        let oldself = mem::replace(&mut self.0, None);
        let (res, newself) = if let Some(mut r) = oldself {
            if let Async::NotReady = r.poll_read() {
                (Ok(Async::NotReady), Some(r))
            } else { match r.read() {
                Ok(Async::Ready(x)) => (Ok(Async::Ready((r, x))), None),
                Ok(Async::NotReady) => (Ok(Async::NotReady), Some(r)),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (Ok(Async::NotReady), Some(r)),
                Err(e) => (Err(e), None),
            }}
        } else {
            (Err(io::Error::new(io::ErrorKind::Other, "ReadFrame.0 should never be None")), None)
        };
        self.0 = newself;
        res
    }
}

struct WriteFrame<I, In>(Option<(I, In)>);
impl<I: FramedIo<In=In, Out=Out>, In, Out> Future for WriteFrame<I, In> where In: Clone {
    type Item = I;
    type Error = io::Error;
    fn poll(&mut self) -> Poll<I, io::Error> {
        let oldself = mem::replace(&mut self.0, None);
        let (res, newself) = if let Some((mut w, x)) = oldself {
            if let Async::NotReady = w.poll_write() {
                (Ok(Async::NotReady), Some((w,x)))
            } else { match w.write(x.clone() /* This clone will go away once FramedIo::write returns the original on NotReady */) {
                Ok(Async::Ready(())) => (Ok(Async::Ready(w)), None),
                Ok(Async::NotReady) => (Ok(Async::NotReady), Some((w, x))),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (Ok(Async::NotReady), Some((w, x))),
                Err(e) => (Err(e), None),
            }}
        } else {
            (Err(io::Error::new(io::ErrorKind::Other, "WriteFrame.0 should never be None")), None)
        };
        self.0 = newself;
        res
    }
}


/*trait FramedIoExt: FramedIo {
    fn read_frame(self) -> Box<Future<Item=(Self, Self::Out), Error=io::Error>> where Self: Sized {
        Box::new(ReadFrame(self))
    }
    fn write_frame(self, Self::In) -> Box<Future<Item=Self, Error=io::Error>>;
}*/


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
                        let reader = ReadFrame(Some(r)).and_then(move |(_, msg)| {
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

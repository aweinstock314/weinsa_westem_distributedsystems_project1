use super::*;

#[derive(Debug)]
pub struct PeerContext<'a> {
    pub selfpid: Pid,
    pub peerpid: Pid,
    pub neighbors: &'a HashSet<Pid>,
}

pub trait MutexAlgorithm<Resource, Message, E> {
    fn request(&mut self) -> (Box<Future<Item=Resource, Error=E>>, Vec<(Pid, Message)>);
    fn release(&mut self) -> Vec<(Pid, Message)>;
    fn handle_message(&mut self, &PeerContext, Message) -> Vec<(Pid, Message)>;
}

pub struct RaymondState<Resource> {
    pub using_resource: bool,
    pub holder: Pid,
    pub requests: VecDeque<Pid>, // enqueue with push_back, dequeue with pop_front
    pub asked: bool,
    pub resolvers: Vec<futures::Complete<Resource>>
}

impl<Resource> RaymondState<Resource> {
    pub fn new(holder: Pid) -> RaymondState<Resource> {
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
pub enum RaymondMessage<Resource> { GrantToken(Resource), Request }

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

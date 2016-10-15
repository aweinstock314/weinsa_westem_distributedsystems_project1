use super::*;

#[derive(Debug)]
pub struct PeerContext<'a> {
    pub selfpid: Pid,
    pub peerpid: Pid,
    pub neighbors: &'a HashSet<Pid>,
}

pub trait MutexAlgorithm<Resource, Message, E> {
    fn request(&mut self) -> (Box<Future<Item=Resource, Error=futures::Canceled> + Send>, Vec<(Pid, Message)>);
    fn release(&mut self) -> Vec<(Pid, Message)>;
    fn handle_message(&mut self, &PeerContext, Message) -> Vec<(Pid, Message)>;
}

pub struct RaymondState<Resource> {
    pub resource: Option<Resource>,
    pub selfpid: Pid,
    pub using_resource: bool,
    pub holder: Pid,
    pub requests: VecDeque<Pid>, // enqueue with push_back, dequeue with pop_front
    pub asked: bool,
    pub resolvers: Vec<futures::Complete<Resource>>
}

impl<Resource> RaymondState<Resource> {
    pub fn new(holder: Pid, selfpid: Pid) -> RaymondState<Resource> {
        RaymondState {
            resource: None,
            selfpid: selfpid,
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
            .field("resource", &self.resource)
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


fn assign_token<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    if state.holder == state.selfpid
        && !state.using_resource
        && !state.requests.is_empty() {

        state.holder = state.requests.pop_front().unwrap();
        state.asked = false;
        if state.holder == state.selfpid {
            state.using_resource = true;
            // Using the resource
            for resolver in state.resolvers.drain(..) {
                resolver.complete(state.resource.clone().unwrap());
            }
        } else {
            return vec![(state.holder, RaymondMessage::GrantToken(state.resource.clone().unwrap()))];
        }
    }
    Vec::new()
}

fn send_request<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    if state.holder != state.selfpid
        && !state.requests.is_empty()
        && !state.asked {

        state.asked = true;
        return vec![(state.holder, RaymondMessage::Request)];
    } 
    Vec::new()
}

fn request_token<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    state.requests.push_back(state.selfpid);
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp

}

fn release_token<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    state.using_resource = false;
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp
}

fn receive_request<Resource: Clone>(state: &mut RaymondState<Resource>, inc_pid: Pid) -> Vec<(Pid, RaymondMessage<Resource>)> {
    state.requests.push_back(inc_pid);
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp
}

fn receive_token<Resource: Clone>(state: &mut RaymondState<Resource>, r: Resource) -> Vec<(Pid, RaymondMessage<Resource>)> {
    state.holder = state.selfpid;
    state.resource = Some(r);
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp
}


impl<Resource: Clone + Send + 'static> MutexAlgorithm<Resource, RaymondMessage<Resource>, ()> for RaymondState<Resource> {
    fn request(&mut self) -> (Box<Future<Item=Resource, Error=futures::Canceled> + Send>, Vec<(Pid, RaymondMessage<Resource>)>) {
        let tmp = request_token(self);
        let (complete, oneshot) = futures::oneshot::<Resource>(); 
        self.resolvers.push(complete);
        (Box::new(oneshot), tmp)
    }

    fn release(&mut self) -> Vec<(Pid, RaymondMessage<Resource>)> {
        release_token(self)
    }

    fn handle_message(&mut self, pc: &PeerContext, msg: RaymondMessage<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
        match msg {
            RaymondMessage::GrantToken(r) => receive_token(self,r),
            RaymondMessage::Request => receive_request(self, pc.peerpid),
        }
    }
}

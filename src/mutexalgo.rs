//TODO Make sure the correct version of resource is being used/passed around everywhere
//TODO Make sure using resource is correctly handled
    // i.e. request, receive, use, release (idk how it goes from use to release)
    // currently it looks like it is using resource in assign token, instead of outside in append)
//TODO do resolvers ever need to be removed?

/// Core file containing Raymond's Algorithm
/// The implementation of Raymond's Algorithm
///  simply expects a state/resource pair, then
///  modifies the state appropriately.
/// There is a public abstraction/interface called MutexAlgorithm
///  that requires the functions request, release, & handle_message
///  Our Raymond implements this, so it can be easily swapped out


use super::*;

// PeerContext
// Represents what you currently know about your peers
// selfpid: my own Pid
// peerpid: the pid of the incoming/received message
// neighbors: set of pids that are direct neighbors in the graph
//  Neighbors is necessary to broadcast create/delete messages
#[derive(Debug)]
pub struct PeerContext<'a> {
    pub selfpid: Pid,
    pub peerpid: Pid,
    pub neighbors: &'a HashSet<Pid>,
}

// Note on (Complete, oneshot) and how it works
// Step 1. Generate the pair
// Step 2. Store the complete value
// Step 3. Call the oneshot when you send the msg
// Step 4. Call complete(value) when a response msg is received
// Step 5. Access your complete value; it will be updated


// MutexAlgorithm
// Abstraction/interface for outside programs to use. Requires three functions:
//   They all return a vector of pid/message pairs. This is a listing of all the
//   outgoing messages the function creates to send on the socket
//   (and corresponding pid). Should only be one, except for create/delete messages
// request: call when you want to send a request for the resource
//   Returns a tuple of ((Complete, oneshot), vector)
//   oneshot is the msg to be sent
//   complete is where the result will go once there is an async response
// release: call when you want to release the resource
//   Returns an outgoing msg vector 
// handle_message: Handles incoming requests or token messages
//   Takes in a PeerContext (to get the pid of the msg sender), as well as the msg
//   Returns an outgoing msg vector
pub trait MutexAlgorithm<Resource, Message> {
    fn request(&mut self) -> (mpsc::Receiver<Resource>, Vec<(Pid, Message)>);
    fn release(&mut self) -> Vec<(Pid, Message)>;
    fn handle_message(&mut self, &PeerContext, Message) -> Vec<(Pid, Message)>;
}

// RaymondState
// Self explanatory. As discussed in class.
// Includes the resource: TODO MAKE SURE THIS IS UPDATED appropriately
// Also includes a list of resolvers
//   This is effectively a list of callbacks called when a sent msg receives an async response
//   When the async response happens, the item will have a valid Complete value
pub struct RaymondState<Resource> {
    pub resource: Option<Resource>,
    pub selfpid: Pid,
    pub using_resource: bool,
    pub holder: Pid,
    pub requests: VecDeque<Pid>, // enqueue with push_back, dequeue with pop_front
    pub asked: bool,
    pub resolvers: Vec<mpsc::Sender<Resource>>
}
// Constructor
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
// Debug printer
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

// Messages sent over our algorithm are either:
//   1. Granting a token (with the resource)
//   2. A request for the token
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaymondMessage<Resource> { GrantToken(Resource), Request }


// These next 6 functions merely implement Raymond's algorithm as discussed in class
// Note: They all return a vector of outgoing pid/msg pairs to send
fn assign_token<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    if state.holder == state.selfpid
        && !state.using_resource
        && !state.requests.is_empty() {

        state.holder = state.requests.pop_front().unwrap();
        state.asked = false;
        if state.holder == state.selfpid {
            println!("Assign Token: Using resource.");
            state.using_resource = true;
            // Using the resource
            for resolver in state.resolvers.drain(..) {
                resolver.send(state.resource.clone().unwrap()).unwrap();
            }
        } else {
            println!("Assign Token: Sending off resource.");
            return vec![(state.holder, RaymondMessage::GrantToken(state.resource.clone().unwrap()))];
        }
    }
    Vec::new()
}

fn send_request<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    if state.holder != state.selfpid
        && !state.requests.is_empty()
        && !state.asked {

        println!("Send Request: Sent request.");
        state.asked = true;
        return vec![(state.holder, RaymondMessage::Request)];
    } 
    Vec::new()
}

fn request_token<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    println!("Request Token");
    state.requests.push_back(state.selfpid);
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp

}

fn release_token<Resource: Clone>(state: &mut RaymondState<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
    println!("Release Token");
    state.using_resource = false;
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp
}

fn receive_request<Resource: Clone>(state: &mut RaymondState<Resource>, inc_pid: Pid) -> Vec<(Pid, RaymondMessage<Resource>)> {
    println!("Receive Request");
    state.requests.push_back(inc_pid);
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp
}

fn receive_token<Resource: Clone>(state: &mut RaymondState<Resource>, r: Resource) -> Vec<(Pid, RaymondMessage<Resource>)> {
    println!("Receive Token");
    state.holder = state.selfpid;
    state.resource = Some(r);
    let mut tmp = assign_token(state);
    tmp.extend_from_slice(&send_request(state));
    tmp
}


// Raymond's algorithm implementation of the MutexAlgorithm interface
impl<Resource: Clone + Send + 'static> MutexAlgorithm<Resource, RaymondMessage<Resource>> for RaymondState<Resource> {
    // request the token, stash the futures value in resolvers where the result will eventually go
    // and return the msgs to be sent as well as the oneshot callback
    fn request(&mut self) -> (mpsc::Receiver<Resource>, Vec<(Pid, RaymondMessage<Resource>)>) {
        println!("MutexAlg: Request");
        let (complete, oneshot) = mpsc::channel::<Resource>();
        self.resolvers.push(complete);
        let tmp = request_token(self);
        (oneshot, tmp)
    }
    fn release(&mut self) -> Vec<(Pid, RaymondMessage<Resource>)> {
        println!("MutexAlg: Release");
        release_token(self)
    }
    // handles an incoming request or token msg
    fn handle_message(&mut self, pc: &PeerContext, msg: RaymondMessage<Resource>) -> Vec<(Pid, RaymondMessage<Resource>)> {
        println!("MutexAlg: Handle Message");
        match msg {
            RaymondMessage::GrantToken(r) => receive_token(self,r),
            RaymondMessage::Request => receive_request(self, pc.peerpid),
        }
    }
}

    /*fn send_message(&mut self, pid: Pid, msg: ApplicationMessage, h: &Handle) -> impl Future<Item=(), Error=io::Error> {
        println!("ApplicationState::send_message: trying to send {:?}", msg);
        if let None = self.cached_peers.get(&pid) {
            let addr = self.nodes.get(&pid).expect(&format!("Tried to contact pid {}, but they don't have a nodes.txt entry (nodes: {:?})", pid, self.nodes));
            println!("ApplicationState::send_message: trying to contact {:?}", addr);
            let sock = TcpStream::connect(&addr.0, h);
            let rw = sock.and_then(move |sock| { split_sock(sock) });
            let r_sender_writer = rw.and_then(|(r, w)| {
                let (sender, writer) = make_stream_writer(w);
                Ok((r, sender, writer))
            }).wait();
            if let Ok((r, sender, writer)) = r_sender_writer {
                println!("tcp connection happened");
            } else {
                println!("tcp connection didn't happen");
            }
        }
        // TODO: 1) address the case where we have a connection cached 2) maybe cache the reader? start a readloop? 3) actually send the message
        futures::lazy(|| Ok(()))
    }*/


                    /*let todo = rw.and_then(move |(r, w)| {
                        // test with:
                        // cargo build --release -- 2
                        // python -c 'import struct; import sys; payload = "{\"fname\":\"hello.txt\",\"ty\":\"CreateFile\"}"; sys.stdout.write(struct.pack("<Q", len(payload)) + payload)' | netcat 0 9002 -p 9004
                        let rff = ReadFrame(Some(ApplicationMessageReader(LengthPrefixedReader::new(r, SizeLimit::Bounded(0x10000)))));
                        let read = rff.and_then(move |(_, msg)| {
                            println!("received msg {:?} from {}", msg, peer_pid);
                            Ok(())
                        });
                        let write1 = write_all(w, b"Hello, world!\n").and_then(move |(w,_)| {
                            println!("wrote hello to {}", peer_pid);
                            Ok(w)
                        });
                        let write2 = write1.and_then(|w| {
                            let msg = ApplicationMessage {
                                fname: "hello.txt".into(),
                                ty: ApplicationMessageType::CreateFile,
                            };
                            WriteFrame(Some((ApplicationMessageWriter(LengthPrefixedWriter::new(w)), msg)))
                                .map(|_| { println!("wrote frame"); })
                        });
                        read.join(write2)
                    });*/


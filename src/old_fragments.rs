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


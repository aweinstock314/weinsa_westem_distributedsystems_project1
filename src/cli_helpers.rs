use super::*;

fn create(args: Vec<&str>, cli_out: &mut net::TcpStream) -> io::Result<()> {
    try!(cli_out.write_all(format!("Called create function.\n").as_bytes()));
    if args.len() != 1 {
        try!(cli_out.write_all(format!("Incorrect number of args: create res_name\n").as_bytes()));
        return Ok(());
    }
    let res_name: &str = args[0];
    let appstate = get_appstate();
    let ourpid = appstate.ourpid;
    if let Some(_) = appstate.files.get(res_name) {
        try!(cli_out.write_all(format!("Cannot create resource {}; already exists!\n", res_name).as_bytes()));
    } else {
        try!(cli_out.write_all(format!("Can create!\n").as_bytes()));
        let appmsg = ApplicationMessage {
            fname: res_name.into(),
            sender: ourpid,
            ty: ApplicationMessageType::CreateFile,
        };
        try!(appstate.send_message_sync(ourpid, appmsg));
    }
    Ok(())
}

fn delete(args: Vec<&str>, cli_out: &mut net::TcpStream) -> io::Result<()> {
    try!(cli_out.write_all(format!("Called delete function.\n").as_bytes()));
    if args.len() != 1 {
        try!(cli_out.write_all(format!("Incorrect number of args: delete res_name\n").as_bytes()));
        return Ok(());
    }
    let res_name: &str = args[0];
    let appstate = get_appstate();
    let ourpid = appstate.ourpid;
    if let Some(_) = appstate.files.get(res_name) {
        try!(cli_out.write_all(format!("Can delete!\n").as_bytes()));
        // TODO: maybe acquire a lock first?
        let appmsg = ApplicationMessage {
            fname: res_name.into(),
            sender: ourpid,
            ty: ApplicationMessageType::DeleteFile,
        };
        try!(appstate.send_message_sync(ourpid, appmsg));
    } else {
        try!(cli_out.write_all(format!("Cannot delete resource {}; doesn't exist!\n", res_name).as_bytes()));
    }
    Ok(())
}

fn read(args: Vec<&str>, cli_out: &mut net::TcpStream) -> io::Result<()> {
    try!(cli_out.write_all(format!("Called read function.\n").as_bytes()));
    if args.len() != 1 {
        try!(cli_out.write_all(format!("Incorrect number of args: read res_name\n").as_bytes()));
        return Ok(());
    }
    let res_name: &str = args[0];
    let ourpid = get_appstate().ourpid;
    if let Some((rchan, mut tosend)) = {
        let mut appstate = get_appstate();
        if let Some(raystate) = appstate.files.get_mut(res_name) {
            try!(cli_out.write_all(format!("Attempting to read the resource:\n").as_bytes()));
            Some(raystate.request())
        } else {
            try!(cli_out.write_all(format!("Cannot read resource {}; doesn't exist!\n", res_name).as_bytes()));
            None
        }
    } {
        for (pid, raymsg) in tosend.drain(..) {
            let appmsg = ApplicationMessage {
                fname: res_name.into(),
                sender: ourpid,
                ty: ApplicationMessageType::Raymond(raymsg),
            };
            let appstate = get_appstate();
            try!(appstate.send_message_sync(pid, appmsg));
        }
        let resource = try!(rchan.recv().map_err(|_| io::Error::new(io::ErrorKind::Other, "mpsc recv failed")));
        trace!("read: Got resource: {}", resource);
        let mut appstate = get_appstate();
        try!(cli_out.write_all(format!("Contents of resource {:?}: {}\n", res_name, resource).as_bytes()));
        let mut tosend = appstate.files.get_mut(res_name).expect(&format!("appstate.files[{}] doesn't exist in the context of a read", res_name)).release();
        for (pid, raymsg) in tosend.drain(..) {
            let appmsg = ApplicationMessage {
                fname: res_name.into(),
                sender: ourpid,
                ty: ApplicationMessageType::Raymond(raymsg),
            };
            try!(appstate.send_message_sync(pid, appmsg));
        }
    }
    trace!("Done with read lock");
    Ok(())
}

fn append(args: Vec<&str>, cli_out: &mut net::TcpStream) -> io::Result<()> {
    try!(cli_out.write_all(format!("Called append function.\n").as_bytes()));
    if args.len() != 2 {
        try!(cli_out.write_all(format!("Incorrect number of args: append res_name data\n").as_bytes()));
        return Ok(());
    }
    let res_name: &str = args[0];
    let ourpid = get_appstate().ourpid;
    if let Some(mut tosend) = {
        if let Some((rchan, mut tosend)) = {
            let mut appstate = get_appstate();
            if let Some(raystate) = appstate.files.get_mut(res_name) {
                try!(cli_out.write_all(format!("Can append!\n").as_bytes()));
                Some(raystate.request())
            } else {
                try!(cli_out.write_all(format!("Cannot append resource {}; doesn't exist!\n", res_name).as_bytes()));
                None
            }
        } {
            for (pid, raymsg) in tosend.drain(..) {
                let appmsg = ApplicationMessage {
                    fname: res_name.into(),
                    sender: ourpid,
                    ty: ApplicationMessageType::Raymond(raymsg),
                };
                let appstate = get_appstate();
                try!(appstate.send_message_sync(pid, appmsg));
            }
            let mut resource = try!(rchan.recv().map_err(|_| io::Error::new(io::ErrorKind::Other, "mpsc recv failed")));
            trace!("append: Got resource: {}", resource);
            resource.extend(args[1].chars());
            trace!("append: Updated resource: {}", resource);
            let mut appstate = get_appstate();
            let mut raystate = appstate.files.get_mut(res_name).expect(&format!("appstate.files[{}] doesn't exist in the context of an append", res_name));
            raystate.resource = resource;
            Some(raystate.release())
        } else {
            None
        }
    } {
        let appstate = get_appstate();
        for (pid, raymsg) in tosend.drain(..) {
            let appmsg = ApplicationMessage {
                fname: res_name.into(),
                sender: ourpid,
                ty: ApplicationMessageType::Raymond(raymsg),
            };
            try!(appstate.send_message_sync(pid, appmsg));
        }
    }
    Ok(())
}

fn ls(args: Vec<&str>, cli_out: &mut net::TcpStream) -> io::Result<()> {
    if args.len() != 0 {
        try!(cli_out.write_all(format!("Incorrect number of args: ls\n").as_bytes()));
        return Ok(());
    }
    let appstate = get_appstate();
    if appstate.files.len() == 0 {
        try!(cli_out.write_all(format!("No Active Resources Found.\n").as_bytes()));
        return Ok(());
    }
    try!(cli_out.write_all(format!("Listing of Active Resources:\n").as_bytes()));
    try!(cli_out.write_all(format!("{:<8} {:<6}\n", "Name", "Holder\n").as_bytes()));
    for (name, filestate) in &appstate.files {
        try!(cli_out.write_all(format!("{:<8} {:<6}\n", name, filestate.holder).as_bytes()));
    }
    Ok(())
}

pub fn handle_clis_in_seperate_thread(ourpid: Pid, port: u16) {
    thread::spawn(move || {
        let listener = net::TcpListener::bind(("0.0.0.0", port)).expect("Failed to bind CLI listener.");
        println!("Management CLI bound to port {}", port);
        for sock in listener.incoming() {
            if let Ok(mut sock) = sock {
                let addr = if let Ok(addr) = sock.peer_addr() { format!("{:?}", addr) } else { "{Error getting peer_addr}".into() };
                debug!("Got a CLI client: {}", addr);
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
                    let peer_addr = addr.clone();
                    let mut reader = BufReader::new(sock);
                    if let Err(e) = (move || { loop {
                        try!(reader.get_mut().write_all(("--> ").as_bytes()));
                        let mut line = "".into();
                        try!(reader.read_line(&mut line));
                        trace!("Got line from {}: {}", peer_addr, line);
                        if let Err(_) = reader.get_mut().write_all(format!("echoing: {}\n", line).as_bytes()) {
                            break;
                        }
                        let mut iter = line.split_whitespace();
                        try!(match iter.next() {
                            Some("create") => create(iter.collect(), reader.get_mut()),
                            Some("delete") => delete(iter.collect(), reader.get_mut()),
                            Some("read") => read(iter.collect(), reader.get_mut()),
                            Some("append") => append(iter.collect(), reader.get_mut()),
                            Some("ls") => ls(iter.collect(), reader.get_mut()),
                            Some(x) => {
                                reader.get_mut().write_all(format!("Invalid command {}\n", x).as_bytes())
                            },
                            None => continue,
                        });
                    } Ok(()) })() {
                        let e: io::Error = e;
                        warn!("An error ocurred while interacting with {:?}: {:?}", addr, e);
                    }
                });
            }
        }
    });
}

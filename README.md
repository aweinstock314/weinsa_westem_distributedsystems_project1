Contributors:
	Avi Weinstock
	Mark Westerhoff

HOW TO INSTALL:
curl https://sh.rustup.rs -sSf | sh
source $HOME/.cargo/env
rustup override set nightly-2016-10-14
sudo yum install -y gcc
sudo yum install -y git
git clone https://github.com/aweinstock314/weinsa_westem_distributedsystems_project1.git
cd weinsa_westem_distributedsystems_project1
cargo build --release

HOW TO RUN:
cargo run PID -t <T> -n <N>
	PID: Required (PID is the current node's process id)
	T: Optional file name for the adjacency list. Defaults to "tree.txt"
	N: Optional file name for nodes/ip file. Defaults to "nodes.txt"


HOW TO UNINSTALL:
curl -sSf https://static.rust-lang.org/rustup.sh | sh -s -- --uninstall
cd ..
rm -rf weinsa_westem_distributedsystems_project1
sudo yum remove git
sudo yum remove gcc
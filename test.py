import argparse
import subprocess
import parse
import socket
import time

treeFile = "tree.txt"
nodeFile = "nodes.txt"
socksize = 1024


class Node:
	def __init__(self, pid, ip, port, cli):
		self.pid = pid
		self.ip = ip
		self.port = int(port)
		self.cli = int(cli)

	def __str__(self):
		return ' '.join([str(self.pid), self.ip, str(self.port), str(self.cli)])
	def __repr__(self):
		return self.__str__()


def start(node):
	proc = subprocess.Popen(["cargo", "run", node.pid, "&"], stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, universal_newlines=True)
	cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	cli.connect((node.ip, node.cli))
	return (proc, cli)

def passCmd(conn, cmd, node)
	conn.sendall(cmd)
	data = conn.recv(socksize)

# check to make sure the output data makes sense
# TODO: Parse data
def create(conn, node, file, expected):
		data = passCmd(conn, " ".join(["create", name]), node)
		assert data == expected

def delete(conn, node, file, expected):
		data = passCmd(conn, " ".join(["delete", name]), node)
		#assert

def read(conn, node, file, expected):
		data = passCmd(conn, " ".join(["read", name]), node)
		#assert

def append(conn, node, file, val, expected):
		data = passCmd(conn, " ".join(["append", name, val]), node)
		#assert

#TODO: Sleeps?
def test_one_node_one_file():
	create( conn=clis[0], node=nodes[0], file="Foo", expected="Success")
	read( conn=clis[0], node=nodes[0], file="Foo", expected="Success")
	append( conn=clis[0], node=nodes[0], file="Foo", val="Hello", expected="Success")
	read( conn=clis[0], node=nodes[0], file="Foo", expected="Hello")
	delete( conn=clis[0], node=nodes[0], file="Foo", expected="Success")

def test_two_nodes_one_file():
	create( conn=clis[0], node=nodes[0], file="Foo", expected="Success")
	read( conn=clis[1], node=nodes[1], file="Foo", expected="Success")
	append( conn=clis[0], node=nodes[0], file="Foo", val="Hello", expected="Success")
	read( conn=clis[1], node=nodes[1], file="Foo", expected="Hello")
	read( conn=clis[0], node=nodes[0], file="Foo", expected="Hello")
	append( conn=clis[1], node=nodes[1], file="Foo", val=" World", expected="Success")
	read( conn=clis[0], node=nodes[0], file="Foo", expected="Hello World")
	delete( conn=clis[1], node=nodes[1], file="Foo", expected="Success")

def test_errors():
	pass


if __name__ == "__main__":
	# Arg parsing
	parser = argparse.ArgumentParser(description="Testing our Raymond's Algo implementation.")
	parser.add_argument("-t", "--tree-file", help='Specify tree file')
>>> parser.add_argument("-n", "--nodes-file", help='Specify nodes/ips file')
	args = parser.parse_args()

	# Reading in nodes.txt
	nodes = list()
	for line in open(nodeFile):
		p = parse.parse(('({pid},"{ip}",{port},{cli})'), line)
		nodes.append( Node(p['pid'], p['ip'], p['port'], p['cli']) )
	print(nodes)

	#reading in trees.txt
	tree = dict()
	for line in open(treeFile):
		p = parse.parse("({p1},{p2})", line)
		(p1, p2) = (p['p1'], p['p2'])
		if tree.get(p1):
			tree[p1].append(p2)
		else:
			tree[p1] = [p2]
		if tree.get(p2):
			tree[p2].append(p1)
		else:
			tree[p2] = [p1]

	# spawning off the necessary procs & CLI socket connections
	# Note: Not checking for conn refused and stuff
	processes = list()
	clis = list()
	for i in nodes:
		(proc, cli) = start(i.pid) 
		processes.append( proc )
		clis.append(cli)

	# #test: only 1 process
	# (proc, cli) = start(nodes[0]) 
	# processes.append(proc)
	# clis.append(cli)

	# Run Tests
	test_one_node_one_file()
	test_two_nodes_one_file()
	test_errors()

	#kill all the processes at the end
	for i in processes:
		i.kill()


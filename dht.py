
#
# dht.py
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#
#
# Messages:
#	ping -> pong
#	store -> ok, err
#	find-nodes -> nodes, err
#	find-value -> nodes, data, err
#

import socket
import datetime
import asyncore
import hashlib
import random

import codec_pb2
import LRU

debugdht = True

def verbose_sendmsg(command):
	if debugdht:
		return True
	return True

def valid_key_len(n):
	if n == 20 or n == 32 or n == 64:
		return True
	return False

def matching_bits(v1, v2):
	for i in xrange(v1.bit_length):
		if ((v1 & (1 << i)) != (v2 & (1 << i))):
			return i
	return v1.bit_length

def bn2bin(v):
	s = bytearray()
	i = bn_bytes(v)
	while i > 0:
		s.append((v >> ((i-1) * 8)) & 0xff)
		i -= 1
	return s

def bin2bn(s):
	l = 0L
	for ch in s:
		l = (l << 8) | ch
	return l

def hash_node_id(node_id):
	node_id_bin = bn2bin(node_id)
	hash = hashlib.sha256(node_id_bin).digest()
	return hash

class NodeDist(object):
	def __init__(self, node_id=0L, distance=0L):
		self.node_id = node_id
		self.distance = distance

class DHTTask(object):
	def __init__(self):
		self.addr = None
		self.msg_start = None
		self.time_start = datetime.datetime.utcnow()
		self.timeout = None
		self.timeout_actor = None
		self.timeout_args = []
		self.task_id = None

class DHTTaskManager(object):
	def __init__(self):
		self.tasks = {}

	def new_task_id(self):
		while True:
			rand_id = random.getrandbits(64)
			if rand_id not in self.tasks:
				return rand_id

	def new_task(self):
		task = DHTTask()
		task.task_id = self.new_task_id()

		self.tasks[task.task_id] = task

		return task

	def delete(self, task_id, addr):
		if task_id not in self.tasks:
			return None

		task = self.tasks[task_id]

		if task.addr != addr:
			return None

		del self.tasks[task_id]

		return task

class DHTNode(object):
	def __init__(self, node_id=0L, ip=None, port=0, flags=0):
		self.node_id = node_id
		self.ip = ip
		self.port = port
		self.flags = flags

		self.first_seen = datetime.datetime.utcnow()
		self.last_seen = datetime.datetime(1980, 1, 1)
		self.rtt = None
		self.bucket_idx = -1
		self.active = False
		self.demoted = 0

class DHTBucket(object):
	def __init__(self):
		self.active_max = 20
		self.demote_max = 3
		self.active = {}
		self.candidates = []

	def full(self):
		return (len(self.active) >= self.active_max)

	def oldest_active(self):
		oldest = datetime.datetime.utcnow()
		old_node = None
		for node in self.active.itervalues():
			if node.last_seen < oldest:
				oldest = node.last_seen
				old_node = node

		return old_node

	def newest_candidate(self):
		tmp_seen = datetime.datetime(1979, 1, 1)
		new_node = None
		for node in self.candidates:
			if node.last_seen > tmp_seen:
				tmp_seen = node.last_seen
				new_node = node

		return new_node

	def refill(self):
		while not bucket.full() and len(self.candidates) > 0:
			new_node = bucket.newest_candidate()
			self.promote_node(bucket, new_node)

	def demote_node(self, node):
		if (not node.active or
		    node.node_id == 0):
			return False

		# demote node to candidate: active -> candidate transition
		del self.active[node.node_id]
		node.active = False
		node.demoted += 1

		# forget node, if demoted too many times
		if node.demoted < self.demote_max:
			self.candidates.append(node)

		return True

	def promote_node(self, node):
		if (node.active or
		    node.node_id == 0):
			return False

		# cannot promote, if bucket full
		if self.full():
			return False

		# promote node to active: candidate -> active transition
		self.candidates.remove(node)
		self.active[node.node_id] = node
		node.active = True

		return True

class DHTRouter(object):
	def __init__(self, dht):
		self.dht = dht
		self.dht_nodes = {}		# key: node_id
		self.all_nodes = {}		# key: (ip,port) tuple

		self.buckets = []
		for i in xrange(64):
			bucket = DHTBucket()
			self.buckets.append(bucket)

	def find_nodes(self, key):
		key_num = bin2bn(key)

		# obtain a distance value for each node, by xor'ing with key
		dists = []
		for node in self.dht_nodes.itervalues():
			nd = NodeDist(node.node_id, node.node_id ^ key_num)
			dists.append(nd)

		# sort by distance value; limit number of returned nodes
		dists = sorted(dists, key=lambda snd: snd.distance)
		dists = dists[:20]

		# build return set of node records
		ret = []
		for nd in dists:
			node = self.dht_nodes[nd.node_id]
			ret.append(node)

		return ret

	def add_node(self, node_msgobj):
		if ((len(node_msgobj.ip) != 4 and
		     len(node_msgobj.ip) != 16) or
		    node_msgobj.port < 1 or
		    node_msgobj.port > 65535):
			return

		node = DHTNode(node_msgobj.node_id,
			       node_msgobj.ip,
			       node_msgobj.port,
			       node_msgobj.flags)

		# pick bucket based on number of matching bits
		# in node idx
		node.bucket_idx = matching_bits(self.node_id,
						node_msgobj.node_id)

		addr_tup = (node.ip, node.port)

		if (addr_tup in self.all_nodes or
		    node.node_id in self.dht_nodes):
			return

		# store in ip-indexed master list
		self.all_nodes[addr_tup] = node

		# store in bucket, based on prefix length
		bucket = self.buckets[node.bucket_idx]
		bucket.candidates.append(node)

		# make node active, if possible
		self.promote_node(bucket, node)

	def demote_node(self, bucket, node):
		if (not node.active or
		    node.node_id not in self.dht_nodes):
			return False

		# demoted from dht_nodes
		del self.dht_nodes[node.node_id]
		bucket.demote_node(node)

		# find new candidates to make active
		# note - possibly makes active the node we just demoted
		bucket.refill()

		return True

	def promote_node(self, bucket, node):
		# bucket already full, ping oldest active
		if bucket.full():
			old_node = bucket.oldest_active()
			self.dht.ping_node(old_node)

		# attempt to make active, within bucket
		if not bucket.promote_node(node):
			return False

		# promoted to dht_nodes
		self.dht_nodes[node.node_id] = node

		return True

	def touch_node(self, addr, node_id, flags, rtt):
		try:
			node = self.all_nodes[addr]
		except KeyError:
			return False

		node.node_id = node_id
		node.flags = flags
		node.last_seen = datetime.datetime.utcnow()
		node.rtt = rtt

		# make node active, if possible
		bucket = self.buckets[node.bucket_idx]
		self.promote_node(bucket, node)

		return True

def ping_node_timeout(task):
	dht = task.timeout_args[0]
	node = task.timeout_args[1]
	bucket = self.buckets[node.bucket_idx]
	dht.dht_router.demote_node(bucket, node)

class DHT(asyncore.dispatcher):
	messagemap = {
		"err",
		"find-nodes",
		"find-value",
		"nodes",
		"ping",
		"pong",
		"store",
	}

	def __init__(self, log, bindport, node_id):
		asyncore.dispatcher.__init__(self)
		self.log = log
		self.bindport = bindport
		self.node_id = node_id
		self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.bind(('', bindport))
		self.state = 'open'
		self.last_sent = 0

		self.dht_cache = LRU.LRU(100000)
		self.dht_router = DHTRouter(self)
		self.taskman = DHTTaskManager()

	def handle_connect(self):
		pass

	def handle_write(self):
		pass

	def handle_close(self):
		self.log.write(self.dstaddr + " close")
		self.state = "closed"
		try:
			self.shutdown(socket.SHUT_RDWR)
			self.close()
		except:
			pass

	def handle_read(self):
		try:
			data, addr = self.recvfrom(2048)
		except:
			self.handle_close()
			return
		if len(data) == 0:
			self.handle_close()
			return
		self.got_packet(data, addr)

	def readable(self):
		if self.state == 'closed':
			return False
		return True

	def writable(self):
		return False

	def got_packet(self, recvbuf, addr):
		if len(recvbuf) < 4 + 12 + 4 + 4:
			return
		if recvbuf[:4] != 'DHT1':
			raise ValueError("got garbage %s" % repr(recvbuf))

		# check checksum
		command = recvbuf[4:4+12].split("\x00", 1)[0]
		msglen = struct.unpack("<I", recvbuf[4+12:4+12+4])[0]
		if msglen > (16 * 1024 * 1024):
			raise ValueError("msglen %u too big" % (msglen,))

		checksum = recvbuf[4+12+4:4+12+4+4]
		if len(recvbuf) < 4 + 12 + 4 + 4 + msglen:
			return
		msg = recvbuf[4+12+4+4:4+12+4+4+msglen]
		th = hashlib.sha256(msg).digest()
		h = hashlib.sha256(th).digest()
		if checksum != h[:4]:
			raise ValueError("got bad checksum %s" % repr(recvbuf))
		recvbuf = recvbuf[4+12+4+4+msglen:]

		if command in self.messagemap:
			if command == "store":
				t = codec_pb2.MsgDHTKeyValue()
			elif command == "nodes":
				t = codec_pb2.MsgDHTNodes()
			elif command == "pong":
				t = codec_pb2.MsgDHTPong()
			elif (command == "find-nodes" or
			      command == "find-value"):
				t = codec_pb2.MsgDHTKey()
			else:
				t = codec_pb2.MsgDHTMisc()

			try:
				t.ParseFromString(msg)
			except google.protobuf.message.DecodeError:
				raise ValueError("bad decode %s" % repr(recvbuf))

			self.got_message(command, t, addr)
		else:
			self.log.write("UNKNOWN COMMAND %s %s" % (command, repr(msg)))

	def send_message(self, command, message, addr):
		if verbose_sendmsg(command):
			self.log.write("SEND %s %s" % (command, str(message)))

		data = message.SerializeToString()
		tmsg = 'DHT1'
		tmsg += command
		tmsg += "\x00" * (12 - len(command))
		tmsg += struct.pack("<I", len(data))

		# add checksum
		th = hashlib.sha256(data).digest()
		h = hashlib.sha256(th).digest()
		tmsg += h[:4]

		tmsg += data
		self.sendto(tmsg, addr)
		self.last_sent = time.time()

	def got_message(self, command, message, addr):
		if verbose_recvmsg(command):
			self.log.write("RECV %s %s" % (command, str(message)))

		if command == "ping":
			msgout = codec_pb2.MsgDHTPong()
			msgout.request_id = message.request_id
			msgout.node_id = self.node_id
			self.send_message("pong", msgout, addr)

		elif command == "pong":
			self.op_pong(message, addr)

		elif command == "store":
			self.op_store(message, addr)

		elif command == "nodes":
			self.op_nodes(message, addr)

		elif command == "find-nodes":
			self.op_find_nodes(message, addr)

		elif command == "find-value":
			self.op_find_value(message, addr)

	def op_pong(self, message, addr):
		msg_end = datetime.datetime.utcnow()

		task = self.taskman.delete(message.request_id, addr)
		if task is None:
			return

		rtt = msg_end - task.time_start

		self.dht_router.touch_node(addr, message.node_id,
					   message.flags, rtt)

	def ping_node(self, node):
		task = self.taskman.new_task()

		msgout = codec_pb2.MsgDHTMisc()
		msgout.request_id = task.task_id

		addr_tup = (node.ip, node.port)

		task.addr = addr_tup
		task.msg_start = msgout

		# clamp to "1s ... (rtt * 10) ... 15s" range
		min_timeout = datetime.timedelta(seconds=1)
		max_timeout = datetime.timedelta(seconds=15)
		if node.rtt is None:
			task.timeout = max_timeout
		else:
			task.timeout = node.rtt * 10
			if task.timeout > max_timeout:
				task.timeout = max_timeout
			if task.timeout < min_timeout:
				task.timeout = min_timeout

		task.timeout_actor = ping_node_timeout
		task.timeout_args = [self, node]

		self.send_message("ping", msgout, addr_tup)

	def op_store(self, message, addr):
		res = "err"
		if (valid_key_len(len(message.key)) and
		    len(message.value) <= 4096):
			self.dht_cache[message.key] = message.value
			res = "ok"

		msgout = codec_pb2.MsgDHTMisc()
		msgout.request_id = message.request_id
		self.send_message(res, msgout, addr)

	def op_nodes(self, message, addr):
		for node in message.nodes:
			self.dht_router.add_node(node)

	def op_find_nodes(self, message, addr):
		if not valid_key_len(len(message.key)):
			msgout = codec_pb2.MsgDHTMisc()
			msgout.request_id = message.request_id
			self.send_message("err", msgout, addr)
			return

		msgout = codec_pb2.MsgDHTNodes()
		msgout.request_id = message.request_id

		nodes = self.dht_router.find_nodes(message.key)
		for node in nodes:
			node_out = msgout.nodes.add()
			node_out.node_id = node.node_id
			node_out.ip = node.ip
			node_out.port = node.port
			if node.flags:
				node_out.flags = node.flags

		self.send_message("nodes", msgout, addr)

	def op_find_value(self, message, addr):
		if message.key in self.dht_cache:
			msgout = codec_pb2.MsgDHTKeyValue()
			msgout.request_id = message.request_id
			msgout.key = message.key
			msgout.value = self.dht_cache[key]
			self.send_message("data", msgout, addr)
			return

		self.op_find_nodes(message, addr)

	def bootstrap(self):
		# send message to nearest nodes (possibly only a handful
		# of DNS bootstrap nodes)
		for node in self.all_nodes:
			# do not send to ourselves
			if node.node_id == self.node_id:
				continue

			self.ping_node(node)


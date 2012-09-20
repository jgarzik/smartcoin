
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
import asyncore
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

def bin2bn(s):
	l = 0L
	for ch in s:
		l = (l << 8) | ch
	return l

class NodeDist(object):
	def __init__(self, node_id=0L, distance=0L):
		self.node_id = node_id
		self.distance = distance

class DHTServer(asyncore.dispatcher):
	messagemap = {
		"ping",
		"store",
		"find-nodes",
		"find-value",
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
		self.dht_nodes = {}

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
		while True:
			if len(recvbuf) < 4:
				return
			if recvbuf[:4] != 'DHT1':
				raise ValueError("got garbage %s" % repr(recvbuf))
			# check checksum
			if len(recvbuf) < 4 + 12 + 4 + 4:
				return
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
			msgout = codec_pb2.MsgDHTMisc()
			msgout.request_id = message.request_id
			self.send_message("pong", msgout, addr)

		elif command == "store":
			self.op_store(message, addr)

		elif command == "find-nodes":
			self.op_find_nodes(message, addr)

		elif command == "find-value":
			self.op_find_value(message, addr)

	def op_store(self, message, addr):
		res = "err"
		if (valid_key_len(len(message.key)) and
		    len(message.value) <= 4096):
			self.dht_cache[message.key] = message.value
			res = "ok"

		msgout = codec_pb2.MsgDHTMisc()
		msgout.request_id = message.request_id
		self.send_message(res, msgout, addr)

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

	def op_find_nodes(self, message, addr):
		if not valid_key_len(len(message.key)):
			msgout = codec_pb2.MsgDHTMisc()
			msgout.request_id = message.request_id
			self.send_message("err", msgout, addr)
			return

		msgout = codec_pb2.MsgDHTNodes()
		msgout.request_id = message.request_id

		nodes = self.find_nodes(message.key)
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


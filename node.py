#!/usr/bin/python
#
# node.py - Distributed bond P2P network node
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import hashlib

import Log
import codec_pb2

class MsgNull(object):
	def __init__(self):
		pass
	def SerializeToString(self):
		return ''
	def ParseFromString(self, data):
		pass

class NodeConn(asyncore.dispatcher):
	messagemap = {
		"version",
		"verack",
		"ping",
		"pong",
		"addr",
		"getaddr",
	}

	def __init__(self, dstaddr, dstport, log, peermgr,
		     netmagic):
		asyncore.dispatcher.__init__(self)
		self.log = log
		self.peermgr = peermgr
		self.netmagic = netmagic
		self.dstaddr = dstaddr
		self.dstport = dstport
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sendbuf = ""
		self.recvbuf = ""
		self.ver_send = MIN_PROTO_VERSION
		self.ver_recv = MIN_PROTO_VERSION
		self.last_sent = 0
		self.getblocks_ok = True
		self.last_block_rx = time.time()
		self.last_getblocks = 0
		self.remote_height = -1
		self.state = "connecting"
		self.hash_continue = None

		#stuff version msg into sendbuf
		vt = msg_version()
		vt.addrTo.ip = self.dstaddr
		vt.addrTo.port = self.dstport
		vt.addrFrom.ip = "0.0.0.0"
		vt.addrFrom.port = 0
		vt.nStartingHeight = self.chaindb.getheight()
		vt.strSubVer = MY_SUBVERSION
		self.send_message(vt, True)

		self.log.write("connecting")
		try:
			self.connect((dstaddr, dstport))
		except:
			self.handle_close()

	def handle_connect(self):
		self.log.write(self.dstaddr + " connected")
		self.state = "connected"
		#send version msg
#		t = msg_version()
#		t.addrTo.ip = self.dstaddr
#		t.addrTo.port = self.dstport
#		t.addrFrom.ip = "0.0.0.0"
#		t.addrFrom.port = 0
#		self.send_message(t)

	def handle_close(self):
		self.log.write(self.dstaddr + " close")
		self.state = "closed"
		self.recvbuf = ""
		self.sendbuf = ""
		try:
			self.shutdown(socket.SHUT_RDWR)
			self.close()
		except:
			pass

	def handle_read(self):
		try:
			t = self.recv(8192)
		except:
			self.handle_close()
			return
		if len(t) == 0:
			self.handle_close()
			return
		self.recvbuf += t
		self.got_data()

	def readable(self):
		return True

	def writable(self):
		return (len(self.sendbuf) > 0)

	def handle_write(self):
		try:
			sent = self.send(self.sendbuf)
		except:
			self.handle_close()
			return
		self.sendbuf = self.sendbuf[sent:]

	def got_data(self):
		while True:
			if len(self.recvbuf) < 4:
				return
			if self.recvbuf[:4] != 'BND1':
				raise ValueError("got garbage %s" % repr(self.recvbuf))
			# check checksum
			if len(self.recvbuf) < 4 + 12 + 4 + 4:
				return
			command = self.recvbuf[4:4+12].split("\x00", 1)[0]
			msglen = struct.unpack("<I", self.recvbuf[4+12:4+12+4])[0]
			if msglen > (16 * 1024 * 1024):
				raise ValueError("msglen %u too big" % (msglen,))

			checksum = self.recvbuf[4+12+4:4+12+4+4]
			if len(self.recvbuf) < 4 + 12 + 4 + 4 + msglen:
				return
			msg = self.recvbuf[4+12+4+4:4+12+4+4+msglen]
			th = hashlib.sha256(msg).digest()
			h = hashlib.sha256(th).digest()
			if checksum != h[:4]:
				raise ValueError("got bad checksum %s" % repr(self.recvbuf))
			self.recvbuf = self.recvbuf[4+12+4+4+msglen:]

			if command in self.messagemap:
				if command == "version":
					t = codec_pb2.MsgVersion()
				elif command == "verack":
					t = MsgNull()
				elif command == "ping":
					t = codec_pb2.MsgPingPong()
				elif command == "pong":
					t = codec_pb2.MsgPingPong()
				elif command == "addr":
					t = codec_pb2.MsgAddresses()
				elif command == "getaddr":
					t = MsgNull()

				try:
					t.ParseFromString(msg)
				except google.protobuf.message.DecodeError:
					raise ValueError("bad decode %s" % repr(self.recvbuf))

				self.got_message(command, t)
			else:
				self.log.write("UNKNOWN COMMAND %s %s" % (command, repr(msg)))

	def send_message(self, command, message, pushbuf=False):
		if self.state != "connected" and not pushbuf:
			return

		if verbose_sendmsg(message):
			self.log.write("send %s" % repr(message))

		data = message.SerializeToString()
		tmsg = 'BND1'
		tmsg += command
		tmsg += "\x00" * (12 - len(command))
		tmsg += struct.pack("<I", len(data))

		# add checksum
		th = hashlib.sha256(data).digest()
		h = hashlib.sha256(th).digest()
		tmsg += h[:4]

		tmsg += data
		self.sendbuf += tmsg
		self.last_sent = time.time()

	def got_message(self, command, message):
		if verbose_recvmsg(message):
			self.log.write("recv %s" % repr(message))

		if command == "version":
			self.ver_send = min(PROTO_VERSION, message.nVersion)

			self.send_message("verack", MsgNull())

		elif command == "verack":
			self.send_message("getaddr", MsgNull())

		elif command == "ping":
			msgout = codec_pb2.MsgPingPong()
			msgout.cookie = message.cookie
			self.send_message("pong", msgout)

class PeerManager(object):
	def __init__(self, log):
		self.log = log
		self.peers = []
		self.addrs = {}
		self.tried = {}

	def add(self, host, port):
		self.log.write("PeerManager: connecting to %s:%d" %
			       (host, port))
		self.tried[host] = True
		c = NodeConn(host, port, self.log)
		self.peers.append(c)

	def new_addrs(self, addrs):
		for addr in addrs:
			if addr.ip in self.addrs:
				continue
			self.addrs[addr.ip] = addr

		self.log.write("PeerManager: Received %d new addresses (%d addrs, %d tried)" %
				(len(addrs), len(self.addrs),
				 len(self.tried)))

	def random_addrs(self):
		ips = self.addrs.keys()
		random.shuffle(ips)
		if len(ips) > 1000:
			del ips[1000:]

		vaddr = []
		for ip in ips:
			vaddr.append(self.addrs[ip])

		return vaddr

	def closeall(self):
		for peer in self.peers:
			peer.handle_close()
		self.peers = []


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print "Usage: node.py CONFIG-FILE"
		sys.exit(1)

	f = open(sys.argv[1])
	for line in f:
		m = re.search('^(\w+)\s*=\s*(\S.*)$', line)
		if m is None:
			continue
		settings[m.group(1)] = m.group(2)
	f.close()

	if 'host' not in settings:
		settings['host'] = '127.0.0.1'
	if 'port' not in settings:
		settings['port'] = 8333
	if 'rpcport' not in settings:
		settings['rpcport'] = 9332
	if 'db' not in settings:
		settings['db'] = '/tmp/chaindb'
	if 'chain' not in settings:
		settings['chain'] = 'mainnet'
	chain = settings['chain']
	if 'log' not in settings or (settings['log'] == '-'):
		settings['log'] = None

	if ('rpcuser' not in settings or
	    'rpcpass' not in settings):
		print "You must set the following in config: rpcuser, rpcpass"
		sys.exit(1)

	settings['port'] = int(settings['port'])
	settings['rpcport'] = int(settings['rpcport'])

	log = Log.Log(settings['log'])

	log.write("\n\n\n\n")

	peermgr = PeerManager(log, mempool, chaindb, netmagic)

	# start HTTP server for JSON-RPC
	s = httpsrv.Server('', settings['rpcport'], rpc.RPCRequestHandler,
			  (log, peermgr, mempool, chaindb,
			   settings['rpcuser'], settings['rpcpass']))

	# connect to specified remote node
	peermgr.add(settings['host'], settings['port'])

	# program main loop
	asyncore.loop()


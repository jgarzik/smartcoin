
#
# dht.py
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
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

class DHTServer(asyncore.dispatcher):
	messagemap = {
		"ping",
		"store",
	}

	def __init__(self, log, bindport):
		asyncore.dispatcher.__init__(self)
		self.log = log
		self.bindport = bindport
		self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.bind(('', bindport))
		self.state = 'open'
		self.last_sent = 0

		self.dht_cache = LRU.LRU(100000)

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
				if command == "ping":
					t = codec_pb2.MsgPingPong()
				elif command == "store":
					t = codec_pb2.MsgDHTStore()

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
			msgout = codec_pb2.MsgPingPong()
			msgout.cookie = message.cookie
			self.send_message("pong", msgout, addr)

		elif command == "store":
			self.op_store(message)

	def op_store(self, message):
		if (not valid_key_len(len(message.key)) or
		    len(message.value) > 4096):
			return

		self.dht_cache[message.key] = message.value


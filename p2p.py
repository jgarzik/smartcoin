#
# p2p.py - Distributed bond P2P network node
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import struct
import hashlib

messagemap = {
	"version",
	"verack",
	"ping",
	"pong",
	"addr",
	"getaddr",
}

class MsgNull(object):
	def __init__(self):
		pass
	def SerializeToString(self):
		return ''
	def ParseFromString(self, data):
		pass
	def __str__(self):
		return "MsgNull()"

def message_read(msg_start, f):
	try:
		recvbuf = f.read(4 + 12 + 4 + 4)
	except IOError:
		return None
	
	# check magic
	if len(recvbuf) < 4:
		return None
	if recvbuf[:4] != msg_start:
		raise ValueError("got garbage %s" % repr(recvbuf))

	# check checksum
	if len(recvbuf) < 4 + 12 + 4 + 4:
		return None

	# remaining header fields: command, msg length, checksum
	command = recvbuf[4:4+12].split("\x00", 1)[0]
	msglen = struct.unpack("<i", recvbuf[4+12:4+12+4])[0]
	checksum = recvbuf[4+12+4:4+12+4+4]

	# read message body
	try:
		recvbuf += f.read(msglen)
	except IOError:
		return None

	msg = recvbuf[4+12+4+4:4+12+4+4+msglen]
	th = hashlib.sha256(msg).digest()
	h = hashlib.sha256(th).digest()
	if checksum != h[:4]:
		raise ValueError("got bad checksum %s" % repr(recvbuf))
	recvbuf = recvbuf[4+12+4+4+msglen:]

	return (command, msg)

def message_to_str(msg_start, command, message):
	data = message.SerializeToString()
	tmsg = msg_start
	tmsg += command
	tmsg += "\x00" * (12 - len(command))
	tmsg += struct.pack("<I", len(data))

	# add checksum
	th = hashlib.sha256(data).digest()
	h = hashlib.sha256(th).digest()
	tmsg += h[:4]

	tmsg += data

	return tmsg


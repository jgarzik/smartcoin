
#
# rpc.py
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import re
import base64
import json
import socket
import cStringIO
import struct

import httpsrv
from coredefs import PROTO_VERSION

VALID_RPCS = {
	"getconnectioncount",
	"getinfo",
	"help",
	"stop",
}


class RPCExec(object):
	def __init__(self, peermgr, httpsrv):
		self.peermgr = peermgr
		self.httpsrv = httpsrv

		self.work_tophash = None
		self.work_blocks = {}

	def help(self, params):
		s = "Available RPC calls:\n"
		s += "getconnectioncount - get P2P peer count\n"
		s += "getinfo - misc. node info\n"
		s += "help - this message\n"
		s += "stop - stop node\n"
		return (s, None)

	def getconnectioncount(self, params):
		return (len(self.peermgr.peers), None)

	def getinfo(self, params):
		d = {}
		d['protocolversion'] = PROTO_VERSION
		return (d, None)

	def stop(self, params):
		self.httpsrv.shutdown(socket.SHUT_RD)
		self.httpsrv.close()

		self.peermgr.closeall()

		return (True, None)


class RPCRequestHandler(httpsrv.RequestHandler):
	def __init__(self, conn, addr, server, privdata):
		httpsrv.RequestHandler.__init__(self, conn, addr, server)
		self.log = privdata[0]
		self.rpc = RPCExec(privdata[1], server)
		self.rpcuser = privdata[2]
		self.rpcpass = privdata[3]
		self.server = server

	def do_GET(self):
		self.send_error(501, "Unsupported method (%s)" % self.command)

	def check_auth(self):
		hdr = self.headers.getheader('authorization')
		if hdr is None:
			return None

		m = re.search('\s*(\w+)\s+(\S+)', hdr)
		if m is None or m.group(0) is None:
			return None
		if m.group(1) != 'Basic':
			return None

		unpw = base64.b64decode(m.group(2))
		if unpw is None:
			return None

		m = re.search('^([^:]+):(.*)$', unpw)
		if m is None:
			return None

		un = m.group(1)
		pw = m.group(2)
		if (un != self.rpcuser or
		    pw != self.rpcpass):
			return None

		return un

	def handle_data(self):
		if self.path != '/':
			self.send_error(404, "Path not found")
			return
		username = self.check_auth()
		if username is None:
			self.send_error(401, "Forbidden")
			return

		try:
			rpcreq = json.loads(self.body)
		except ValueError:
			self.send_error(400, "Unable to decode JSON data")
			return
		if isinstance(rpcreq, dict):
			self.handle_rpc_singleton(rpcreq)
		elif isinstance(rpcreq, list):
			self.handle_rpc_batch(rpcreq)
		else:
			self.send_error(400, "Not a valid JSON-RPC request")

	def handle_rpc(self, rpcreq):
		id = None
		if 'id' in rpcreq:
			id = rpcreq['id']
		if ('method' not in rpcreq or
		    (not isinstance(rpcreq['method'], str) and
		     not isinstance(rpcreq['method'], unicode))):
			resp = { "id" : id, "error" :
				  { "code" : -1,
				    "message" : "method not specified" } }
			return resp
		if ('params' not in rpcreq or
		    not isinstance(rpcreq['params'], list)):
			resp = { "id" : id, "error" :
				  { "code" : -2,
				    "message" : "invalid/missing params" } }
			return resp

		(res, err) = self.jsonrpc(rpcreq['method'], rpcreq['params'])

		if err is None:
			resp = { "result" : res, "error" : None, "id" : id }
		else:
			resp = { "error" : err, "id" : id }

		return resp

	def json_response(self, resp):
		respstr = json.dumps(resp) + "\n"

		self.send_response(200)
		self.send_header("Content-type", "application/json")
		self.send_header("Content-length", len(respstr))
		self.end_headers()
		self.log_request(self.code, len(respstr))
		self.outgoing.append(respstr)
		self.outgoing.append(None)

	def handle_rpc_singleton(self, rpcreq):
		resp = self.handle_rpc(rpcreq)
		self.json_response(resp)

	def handle_rpc_batch(self, rpcreq_list):
		res = []
		for rpcreq in rpcreq_list:
			resp = self.handle_rpc(rpcreq)
			res.append(resp)
		self.json_response(res)

	def jsonrpc(self, method, params):
		if method not in VALID_RPCS:
			return (None, { "code" : -32601,
					"message" : "method not found" })
		rpcfunc = getattr(self.rpc, method)
		return rpcfunc(params)

	def log_message(self, format, *args):
		self.log.write("HTTP %s - - [%s] %s \"%s\" \"%s\"" %
			(self.address_string(),
			 self.log_date_time_string(),
			 format%args,
			 self.headers.get('referer', ''),
			 self.headers.get('user-agent', '')
			 ))


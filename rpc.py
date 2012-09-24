
#
# rpc.py
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import re
import base64
import json
import sys

from coredefs import PROTO_VERSION

VALID_RPCS = {
	"getconnectioncount",
	"getinfo",
	"help",
	"stop",
}


class RPCExec(object):
	def __init__(self, peermgr, log, rpcuser, rpcpass):
		self.peermgr = peermgr
		self.rpcuser = rpcuser
		self.rpcpass = rpcpass
		self.log = log

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
		self.peermgr.closeall()

		return (True, None)

	def handle_request(self, environ, start_response):
		try:
			# Posts only
			if environ['REQUEST_METHOD'] != 'POST':
				raise RPCException('501', "Unsupported method (%s)" % environ['REQUEST_METHOD'])

			# Only accept default path
			if environ['PATH_INFO'] + environ['SCRIPT_NAME'] != '/':
				raise RPCException('404', "Path not found")

			# RPC authentication
			username = self.check_auth(environ['HTTP_AUTHORIZATION'])
			if username is None:
				raise RPCException('401', 'Forbidden')

			# Dispatch the RPC call
			length = environ['CONTENT_LENGTH']
			body = environ['wsgi.input'].read(length)
			try:
				rpcreq = json.loads(body)
			except ValueError:
				raise RPCException('400', "Unable to decode JSON data")

			if isinstance(rpcreq, dict):
				resp = self.handle_rpc(rpcreq)
			elif isinstance(rpcreq, list):
				resp = self.handle_rpc_batch(rpcreq)
			else:
				raise RPCException('400', "Not a valid JSON-RPC request")
			respstr = json.dumps(resp) + "\n"

			# Return a json response
			start_response('200 OK', [('Content-Type', 'application/json')])
			return respstr

		except RPCException, e:
			start_response(e.status, [('Content-Type', 'text/plain')], sys.exc_info())
			return e.message


	def check_auth(self, hdr):
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
		pass

	def handle_rpc_batch(self, rpcreq_list):
		res = []
		return ''.join(map(self.handle_rpc, repcreq_list))

	def jsonrpc(self, method, params):
		if method not in VALID_RPCS:
			return (None, { "code" : -32601,
					"message" : "method not found" })
		rpcfunc = getattr(self, method)
		return rpcfunc(params)

	def log_message(self, format, *args):
		self.log.write("HTTP %s - - [%s] %s \"%s\" \"%s\"" %
			(self.address_string(),
			 self.log_date_time_string(),
			 format%args,
			 self.headers.get('referer', ''),
			 self.headers.get('user-agent', '')
			 ))

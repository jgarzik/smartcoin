#
# scdb.py
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import gdbm
import codec_pb2

class SCDb(object):
	def __init__(self, dbfilename, log):
		self.log = log
		self.dbm = gdbm.open(dbfilename, 'cs')
		self.issues = set()

		self.initialize()
	
	def initialize(self):
		if 'issue_list' not in self.dbm:
			return

		list_str = self.dbm['issue_list']
		self.issues = set(list_str.split())

	def get_obj(self, key_prefix, id, newobj):
		key = key_prefix + ' ' + id
		if key not in self.dbm:
			return False

		try:
			newobj.ParseFromString(self.dbm[key])
		except google.protobuf.message.DecodeError:
			return False

		return True

	def put_obj(self, key_prefix, id, obj):
		key = key_prefix + ' ' + id
		value = obj.SerializeToString()
		self.dbm[key] = value

	def del_obj(self, key_prefix, id):
		key = key_prefix + ' ' + id
		if key not in self.dbm:
			return False

		del self.dbm[key]
		return True

	def get_issue(self, id):
		if id not in self.issues:
			return None

		issue = codec_pb2.Issue()
		rc = self.get_obj('issue', id, issue)
		if rc:
			return issue
		else:
			return None

	def put_issue(self, issue):
		id = issue.display_shortname
		self.put_obj('issue', id, issue)

		if id not in self.issues:
			self.issues.add(id)
			self.dbm['issue_list'] = ' '.join(self.issues)

	def del_issue(self, id):
		try:
			self.issues.remove(id)
		except KeyError:
			return False
		self.dbm['issue_list'] = ' '.join(self.issues)
		self.del_obj('issue', id)
		return True


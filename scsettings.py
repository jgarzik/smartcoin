#
# scsettings.py
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import re

class Settings(object):
	def __init__(self):
		self.d = {}

	def read(self, sys_argv):
		for i in xrange(len(sys_argv)):
			if i == 0:
				continue
			m = re.search('^(\w+)\s*=\s*(\S.*)$', sys_argv[i])
			if m is None:
				continue
			self.d[m.group(1)] = m.group(2)

		if 'cfg' in self.d:
			filename = self.d['cfg']
		else:
			filename = 'smartcoin.cfg'

		try:
			f = open(filename)
			for line in f:
				m = re.search('^(\w+)\s*=\s*(\S.*)$', line)
				if m is None:
					continue
				self.d[m.group(1)] = m.group(2)
			f.close()
		except OSError, IOError:
			pass


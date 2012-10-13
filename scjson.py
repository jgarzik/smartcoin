#
# scjson.py
#
# Distributed under the MIT/X11 software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
#

import json
import codec_pb2

def json_to_issue(obj):
	issue = codec_pb2.Issue()

	obj_outpt = obj['outpoint']
	outpt = codec_pb2.OutPoint()
	outpt.tx_hash = obj_outpt['tx_hash'].decode('hex')
	outpt.index = obj_outpt['index']
	issue.start_point = outpt

	obj_issuer = obj['issuer']
	issuer = codec_pb2.Issuer()
	issuer.pubkey = obj_issuer['pubkey'].decode('hex')
	if 'email' in obj_issuer:
		issuer.email = obj_issuer['email']
	issuer.display_name = obj_issuer['display_name']
	if 'display_url' in obj_issuer:
		issuer.display_url = obj_issuer['display_url']
	issuer.pay_to_script = obj_issuer['pay_to_script'].decode('hex')
	issue.issuer = issuer

	issue.issue_count = obj['issue_count']
	issue.value = obj['value']
	issue.coupon_value = obj['coupon_value']
	issue.display_shortname = obj['display_shortname']
	issue.display_name = obj['display_name']
	issue.repayment_value = obj['repayment_value']
	issue.timestamp = obj['timestamp']
	if 'peer_url' in obj:
		issue.peer_url = obj['peer_url']
	
	return issue

def jsonfile_to_issue(filename):
	try:
		f = open(filename)
		obj = json.load(f)
	except OSError, IOError:
		return None
	
	try:
		issue = json_to_issue(obj)
	except KeyError:
		return None
	
	return issue


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

	obj_outpt = obj['start_point']
	issue.start_point.tx_hash = obj_outpt['tx_hash'].decode('hex')
	issue.start_point.index = obj_outpt['index']

	obj_issuer = obj['issuer']
	issue.issuer.pubkey = obj_issuer['pubkey'].decode('hex')
	if 'email' in obj_issuer:
		issue.issuer.email = obj_issuer['email']
	issue.issuer.display_name = obj_issuer['display_name']
	if 'display_url' in obj_issuer:
		issue.issuer.display_url = obj_issuer['display_url']
	issue.issuer.pay_to_script = obj_issuer['pay_to_script'].decode('hex')

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

def issue_to_json(issue):
	obj = {
		'start_point' : {
			'tx_hash' : issue.start_point.tx_hash.encode('hex'),
			'index' : issue.start_point.index
		},

		'issuer' : {
			'pubkey' : issue.issuer.pubkey.encode('hex'),
			'display_name' : issue.issuer.display_name,
			'pay_to_script' : issue.issuer.pay_to_script.encode('hex')
		},

		'issue_count' : issue.issue_count,
		'value' : issue.value,
		'coupon_value' : issue.coupon_value,
		'display_shortname' : issue.display_shortname,
		'display_name' : issue.display_name,
		'repayment_value' : issue.repayment_value,
		'timestamp' : issue.timestamp,
	}
	if issue.issuer.HasField('email'):
		obj['issuer']['email'] = issue.issuer.email
	if issue.issuer.HasField('display_url'):
		obj['issuer']['display_url'] = issue.issuer.display_url
	if issue.HasField('peer_url'):
		obj['peer_url'] = issue.peer_url

	return obj

def issue_to_jsonfile(filename, issue):
	obj = issue_to_json(issue)

	try:
		f = open(filename, "wb")
		json.dump(obj, f)
	except OSError, IOError:
		return False
	
	return True

def issue_to_jsonstr(issue):
	obj = issue_to_json(issue)
	s = json.dumps(obj, indent=4)
	return s


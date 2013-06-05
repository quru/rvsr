from datetime import datetime
from lxml import etree
import networkx as nx
from networkx.algorithms.traversal.depth_first_search import *
import os
import re
import requests
from requests.auth import HTTPBasicAuth
import string
import difflib

class Conf:

	xml = etree.parse('config.xml')

	@staticmethod
	def getReplicate():
		return Util.getXMLBoolean(Conf.xml.xpath('/rvmsr/general/replicate')[0].text)

	@staticmethod
	def getVerifySSL():
		return Util.getXMLBoolean(Conf.xml.xpath('/rvmsr/general/verify_ssl')[0].text)

	@staticmethod
	def getPlans():
		return Conf.xml.xpath('/rvmsr/replication')[0]

	@staticmethod
	def getVar():
		return Conf.xml.xpath('/rvmsr/advanced/var')[0].text

	@staticmethod
	def getRefresh():
		return Util.getXMLBoolean(Conf.xml.xpath('/rvmsr/general/refresh')[0].text)


class Util:

	@staticmethod
	def getXMLBoolean(XMLText):
		if string.lower(XMLText) in ['true', 'yes', 'y', '1']:
			return True
		else:
			return False

	@staticmethod
	def httpGet(url, user, passwd):
		headers = {}
		return requests.get(url, headers=headers,
							auth=HTTPBasicAuth(user, passwd), verify=Conf.getVerifySSL()).content

	@staticmethod
	def httpPost(url, user, passwd, payload):
		headers = {'content-type': 'application/xml'}
		r = requests.post(url, data=payload, headers=headers,
						  auth=HTTPBasicAuth(user, passwd), verify=Conf.getVerifySSL())
		return (r.status_code, r.content)

	@staticmethod
	def httpPut(url, user, passwd, payload):
		headers = {'content-type': 'application/xml'}
		r = requests.put(url, data=payload, headers=headers,
						 auth=HTTPBasicAuth(user, passwd), verify=Conf.getVerifySSL())
		return (r.status_code, r.content)

	@staticmethod
	def httpDelete(url, user, passwd):
		headers = {}
		r = requests.delete(url, headers=headers,
							auth=HTTPBasicAuth(user, passwd), verify=Conf.getVerifySSL())
		return (r.status_code, r.content)

	@staticmethod
	def writeElementToFile(element):
		fh = open('%s/%s.xml' % (_dir, element.get('id')), 'w')
		fh.write(etree.tostring(element))
		fh.close

	@staticmethod
	def writeGraphToFile(graph, name):
		nx.write_gpickle(gr, '%s/%s.dat' % (_dir, name))

	@staticmethod
	def readGraphFromFile(cluster_name):
		return nx.read_gpickle('%s/%s.dat' % (_dir, cluster_name))

	@staticmethod
	def xpathNotNone(xml, xpath):
		try:
			text = xml.xpath(xpath)[0].text
			if text == None:
				text = ''
		except:
			text = ''

		return text

	@staticmethod
	def xpathAttribNotNone(xml, xpath, attrib):
		try:
			text = xml.xpath(xpath)[0].attrib[attrib]
			if text == None:
				text = ''
		except:
			text = ''

		return text

	@staticmethod
	def findNearestNameInXMLList(haystack_xml_list, needle_xml, nearly_match_string, extra_xpath_matches = []):

		filtered_haystack = []
		# first, filter the elements where the extra xpaths specified absolutely match
		for haystack_element_xml in haystack_xml_list:
			element_qualifies = True
			for xpath in extra_xpath_matches:
				if Util.xpathNotNone(needle_xml, xpath) == Util.xpathNotNone(haystack_element_xml, xpath):
					# keep checking
					pass
				else:
					# element disqualified, break loop and continue with the next element
					element_qualifies = False
					break

			# has the element survived the test?
			if element_qualifies == True:
				# accept it for further processing
				filtered_haystack.append(haystack_element_xml)

		candidate_names = []
		for element in filtered_haystack:
			candidate_names.append(Util.xpathNotNone(element, 'name'))

		closest_matching_names = difflib.get_close_matches(nearly_match_string, candidate_names, 1, 0)
		closest_matching_name = closest_matching_names[0]

		for element in filtered_haystack:
			if Util.xpathNotNone(element, 'name') == closest_matching_name:
				closest_matching_uuid = element.get('id')
			break

		return closest_matching_uuid


class PlanInfo:

	@staticmethod
	def getUrl(XMLPlan, site):
		return XMLPlan.xpath('%s/api_url' % site)[0].text

	@staticmethod
	def getUser(XMLPlan, site):
		return XMLPlan.xpath('%s/api_username' % site)[0].text + '@' + XMLPlan.xpath('%s/api_domain' % site)[0].text

	@staticmethod
	def getPass(XMLPlan, site):
		return XMLPlan.xpath('%s/api_password' % site)[0].text

	@staticmethod
	def getDc(XMLPlan, site):
		return XMLPlan.xpath('%s/datacentre_name' % site)[0].text

	@staticmethod
	def getCl(XMLPlan, site):
		return XMLPlan.xpath('%s/cluster_name' % site)[0].text

	@staticmethod
	def getTag(XMLPlan):
		return XMLPlan.xpath('tag')[0].text


class Sites:

	plan = None

	def __init__(self, plan):
		self.plan = plan

	def get(self, subpath, site='primary'):

		# stripping leading '/api' if that was supplied in the subpath, it may have been if the subpath was supplied from
		# an 'href' attribute of an element
		subpath = re.sub('^/api/', '/', subpath)

		content = Util.httpGet(PlanInfo.getUrl(plan, site) + subpath, PlanInfo.getUser(plan, site), PlanInfo.getPass(plan, site))
		xml = etree.fromstring(content)
		return xml

	def PostOrPut(self, subpath, site, payload, PostOrPut):

		# stripping leading '/api' if that was supplied in the subpath, it may have been if the subpath was supplied from
		# an 'href' attribute of an element
		subpath = re.sub('^/api/', '/', subpath)

		if PostOrPut == 'post':
			(status_code, content) = Util.httpPost(PlanInfo.getUrl(plan, site) + subpath, PlanInfo.getUser(plan, site),
											   PlanInfo.getPass(plan, site), payload)
		elif PostOrPut == 'put':
			(status_code, content) = Util.httpPut(PlanInfo.getUrl(plan, site) + subpath, PlanInfo.getUser(plan, site),
											   PlanInfo.getPass(plan, site), payload)

		payload_xml = etree.fromstring(payload)

		if status_code >= 200 and status_code < 300:

			worked = True

			xml = etree.fromstring(content)
			uuid = xml.xpath('/%s' % (payload_xml.tag))[0].get('id')

		else:

			worked = False

			uuid = None

			# collect logging information here
			# TODO


		return (worked, uuid)

	def Delete(self, subpath, site):

		# stripping leading '/api' if that was supplied in the subpath, it may have been if the subpath was supplied from
		# an 'href' attribute of an element
		subpath = re.sub('^/api/', '/', subpath)

		(status_code, content) = Util.httpDelete(PlanInfo.getUrl(plan, site) + subpath, PlanInfo.getUser(plan, site),
										   PlanInfo.getPass(plan, site))

		if status_code >= 200 and status_code < 300:

			worked = True

		else:

			worked = False

			# collect logging information here
			# TODO

		return worked

	def post(self, subpath, site, payload):
		return self.PostOrPut(subpath, site, payload, 'post')

	def put(self, subpath, site, payload):
		return self.PostOrPut(subpath, site, payload, 'put')

	def delete(self, subpath, site):
		return self.Delete(subpath, site)


class Vm:
	
	xml = None
	plan = None

	def __init__(self, uuid, plan):
		self.xml = etree.parse("%s/%s.xml" % (_dir, uuid))
		self.plan = plan

	def getName(self):
		original_name = self.xml.xpath('/vm/name')[0].text
		return "%s_%s" % (PlanInfo.getTag(plan), original_name)

	def createIfNotExist(self):

		# find out if an object with the expected name exists already
		vm = manyVms2.xpath('vm[name="%s"]' % self.getName())
		if len(vm) == 0:
			# does not exist. create and retrieve the uuid.
			payload = '''
			<vm>

				<!-- mandatory start -->
				
				<name>%s</name>

				<!-- custom templates are not suppored -->
				<template>
					<name>%s</name>
				</template>
				<!-- -->
				
				<cluster>
					<name>%s</name>
				</cluster>

				<!-- mandatory end -->

				<!-- rather specify now start -->

				<type>%s</type>

				<!-- rather specify now end -->
			</vm>
			''' % (
					self.getName(),
					'Blank',
					PlanInfo.getCl(self.plan, 'secondary'),
					Util.xpathNotNone(self.xml, '/vm/type'),
					)

			(success, uuid) = sites.post('/vms', 'secondary', payload)

			# tag the virtual machine
			if success == True:
				payload = '''
					<tag>
						<name>%s</name>
					</tag>
					''' % (
							PlanInfo.getTag(self.plan)
							)

				(success, uuid) = sites.post('/vms/%s/tags' % (uuid), 'secondary', payload)

		# vm exists already
		else:
			# retrieve the uuid
			uuid =  vm[0].attrib['id']

		# TODO, update only if required
		# update the little details in the vm anyway
		payload = '''
		<vm>
			<!-- optional start -->

			<description>%s</description>
			<memory>%s</memory>
			<cpu>
				<topology cores="%s" sockets="%s" />
			</cpu>
			<os type="%s">
				<boot dev="%s" />

				<!-- further boot devices are not supported -->
				<!--
				<boot dev="" />
				-->
				<!-- -->

				<kernel>%s</kernel>
				<initrd>%s</initrd>
				<cmdline>%s</cmdline>
			</os>
			<high_availability>
				<enabled>%s</enabled>
				<priority>%s</priority>
			</high_availability>
			<display>
				<type>%s</type>
				<!-- monitors set to 1 since the UI doesnt allow this change anyway -->
				<!--
				<monitors>1</monitors>
				-->
				<!-- -->
			</display>
			<timezone>%s</timezone>
			<domain>%s</domain>
			<stateless>%s</stateless>
			<placement_policy>
				<affinity>%s</affinity>
			</placement_policy>
			<memory_policy>
				<guaranteed>%s</guaranteed>
			</memory_policy>
			<usb>
				<enabled>%s</enabled>
			</usb>

			<!-- custom properties are not supported -->
			<!--
			<custom_properties></custom_properties>
			-->
			<!-- -->

			<!-- optional end -->

		</vm>
		''' % (
				Util.xpathNotNone(self.xml, '/vm/description'),
				Util.xpathNotNone(self.xml, '/vm/memory'),
				Util.xpathAttribNotNone(self.xml, '/vm/cpu/topology', 'cores'),
				Util.xpathAttribNotNone(self.xml, '/vm/cpu/topology', 'sockets'),
				Util.xpathAttribNotNone(self.xml, '/vm/os', 'type'),
				Util.xpathAttribNotNone(self.xml, '/vm/os/boot', 'dev'),
				Util.xpathNotNone(self.xml, '/vm/os/kernel'),
				Util.xpathNotNone(self.xml, '/vm/os/initrd'),
				Util.xpathNotNone(self.xml, '/vm/os/cmdline'),
				Util.xpathNotNone(self.xml, '/vm/high_availability/enabled'),
				Util.xpathNotNone(self.xml, '/vm/high_availability/priority'),
				Util.xpathNotNone(self.xml, '/vm/display/type'),
				Util.xpathNotNone(self.xml, '/vm/timezone'),
				Util.xpathNotNone(self.xml, '/vm/domain'),
				Util.xpathNotNone(self.xml, '/vm/stateless'),
				Util.xpathNotNone(self.xml, '/vm/placement_policy/affinity'),
				Util.xpathNotNone(self.xml, '/vm/memory_policy/guaranteed'),
				Util.xpathNotNone(self.xml, '/vm/usb/enabled'),
				)
			
		success = sites.put('/vms/%s' % (uuid), 'secondary', payload)

		return uuid

class Disk:

	xml = None
	plan = None

	# attributes considered, only those which are accessible from the UI
	# TODO, include 'type' back
	#attrs_c = ['size', 'type', 'interface', 'bootable', 'wipe_after_delete']
	attrs_c = ['size', 'interface', 'bootable', 'wipe_after_delete']

	def __init__(self, uuid, plan):
		self.xml = etree.parse("%s/%s.xml" % (_dir, uuid))
		self.plan = plan

	def getName(self):
		original_name = self.xml.xpath('/disk/name')[0].text
		return "%s_%s" % (PlanInfo.getTag(plan), original_name)

	def createIfNotExist(self):

		# get all disks
		manyDisks = sites.get('/vms/%s/disks' % (last_vm_uuid), 'secondary')

		matched_disks = []
		# check each disk for matching attributes
		for disk in manyDisks:
			bad_disk = False
			for attr in self.attrs_c:
				if Util.xpathNotNone(disk, attr) != Util.xpathNotNone(self.xml, attr):
					bad_disk = True
					break
			if bad_disk == False:
				matched_disks.append(disk)

		# delete the remaining disks (of which a match wasnt found)
		not_found_disks = set(manyDisks).difference(set(matched_disks))
		for disk in not_found_disks:
			sites.delete('/vms/%s/disks/%s' % (last_vm_uuid, disk.get('id')), 'secondary')
			pass
		
		# if a matching disk wasnt found, create one
		if len(matched_disks) == 0:
			# find out the original storage domain name
			storage_domain_uuid = Util.xpathAttribNotNone(self.xml, 'storage_domains/storage_domain', 'id')
			storage_domain_xml = etree.parse("%s/%s.xml" % (_dir, storage_domain_uuid))
			storage_domain_name = Util.xpathNotNone(storage_domain_xml, '/storage_domain/name')

			nearest_storage_domain_uuid = Util.findNearestNameInXMLList(manyStorageDomains2, storage_domain_xml,
											storage_domain_name, ['type'])

			payload = '''
			<disk>
				<storage_domains>
					<storage_domain id="%s" />
				</storage_domains>
				<size>%s</size>
				<type>%s</type>
				<interface>%s</interface>
				<bootable>%s</bootable>
				<wipe_after_delete>%s</wipe_after_delete>

				<!-- enforcing format cow -->
				<format>cow</format>
				<!-- -->
				
			</disk>
			''' % (
					nearest_storage_domain_uuid,
					Util.xpathNotNone(self.xml, '/disk/size'),
					Util.xpathNotNone(self.xml, '/disk/type'),
					Util.xpathNotNone(self.xml, '/disk/interface'),
					Util.xpathNotNone(self.xml, '/disk/bootable'),
					Util.xpathNotNone(self.xml, '/disk/wipe_after_delete'),
					)

			(success, uuid) = sites.post('/vms/%s/disks' % (last_vm_uuid), 'secondary', payload)
			

class Nic:

	xml = None
	plan = None

	# attributes considered, only those which are accessible from the UI
	attrs_c = ['interface']

	# attributes' attributes considered, only those which are accessible from the UI
	attrs_attrs_c = {'mac': ['address']}

	def __init__(self, uuid, plan):
		self.xml = etree.parse("%s/%s.xml" % (_dir, uuid))
		self.plan = plan

	def getName(self):
		original_name = self.xml.xpath('/nic/name')[0].text
		return "%s_%s" % (PlanInfo.getTag(plan), original_name)

	def createIfNotExist(self):

		# get all nics
		manyNics = sites.get('/vms/%s/nics' % (last_vm_uuid), 'secondary')

		matched_nics = []
		# check each nic for matching attributes
		for nic in manyNics:
			bad_nic = False

			# check 1
			for attr in self.attrs_c:
				if Util.xpathNotNone(nic, attr) != Util.xpathNotNone(self.xml, attr):
					bad_nic = True
					break
					
			# check 2
			for l1 in self.attrs_attrs_c:
				for l2 in self.attrs_attrs_c[l1]:
					if Util.xpathAttribNotNone(nic, l1, l2) != Util.xpathAttribNotNone(self.xml, l1, l2):
						bad_nic = True
						break
				
			if bad_nic == False:
				matched_nics.append(nic)

		# delete the remaining nics (of which a match wasnt found)
		not_found_nics = set(manyNics).difference(set(matched_nics))
		for nic in not_found_nics:
			sites.delete('/vms/%s/nics/%s' % (last_vm_uuid, nic.get('id')), 'secondary')
			pass

		# if a matching nic wasnt found, create one
		if len(matched_nics) == 0:
			# find out the original network name
			network_uuid = Util.xpathAttribNotNone(self.xml, '/nic/network', 'id')
			network_xml = etree.parse("%s/%s.xml" % (_dir, network_uuid))
			network_name = Util.xpathNotNone(network_xml, '/network/name')

			nearest_network_uuid = Util.findNearestNameInXMLList(manyNetworks2, network_xml,
											network_name)

			payload = '''
			<nic>
				<interface>%s</interface>
				<name>%s</name>
				<network id="%s">
				</network>
				<mac address="%s" />
			</nic>
			''' % (
					Util.xpathNotNone(self.xml, '/nic/interface'),
					Util.xpathNotNone(self.xml, '/nic/name'),
					nearest_network_uuid,
					self.xml.xpath('/nic/mac')[0].get('address'),
					)

			(success, uuid) = sites.post('/vms/%s/nics' % (last_vm_uuid), 'secondary', payload)


plans = Conf.getPlans()

if Conf.getRefresh() is True:

	# note the time
	_now = datetime.utcnow()

	# create the subdirectory for storing configuration as we grab it
	_dir = '%s/%s' % (Conf.getVar(), str(_now))
	os.makedirs(_dir)

	plan_nr = 0

	for plan in plans:

		plan_nr += 1

		sites = Sites(plan)

		# retrieving information from the primary site

		## initialising an empty graph to store the information soon obtained
		gr = nx.DiGraph()

		## discovering clusters
		manyClusters = sites.get('/clusters')
		# adding these to the graph
		for cluster in manyClusters:
			Util.writeElementToFile(cluster)
			gr.add_node(cluster.get('id'), label='cluster: ' + cluster.xpath('name')[0].text, style='filled',
						fillcolor='green', shape='house', type='cluster')

		## discovering storage domains
		manyStorageDomains = sites.get('/storagedomains')
		# adding these to the graph
		for storagedomain in manyStorageDomains:
			Util.writeElementToFile(storagedomain)
			gr.add_node(storagedomain.get('id'), label='storage: ' + storagedomain.xpath('name')[0].text, style='filled',
						fillcolor='red', shape='ellipse', type='storagedomain')

		## discovering networks
		manyNetworks = sites.get('/networks')
		# adding these to the graph
		for network in manyNetworks:
			Util.writeElementToFile(network)
			gr.add_node(network.get('id'), label='network: ' + network.xpath('name')[0].text, style='filled',
						fillcolor='blue', shape='rectangle', type='network')

		## finding out the cluster id we are after
		cluster = manyClusters.xpath('cluster[name="%s"]' % (PlanInfo.getCl(plan, 'primary')))[0]

		### getting vms under this cluster id
		manyVms = sites.get('/vms')
		vms = manyVms.xpath('vm[cluster[@id="%s"]]' % (cluster.get('id')))

		for vm in vms:

			#### storing this vm into the graph
			Util.writeElementToFile(vm)
			gr.add_node(vm.get('id'), label='vm: ' + vm.xpath('name')[0].text, style='filled', fillcolor='yellow',
						shape='diamond', type='vm')
			# linking it to the cluster
			gr.add_edge(cluster.get('id'), vm.get('id'), color='green')

			#### getting vm's storage details
			manyDisks = sites.get(vm.xpath('link[@rel="disks"]')[0].get('href'))
			for disk in manyDisks:
				Util.writeElementToFile(disk)
				gr.add_node(disk.get('id'), label='disk: ' + disk.xpath('name')[0].text, style='filled', fillcolor='pink',
							shape='ellipse', type='disk')
				# link disk to vm
				gr.add_edge(vm.get('id'), disk.get('id'), color='yellow')
				# link disk to storage domain
				gr.add_edge(disk.xpath('storage_domains/storage_domain')[0].get('id'), disk.get('id'), color='red')

			#### getting vm's network details
			manyNics = sites.get(vm.xpath('link[@rel="nics"]')[0].get('href'))
			for nic in manyNics:
				Util.writeElementToFile(nic)
				gr.add_node(nic.get('id'), label='nic: ' + nic.xpath('name')[0].text, style='filled', fillcolor='cyan',
							shape='rectangle', type='nic')
				# link nic to vm
				gr.add_edge(vm.get('id'), nic.get('id'), color='yellow')
				# link nic to network
				gr.add_edge(nic.xpath('network')[0].get('id'), nic.get('id'), color='blue')

		# cleaning up the graph, removing nodes without any edges
		degrees = gr.degree()
		for node in degrees:
			if degrees[node] == 0:
				gr.remove_node(node)

		## persisting the information
		Util.writeGraphToFile(gr, cluster.xpath('name')[0].text)
        try:
            nx.write_dot(gr, '/tmp/%s.dot' % (str(plan_nr)))
            #nx.write_dot(gr, '/tmp/%s.dot' % cluster.xpath('name')[0].text)
        except:
            # Clearly pygraphviz isn't installed...    
		# deleting the graph from memory for various reasons
		del gr

		# end of `plan` loop

# if Refresh wasn't True, figure out the latest available rvmsr [backup] dump
else:
	rvmsr_dumps = os.listdir(Conf.getVar())
	_now = sorted(rvmsr_dumps).pop()
	_dir = '%s/%s' % (Conf.getVar(), str(_now))

# if asked to, planting the information in the secondary site
if Conf.getReplicate():

	# iterate over the plans again
	for plan in plans:
		sites = Sites(plan)

		# just loading the list of vms (will be used later/elsewhere)
		manyVms2 = sites.get('/vms', 'secondary')

		# just loading the list of storage domains (will be used later/elsewhere)
		manyStorageDomains2 = sites.get('/storagedomains', 'secondary')

		# just loading the list of networks (will be used later/elsewhere)
		manyNetworks2 = sites.get('/networks', 'secondary')

		# check if the tag exists
		manyTags = sites.get('/tags', 'secondary')
		try:
			tag = manyTags.xpath('tag[name="%s"]' % (PlanInfo.getTag(plan)))[0]
		except:
			# create the tag
			payload = '<tag><name>%s</name></tag>' % (PlanInfo.getTag(plan))
			(success, uuid) = sites.post('/tags', 'secondary', payload)

		# load the source (primary site) cluster graph from disk of this plan
		gr = Util.readGraphFromFile(PlanInfo.getCl(plan, 'primary'))

		# obtaining a list of all nodes (with attributes)
		nodes = gr.nodes(data=True)

		# reversing the list before we search for the 'cluster' node
		nodes.reverse()

		for node, attrs in nodes:
			if attrs['type'] == 'cluster':
				# we've found the 'cluster' node
				cluster_uuid = node
				# quit the search
				break

		# iterate over objects

		nodes = dfs_preorder_nodes(gr, cluster_uuid)

		for node in nodes:
			# if the node is of a type we don't want to create, skip it
			if gr.node[node]['type'] in ['cluster', 'storagedomain', 'network']:
				continue
			
			# else continue with checking for existence or creation
			else:
				
				# virtual machines
				if gr.node[node]['type'] == 'vm':
					vobj = Vm(node, plan)
					last_vm_uuid = vobj.createIfNotExist()

				# disks
				elif gr.node[node]['type'] == 'disk':
					vobj = Disk(node, plan)
					last_disk_uuid = vobj.createIfNotExist()

				# nics
				elif gr.node[node]['type'] == 'nic':
					vobj = Nic(node, plan)
					last_nic_uuid = vobj.createIfNotExist()

				else:
					# better skip this unknown object!
					continue

		# see if they exist

		# see if the essential details match

		# update if required

		# else create the object

else:
	# just quit
	pass

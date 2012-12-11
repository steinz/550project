#!/usr/bin/env python

import json
import sys
from message_handler_node import define_message_types, handlesrequest, MessageHandlerNode, NodeMessage
from directory_server_messages import *
import random

LOCALHOST = '127.0.0.1'

class DirectoryServer(MessageHandlerNode):
  def __init__(self, ip=LOCALHOST, port=None):
    MessageHandlerNode.__init__(self, ip, port)
    self.nodes_by_ring = {}

  def load_config(self, config='bootstrap.json'):
    sys.stdout.write('loading bootstrap config\n')
    obj = {}
    with open(config, 'r') as f:
      obj = json.loads(f.read())
    if 'nodes' in obj:
      for node in obj['nodes']:
        ring_id = node['ring_id']
        if node['ring_id'] not in self.nodes_by_ring:
          self.nodes_by_ring[node['ring_id']] = set()
        self.nodes_by_ring[node['ring_id']].add((node['ip'], node['port']))
        sys.stdout.write('  added node %s:%s to ring %d\n' % (node['ip'], node['port'], node['ring_id']))
    sys.stdout.write('bootstrap complete\n')

  @handlesrequest(DirectoryServerJoin)
  def got_join(self, src_ip, src_port, message):
    ring_set = self.nodes_by_ring.get(message.ring_id) 
    primary_ring_contact = random.sample(ring_set, 1)[0]
    ring_set.add((src_ip, src_port))
    sys.stdout.write('added node %s:%s to ring %d\n' % (src_ip, src_port, message.ring_id))
    #sys.stdout.write('  contact %s:%s\n' % (primary_ring_contact[0], primary_ring_contact[1]))
    other_ring_contacts = []
    for ring_id in self.nodes_by_ring:
      if ring_id == message.ring_id:
        continue
      node = random.sample(self.nodes_by_ring.get(ring_id), 1)[0]
      other_ring_contacts.append((ring_id, node[0], node[1]))

    self.send_obj(src_ip, src_port, DirectoryServerJoinContact(
      ring_id = message.ring_id,
      contact_ip = primary_ring_contact[0],
      contact_port = primary_ring_contact[1],
      other_ring_contacts = other_ring_contacts
      )
      )

  @handlesrequest(DirectoryServerLeave)
  def got_leave(self, src_ip, src_port, message):
    ring_set = self.nodes_by_ring[message.ring_id]
    if (src_ip, src_port) in ring_set:
      sys.stdout.write('removed node %s:%s from ring %d\n' % (src_ip, src_port, message.ring_id))
      ring_set.remove((src_ip, src_port))

  @handlesrequest(DirectoryServerGetRingContact)
  def got_get_ring_contact(self, src_ip, src_port, message):
    ring_set = self.nodes_by_ring.get(message.ring_id)
    node = random.sample(ring_set, 1)
    self.send_obj(src_ip, src_port, DirectoryServerReturnRingContact(
      ring_id = message.ring_id,
      contact_ip = node[0],
      contact_port = node[1]      
      )
      )

DirectoryServer.discover_message_handlers()

def directory_server_run(argv):
  def usage(argv):
    print 'usage: %s port' % argv[0]
    return 1

  if len(argv) < 2:
    return usage(argv)

  port = int(argv[1])
  server = DirectoryServer(port=port)
  server.load_config()
  server.listen_loop()
  return 0

if __name__ == '__main__':
  sys.exit(directory_server_run(sys.argv))

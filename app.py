#!/usr/bin/env python

import sys
import json
from node import Node
from keyspace import *

class P2PSocialStore(Node):
  def __init__(self, config, node_id):
    self.config = config
    self.node_id = node_id
    self.node_config = config['nodes'][node_id]
    id = int_to_key(int(self.node_config['id'])) if self.node_config.get('id') != None else None
    Node.__init__(self, ring_id=self.node_config['ring_id'], id=id, ip=self.node_config['ip'], port=self.node_config['port'])
    self.join()    
    self.start()

  def join(self):
    print 'node %s (%s:%s) joining...' % (self.node_id, self.node_config['ip'], self.node_config['port'])
    if self.node_id != '0':
      Node.join(self, self.config['nodes']['0']['ip'], self.config['nodes']['0']['port'])

  #def received_msg(self, contact, obj):
  #  Node.received_msg(self, contact, obj)
  #  print obj

  def new_connection(self, contact):
    print 'now connected to %s' % contact


def read_config(filename='config.json'):
  with open(filename, 'r') as f:
    return json.loads(f.read())

def run(argv):
  if len(argv) < 2:
    print 'usage: %s node_id' % argv[0]
    return 1
  
  node_id = argv[1]

  config = read_config()
  if node_id not in config['nodes']:
    print 'invalid node id'
    return 2

  node_config = config['nodes'][node_id]
  node = P2PSocialStore(config, node_id)
  return 0

if __name__ == '__main__':
  sys.exit(run(sys.argv))

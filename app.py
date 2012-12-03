#!/usr/bin/env python

import sys
import json
from node import Node, rpcmethod
import twisted.internet.reactor

class P2PSocialStore(Node):
  def __init__(self, config, node_id):
    self.config = config
    self.node_id = node_id
    self.node_config = config['nodes'][node_id]
    Node.__init__(self, port=self.node_config['port'])
    self.start()
    self.join()

  def join(self):
    print 'node %s (%s:%s) joining...' % (self.node_id, self.node_config['ip'], self.node_config['port'])
    if self.node_id == '1':
      self.known_node(self.config['nodes']['0']['ip'], self.config['nodes']['0']['port'])

  def new_connection(self, contact):
    contact.test('remote node id: %s' % self.node_id)

    if self.node_id == '1':
      twisted.internet.reactor.callLater(5, self.contacts[self.contacts.keys()[0]].test, 'late')

  @rpcmethod
  def test(self, name):
    print 'hi there! %s' % name

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
  twisted.internet.reactor.run()
  return 0

if __name__ == '__main__':
  sys.exit(run(sys.argv))

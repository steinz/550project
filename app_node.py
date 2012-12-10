from keyspace import *
from node import Node
from multiprocessing import Queue
from Queue import Empty

class P2PSocialStore(Node):
  def __init__(self, config, node_id):
    self.request_queue = None
    self.config = config
    self.node_id = node_id
    self.node_config = config['nodes'][node_id]
    id = int_to_key(int(self.node_config['id'])) if self.node_config.get('id') != None else None
    Node.__init__(self,
      ring_id=self.node_config['ring_id'],
      id=id,
      ip=self.node_config['ip'],
      port=self.node_config['port'])
    self.join()

  def start(self, request_queue):
    self.request_queue = request_queue
    Node.start(self)

  def about_to_receive(self):
    Node.about_to_receive(self)

    # handle anything in queue
    try:
      request = self.request_queue.get(block=False)
      request.execute(self)
    except Empty:
      pass

  def join(self):
    print 'node %s (%s:%s) joining...' % (self.node_id, self.node_config['ip'], self.node_config['port'])
    if self.node_id != '0':
      Node.join(self, self.config['nodes']['0']['ip'], self.config['nodes']['0']['port'])


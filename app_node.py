from multiring_node import MultiRingNode

# Queue for REPL client to communicate with network process
from multiprocessing import Queue
from Queue import Empty

class AppNode(MultiRingNode):
  """
   Application-level node
   Our sample application provides a REPL client
   to let the user submit raw commands to the DHT
  """

  def __init__(self, config, node_id):
    self.request_queue = None
    self.config = config
    self.node_id = node_id
    self.node_config = config['nodes'][node_id]
    id = int_to_key(int(self.node_config['id'])) if self.node_config.get('id') != None else None
    ring_id = self.node_config['ring_id']
    MultiRingNode.__init__(self,
      ring_id = self.node_config['ring_id'],
      id = id,
      ip = self.node_config['ip'],
      port = self.node_config['port'],
      user_id = self.node_config['user_id'],
      directory_server_ip = config['directory_servers'][str(ring_id)]['ip'],
      directory_server_port = config['directory_servers'][str(ring_id)]['port']
      )    

  def start(self, request_queue):
    self.request_queue = request_queue
    self.join_ring()
    self.listen_loop()

  def about_to_receive(self):
    MultiRingNode.about_to_receive(self)

    # handle anything in queue
    try:
      request = self.request_queue.get(block=False)
      request.execute(self)
    except Empty:
      pass


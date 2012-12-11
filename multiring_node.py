from node import Node

REPLICATION_FACTOR = 3


class MultiRingNode(Node):
  """
   A chord node, but also supports cross-ring operations
   (e.g. fast get by looking up multiple physical keys in parallel)
  """
  
  def __init__(self, ring_id=None, id=None, ip=None, port=None, user_id=None):
    """
     ring_id: primary ring for this node
       (e.g. nodes in USA are all in the same ring => have the same ring_id)
     id: used to define which part of the keyspace this node owns
     ip, port
     user_id: application-level user identifier used to provide basic consistency
    """
    Node.__init__(
      self,
      ring_id = ring_id,
      id = id,
      ip = ip,
      port = port,
      user_id = user_id
      )

  def multiple_append(self, key, value, callback, requires):
    """
     append to all physical keys representing the same virtual key
     call the callback when data is replicated everywhere

     detail: some physical keys should be in different rings
             to guarantee availability
    """
    pass

  def multiple_get(self, key, value, callback):
    """
     send get requests to all physical keys representing the same virtual key
     call the callback with data from the one that responds first
    """
    pass

from node import Node
from contact import Contact
from directory_server_messages import *
from console_format import *
from message_handler_node import MessageHandlerNode, handlesrequest

REPLICATION_FACTOR = 3

class MultiRingNode(Node):
  """
   A chord node, but also supports cross-ring operations
   (e.g. fast get by looking up multiple physical keys in parallel)
  """
  
  def __init__(self,
    ring_id=None,
    id=None,
    ip=None, port=None,
    user_id=None,
    directory_server_ip=None, directory_server_port=None
    ):
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
    self.directory_server_ip = directory_server_ip
    self.directory_server_port = directory_server_port

  def received_obj(self, ip, port, obj):
    if isinstance(obj, DirectoryServerMessage):
      MessageHandlerNode.received_obj(self, ip, port, obj)
    else:
      Node.received_obj(self, ip, port, obj)

  def send_to_directory_server(self, obj):
    self.send_obj(self.directory_server_ip, self.directory_server_port, obj)  

  def join_ring(self):
    sys.stdout.write(color('called join_ring\n', 'red', bold=True))
    self.send_to_directory_server(DirectoryServerJoin(ring_id=self.ring_id))

  @handlesrequest(DirectoryServerJoinContact)
  def got_directory_server_join_contact(self, ip, port, obj):
    sys.stdout.write(color('got contact from directory server: %s:%s\n' % (obj.contact_ip, obj.contact_port), 'red', bold=True))

    for (ring_id, contact_ip, contact_port) in obj.other_ring_contacts:
      self.contacts.add(
        Contact(
          ring_id = ring_id,
          ip = contact_ip,
          port = contact_port,
          network_protocol = self
          )
        )

    # here ip, port are the directory server
    self.join(obj.contact_ip, obj.contact_port)

  def leave_ring(self):
    self.send_to_directory_server(DirectoryServerLeave(ring_id=self.ring_id))
    self.leave()

  def get_directory_server_contact(self, ring_id, callback):
    request_id = self.callback_manager.register(callback)
    self.send_to_directory_server(
      DirectoryServerGetRingContact(
        ring_id = ring_id,
        request_id = request_id
        )
      )
    return request_id

  @handlesrequest(DirectoryServerReturnRingContact)
  def got_directory_server_contact(self, ip, port, obj):
    # here ip, port are the directory server
    contact = Contact(
      ring_id = obj.ring_id,
      ip = obj.contact_ip,
      port = obj.contact_port,
      network_protocol=self
      )
    self.contacts.add(contact)
    self.callback_manager.call(obj.request_id, contact)

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

MultiRingNode.discover_message_handlers()

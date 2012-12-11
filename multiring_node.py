from dht_node import DHTNode
from contact import Contact
from directory_server_messages import *
from console_format import *
from message_handler_node import MessageHandlerNode, handlesrequest

LOCAL_REPLICAS = 3
REMOTE_REPLICAS = 1

class MultiRingNode(DHTNode):
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
    DHTNode.__init__(
      self,
      ring_id = ring_id,
      id = id,
      ip = ip,
      port = port,
      user_id = user_id
      )
    self.directory_server_ip = directory_server_ip
    self.directory_server_port = directory_server_port

    # map single-operation request_ids to multiple-operation request_ids
    self.multiple_request_map = {}

  def received_obj(self, ip, port, obj):
    if isinstance(obj, DirectoryServerMessage):
      MessageHandlerNode.received_obj(self, ip, port, obj)
    else:
      DHTNode.received_obj(self, ip, port, obj)

  def send_to_directory_server(self, obj):
    self.send_obj(self.directory_server_ip, self.directory_server_port, obj)  

  def join_ring(self):
    self.send_to_directory_server(DirectoryServerJoin(ring_id=self.ring_id))

  @handlesrequest(DirectoryServerJoinContact)
  def got_directory_server_join_contact(self, ip, port, obj):
    #sys.stdout.write('%s\n' % str(obj.__dict__))
    sys.stdout.write('got contact from directory server: %s:%s\n' % (
      obj.contact_ip, obj.contact_port))

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

  def leave_ring(self, callback):
    self.send_to_directory_server(
      DirectoryServerLeave(
        ring_id = self.ring_id
        )
      )
    self.leave(callback)

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

  def iter_physical_keys(self, virtual_key, ring_id=None):
    ring_id = (ring_id if ring_id != None else self.ring_id)
    replicas = (LOCAL_REPLICAS if ring_id == self.ring_id else REMOTE_REPLICAS)
    for i in xrange(replicas):
      physical_key = '%d_%s' % (i, virtual_key)
      yield physical_key

  def multiple_append(self, key, value, requires=None, one_completed_callback=None, all_completed_callback=None):
    """
     append to all physical keys representing the same virtual key
     call the callback when data is replicated everywhere

     detail: some physical keys should be in different rings
             to guarantee availability
    """
    multiple_append_request_id = self.callback_manager.register(all_completed_callback)
    multiple_append_data = self.callback_manager.get_data(multiple_append_request_id)
    multiple_append_data['virtual_key'] = key
    multiple_append_data['one_completed_callback'] = one_completed_callback

    # store append responses (e.g. to compare resulting version numbers)
    multiple_append_data['keys'] = {self.ring_id:{}}    

    # local ring
    for physical_key in self.iter_physical_keys(key):
      multiple_append_data['keys'][self.ring_id][physical_key] = None
      request_id = self.append(
        key = physical_key,
        value = value,
        callback = self.single_append_callback,
        requires = requires
        )
      data = self.callback_manager.get_data(request_id)
      data['physical_key'] = physical_key
      self.multiple_request_map[request_id] = multiple_append_request_id

    # remote rings
    for ring_id in self.contacts.iter_other_rings():
      multiple_append_data['keys'][ring_id] = {}
      for physical_key in self.iter_physical_keys(key, ring_id = ring_id):
        multiple_append_data['keys'][ring_id][physical_key] = None
        request_id = self.append(
          key = physical_key,
          value = value,
          callback = self.single_append_callback,
          requires = requires,
          ring_id = ring_id
          )
        data = self.callback_manager.get_data(request_id)
        data['physical_key'] = physical_key
        self.multiple_request_map[request_id] = multiple_append_request_id

    return multiple_append_request_id

  def single_append_callback(self, request_id, (node, contact, version)):
    multiple_append_request_id = self.multiple_request_map.get(request_id)
    if multiple_append_request_id == None:
      # too bad
      return
    del self.multiple_request_map[request_id]
    data = self.callback_manager.get_data(request_id)
    multiple_append_data = self.callback_manager.get_data(multiple_append_request_id)
    multiple_append_data['keys'][contact.ring_id][data['physical_key']] = (contact, version)
    
    if multiple_append_data['one_completed_callback'] != None:
      multiple_append_data['one_completed_callback'](
        multiple_append_request_id,
        (self, multiple_append_data, contact, data['physical_key'])
        )

    # have all appends completed? if so, call callback
    all_completed = True
    for ring_id in multiple_append_data['keys']:
      for physical_key in multiple_append_data['keys'][ring_id]:
        if multiple_append_data['keys'][ring_id][physical_key] == None:
          all_completed = False

    if all_completed:
      self.callback_manager.call(multiple_append_request_id, (self, multiple_append_data))


  def multiple_get(self, key, one_completed_callback=None, all_completed_callback=None):
    """
     send get requests to all physical keys representing the same virtual key
     call the callback with data from the one that responds first
    """
    multiple_get_request_id = self.callback_manager.register(all_completed_callback)
    multiple_get_data = self.callback_manager.get_data(multiple_get_request_id)
    multiple_get_data['virtual_key'] = key
    multiple_get_data['one_completed_callback'] = one_completed_callback

    # store get responses
    multiple_get_data['keys'] = {self.ring_id:{}}    

    # local ring
    for physical_key in self.iter_physical_keys(key):
      multiple_get_data['keys'][self.ring_id][physical_key] = None
      request_id = self.get(
        key = physical_key,
        callback = self.single_get_callback,
        )
      data = self.callback_manager.get_data(request_id)
      data['physical_key'] = physical_key
      self.multiple_request_map[request_id] = multiple_get_request_id

    # remote rings
    for ring_id in self.contacts.iter_other_rings():
      multiple_get_data['keys'][ring_id] = {}
      for physical_key in self.iter_physical_keys(key, ring_id = ring_id):
        multiple_get_data['keys'][ring_id][physical_key] = None
        request_id = self.get(
          key = physical_key,
          callback = self.single_get_callback,
          ring_id = ring_id
          )
        data = self.callback_manager.get_data(request_id)
        data['physical_key'] = physical_key
        self.multiple_request_map[request_id] = multiple_get_request_id

    return multiple_get_request_id

  def single_get_callback(self, request_id, (node, contact, key, value)):
    multiple_get_request_id = self.multiple_request_map.get(request_id)
    if multiple_get_request_id == None:
      # too bad
      return
    del self.multiple_request_map[request_id]
    data = self.callback_manager.get_data(request_id)
    multiple_get_data = self.callback_manager.get_data(multiple_get_request_id)
    multiple_get_data['keys'][contact.ring_id][data['physical_key']] = (contact, value)
    
    if multiple_get_data['one_completed_callback'] != None:
      multiple_get_data['one_completed_callback'](
        multiple_get_request_id,
        (self, multiple_get_data, contact, data['physical_key'])
        )

    # have all gets completed? if so, call callback
    all_completed = True
    for ring_id in multiple_get_data['keys']:
      for physical_key in multiple_get_data['keys'][ring_id]:
        if multiple_get_data['keys'][ring_id][physical_key] == None:
          all_completed = False

    if all_completed:
      self.callback_manager.call(multiple_get_request_id, (self, multiple_get_data))

MultiRingNode.discover_message_handlers()

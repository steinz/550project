#!/usr/bin/env python

import json
import math
import sys

from console_format import *

from keyspace import *
from contact import Contact
from chord_contacts import MultiRingChordContacts
from callback_manager import CallbackManager
from vector_version import VectorVersion
from message_handler_node import define_message_types, handlesrequest, MessageHandlerNode, NodeMessage

class DHTNodeMessage(NodeMessage):
  pass

message_types = [
  'ForwardMessage',
  'PingMessage', 'PingResponse',
  'JoinMessage', 'JoinResponse',
  'StabilizeGetSuccessorPredecessor', 'StabilizeGotSuccessorPredecessor', 'StabilizeNotify',  
  'FindMessage', 'FindResponse',
  'GetMessage', 'GetResponse',  
  'AppendMessage', 'AppendResponse',
  'LeaveGetPredecessorSuccessor', 'LeaveMessage', 'LeaveResponse', 'LeaveUpdateSuccessor'
]
define_message_types(
  sys.modules[__name__],
  message_types,
  lambda name, parent, members:type(name, (DHTNodeMessage,), members)
  )

def message_to_string(msg):
  d = msg.__dict__.copy()
  if 'id' in d:
    d['id'] = key_to_int(d['id'])
  return '%s: %s' % (msg.__class__.__name__, str(d))

class DHTNode(MessageHandlerNode):
  """
   Chord node

   Messages sent recursively
   Maintains an in-memory dict of node data
   Shuffles data when nodes join and leave
   Provides some consistency through vector version numbers
  """

  def __init__(self, ring_id=1, id=None, ip='127.0.0.1', port=8080, user_id=None):
    MessageHandlerNode.__init__(self, ip, port)
    self.set_timeout(0.25)
    self.id = id if id != None else random_key()
    sys.stdout.write('me: %s\n' % key_to_int(self.id))
    self.ring_id = ring_id
    self.user_id = user_id
    self.contacts = MultiRingChordContacts(Contact(ring_id=ring_id, id=self.id, ip=self.ip, port=self.port, network_protocol=self))
    self.messages_received = 0
    self.message_limit = None

    # self.data['physical_key'] = { 'data': value, 'version': VectorClock(), 'requires': VectorClock() }
    self.data = {}
    self.callback_manager = CallbackManager()
    self.next_finger_to_fix = 1

  def about_to_receive(self):
    pass

  def received_obj(self, ip, port, obj):
    if not isinstance(obj, DHTNodeMessage):
      return
    contact = Contact(ring_id=obj.ring_id, id=obj.id, ip=ip, port=port, network_protocol=self)
    if hasattr(obj, 'join') and obj.join:
      # this node is trying to join
      # don't include it in any finger tables for now      
      pass
    else:
      self.add_contact(contact)
    sys.stdout.write('received %s from %s\n' % (message_to_string(obj), contact))
    self.received_msg(contact, obj)

  def received_msg(self, contact, obj):
    self.messages_received += 1
    if self.message_limit and (self.messages_received >= self.message_limit):
      sys.stdout.write('reached message limit\n')
      sys.exit(1)
    # figure out which function handles this message type
    handler = DHTNode.get_message_handler(obj)
    if handler:
      # invoke handler
      handler(self, contact, obj)

  def add_contact(self, contact):
    self.contacts.add(contact)
  
  def remove_contact(self, contact_id):
    del self.contacts[contact_id]

  def owns_key(self, key):
    return keyspace_compare(self.id, self.contacts.get_successor().id, key)  

# JOIN
  def join(self, ip, port):
    dummy_contact = Contact(ring_id=None, id=None, ip=ip, port=port, network_protocol=self)
    request_id = self.callback_manager.register(self.join_response_callback)
    dummy_contact.send(JoinMessage(request_id=request_id, join=True))
    return request_id

  @handlesrequest(JoinMessage)
  def got_join_message(self, contact, obj):
    if not self.owns_key(contact.id):
      self.forward(
        key = contact.id,
        raw_key = True,
        message = obj,
        requester = contact
      )
      return

    old_successor = self.contacts.get_successor()

    critical_data = {}
    for key in self.data:
      if keyspace_compare(contact.id, old_successor.id, string_to_key(key)):
        critical_data[key] = self.value_to_wire(self.data[key])

    # data shuffle: potentially large message
    contact.send(
      JoinResponse(
        request_id = obj.request_id,
        successor = old_successor.to_tuple(),
        data = critical_data
        )
      )
    self.contacts.set_successor(contact)

  @handlesrequest(JoinResponse)
  def got_join_response(self, contact, obj):
    for key in obj.data:
      self.data[key] = self.value_from_wire(obj.data[key])
    self.callback_manager.call(obj.request_id, (contact, obj))
    self.stabilize()

  def join_response_callback(self, request_id, (contact, obj)):
    self.contacts.set_successor(Contact.from_tuple(obj.successor, self))
    self.contacts.set_predecessor(contact)
    sys.stdout.write('joined!\n')
    sys.stdout.write('  predecessor: %s\n' % key_to_int(self.contacts.get_predecessor().id))
    sys.stdout.write('  me: %s\n' % key_to_int(self.id))
    sys.stdout.write('  successor: %s\n' % key_to_int(self.contacts.get_successor().id))

# LEAVE
  def leave(self, callback):
    leave_request_id = self.callback_manager.register(callback)
    previous_key = key_subtract_circular(self.id, int_to_key(1))
    sys.stdout.write(color('find(%s)\n' % key_to_int(previous_key), 'red', bold=True))
    find_request_id = self.find(previous_key, callback=self.leave_got_predecessor, raw_key=True)
    find_request_data = self.callback_manager.get_data(find_request_id)
    find_request_data['leave_request_id'] = leave_request_id
    return leave_request_id

  def leave_got_predecessor(self, find_request_id, contact):
    sys.stdout.write(color('predecessor is %s\n' % contact, 'red', bold=True))
    find_request_data = self.callback_manager.get_data(find_request_id)

    critical_data = {}
    for key in self.data:
      raw_key = string_to_key(key)
      if keyspace_compare(self.id, self.contacts.get_successor().id, raw_key):
        critical_data[key] = self.value_to_wire(self.data[key])
      else:
        # cached data of which we are not the owner
        # can be discarded
        pass

    # data shuffle: potentially large message
    contact.send(
      LeaveMessage(
        data = critical_data,
        successor = self.contacts.get_successor().to_tuple(),
        request_id = find_request_data['leave_request_id']
        )
      )
    # HACK: don't accept any more messages
    self.id = self.contacts.get_successor().id

  @handlesrequest(LeaveMessage)
  def got_leave_message(self, contact, obj):
    # special case: only one node in the system
    if contact == self.contacts.me():
      # don't bother doing anything, just call the callback
      self.callback_manager.call(obj.request_id, contact)
      return

    for key in obj.data:
      self.data[key] = self.value_from_wire(obj.data[key])
    self.contacts.set_successor(Contact.from_tuple(obj.successor, self))
    self.contacts.get_successor().send(LeaveUpdateSuccessor(leaving=contact.to_tuple()))
    self.contacts.remove(contact)
    contact.send(LeaveResponse(request_id=obj.request_id))

  @handlesrequest(LeaveResponse)
  def got_leave_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, contact)

  @handlesrequest(LeaveUpdateSuccessor)
  def got_leave_update_successor(self, contact, obj):
    self.contacts.remove(Contact.from_tuple(obj.leaving, self))
    self.contacts.set_predecessor(contact)

# STABILIZE
  def stabilize(self):
    self.contacts.get_successor().send(StabilizeGetSuccessorPredecessor())

  @handlesrequest(StabilizeGetSuccessorPredecessor)
  def stabilize_get_successor_predecessor(self, contact, obj):
    contact.send(StabilizeGotSuccessorPredecessor(predecessor=self.contacts.get_predecessor().to_tuple()))

  @handlesrequest(StabilizeGotSuccessorPredecessor)
  def stabilize_got_successor_predecessor(self, contact, obj):
    x = Contact.from_tuple(obj.predecessor, self)
    if keyspace_compare(self.id, self.contacts.get_successor().id, x.id):
      self.contacts.set_successor(x)
    self.contacts.get_successor().send(StabilizeNotify())

  @handlesrequest(StabilizeNotify)
  def stabilize_notify(self, contact, obj):
    if not self.contacts.get_predecessor() or \
       keyspace_compare(self.contacts.get_predecessor().id, self.id, contact.id):
      self.contacts.set_predecessor(contact)

# FINGERS
  def fix_fingers(self):
    self.next_finger_to_fix += 1
    if self.next_finger_to_fix >= math.log(keyspace_size(), 2):
      self.next_finger_to_fix = 1
    self.find(keyspace_add_circular(self.id, 2**(self.next_finger_to_fix-1)), self.fix_finger_callback)
    data = self.callback_manager.get_data(request_id)
    data['next'] = self.next_finger_to_fix

  def fix_finger_callback(self, request_id, contact):
    data = self.callback_manager.get_data(request_id)
    self.contacts[data['next']] = contact

  def check_predecessor(self):
    if not self.get_predecessor():
      return
    request_id = self.ping(self.get_predecessor(), self.predecessor_ping_response)
    data = self.callback_manager.get_data(request_id)
    # TODO: set timeout and expire predecessor
    # if we don't hear back from the ping within the timeout interval
    
  def predecessor_ping_response(self, request_id, contact):
    data = self.callback_manager.get_data(request_id)
    # TODO: cancel timeout previously set


# PING
  def ping(self, contact, callback):
    request_id = self.callback_manager.register(callback)
    contact.send(PingMessage(request_id=request_id))
    return request_id

  @handlesrequest(PingMessage)
  def got_ping(self, contact, obj):
    contact.send(PingResponse(request_id=obj.request_id))

  @handlesrequest(PingResponse)
  def got_ping_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, contact)

  def resolve_key(self, key, raw_key=False):
    return (key if raw_key else string_to_key(key))

# FORWARD
  def forward(self, key, message, raw_key=False, requester=None, ring_id=None):
    ring_id = (ring_id if ring_id != None else self.ring_id)
    test_key = self.resolve_key(key, raw_key)
    requester = (requester if requester else self.contacts.me())

    if ring_id == self.ring_id:
      if self.owns_key(test_key):
        message.key = key
        message.raw_key = raw_key
        self.received_msg(requester, message)
      elif ring_id == self.ring_id:
        contact = self.contacts.nearest_contact_less_than(test_key)
        #sys.stdout.write(color('nearest contact to %s: %s\n' % (key_to_int(test_key), contact), 'red', bold=True))
        contact.send(
          ForwardMessage(
            key = key,
            raw_key = raw_key,
            message = message,
            requester = requester.to_tuple()
            )
          )
    else:
      contact = self.contacts.get_ring_contact(ring_id)
      contact.send(
        ForwardMessage(
          key = key,
          raw_key = raw_key,
          message = message,
          requester = requester.to_tuple()
          )
        )

  @handlesrequest(ForwardMessage)
  def got_forward_message(self, contact, obj):
    self.forward(
      key=obj.key,
      message=obj.message,
      raw_key=obj.raw_key,
      requester=Contact.from_tuple(obj.requester, self)
      )

  def get_requester(self, contact, obj):
    if hasattr(obj, 'requester'):
      return Contact.from_tuple(obj.requester, self)
    return contact


# FIND(raw key: 20 bytes)
  def find(self, key, callback, raw_key=True, ring_id=None):
    request_id = self.callback_manager.register(callback)
    self.forward(
      key = key,
      raw_key = raw_key,
      message = FindMessage(request_id = request_id),
      ring_id = ring_id
      )
    return request_id
    
  @handlesrequest(FindMessage)
  def got_find(self, contact, obj):
    requester = self.get_requester(contact, obj)
    contact.send(
      FindResponse(
        request_id = obj.request_id,
        key = obj.key,
        raw_key = obj.raw_key
        )
      )

  @handlesrequest(FindResponse)
  def got_find_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, contact)

# GET(physical key: a string)
  def get(self, key, callback, ring_id=None):
    request_id = self.callback_manager.register(callback)
    self.forward(
      key = key,
      raw_key = False,
      message = GetMessage(request_id = request_id),
      ring_id = ring_id
      )
    return request_id

  @handlesrequest(GetMessage)
  def got_get_message(self, contact, obj):
    key = self.resolve_key(obj.key, obj.raw_key)
    contact.send(
      GetResponse(
        request_id = obj.request_id,
        key = obj.key,
        value = self.value_to_wire(self.data[key])
        )
      )

  @handlesrequest(GetResponse)
  def got_get_response(self, contact, obj):
    self.callback_manager.call(
      obj.request_id,
      (self, contact, obj.key, self.value_from_wire(obj.value))
      )

# TODO: PUT, DELETE?

  def value_to_wire(self, data):
    return {
      'data': data['data'],
      'requires': data['requires'].to_tuples(),
      'version': data['version'].to_tuples(),
    }

  def value_from_wire(self, data):
    return {
      'data': data['data'],
      'requires': VectorVersion.from_tuples(data['requires']),
      'version': VectorVersion.from_tuples(data['version']),
    }

# APPEND(physical key: a string, value: anything)
  def append(self, key, value, callback, requires=None, ring_id=None):
    request_id = self.callback_manager.register(callback)
    self.forward(
      key = key,
      raw_key = False,
      message = AppendMessage(
        request_id = request_id,
        value = value,
        requires = (requires.to_tuples() if requires else []),
        user_id = self.user_id
        ),
      ring_id = ring_id
      )
    return request_id

  @handlesrequest(AppendMessage)
  def got_append(self, contact, obj):
    key = self.resolve_key(obj.key, obj.raw_key)
    if key not in self.data:
      self.data[key] = {
        'data': [],
        'requires': VectorVersion(),
        'version': VectorVersion(),
      }
    self.data[key]['data'].append(obj.value)
    self.data[key]['requires'].merge(VectorVersion.from_tuples(obj.requires))
    self.data[key]['version'].increment(obj.user_id)

    contact.send(
      AppendResponse(
        request_id = obj.request_id,
        version = self.data[key]['version'].to_tuples()
        )
      )

  @handlesrequest(AppendResponse)
  def got_append_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, (self, contact, obj.version))

# discover functions in Node with the handlesrequest decorator
# store a dict of message_type => handler function
DHTNode.discover_message_handlers()


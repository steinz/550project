#!/usr/bin/env python

import inspect
import json
import math
import sys

from keyspace import *
from contact import Contact
from chord_contacts import MultiRingChordContacts
from buffered_udp_listener import BufferedUDPListener
from callback_manager import CallbackManager

class NodeMessage(object):
  def __init__(self, **kwargs):
    for key in kwargs:
      setattr(self, key, kwargs[key])

message_types = [
  'JoinMessage', 'JoinProxyMessage', 'JoinResponse',
  'StabilizeGetSuccessorPredecessor', 'StabilizeGotSuccessorPredecessor', 'StabilizeNotify',
  'PingMessage', 'PingResponse',
  'ForwardMessage',
  'FindMessage', 'FindResponse',
  'GetMessage', 'GetResponse',  
  'AppendMessage', 'AppendResponse'
]

for x in message_types:
  obj = type(x, (NodeMessage,), {})
  setattr(sys.modules[__name__], x, obj)

def message_to_string(msg):
  d = msg.__dict__.copy()
  if 'id' in d:
    d['id'] = key_to_int(d['id'])
  return '%s: %s' % (msg.__class__.__name__, str(d))

def handlesrequest(message_type):
  def decorator(func):
    func.message_type = message_type
    return func
  return decorator

class Node(BufferedUDPListener):
  def __init__(self, ring_id=1, id=None, ip='127.0.0.1', port=8080):
    BufferedUDPListener.__init__(self, ip, port)
    self.set_timeout(0.25)
    self.id = id if id != None else random_key()
    print 'me: %s' % key_to_int(self.id)
    self.ring_id = ring_id
    self.contacts = MultiRingChordContacts(Contact(ring_id=ring_id, id=self.id, ip=self.ip, port=self.port, network_protocol=self))

    # self.data['virtual_key'] = { 'data': value, 'version': vector_clock, 'requires': vector_clock, 'publisher': ? }
    self.data = {}
    self.callback_manager = CallbackManager()
    self.next_finger_to_fix = 1
    self.shutdown = False

  def about_to_receive(self):
    pass

  def received_obj(self, ip, port, obj):
    contact = Contact(ring_id=obj.ring_id, id=obj.id, ip=ip, port=port, network_protocol=self)
    if hasattr(obj, 'join') and obj.join:
      # this node is trying to join
      # don't include it in any finger tables for now      
      pass
    else:
      self.add_contact(contact)
    print 'received %s from %s' % (message_to_string(obj), contact)
    self.received_msg(contact, obj)

  def received_msg(self, contact, obj):
    handler = Node.message_handlers.get(obj.__class__)
    if handler:
      handler(self, contact, obj)

  def start(self):
    self.listen_loop()

  def add_contact(self, contact):
    existing = self.contacts.add(contact)
    #if not existing:
    #  print 'added new contact %s' % contact
    #  self.new_connection(contact)
  
  def remove_contact(self, contact_id):
    del self.contacts[contact_id]

  def owns_key(self, key):
    return keyspace_compare(self.id, self.contacts.get_successor().id, key)  

# JOIN
  def join(self, ip, port):
    dummy_contact = Contact(ring_id=None, id=None, ip=ip, port=port, network_protocol=self)
    request_id = self.callback_manager.register(self.join_response_callback)
    dummy_contact.send(JoinMessage(request_id=request_id, join=True))

  @handlesrequest(JoinMessage)
  def got_join_message(self, contact, obj):
    print 'got join request from %s' % contact
    if self.owns_key(contact.id):

      old_successor = self.contacts.get_successor()

      # TODO: fix this so that we only set successor
      # after data has been pushed to the new node
      self.contacts.set_successor(contact)

      contact.send(JoinResponse(request_id=obj.request_id, successor=old_successor.to_tuple()))
      return
    else:
      # Figure out who the node should be talking to to join
      request_id = self.find(contact.id, self.join_correct_node_callback)
      data = self.callback_manager.get_data(request_id)
      data['joiner'] = contact
      data['request_id'] = obj.request_id

  def join_correct_node_callback(self, request_id, contact):
    data = self.callback_manager.get_data(request_id)
    contact.send(JoinProxyMessage(request_id=data['request_id'], joiner=data['joiner'].to_tuple()))

  @handlesrequest(JoinProxyMessage)
  def got_join_proxy_message(self, contact, obj):
    joiner = Contact.from_tuple(obj.joiner, self)
    self.got_join_message(joiner, obj)

  @handlesrequest(JoinResponse)
  def got_join_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, (contact, obj))
    self.stabilize()

  def join_response_callback(self, request_id, (contact, obj)):
    self.contacts.set_successor(Contact.from_tuple(obj.successor, self))
    self.contacts.set_predecessor(contact)
    print 'joined!'
    print '  predecessor: %s' % key_to_int(self.contacts.get_predecessor().id)
    print '  me: %s' % key_to_int(self.id)
    print '  successor: %s' % key_to_int(self.contacts.get_successor().id)


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

  def forward(self, key, message, raw_key=False, requester=None):
    test_key = self.resolve_key(key, raw_key)
    requester = (requester if requester else self.contacts.me())
    if self.owns_key(test_key):
      message.key = key
      message.raw_key = raw_key
      self.received_msg(requester, message)
    else:
      contact = self.contacts.nearest_contact_less_than(test_key)
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
  def find(self, key, callback, raw_key=True):
    request_id = self.callback_manager.register(callback)
    self.forward(
      key = key,
      raw_key = raw_key,
      message = FindMessage(request_id = request_id),
      )
    return request_id
    
  @handlesrequest(FindMessage)
  def got_find(self, contact, obj):
    requester = self.get_requester(contact, obj)
    contact.send(FindResponse(request_id=obj.request_id, key=obj.key, raw_key=obj.raw_key))

  @handlesrequest(FindResponse)
  def got_find_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, contact)

# GET(physical key: a string)
  def get(self, key, callback):
    request_id = self.callback_manager.register(callback)
    self.forward(
      key = key,
      raw_key = False,
      message = GetMessage(request_id = request_id)
      )
    return request_id

  @handlesrequest(GetMessage)
  def got_get_message(self, contact, obj):
    key = self.resolve_key(obj.key, obj.raw_key)
    value = self.data.get(key)
    contact.send(
      GetResponse(
        request_id = obj.request_id,
        key = obj.key,
        value = value
        )
      )

  @handlesrequest(GetResponse)
  def got_get_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, (contact, obj.key, obj.value))

# TODO: PUT, DELETE?

# APPEND(physical key: a string, value: anything)
  def append(self, key, value, callback):
    request_id = self.callback_manager.register(callback)
    self.forward(
      key = key,
      raw_key = False,
      message = AppendMessage(
        request_id = request_id,
        value = value
        )
      )

  @handlesrequest(AppendMessage)
  def got_append(self, contact, obj):
    key = self.resolve_key(obj.key, obj.raw_key)
    if key not in self.data:
      self.data[key] = {
        'data': [],
        'requires': None,
        'version': None
      }
    self.data[key]['data'].append(obj.value)
    print json.dumps(self.data[key], indent=2)
    contact.send(AppendResponse(request_id=obj.request_id))

  @handlesrequest(AppendResponse)
  def got_append_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, contact)

  
# put message handlers into a dict
Node.message_handlers = {}
for name in dir(Node):
  x = getattr(Node, name)
  if not inspect.ismethod(x) or not hasattr(x, 'message_type'):
    continue
  message_type = getattr(x, 'message_type')
  if not issubclass(message_type, NodeMessage):
    continue
  Node.message_handlers[message_type] = x


#!/usr/bin/env python

from keyspace import *
from contact import Contact
from chord_contacts import MultiRingChordContacts
from buffered_udp_listener import BufferedUDPListener
from callback_manager import CallbackManager

class NodeMessage(object):
  def __init__(self, **kwargs):
    for key in kwargs:
      setattr(self, key, kwargs[key])

class PingMessage(NodeMessage):
  pass
class PingResponse(NodeMessage):
  pass
class FindMessage(NodeMessage):
  pass
class FindResponse(NodeMessage):
  pass
class GetMessage(NodeMessage):
  pass
class GetResponse(NodeMessage):
  pass
class JoinMessage(NodeMessage):
  pass
class JoinProxyMessage(NodeMessage):
  pass
class JoinResponse(NodeMessage):
  pass

def message_to_string(msg):
  d = msg.__dict__.copy()
  if 'id' in d:
    d['id'] = key_to_int(d['id'])
  return '%s: %s' % (msg.__class__.__name__, str(d))

class Node(BufferedUDPListener):
  

  def __init__(self, ring_id=1, id=None, ip='127.0.0.1', port=8080):
    BufferedUDPListener.__init__(self, ip, port)
    self.id = id if id != None else random_key()
    print 'me: %s' % key_to_int(self.id)
    self.ring_id = ring_id
    self.contacts = MultiRingChordContacts(Contact(ring_id=ring_id, id=self.id, ip=self.ip, port=self.port, network_protocol=self))

    # self.data['virtual_key'] = { 'data': value, 'version': vector_clock, 'requires': vector_clock, 'publisher': ? }
    self.data = {}
    
    self.callback_manager = CallbackManager()
  
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

  def got_join_proxy_message(self, contact, obj):
    joiner = Contact.from_tuple(obj.joiner, self)
    self.got_join_message(joiner, obj)

  def got_join_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, (contact, obj))

  def join_response_callback(self, request_id, (contact, obj)):
    self.contacts.set_successor(Contact.from_tuple(obj.successor, self))
    self.contacts.set_predecessor(contact)
    print 'joined!'
    print '  predecessor: %s' % key_to_int(self.contacts.get_predecessor().id)
    print '  me: %s' % key_to_int(self.id)
    print '  successor: %s' % key_to_int(self.contacts.get_successor().id)


# PING
  def ping(self, contact, callback):
    request_id = self.callback_manager.register(callback)
    contact.send(PingMessage(request_id=request_id))

  def got_ping(self, contact, obj):
    contact.send(PingResponse(request_id=obj.request_id))

  def got_ping_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, contact)

# FIND
  def find(self, key, callback):
    request_id = self.callback_manager.register(callback)
    if self.owns_key(key):
      self.callback_manager.call(request_id, self.contacts.me)
      return request_id
    other_contact = self.contacts.nearest_contact_less_than(key)
    other_contact.send(FindMessage(request_id=request_id, key=key))
    return request_id
    
  def got_find(self, contact, obj):
    if self.owns_key(obj.key):
      contact.send(FindResponse(request_id=obj.request_id, key=obj.key))
      return
    other_contact = self.contacts.nearest_contact_less_than(obj.key)
    other_contact.send(FindMessage(request_id=obj.request_id, key=obj.key))

  def got_find_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, contact)

# GET
  def get(self, key):
    request_id = self.callback_manager.register(callback)
    if self.owns_key(key):
      self.callback_manager.call(request_id, (self.contacts.me, self.data.get(key)))
      return request_id
    other_contact = self.contacts.nearest_contact_less_than(key)
    other_contact.send(GetMessage(request_id=request_id, key=key))
    return request_id

  def got_get_message(self, contact, obj):
    if self.owns_key(obj.key):
      contact.send(GetResponse(request_id=obj.request_id, key=obj.key, value=self.data.get(key)))
      return
    other_contact = self.contacts.nearest_contact_less_than(obj.key)
    other_contact.send(GetMessage(request_id=obj.request_id, key=obj.key))

  def got_get_response(self, contact, obj):
    self.callback_manager.call(obj.request_id, (contact, obj.value))

  def put(self, key, value):
    pass

  def delete(self, key):
    pass

  message_handlers = {
    PingMessage: got_ping,
    PingResponse: got_ping_response,
    FindMessage: got_find,
    FindResponse: got_find_response,
    GetMessage: got_get_message,
    GetResponse: got_get_response,
    JoinMessage: got_join_message,
    JoinProxyMessage: got_join_proxy_message,
    JoinResponse: got_join_response
  }
  


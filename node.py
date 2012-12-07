#!/usr/bin/env python

from keyspace import *
from udp_rpc import UDP_RPC, rpcmethod
import twisted.internet import reactor
import twisted.internet.threads
from contact import Contact
from contacts import Contacts

class Node(object):
  def __init__(self, ring_id=1, id=None, ip='127.0.0.1', port=8080):
    self.id = id if id else random_key()
    self.ring_id = ring_id
    self.ip = ip
    self.port = port
    self.udp_rpc = UDP_RPC(self)
    self.me = Contact(ring_id=ring_id, id=self.id, ip=self.ip, port=self.port, network_protocol=self.udp_rpc)
    self.contacts = Contacts(self.me)
    self.prev = self.me
    self.next = self.me

  def __del__(self):
    # TODO: Leave action
    self.udp_rpc.stopListening()

  def start(self):
    twisted.internet.reactor.listenUDP(self.port, self.udp_rpc)

  def add_contact(self, contact):
    existing = self.contacts.add(contact)
    if not existing:
      self.new_connection(contact)
  
  def remove_contact(self, contact_id):
    del self.contacts[contact_id]

  def new_connection(self, contact):
    pass

  def known_node(self, ip, port):
    dummy_contact = Contact(ring_id=self.ring_id, id=None, ip=ip, port=port, network_protocol=self.udp_rpc)
    dummy_contact.ping()

  @rpcmethod
  def owns_key(self, key):
    return keyspace_compare(self.id, self.next.id, key)

  def iterative_find(self, key):
    def find_fn(self, key):
      
    reactor.callInThread(find_fn, self, key)
    
  @rpcmethod
  def ping(self):
    return

  

  

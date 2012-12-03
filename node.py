#!/usr/bin/env python

import hashlib
import random

from udp_rpc import UDP_RPC, rpcmethod
import twisted.internet.reactor
import twisted.internet.threads
from contact import Contact

class Node(object):
  def __init__(self, id=None, port=8080):
    self.id = id if id else self.generate_id()
    self.port = port
    self.udp_rpc = UDP_RPC(self)
    self.contacts = {}

  def __del__(self):
    self.udp_rpc.stopListening()

  def start(self):
    twisted.internet.reactor.listenUDP(self.port, self.udp_rpc)

  def add_contact(self, contact):
    existing = contact.id in self.contacts
    self.contacts[contact.id] = contact
    if not existing:
      self.new_connection(contact)
  
  def new_connection(self, contact):
    pass

  def known_node(self, ip, port):
    dummy_contact = Contact(None, ip, port, self.udp_rpc)
    dummy_contact.ping()

  @rpcmethod
  def ping(self):
    return

  def remove_contact(self, contact_id):
    if contact_id in self.contacts:
      del self.contacts[contact_id]

  def generate_id(self):
    hash = hashlib.sha1()
    hash.update(str(random.getrandbits(255)))
    return hash.digest()

  

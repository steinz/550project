#!/usr/bin/env python

from keyspace import *

class NetworkError(StandardError):
  pass

class Contact(object):
  def __init__(self, ring_id=1, id=None, ip=None, port=None, network_protocol=None):
    self.ring_id = ring_id
    self.id = id if id else random_key()
    self.ip = ip
    self.port = port
    self.network_protocol = network_protocol
      
  def __hash__(self):
    return hash((self.ring_id, self.id))

  def __eq__(self, other):
    if not isinstance(other, Contact):
      return False
    return (self.ring_id == other.ring_id and\
            self.id == other.id and\
            self.ip == other.ip and\
            self.port == other.port)
        
  def __str__(self):
    return '%s:%s' % (self.ip, self.port)

  def to_tuple(self):
    return (self.ring_id, self.id, self.ip, self.port)
  
  @classmethod
  def from_tuple(cls, the_tuple, network_protocol=None):
    return cls(ring_id=the_tuple[0], id=the_tuple[1], ip=the_tuple[2], port=the_tuple[3], network_protocol=network_protocol)

  def send(self, obj):
    if not self.network_protocol:
      raise NetworkError('no bound network protocol object')

    # embed local (id, ring_id) in every message
    obj.id = self.network_protocol.id
    obj.ring_id = self.network_protocol.ring_id

    # send message to remote (ip, port)
    self.network_protocol.send_obj(self.ip, self.port, obj)
    

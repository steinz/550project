import math
from keyspace import *


class FingerTable(object):
  def __init__(self, me):
    # kth entry is me.id + 2^(k-1) mod keyspace size
    self.entries = [me]

  def __getitem__(self, index):
    if index >= len(self.entries):
      return None
    return self.entries[index]

  def __setitem__(self, index, value):
    num_items_to_add = index - len(self.entries) + 1
    print 'called FingerTable.__setitem__; index=%s' % index
    if num_items_to_add > 0:
      self.entries.extend([None] * num_items_to_add)
    self.entries[index] = value

  def get_key_index(self, lookup_key):
    my_key = self.entries[0].id

    #print 'called get_key_index; lookup_key=%s; my_key=%s' % (key_to_int(lookup_key), key_to_int(my_key))    

    distance = key_to_int(key_subtract_circular(lookup_key, my_key))
    if distance == 0:
      return 0

    # TODO: FIXME! floor(log) is broken here because we don't have enough precision
    return int(math.log(distance, 2))

  def add(self, contact):
    index = self.get_key_index(contact.id)
    self[index] = contact

  def nearest_contact_less_than(self, lookup_key):
    distance_index_space = self.get_key_index(lookup_key)
    index = max(distance_index_space, len(self.entries)-1)
    while self[index] == None:
      index -= 1
    return self[index]


class ChordContacts(FingerTable):
  """
   Stores one finger table + predecessor
  """
  def __init__(self, me):
    FingerTable.__init__(self, me)
    self.set_successor(me)
    self.set_predecessor(me)

  def get_successor(self):
    return self[1]

  def set_successor(self, successor):
    self[1] = successor  

  def get_predecessor(self):
    return self.predecessor

  def set_predecessor(self, predecessor):
    self.predecessor = predecessor


class MultiRingChordContacts(ChordContacts):
  """
   Stores one finger table + predecessor
   + a dict of contacts from other rings
  """
  def __init__(self, me):
    ChordContacts.__init__(self, me)
    self.other_rings = {}

  def add(self, contact):
    if contact.ring_id == self[0].ring_id:
      ChordContacts.add(self, contact)
    else:
      if contact.ring_id not in self.other_rings:
        self.other_rings[contact.ring_id] = []
      self.other_rings[contact.ring_id].append(contact)

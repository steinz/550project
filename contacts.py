from keyspace import *

class Contacts(object):
  """
   Manages a collection of Contacts
   Contacts can be quickly accessed by id (in the key space)
   Also supports fast lookup of the closest known contact to a given key
  """
  def __init__(self, me):
    self.my_ring_by_id = { me.id: me }
    self.other_rings = {}
    self.by_ip_port = { (me.address, me.port): me }
    self.me = me
    me.prev = me
    me.next = me

  def add(self, contact):
    existing = contact in self
    self[(contact.ring_id, contact.id)] = contact
    return existing

  def nearest_contact_less_than(self, key, random_start=True):
    cur = self.me
    if random_start:
      keys = self.my_ring_by_id.keys()
      index = random.randint(0, len(keys) - 1)
      key = keys[index]
      cur = self.my_ring_by_id[key]
    
    if len(keys) == 1:
      return cur

    while not keyspace_compare(cur, cur.next):
      cur = cur.next

    return cur

  def keys(self):
    return self.my_ring_by_id.keys()

  def addresses(self):
    return self.by_ip_port.keys()

  def __getitem__(self, obj):
    ring_id, key, ip, port = self.get_ring_id_key_ip_port(obj)
    if ring_id:
      if ring_id == self.me.ring_id:
        return self.my_ring_by_id.get(key)
      elif ring_id in self.other_rings:
        return self.other_rings[ring_id].get(key)
    else:
      return self.by_ip_port.get((ip, port))

  def __setitem__(self, (ring_id, key), contact):
    if contact.ring_id == self.me.ring_id:
      if (contact.ring_id, contact.id) in self:
        contact.prev = self[(contact.ring_id, contact.id)].prev
        contact.next = self[(contact.ring_id, contact.id)].next
      else:
        contact.prev = self.nearest_contact_less_than(contact.id)
        contact.next = contact.prev.next
        contact.prev.next = contact
        contact.next.prev = contact
      self.my_ring_by_id[contact.id] = contact
    else:
      if contact.ring_id not in self.other_rings:
        self.other_rings[contact.ring_id] = {}
      self.other_rings[contact.ring_id][contact.id] = contact

    self.by_ip_port[(contact.address, contact.port)] = contact

  def get_ring_id_key_ip_port(self, obj):
    from contact import Contact
    if isinstance(obj, Contact):
      return obj.ring_id, obj.id, obj.address, obj.port
    elif isinstance(obj, tuple):
      if not len(obj) == 2:
        raise ValueError('obj must be a tuple of length 2')

      if isinstance(obj[0], basestring) and isinstance(obj[1], int):
        # ip, port
        return None, None, obj[0], obj[1]

      elif isinstance(obj[0], int) and isinstance(obj[1], basestring):
        # ring_id, key
        return obj[0], obj[1], None, None
    raise ValueError('obj must be a Contact or a tuple (ring_id, "key") or a tuple ("ip", port)')

  def __delitem__(self, obj):
    ring_id, key, ip, port = self.get_ring_id_key_ip_port(obj)

    if not ring_id:
      obj = self.by_ip_port.get(item.address, item.port)
      if not obj:
        return
      del self[(obj.ring_id, obj.id)]
      return

    if not (ring_id, key) in self:
      return
    if ring_id == self.me.ring_id:
      self.my_ring_by_id[key].prev.next = self.my_ring_by_id[key].next
      self.my_ring_by_id[key].next.prev = self.my_ring_by_id[key].prev
      item = self.my_ring_by_id[key]
      del self.my_ring_by_id[key]
      del self.by_ip_port[(item.address, item.port)]
    elif ring_id != self.me.ring_id:
      item = self.other_rings[ring_id][key]
      del self.other_rings[ring_id][key]
      del self.by_ip_port[(item.address, item.port)]

  def __contains__(self, obj):
    ring_id, key, ip, port = self.get_ring_id_key_ip_port(obj)
    if ring_id:
      if ring_id == self.me.ring_id:
        return key in self.my_ring_by_id
      else:
        return ring_id in self.other_rings and key in self.other_rings[ring_id]
    else:
      return (ip, port) in self.by_ip_port
      

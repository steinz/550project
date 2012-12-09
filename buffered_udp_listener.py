from udp_listener import UDPListener
import cPickle as pickle
import struct

# 1200
MAX_MESSAGE_SIZE=30

TYPE_OBJ_START = 0
TYPE_OBJ_DATA = 1
TYPE_ACK = 2

class RawMessage(object):
  wire_header = 'HHI'
  wire_header_size = struct.calcsize(wire_header)

  def __init__(self, msg_type = TYPE_OBJ_DATA, fragments=1, data_str=''):
    self.msg_type = msg_type
    self.fragments = fragments
    self.seq = 0  
    self.data_str = data_str

    self.received = False

  def to_wire(self):
    return struct.pack(self.wire_header, self.msg_type, self.fragments, self.seq) + self.data_str

  @classmethod
  def from_wire(cls, msg):
    raw_msg = cls()
    raw_msg.msg_type, raw_msg.fragments, raw_msg.seq = struct.unpack(cls.wire_header, msg[0:cls.wire_header_size])
    raw_msg.data_str = msg[cls.wire_header_size:]
    return raw_msg

  @staticmethod
  def from_object(obj, max_msg_size=MAX_MESSAGE_SIZE):
    obj_str = pickle.dumps(obj)
    offsets = range(0, len(obj_str), max_msg_size)
    fragments = len(offsets)
    for offset in offsets:
      msg_type = (TYPE_OBJ_START if offset == 0 else TYPE_OBJ_DATA)
      yield RawMessage(msg_type=msg_type, fragments=fragments, data_str=obj_str[offset:offset+max_msg_size])

  @staticmethod
  def to_object(raw_msgs):
    obj_str = ''
    for raw_msg in raw_msgs:
      obj_str += raw_msg.data_str
    return pickle.loads(obj_str)


class UDPContact(object):
  def __init__(self):
    self.seq_out = 0
    self.buf_in = {}

class UDPContacts(object):
  def __init__(self):
    self.contacts = {}

  def __getitem__(self, addr):
    contact = self.contacts.get(addr)
    if not contact:
      contact = self.contacts[addr] = UDPContact()
    return contact

  def __contains__(self, addr):
    return addr in self.contacts

  def __delitem__(self, addr):
    if addr in self.contacts:
      del self.contacts[addr]

class BufferedUDPListener(UDPListener):
  def __init__(self, ip, port):
    UDPListener.__init__(self, ip, port)
    self.udp_contacts = UDPContacts()

  def received(self, ip, port, msg):
    contact = self.udp_contacts[(ip, port)]
    raw_msg = RawMessage.from_wire(msg)
    contact.buf_in[raw_msg.seq] = raw_msg
    self.test_received_whole_obj(ip, port, contact, raw_msg)

  def test_received_whole_obj(self, ip, port, contact, raw_msg):
    cur = raw_msg.seq
    while raw_msg.msg_type != TYPE_OBJ_START:
      cur -= 1
      raw_msg = contact.buf_in.get(cur)
      if not raw_msg:
        return

    if contact.buf_in[cur].received:
      return
  
    raw_msgs = []
    for i in range(0, raw_msg.fragments):
      if cur + i not in contact.buf_in:
        return
      raw_msgs.append(contact.buf_in.get(cur + i))

    contact.buf_in[cur].received = True
    obj = RawMessage.to_object(raw_msgs)
    self.received_obj(ip, port, obj)

  def received_obj(self, ip, port, obj):
    pass

  def send_obj(self, ip, port, obj):
    contact = self.udp_contacts[(ip, port)]
    for raw_msg in RawMessage.from_object(obj):      
      raw_msg.seq = contact.seq_out
      contact.seq_out += 1
      self.send(ip, port, raw_msg.to_wire())
    

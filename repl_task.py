import json
import sys

from repl import QueueTask, get_first_word
from keyspace import *
from console_format import *

class PrintIdTask(QueueTask):
  command = 'id'

  @classmethod
  def describe(cls):
    return 'print node id'

  @classmethod
  def help(cls):
    return 'usage: %s (no arguments)' % cls.command

  def execute(self, node):
    sys.stdout.write('\nmy id: %s\n' % key_to_int(node.id))

class PrintPointersTask(QueueTask):
  command = 'pointers'

  @classmethod
  def describe(cls):
    return 'list node ids for predecessor, me, successor'

  @classmethod
  def help(cls):
    return 'usage: %s (no arguments)' % cls.command

  def execute(self, node):
    predecessor = key_to_int(node.contacts.get_predecessor().id)
    me = key_to_int(node.id)
    successor = key_to_int(node.contacts.get_successor().id)
    sys.stdout.write('\npredecessor: %s\n' % predecessor)
    sys.stdout.write(color('me: %s\n' % me, 'red'))
    sys.stdout.write('successor: %s\n' % successor)

class GetKeyTask(QueueTask):
  command = 'get'

  @classmethod
  def describe(cls):
    return 'perform a distributed key lookup'
  
  @classmethod
  def help(cls):
    return 'usage: %s key [...]' % cls.command

  def execute(self, node):
    key = self.args
    node.get(key, self.get_callback)

  def get_callback(self, request_id, (node, owner, key, value)):
    sys.stdout.write('\nget(%s):\n' % key)
    sys.stdout.write('%s\n' % json.dumps(node.value_to_wire(value), indent=2))
    sys.stdout.flush()

class AppendTask(QueueTask):
  command = 'append'

  @classmethod
  def describe(cls):
    return 'append to a key in the DHT'

  @classmethod
  def help(cls):
    return 'usage: %s key value' % cls.command
  
  def execute(self, node):
    key, value = get_first_word(self.args)
    node.append(key, value, self.append_callback)    

  def append_callback(self, request_id, (node, contact, version)):
    sys.stdout.write('\nappended. new version: %s\n' % version)

class LeaveTask(QueueTask):
  command = 'leave'
  
  def __init__(self, args=''):
    QueueTask.__init__(self, args)

    # stop REPL after executing
    self.terminal = True

  @classmethod
  def describe(cls):
    return 'leave the DHT, transfer data to other nodes'

  @classmethod
  def help(cls):
    return 'usage: %s (no arguments)' % cls.command

  def execute(self, node):
    node.leave_ring(self.leave_callback)

  def leave_callback(self, request_id, predecessor):
    print 'left.'
    sys.exit(0)

class FindTask(QueueTask):
  command = 'find'
  
  @classmethod
  def describe(cls):
    return 'lookup the node containing the specified integer key'

  @classmethod
  def help(cls):
    return 'usage: %s key\nkey must be an integer between 0 and keyspace_size-1 (usually 2^60-1)' % cls.command

  def execute(self, node):
    key = int(self.args)
    node.find(int_to_key(key), self.find_callback, raw_key=True)

  def find_callback(self, request_id, contact):
    sys.stdout.write('%s\n' % contact)

class LimitTask(QueueTask):
  command = 'limit'

  @classmethod
  def describe(cls):
    return 'halt after receiving n more messages'

  @classmethod
  def help(cls):
    return 'usage: %s n' % cls.command

  def execute(self, node):
    try:
      n = int(self.args)
      if n == 0:
        node.message_limit = None
        sys.stdout.write('limit removed\n')
      else:
        node.message_limit = node.messages_received + n
        sys.stdout.write('will halt after receiving %d more messages\n' % n)
    except ValueError:
      sys.stdout.write('n must be an integer\n')

class FingerTask(QueueTask):
  command = 'finger'

  @classmethod
  def describe(cls):
    return 'get the next hop when sending to key identified by integer k'

  @classmethod
  def help(cls):
    return 'usage: %s k' % cls.command

  def execute(self, node):
    try:
      k = int(self.args)
      sys.stdout.write('\nnext hop: %s\n' % node.contacts.nearest_contact_less_than(int_to_key(k)))
    except ValueError:
      sys.stdout.write('\nk must be an integer\n')
    sys.stdout.flush()

class PrintFingerTableTask(QueueTask):
  command = 'fingertable'

  @classmethod
  def describe(cls):
    return 'print the finger table'

  @classmethod
  def help(cls):
    return 'usage: %s (no arguments)' % cls.command

  def execute(self, node):
    sys.stdout.write('\nfinger table size: %d\n' % len(node.contacts))
    for i in xrange(len(node.contacts)):
      e = node.contacts[i]
      if e != None:
        # kth entry is me.id + 2^(k-1) mod keyspace size
        key_int = None
        if i == 0:
          key_int = key_to_int(e.id)
        else:
          key_int = key_to_int(
            key_add_circular(e.id, int_to_key(2 ** (i-1) ) )
            )
        sys.stdout.write('%d:\t%s\t%d\n' % (i, e, key_int))
    sys.stdout.write('---\n')
    sys.stdout.flush()


class MultipleAppendTask(QueueTask):
  command = 'multiple_append'

  @classmethod
  def describe(cls):
    return 'append to all replicas in this ring and other rings'

  @classmethod
  def help(cls):
    return 'usage: %s virtual_key value' % cls.command

  def execute(self, node):
    key, value = get_first_word(self.args)
    node.multiple_append(
      key = key,
      value = value,
      requires = None,
      one_completed_callback=self.one_completed,
      all_completed_callback=self.all_completed
      )

  def one_completed(self, multiple_append_request_id,
        (node, multiple_append_data, contact, physical_key)):

    version = multiple_append_data['keys'][contact.ring_id][physical_key][1]

    sys.stdout.write('\nappended:\n')
    sys.stdout.write('  ring_id: %s\n' % contact.ring_id)
    sys.stdout.write('  node: %s:%s\n' % (contact.ip, contact.port))
    sys.stdout.write('  virtual key: %s\n' % multiple_append_data['virtual_key'])
    sys.stdout.write('  physical key: %s\n' % physical_key)
    sys.stdout.write('  resulting version: %s\n\n' % version)
    sys.stdout.flush()

  def all_completed(self, multiple_append_request_id, (node, multiple_append_data)):
    sys.stdout.write('\nall appends completed.\n')
    sys.stdout.flush()


class MultipleGetTask(QueueTask):
  command = 'multiple_get'

  @classmethod
  def describe(cls):
    return 'get from all replicas in this ring and other rings'

  @classmethod
  def help(cls):
    return 'usage: %s virtual_key value' % cls.command

  def execute(self, node):
    key, value = get_first_word(self.args)
    node.multiple_get(
      key = key,
      one_completed_callback=self.one_completed,
      all_completed_callback=self.all_completed
      )

  def one_completed(self, multiple_get_request_id,
        (node, multiple_get_data, contact, physical_key)):
    value = multiple_get_data['keys'][contact.ring_id][physical_key][1]
    sys.stdout.write('\nget result:\n')
    sys.stdout.write('  ring_id: %s\n' % contact.ring_id)
    sys.stdout.write('  node: %s:%s\n' % (contact.ip, contact.port))
    sys.stdout.write('  virtual key: %s\n' % multiple_get_data['virtual_key'])
    sys.stdout.write('  physical key: %s\n' % physical_key)
    sys.stdout.write('  data: %s\n\n' % json.dumps(node.value_to_wire(value), indent=2))
    sys.stdout.flush()

  def all_completed(self, multiple_get_request_id, (node, multiple_get_data)):
    sys.stdout.write('\nall gets completed.\n')
    sys.stdout.flush()

class DataTask(QueueTask):
  command = 'data'

  @classmethod
  def describe(cls):
    return 'print data stored by the local node'

  @classmethod
  def help(cls):
    return 'usage: %s (no arguments)' % cls.command

  def execute(self, node):
    sys.stdout.write('\n%d keys:\n' % len(node.data))
    for k, v in node.data.iteritems():
      sys.stdout.write('  %s: %s\n' % (k, json.dumps(node.value_to_wire(v), indent=2)))
    sys.stdout.write('---\n')
    sys.stdout.flush()

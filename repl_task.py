import json

from repl import QueueTask, get_first_word
from keyspace import *

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
    print
    print 'predecessor: %s' % predecessor
    print 'me: %s' % me
    print 'successor: %s' % successor

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

  def get_callback(self, request_id, (owner, key, value)):
    print    
    print 'get(%s):' % key
    print json.dumps(value, indent=2)

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

  def append_callback(self, request_id, contact):
    print
    print 'appended.'


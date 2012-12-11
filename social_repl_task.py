import json
import sys

from repl import QueueTask, get_first_word

class ReadWallTask(QueueTask):
  command = 'wall_read'

  @classmethod
  def describe(cls):
    return 'read a user\'s wall'

  @classmethod
  def help(cls):
    return 'usage: %s user_id' % cls.command

  def execute(self, node):
    other_user_id = self.args
    node.multiple_get(
      key = '%s_wall' % other_user_id,
      one_completed_callback=self.one_completed,
      )

  def one_completed(self, multiple_get_request_id,
        (node, multiple_get_data, contact, physical_key)):
    value = multiple_get_data['keys'][contact.ring_id][physical_key][1]
    sys.stdout.write('\nwall read result:\n')
    sys.stdout.write('  ring_id: %s\n' % contact.ring_id)
    sys.stdout.write('  node: %s:%s\n' % (contact.ip, contact.port))
    sys.stdout.write('  virtual key: %s\n' % multiple_get_data['virtual_key'])
    sys.stdout.write('  physical key: %s\n' % physical_key)
    sys.stdout.write('  data: %s\n\n' % json.dumps(node.value_to_wire(value), indent=2))
    sys.stdout.flush()

class PostToWallTask(QueueTask):
  command = 'wall_post'

  @classmethod
  def describe(cls):
    return 'post to a user\'s wall'

  @classmethod
  def help(cls):
    return 'usage: %s user_id contents' % cls.command

  def execute(self, node):
    other_user_id, value = get_first_word(self.args)
    node.multiple_append(
      key = '%s_wall' % other_user_id,
      value = value,
      requires = None,
      all_completed_callback=self.all_completed
      )

  def all_completed(self, multiple_append_request_id, (node, multiple_append_data)):
    sys.stdout.write('\nwall post appends completed.\n')
    sys.stdout.flush()

class FriendTask(QueueTask):
  command = 'friend'

  @classmethod
  def describe(cls):
    return 'add the user as a friend'

  @classmethod
  def help(cls):
    return 'usage: %s user_id' % cls.command

  def execute(self, node):
    other_user_id = self.args
    node.multiple_append(
      key = '%s_friends' % node.user_id,
      value = 'add %s' % other_user_id,
      requires = None,
      all_completed_callback=self.all_completed
      )

  def all_completed(self, multiple_append_request_id, (node, multiple_append_data)):
    sys.stdout.write('\nfriend appends completed.\n')
    sys.stdout.flush()

class DefriendTask(QueueTask):
  command = 'defriend'

  @classmethod
  def describe(cls):
    return 'remove the user as a friend'

  @classmethod
  def help(cls):
    return 'usage: %s user_id' % cls.command

  def execute(self, node):
    other_user_id = self.args
    node.multiple_append(
      key = '%s_friends' % node.user_id,
      value = 'remove %s' % other_user_id,
      requires = None,
      all_completed_callback=self.all_completed
      )

  def all_completed(self, multiple_append_request_id, (node, multiple_append_data)):
    sys.stdout.write('\ndefriend appends completed.\n')
    sys.stdout.flush()

class ListFriendsTask(QueueTask):
  command = 'friend_list'

  @classmethod
  def describe(cls):
    return 'get the friend list for a user'

  @classmethod
  def help(cls):
    return 'usage: %s user_id' % cls.command

  def execute(self, node):
    other_user_id = self.args
    node.multiple_get(
      key = '%s_friends' % other_user_id,
      one_completed_callback=self.one_completed,
      )

  def one_completed(self, multiple_get_request_id,
        (node, multiple_get_data, contact, physical_key)):
    value = multiple_get_data['keys'][contact.ring_id][physical_key][1]
    sys.stdout.write('\nfriend list result:\n')
    sys.stdout.write('  ring_id: %s\n' % contact.ring_id)
    sys.stdout.write('  node: %s:%s\n' % (contact.ip, contact.port))
    sys.stdout.write('  virtual key: %s\n' % multiple_get_data['virtual_key'])
    sys.stdout.write('  physical key: %s\n' % physical_key)
    sys.stdout.write('  data: %s\n\n' % json.dumps(node.value_to_wire(value), indent=2))
    sys.stdout.flush()

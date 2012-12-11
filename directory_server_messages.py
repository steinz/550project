import sys

from message_handler_node import define_message_types, NodeMessage

class DirectoryServerMessage(NodeMessage):
  pass

directory_server_messages = [
  'DirectoryServerJoin',
  'DirectoryServerJoinContact',
  'DirectoryServerLeave',
  'DirectoryServerGetRingContact',
  'DirectoryServerReturnRingContact'
]
define_message_types(
  sys.modules[__name__],
  directory_server_messages,
  lambda name, parent, members:type(name, (DirectoryServerMessage,), members)
  )

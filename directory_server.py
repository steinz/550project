#!/usr/bin/env python

import sys
from message_handler_node import define_message_types, handlesrequest, MessageHandlerNode

def usage(argv):
  print 'usage: %s port' % argv[0]
  return 1

nodes = set()

LOCALHOST = '127.0.0.1'

directory_server_messages = [
  'DirectoryServerJoin',
  'DirectoryServerLeave',
  'DirectoryServerGetNodeInRing',
]
define_message_types(
  sys.modules[__name__],
  directory_server_messages,
  lambda name, parent, members:type(name, parent, members)
  )

class DirectoryServer(MessageHandlerNode):
  def __init__(self, ip=LOCALHOST, port=None):
    MessageHandlerNode.__init__(self, ip, port)

  @handlesrequest(DirectoryServerJoin)
  def got_join(self, src_ip, src_port, message):
    print message

DirectoryServer.discover_message_handlers()

def run(argv):
  if len(argv) < 2:
    return usage(argv)

  port = int(argv[1])
  server = DirectoryServer(port=port)
  server.send_obj(LOCALHOST, port, DirectoryServerJoin())
  server.listen_loop()
  return 0

if __name__ == '__main__':
  sys.exit(run(sys.argv))

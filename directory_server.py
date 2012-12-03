import sys
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor

def usage(argv):
  print 'usage: %s port' % argv[0]
  return 1

nodes = set()

class DirectoryServer(DatagramProtocol):
  def datagramReceived(self, data, (host, port)):
    if len(data) < 1:
      return

    if data[0] == 'j':
      nodes

    self.transport.write(data, (host, port))

def run(argv):
  if len(argv) < 2:
    return usage(argv)

  port = int(argv[1])

  reactor.listenUDP(port, DirectoryServer())
  reactor.run()
  return 0

if __name__ == '__main__':
  sys.exit(run(sys.argv))

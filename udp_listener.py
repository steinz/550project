import socket

DEFAULT_DATA_SIZE=10000
DEFAULT_TIMEOUT=2.0

class UDPListener(object):
  def __init__(self, ip, port):
    self.ip = ip
    self.port = port
    self.socket = socket.socket(socket.AF_INET,    # Internet socket
                                socket.SOCK_DGRAM) # UDP
    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.socket.bind((ip, port))
    self.set_timeout()

  def __del__(self):
    self.stop_listening()

  def send(self, ip, port, msg):
    self.socket.sendto(msg, (ip, port))

  def set_timeout(self, timeout=DEFAULT_TIMEOUT):
    self.socket.settimeout(timeout)

  def receive(self, data_size=DEFAULT_DATA_SIZE):
    msg, (ip, port) = self.socket.recvfrom(data_size)
    return ip, port, msg

  def received(self, ip, port, msg):
    pass

  def timed_out(self):
    pass

  def about_to_receive(self):
    pass

  def stop_listening(self):
    self.stop = True

  def listen_loop(self):
    self.stop = False
    while not self.stop:
      try:
        self.about_to_receive()

        # blocking
        ip, port, message = self.receive()
        
        self.received(ip, port, message)
      except socket.timeout:
        self.timed_out()
      except KeyboardInterrupt as e:
        print        
        print 'bye'
        break

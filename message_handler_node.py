import inspect
import sys

from buffered_udp_listener import BufferedUDPListener

# base message
# store any attributes given to the constructor
class NodeMessage(object):
  def __init__(self, **kwargs):
    for key in kwargs:
      setattr(self, key, kwargs[key])

def define_message_types(module, message_types, creator):
  """
   define classes for every item in message_types
   to use:
    define_message_types(
      sys.modules[__name__],
      message_types,
      lambda name, parent, members:type(name, parent, members)
      )
    the lambda bit is required from the caller to get the newly created classes
    to exist in the calling module
  """
  for x in message_types:
    obj = creator(x, (NodeMessage,), {})
    setattr(module, x, obj)

def handlesrequest(message_type):
  """
   decorator for functions to define which message type they handle
   used to dynamically map messages to functions
  """
  def decorator(func):
    func.message_type = message_type
    return func
  return decorator

class MessageHandlerNode(BufferedUDPListener):
  def __init__(self, ip, port):
    BufferedUDPListener.__init__(self, ip, port)

  def received_obj(self, ip, port, obj):
    handler = self.__class__.get_message_handler(obj)
    if handler:
      # invoke handler
      handler(self, ip, port, obj)

  @classmethod
  def get_message_handler(cls, msg):
    return cls.message_handlers.get(msg.__class__)

  @classmethod
  def discover_message_handlers(cls):
    """
     discover functions in Node with the handlesrequest decorator
     store a dict of message_type => handler function
     to use: discover_message_handlers([subclass of MessageHandlerNode])
    """
    cls.message_handlers = {}
    for name in dir(cls):
      x = getattr(cls, name)
      if not inspect.ismethod(x) or not hasattr(x, 'message_type'):
        continue
      message_type = getattr(x, 'message_type')
      if not issubclass(message_type, NodeMessage):
        continue
      cls.message_handlers[message_type] = x


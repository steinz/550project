import inspect

# base message
# store any attributes given to the constructor
class NodeMessage(object):
  def __init__(self, **kwargs):
    for key in kwargs:
      setattr(self, key, kwargs[key])

def define_message_types(module_name, message_types):
  """
   define classes for every item in message_types
   to use: when calling module_name = __name__
  """
  for x in message_types:
    obj = type(x, (NodeMessage,), {})
    setattr(sys.modules[module_name], x, obj)

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
  def __init__(self):
    pass

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


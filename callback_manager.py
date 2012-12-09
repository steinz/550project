import itertools

class CallbackManager(object):
  def __init__(self):
    self.next_request_id = itertools.count()
    self.request_callbacks = {}
    self.request_data = {}

  def get_request_id(self):
    return self.next_request_id.next()

  def register(self, callback):
    request_id = self.get_request_id()
    self.request_callbacks[request_id] = callback
    self.request_data[request_id] = {}
    return request_id

  def get_data(self, request_id):
    return self.request_data.get(request_id)

  def call(self, request_id, arg):
    callback = self.request_callbacks.get(request_id)
    if callback:
      callback(request_id, arg)
      del self.request_callbacks[request_id]
      del self.request_data[request_id]

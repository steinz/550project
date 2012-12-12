
class VectorVersion(object):
  def __init__(self):
    self.elements = {}    

  def copy(self):
    return VectorVersion.from_tuples(self.to_tuples())

  def merge(self, other):
    def merge_element(slave, master):
      if master and not slave:
        return master
      return max(slave, master)

    if not isinstance(other, VectorVersion):
      raise TypeError('other must be a VectorVersion instance')
    
    for k, v in other.elements.iteritems():
      self.elements[k] = merge_element(self.elements.get(k), v)

  def increment(self, user_id):
    if user_id not in self.elements:
      self.elements[user_id] = 0
    self.elements[user_id] += 1

  def to_tuples(self):
    tuples = []
    for k, v in self.elements.iteritems():
      tuples.append((k, v))
    return tuples

  @classmethod
  def from_tuples(cls, tuples):
    vector = VectorVersion()
    for x in tuples:
      vector.elements[x[0]] = x[1]
    return vector

  def __repr__(self):
    return 'VectorVersion(%s)' % str(self.to_tuples())


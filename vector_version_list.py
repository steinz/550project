from vector_version import VectorVersion

class VectorVersionList:
  def __init__(self):
    self.versions = {}  

  def add(self, key, version):
    if key not in self.versions:
      self.versions[key] = version.copy()
    else:
      self.versions[key].merge(version)

  def merge(self, other):
    for key, version in other.versions.iteritems():
      self.add(key, version)

  def to_tuples(self):
    tuples = []
    for key, version in self.versions.iteritems():
      tuples.append((key, version.to_tuples()))
    return tuples

  @classmethod
  def from_tuples(cls, tuples):
    version_list = VectorVersionList()
    for x in tuples:
      version_list.add(x[0], VectorVersion.from_tuples(x[1]))
    return version_list
      

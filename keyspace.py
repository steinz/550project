import hashlib
import random

# determines keyspace size
hash_algorithm = 'sha1'

key_size_bytes = hashlib.new(hash_algorithm).digest_size

def random_key():
  hash = hashlib.new(hash_algorithm)
  hash.update(str(random.getrandbits(key_size_bytes * 8 * 2)))
  return hash.digest()

def keyspace_size():
  return 2 ** (8 * key_size_bytes)

def keyspace_compare(prev, next, search):
  """
   returns True if search is between prev (inclusive) and next (exclusive)
   e.g. prev=98, next=0, search=99 => True
        prev=5, next=6, search=5   => True
        prev=5, next=6, search=6   => False
        prev=5, next=3, search=2   => True
  """
  prev = key_to_int(prev)
  next = key_to_int(next)

  if prev == next:
    return True  

  search = key_to_int(search)

  if next < prev:
    next += keyspace_size()
  if search < prev:
    search += keyspace_size()

  return prev <= search and search < next

def key_to_hex(key):
  return ''.join(map(lambda x: ('0' if ord(x) < 16 else '')+hex(ord(x))[2:], key))

def key_to_int(key):
  return int(key_to_hex(key), 16)

def int_to_key(integer):
  hex_str = hex(integer)[2:].rstrip('L')
  if len(hex_str) & 1 == 1:
    hex_str = '0' + hex_str
  shorter_hex_str = ''
  for i in range(0, len(hex_str), 2):
    shorter_hex_str += chr(int(hex_str[i:i+2], 16))
  return ''.join(['\x00'] * (key_size_bytes - len(shorter_hex_str))) + shorter_hex_str

def key_add_circular(k1, k2):
  return int_to_key((key_to_int(k1) + key_to_int(k2)) % keyspace_size())

def key_subtract_circular(k1, k2):
  k1 = key_to_int(k1)
  k2 = key_to_int(k2)
  return int_to_key(k1 - k2 if k1 >= k2 else k1 + keyspace_size() - k2)

def string_to_key(key_string):
  hash = hashlib.new(hash_algorithm)
  hash.update(str(key_string))
  return hash.digest()

#!/usr/bin/env python

import json
from multiprocessing import Process, Queue
import sys
import time

from app_node import P2PSocialStore
from repl import REPL
import repl_task



def read_config(filename='config.json'):
  with open(filename, 'r') as f:
    return json.loads(f.read())

def run(argv):
  if len(argv) < 2:
    print 'usage: %s node_id' % argv[0]
    return 1
  
  node_id = argv[1]

  config = read_config()
  if node_id not in config['nodes']:
    print 'invalid node id'
    return 2

  node_config = config['nodes'][node_id]
  node = P2PSocialStore(config, node_id)

  request_queue = Queue()
  network_process = Process(target=node.start, args=(request_queue,))
  network_process.start()

  # don't let this thread touch node ever again
  # it's running in a separate process
  del node
 
  repl = REPL(prompt='dht>> ', command_queue=request_queue)
  repl.add_commands_from_module(repl_task)
  repl.loop()

  # TODO: graceful shutdown
  #node.shutdown = True
  #network_process.join()

  # For now: kill
  network_process.terminate()

  return 0

if __name__ == '__main__':
  sys.exit(run(sys.argv))

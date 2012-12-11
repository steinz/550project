#!/usr/bin/env python

import json
from multiprocessing import Process, Queue
import sys
import time

from app_node import AppNode
from repl import REPL
import repl_task
import social_repl_task
from console_format import *


def read_config(filename='config.json'):
  try:
    with open(filename, 'r') as f:
      return json.loads(f.read())
  except IOError as e:
    sys.stderr.write('%s\ndid you run\n  make CONFIG=config-name\n\ndid you read the README?\n' % color('failed to read \'%s\'' % e.filename, 'red', bold=True))
    sys.exit(1)

def run(argv):
  if len(argv) < 2:
    sys.stderr.write(color('usage: %s node_id\n' % argv[0], 'red', bold=True))
    return 1
  
  node_id = argv[1]

  config = read_config()
  if node_id not in config['nodes']:
    sys.stderr.write('%s\ncheck config file\n' % color('invalid node id', 'red', bold=True))
    return 2

  node_config = config['nodes'][node_id]
  node = AppNode(config, node_id)

  request_queue = Queue()
  network_process = Process(target=node.start, args=(request_queue,))
  network_process.start()

  # don't let this thread touch node ever again
  # it's running in a separate process
  del node
 
  repl = REPL(prompt='dht>> ', command_queue=request_queue)
  repl.add_commands_from_module(repl_task)
  repl.add_commands_from_module(social_repl_task)
  repl.loop()

  request_queue.put(repl_task.LeaveTask())
  network_process.join()

  return 0

if __name__ == '__main__':
  sys.exit(run(sys.argv))

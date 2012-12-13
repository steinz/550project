#!/usr/bin/env python

import random

PRIME = 187680587

class Simulation:
  def __init__(self, nodes=10000, regions=4):
    if nodes % regions != 0:
      raise StandardError

    self.nodes = nodes
    self.regions = regions

    self.nodes_per_region = self.nodes / self.regions

    self.alive_count = self.nodes
    self.alive = []
    for i in range(self.regions):
      self.alive.append([True] * self.nodes_per_region)

  def flip_alive(self, rid, nid):
    alive = self.alive[rid][nid]
    self.alive_count += -1 if alive else 1
    self.alive[rid][nid] = not alive

  def __str__(self):
    return 'Simulation(nodes: {0}, regions: {1}, nodes_per_region: {2}, alive_count: {3})'.format(
      self.nodes, self.regions, self.nodes_per_region, self.alive_count)

def gnid_to_lnid(simulation, i):
  return i % (simulation.nodes_per_region)

def gnid_to_rid(simulation, node_id):
  return node_id / (simulation.nodes_per_region)

def rid_lid_to_gnid(simulation, region_id, local_node_id):
  return local_node_id + simulation.nodes_per_region * region_id
  
def crash_random(simulation, prob):
  for rid in range(simulation.regions):
    for nid in range(simulation.nodes_per_region):
      if simulation.alive[rid][nid] and random.random() < prob:
        simulation.flip_alive(rid, nid)

def crash_region(simulation, rid):
  for nid in range(simulation.nodes_per_region):
    if simulation.alive[rid][nid]:
      simulation.flip_alive(rid, nid)

def lookup(simulation):
  failed_lookups = 0
  for key in range(simulation.nodes_per_region):
    keys_for_regions = map(lambda rid: rid * PRIME + key, range(simulation.regions))
    local_node_ids_for_regions = map(lambda key: gnid_to_lnid(simulation, key), keys_for_regions)
    
    alive = 0
    for rid in range(simulation.regions):
      lnid = local_node_ids_for_regions[rid]
      if simulation.alive[rid][lnid]:
        alive += 1
        
    if alive == 0:
      failed_lookups += 1

  return failed_lookups

def sim(nodes, regions, rounds):
  simulation = Simulation(nodes, regions)
  print simulation

  for t in range(rounds):
    crash_random(simulation, 0.01)
    failed_lookups = lookup(simulation)
    print 'round %u - %u failed lookups (%u nodes alive)' % (t, failed_lookups, simulation.alive_count)

def repl():
  simulation = Simulation()

  while True:
    print
    print simulation
    
    try:
      cmd = raw_input('sim> ')
    except EOFError:
      return

    cmd = cmd.lower().split(' ')
    cmd, args = cmd[0], cmd[1:]

    if cmd == 'exit':
      return

    elif cmd == 'crash_random':
      try:
        if len(args) > 0:
          prob = float(args[0])
        else:
          prob = 0.01
        crash_random(simulation, prob)
      except ValueError:
        print 'usage: crash_random [prob]'

    elif cmd == 'crash_region':
      try:
        rid = int(args[0])
        crash_region(simulation, rid)
      except (IndexError, ValueError):
        print 'usage: crash_region rid'

    elif cmd == 'lookup':
      failed_lookups = lookup(simulation)
      p = 100.0 * failed_lookups / simulation.nodes_per_region
      print '%u/%u lookups failed (%.2f%%)'  % (failed_lookups, simulation.nodes_per_region, p)

    elif cmd == 'reset':
      try:
        if len(args) > 1:
          regions = int(args[1])
        else:
          regions = 4

        if len(args) > 0:
          nodes = int(args[0])
        else:
          nodes = 10**4

        simulation = Simulation(nodes, regions)
      except ValueError:
        print 'usage: reset [node_count] [region_count]'
      except StandardError:
        print 'error: node_count must evenly divide region_count'

    else:
      print 'usage:'
      print '  crash_random [prob]'
      print '  crash_region rid'
      print '  lookup'
      print '  reset [nodes] [regions]'

if __name__ == '__main__':
  sim(10**6, 1, 10)
  print
  sim(10**6, 4, 10)

#!/usr/bin/env python

import math
import networkx as nx

dunbars_number = 150

def gen_ring_graph(nodes):
  return nx.watts_strogatz_graph(nodes, dunbars_number, 0.05)

def d(a, b):
  return sum(map(lambda p: abs(p[0] - p[1]), zip(a,b)))

def gen_lattice_graph(nodes):
  dim = 2
  n = int(nodes ** (1.0/dim))
  p = 2 # short range connection diameter
  short_connection_count = 2 * p * (p + 1)
  connections_per_node = 50
  q = connections_per_node - short_connection_count # long range connection count
  return nx.navigable_small_world_graph(n, p, q, 2, dim)

def print_graph_info(graph):
  e = nx.eccentricity(graph)
  print 'graph with %u nodes, %u edges' % (len(graph.nodes()), len(graph.edges()))
  print 'radius: %s' %  nx.radius(graph, e) # min e
  print 'diameter: %s' % nx.diameter(graph, e) # max e
  print 'len(center): %s' % len(nx.center(graph, e)) # e == radius
  print 'len(periphery): %s' % len(nx.periphery(graph, e)) # e == diameter

def pairs(arr):
  l = len(arr)
  for i in range(l):
    for j in range(i+1, l):
      yield arr[i], arr[j]

def freq_map(arr):
  freq = {}
  for i in arr:
    count = freq.get(i,0)
    freq[i] = count + 1
  return freq

def latencies(graph):
  latencies = []
  nodes = graph.nodes()
  for i,j in pairs(nodes):
    latencies.append(d(i,j))
  return latencies

def avg_latency(nodes):
  g = gen_lattice_graph(nodes)
  print
  print_graph_info(g)
  l = latencies(g)
  avg_latency = 1.0*sum(l)/len(l)
  return avg_latency
  f = freq_map(l)


def latency(nodes, regions, local_latency, foreign_latency):
  print '%u nodes' % nodes
  print '%u regions' % regions

  nodes_per_region = nodes / regions
  print '%u nodes/region' % nodes_per_region
  print '%.2f%% of total data on each node' % (100.0 / nodes_per_region)
  
  messages_per_request = math.log(nodes_per_region, 2)
  print '~%u messages_per_request' % round(messages_per_request)

  local_request_latency = messages_per_request * local_latency
  print 'local_request_latency: %u' % local_request_latency

  foreign_request_latency = foreign_latency + local_request_latency
  print 'foreign_request_latency: %u' % foreign_request_latency

def compare_latencies():  
  avg_1000 = avg_latency(1000)
  avg_500 = avg_latency(500)
  avg_250 = avg_latency(250)
  avg_100 = avg_latency(100)
  avg_50 = avg_latency(50)

  print
  print 'avg_latency(1000): %u' % avg_1000
  print 'avg_latency(500): %u' % avg_500
  print 'avg_latency(250): %u' % avg_250
  print 'avg_latency(100): %u' % avg_100
  print 'avg_latency(50): %u' % avg_50

  print
  latency(1000, 1, avg_1000, avg_1000)

  print
  latency(1000, 2, avg_500, avg_1000)

  print
  latency(1000, 4, avg_250, avg_1000)

  print
  latency(1000, 10, avg_100, avg_1000)

  print
  latency(1000, 20, avg_50, avg_1000)



def request_latency(num_hops, avg_hop_latency):
  return num_hops * avg_hop_latency

if __name__ == '__main__':
  compare_latencies()

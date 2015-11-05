"""
    This file contains two classes:
    - Class to represent graph vertex
    - Class to represent graph
    
    For the hashtag degree problem, it uses list representaion of graph
"""

from datetime import datetime
from datetime import timedelta

"""
    tweet_vertex class preresents the hashtag node
    It tracks the nodes connected as well as the timestamps when 
    each connection is established

    It uses dictionary to track the neighbors. The key of the dictionary
    is the neighbor's node name, and the value is the list of timestamps
    when the neighbors are added

    The class also support neighbor eviction. The eviction is based on
    the timestamp when evict operation is issued.
"""

class tweet_vertex:
    def __init__(self, key):
        self.id = key
        self.connected_to = {}
        self.window = timedelta(seconds=60)

    """
        Add neighbor to the current vertex's neighbor list. Also record
        the timestamp when it is added
        If the neighbor already exists, don't create new connection, but 
        just add the new timestamp to the list
    """
    def add_neighbor(self, nbr, timestamp):
        if nbr in self.connected_to.keys():
            self.connected_to[nbr].append(timestamp);
        else:
            self.connected_to[nbr] = [timestamp]

    def get_connections(self):
        return self.connected_to.keys()

    # Only used for testing purpose for now
    def get_timestamp(self, nbr):
        if nbr in self.connected_to.keys():
            return self.connected_to[nbr]
        else:
            return None

    def get_id(self):
        return self.id

    def get_degree(self):
        return len(self.connected_to.keys())

    """
        If the current vertex has no connection. The wrapper class would
        need to remove the vertex from its list
    """
    def evict(self, timestamp):
        if self.get_degree() <= 0:
            return

        nts = timestamp

        # Looping through all the neighbors. 
        # For each neighbor, generate the list of timestamps that is 60s away
        # If some neighbor contains only timestamp that is more than 60s ways
        # the neighbor would need to be removed
        for node in self.connected_to.keys():
            all_ts = self.connected_to[node]
            all_ts = [ets for ets in all_ts if (nts - ets) <= self.window]

#            print "nts = " + str(nts) + " all_ts = " + str(all_ts)

            if len(all_ts) == 0:
               del self.connected_to[node]
            else:
               self.connected_to[node] = all_ts

    def __repr__(self):
        result = 'id = \"' + self.id + '\" connected to: ' + str([x.id for x in self.connected_to.keys()])

        for nbr in self.connected_to.keys():
            result += nbr.id
            result = result + " timestamp: " + str(self.connected_to[nbr])

        return result

"""
    This class represent tweet hashtag graph
    - It supports vertex/edge add/removal
    - It keeps track of the timestamp when each vertex is added 
    - It supports calculation of average degree
"""
class tweet_graph:
    def __init__(self):
        self.verts = {}         # dictionary of all vertexes
        self.timestamps = {}    # dictionary of all timestamps
                                # This is the "reverse indexing" of verts
                                # to speed up searching for nodes which
                                # expires when eviction happens
        self.num_verts = 0
        self.window = timedelta(seconds=60)

    """
        Add vertex to both verts and timestamps dictionaries
    """
    def add_vertex(self, key, timestamp):
        self.evict(timestamp)

        if key not in self.verts:
            self.num_verts += 1
            new_vertex = tweet_vertex(key)
            self.verts[key] = new_vertex
        else:
            new_vertex = self.verts[key]

        ts = timestamp

        if ts not in self.timestamps.keys():
            self.timestamps[timestamp] = [key]
        else:
            self.timestamps[timestamp].append(key)

        return new_vertex

    def get_vertex(self, vertex):
        if vertex in self.verts:
            return self.verts[vertex]
        else:
            return None

    """
        process of eviction:
        - Get the list of nodes that needs eviction from timestamps dictionary
        - For each node, evict it's own neighbors
        - If the node contains no neighbor after eviction, remove the node
        - Remove the expired timestamp from the timestamps dictionary
    """
    def evict(self, timestamp):
        nts = timestamp
        evict_ts = [ets for ets in self.timestamps.keys() if (nts - ets) > self.window]

        evict_nodes = []

        for cur_ts in evict_ts:
            evict_nodes = evict_nodes + self.timestamps[cur_ts]
            del self.timestamps[cur_ts]

        evict_nodes = list(set(evict_nodes))

#        print "evict_nodes = " + str(evict_nodes)

        for node in evict_nodes:
            cur_vertex = self.get_vertex(node)
            cur_vertex.evict(timestamp)
            if cur_vertex.get_degree() == 0:
                del self.verts[node]
                self.num_verts -= 1

    def __contains__(self, key):
        return key in self.verts

    def add_edge(self, source, sink, timestamp):
        if source.id not in self.verts:
            nv = self.add_vertex(source.id, timestamp)
        if sink.id not in self.verts:
            nv = self.add_vertex(sink.id, timestamp)
        self.verts[source.id].add_neighbor(self.verts[sink.id], timestamp)
        self.verts[sink.id].add_neighbor(self.verts[source.id], timestamp)

    def get_vertices(self):
        return self.verts.keys()

    def get_timestamps(self):
        return self.timestamps

    def average_degree(self):
        degree = 0
        vertices = self.get_vertices()

        for vertex in vertices: 
            degree += self.verts[vertex].get_degree()

        if (len(vertices) == 0):
            return 0
        else:
            return (degree * 1.0 / len(vertices))

    def __iter__(self):
        return iter(self.verts.values())

    def print_graph(self):
        for vertex in self.get_vertices():
            print self.verts[vertex]

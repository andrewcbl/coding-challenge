from datetime import datetime
from datetime import timedelta

class tweet_vertex:
    def __init__(self, key):
        self.id = key
        self.connected_to = {}
        self.window = timedelta(seconds=60)

    def add_neighbor(self, nbr, timestamp):
        if nbr in self.connected_to.keys():
            self.connected_to[nbr].append(timestamp);
        else:
            self.connected_to[nbr] = [timestamp]

    def get_connections(self):
        return self.connected_to.keys()

    def get_id(self):
        return self.id

    def get_degree(self):
        return len(self.connected_to.keys())

    def evict(self, timestamp):
        if self.get_degree() <= 0:
            return

        nts = timestamp

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

class tweet_graph:
    def __init__(self):
        self.verts = {}
        self.timestamps = {}
        self.num_verts = 0
        self.window = timedelta(seconds=60)

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

    def average_degree(self):
        degree = 0
        vertices = self.get_vertices()

        for vertex in vertices: 
            degree += self.verts[vertex].get_degree()

        if (len(vertices) == 0):
            return 0
        else:
            return "%0.2f" % (degree * 1.0 / len(vertices))

    def __iter__(self):
        return iter(self.verts.values())

    def print_graph(self):
        for vertex in self.get_vertices():
            print self.verts[vertex]

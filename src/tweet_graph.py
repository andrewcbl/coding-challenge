class tweet_vertex:
    def __init__(self, key):
        self.id = key
        self.connected_to = {}

    def add_neighbor(self, nbr, timestamp):
        print self.id, nbr, timestamp
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

    def __repr__(self):
        return self.id + ' connected to: ' + str([x.id for x in self.connected_to])


class tweet_graph:
    def __init__(self):
        self.verts = {}
        self.num_verts = 0

    def add_vertex(self, key):
        self.num_verts += 1
        new_vertex = tweet_vertex(key)
        self.verts[key] = new_vertex
        return new_vertex

    def get_vertex(self, vertex):
        if vertex in self.verts:
            return self.verts[vertex]
        else:
            return None

    def __contains__(self, key):
        return key in self.verts

    def add_edge(self, source, sink, timestamp):
        if source not in self.verts:
            nv = self.add_vertex(source)
        if sink not in self.verts:
            nv = self.add_vertex(sink)
        self.verts[source].add_neighbor(self.verts[sink], timestamp)
        self.verts[sink].add_neighbor(self.verts[source], timestamp)

    def get_vertices(self):
        return self.verts.keys()

    def average_degree(self):
        degree = 0
        vertices = self.get_vertices()

        for vertex in vertices: 
            degree += self.verts[vertex].get_degree()

        return degree * 1.0 / len(vertices)

    def __iter__(self):
        return iter(self.verts.values())

    def print_graph(self):
        for vertex in self.get_vertices():
            print self.verts[vertex]

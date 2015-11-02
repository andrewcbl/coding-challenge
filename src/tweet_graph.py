class tweet_vertex:
    def __init__(self, key):
        self.id = key
        self.connected_to = {}

    def add_neighbor(self, nbr, timestamp):
#        print "add_neighbor"
#        print self.id, nbr, timestamp, self.connected_to.keys()
#        print "-------"
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
        return 'id = \"' + self.id + '\" connected to: ' + str([x.id for x in self.connected_to.keys()])


class tweet_graph:
    def __init__(self):
        self.verts = {}
        self.num_verts = 0

    def add_vertex(self, key):
        new_vertext = None

        if key not in self.verts:
            self.num_verts += 1
            new_vertex = tweet_vertex(key)
            self.verts[key] = new_vertex
        else:
            new_vertex = self.verts[key]

        return new_vertex

    def get_vertex(self, vertex):
        if vertex in self.verts:
            return self.verts[vertex]
        else:
            return None

    def __contains__(self, key):
        return key in self.verts

    def add_edge(self, source, sink, timestamp):
        if source.id not in self.verts:
            nv = self.add_vertex(source.id)
        if sink.id not in self.verts:
            nv = self.add_vertex(sink.id)
        self.verts[source.id].add_neighbor(self.verts[sink.id], timestamp)
        self.verts[sink.id].add_neighbor(self.verts[source.id], timestamp)

    def get_vertices(self):
        return self.verts.keys()

    def average_degree(self):
        degree = 0
        vertices = self.get_vertices()

        for vertex in vertices: 
            degree += self.verts[vertex].get_degree()

        return "%0.2f" % (degree * 1.0 / len(vertices))

    def __iter__(self):
        return iter(self.verts.values())

    def print_graph(self):
        for vertex in self.get_vertices():
            print self.verts[vertex]

import env
import unittest
import json
from src.tweet import Tweet
from src.tweet_graph import tweet_vertex, tweet_graph
from datetime import datetime

class test_tweet_graph(unittest.TestCase):

    def setUp(self):
        self.format = "%a %b %d %H:%M:%S +0000 %Y"

        self.tweets_json = [
           """
              {"created_at":"Thu Oct 29 17:51:01 +0000 2015",
               "text":"Spark Summit East this week! #Spark #Apache",
               "entities":{"hashtags":[{"text":"Spark","indices":[29,35]},{"text":"Apache","indices":[36,43]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:51:30 +0000 2015",
               "text":"Just saw a great post on Insight Data Engineering #Apache #Hadoop #Storm",
               "entities":{"hashtags":[{"text":"Storm","indices":[29,35]},{"text":"Apache","indices":[36,43]},{"text":"Hadoop","indices":[37,48]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:51:55 +0000 2015",
               "text":"Doing great work #Apache",
               "entities":{"hashtags":[{"text":"Apache","indices":[29,35]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:51:56 +0000 2015",
               "text":"Excellent post on #Flink and #Spark",
               "entities":{"hashtags":[{"text":"Flink","indices":[29,35]},{"text":"Spark","indices":[29,35]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:51:59 +0000 2015",
               "text":"New and improved #HBase connector for #Spark",
               "entities":{"hashtags":[{"text":"HBase","indices":[29,35]},{"text":"Spark","indices":[29,35]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:52:05 +0000 2015",
               "text":"New 2.7.1 version update for #Hadoop #Apache",
               "entities":{"hashtags":[{"text":"Hadoop","indices":[29,35]},{"text":"Apache","indices":[29,35]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:52:31 +0000 2015",
               "text":"Try to evict the storm vertex",
               "entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:52:56 +0000 2015",
               "text":"New 2.7.1 version update for #Unrelated",
               "entities":{"hashtags":[{"text":"Unrelated","indices":[29,35]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:52:57 +0000 2015",
               "text":"Excellent post on #Flink and #Spark",
               "entities":{"hashtags":[{"text":"Flink","indices":[29,35]},{"text":"Spark","indices":[29,35]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:54:57 +0000 2015",
               "text":"Another Excellent post on #Flink",
               "entities":{"hashtags":[{"text":"Flink","indices":[29,35]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

        ]

        self.tweets = [Tweet(json.loads(cur_tweet_json)) for cur_tweet_json in self.tweets_json]
        self.timestamps = [datetime.strptime(tweet.get_timestamp(), self.format) for tweet in self.tweets]


    def pairwise(self, hashtags):
        n = len(hashtags)
        return [(hashtags[i], hashtags[j]) for i in range(n) for j in range(n) if i < j]


    def update_graph_with_tweet(self, graph, tindex):
        tweet    = self.tweets[tindex]
        hashtags = tweet.get_hashtags()
        cur_ts   = datetime.strptime(tweet.get_timestamp(), self.format)
        
        if (len(hashtags) >= 2):
            for hashtag in hashtags:
                graph.add_vertex(hashtag, cur_ts)

            edges = self.pairwise(hashtags)

            for edge in edges:
                graph.add_edge(graph.get_vertex(edge[0]),
                               graph.get_vertex(edge[1]),
                               cur_ts)

        else:
            graph.evict(cur_ts)

        return graph


    def check_one_tweet_graph(self, graph):
        cur_vertex = graph.get_vertex('apache')
        self.assertEqual([nbr.id for nbr in cur_vertex.get_connections()], ['spark'])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
        self.assertEqual(cur_vertex.get_degree(), 1)

        cur_vertex = graph.get_vertex('spark')
        self.assertEqual([nbr.id for nbr in cur_vertex.get_connections()], ['apache'])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
        self.assertEqual(cur_vertex.get_degree(), 1)
 
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted(['apache', 'spark']))
        self.assertAlmostEqual(graph.average_degree(), 1)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(graph_timestamps.keys(), [self.timestamps[0]])
        self.assertEqual(sorted(graph_timestamps[self.timestamps[0]]), sorted(['apache', 'spark']))


    # This test covers the graph behavior when one tweet is inserted
    def test_one_tweet(self):
        graph = tweet_graph()

        # Insert the 1st tweet to the graph
        graph = self.update_graph_with_tweet(graph, 0)

        self.check_one_tweet_graph(graph)


    def check_two_tweets_graph(self, graph):
        # 'apache' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('apache')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark', 'storm', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 3)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])

        # 'spark' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('spark')
        self.assertEqual([nbr.id for nbr in cur_vertex.get_connections()], ['apache'])
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
 
        # 'storm' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('storm')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
 
        # 'hadoop' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hadoop')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'storm']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
 
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted(['apache', 'spark', 'storm', 'hadoop']))
        self.assertAlmostEqual(graph.average_degree(), 2)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(graph_timestamps.keys(), [self.timestamps[0], self.timestamps[1]])
        self.assertEqual(sorted(graph_timestamps[self.timestamps[0]]), sorted(['apache', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[1]]), sorted(['apache', 'storm', 'hadoop']))


    # This test covers the graph behavior when two tweets are inserted
    def test_two_tweet(self):
        graph = tweet_graph()

        # Insert the two tweets to the graph
        num_tweets = 2;

        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)


        self.check_two_tweets_graph(graph)


    # This test covers the graph behavior when three tweets are inserted
    # The third tweet contains only one hashtag and would not update graph
    def test_three_tweet(self):
        graph = tweet_graph()

        # Insert the three tweets to the graph
        num_tweets = 3;

        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        self.check_two_tweets_graph(graph)


    def check_four_tweets_graph(self, graph):
        # 'apache' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('apache')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark', 'storm', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 3)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])

        # 'spark' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('spark')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'flink']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('flink'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
 
        # 'storm' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('storm')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
 
        # 'hadoop' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hadoop')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'storm']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])

        # 'flink' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('flink')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
 
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted(['apache', 'spark', 'storm', 'hadoop', 'flink']))
        self.assertAlmostEqual(graph.average_degree(), 2)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(graph_timestamps.keys(), [self.timestamps[0], self.timestamps[1], self.timestamps[3]])
        self.assertEqual(sorted(graph_timestamps[self.timestamps[0]]), sorted(['apache', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[1]]), sorted(['apache', 'storm', 'hadoop']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[3]]), sorted(['flink', 'spark']))


    # This test covers the graph behavior when four tweets are inserted
    def test_four_tweet(self):
        graph = tweet_graph()
        num_tweets = 4;

        # Insert the two tweets to the graph
        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        self.check_four_tweets_graph(graph)


    def check_five_tweets_graph(self, graph):
        # 'apache' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('apache')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark', 'storm', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 3)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])

        # 'spark' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('spark')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'flink', 'hbase']))
        self.assertEqual(cur_vertex.get_degree(), 3)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[0].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('flink'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hbase'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        # 'storm' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('storm')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
 
        # 'hadoop' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hadoop')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'storm']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
 
        # 'flink' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('flink')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
 
        # 'hbase' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hbase')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted(['apache', 'spark', 'storm', 'hadoop', 'flink', 'hbase']))
        self.assertAlmostEqual(graph.average_degree(), 2)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(sorted(graph_timestamps.keys()), sorted([self.timestamps[0], self.timestamps[1], self.timestamps[3], self.timestamps[4]]))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[0]]), sorted(['apache', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[1]]), sorted(['apache', 'storm', 'hadoop']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[3]]), sorted(['flink', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[4]]), sorted(['hbase', 'spark']))


    def test_five_tweet(self):
        graph = tweet_graph()
        num_tweets = 5;

        # Insert the two tweets to the graph
        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        self.check_five_tweets_graph(graph)


    def check_six_tweets_graph(self, graph):
        # 'apache' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('apache')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['storm', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format),
                                          datetime.strptime(self.tweets[5].get_timestamp(), self.format)])

        # 'spark' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('spark')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['flink', 'hbase']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('flink'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hbase'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        # 'storm' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('storm')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
 
        # 'hadoop' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hadoop')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache', 'storm']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format),
                                          datetime.strptime(self.tweets[5].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('storm'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[1].get_timestamp(), self.format)])
 
        # 'flink' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('flink')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
 
        # 'hbase' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hbase')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted(['apache', 'spark', 'storm', 'hadoop', 'flink', 'hbase']))
        self.assertAlmostEqual(graph.average_degree(), 1.66666667)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(sorted(graph_timestamps.keys()), sorted([self.timestamps[1], self.timestamps[3], self.timestamps[4], self.timestamps[5]]))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[1]]), sorted(['apache', 'storm', 'hadoop']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[3]]), sorted(['flink', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[4]]), sorted(['hbase', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[5]]), sorted(['apache', 'hadoop']))


    def test_six_tweet(self):
        graph = tweet_graph()
        num_tweets = 6;

        # Insert the two tweets to the graph
        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        # Graph is disconnected into two parts
        self.check_six_tweets_graph(graph)


    def check_seven_tweets_graph(self, graph):
        # 'apache' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('apache')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[5].get_timestamp(), self.format)])

        # 'spark' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('spark')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['flink', 'hbase']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('flink'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hbase'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        # 'hadoop' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hadoop')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[5].get_timestamp(), self.format)])
 
        # 'flink' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('flink')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[3].get_timestamp(), self.format)])
 
        # 'hbase' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hbase')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted(['apache', 'spark', 'hadoop', 'flink', 'hbase']))
        self.assertAlmostEqual(graph.average_degree(), 1.2)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(sorted(graph_timestamps.keys()), sorted([self.timestamps[3], self.timestamps[4], self.timestamps[5]]))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[3]]), sorted(['flink', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[4]]), sorted(['hbase', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[5]]), sorted(['apache', 'hadoop']))


    def test_seven_tweet(self):
        graph = tweet_graph()
        num_tweets = 7;

        # Insert the seven tweets to the graph
        # "storm" node is retired
        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        self.check_seven_tweets_graph(graph)


    # Should be no change after 8th tweet since it is 60 seconds away from 2nd tweet
    # And there is only 1 hashtag
    def test_eight_tweet(self):
        graph = tweet_graph()
        num_tweets = 8;

        # Insert the seven tweets to the graph
        # "storm" node is retired
        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        self.check_seven_tweets_graph(graph)


    def check_nine_tweets_graph(self, graph):
        # 'apache' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('apache')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['hadoop']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hadoop'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[5].get_timestamp(), self.format)])

        # 'spark' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('spark')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['flink', 'hbase']))
        self.assertEqual(cur_vertex.get_degree(), 2)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('flink'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[8].get_timestamp(), self.format)])
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('hbase'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        # 'hadoop' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hadoop')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['apache']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('apache'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[5].get_timestamp(), self.format)])
 
        # 'flink' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('flink')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[8].get_timestamp(), self.format)])
 
        # 'hbase' vertex, check neighbors, timestamps for each neighbor
        cur_vertex = graph.get_vertex('hbase')
        self.assertEqual(sorted([nbr.id for nbr in cur_vertex.get_connections()]), sorted(['spark']))
        self.assertEqual(cur_vertex.get_degree(), 1)
        cur_timestamps = cur_vertex.get_timestamp(graph.get_vertex('spark'))
        self.assertEqual(cur_timestamps, [datetime.strptime(self.tweets[4].get_timestamp(), self.format)])
 
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted(['apache', 'spark', 'hadoop', 'flink', 'hbase']))
        self.assertAlmostEqual(graph.average_degree(), 1.2)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(sorted(graph_timestamps.keys()), sorted([self.timestamps[4], self.timestamps[5], self.timestamps[8]]))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[4]]), sorted(['hbase', 'spark']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[5]]), sorted(['apache', 'hadoop']))
        self.assertEqual(sorted(graph_timestamps[self.timestamps[8]]), sorted(['flink', 'spark']))


    # Replace old edge with new edge should have no impact on degree. But timestamp
    # should get updated
    def test_nine_tweet(self):
        graph = tweet_graph()
        num_tweets = 9;

        # Insert the seven tweets to the graph
        # "storm" node is retired
        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        self.check_nine_tweets_graph(graph)


    def check_ten_tweets_graph(self, graph):
        verts = graph.get_vertices()
        self.assertEqual(sorted(verts), sorted([]))
        self.assertAlmostEqual(graph.average_degree(), 0)

        graph_timestamps = graph.get_timestamps()
        self.assertEqual(len(graph_timestamps.keys()), 0)


    # One tweet evicts all
    def test_ten_tweet(self):
        graph = tweet_graph()
        num_tweets = 10;

        # Insert the seven tweets to the graph
        # "storm" node is retired
        for i in range(num_tweets):
            graph = self.update_graph_with_tweet(graph, i)

        self.check_ten_tweets_graph(graph)


if __name__ == '__main__':
    unittest.main()

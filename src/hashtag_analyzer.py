import sys
import json

from datetime import datetime

from tweet import Tweet
from tweet_graph import tweet_vertex, tweet_graph

class hashtag_analyzer:
    def __init__(self):
        self.format = "%a %b %d %H:%M:%S +0000 %Y"
        self.input_file  = None
        self.output_file = None
        
    def __init__(self, input_file, output_file):
        self.format = "%a %b %d %H:%M:%S +0000 %Y"
        self.input_file  = input_file
        self.output_file = output_file

    """
        Generate pairwise list of tuples from the list of hashtags
    """
    def pairwise(self, hashtags):
        n = len(hashtags)
        return [(hashtags[i], hashtags[j]) for i in range(n) for j in range(n) if i < j]

    """
        Steps of calculating average degree:
        1. Read the tweet file and parse with json library
        2. Extract hashtags from the tweet
        3. Generate pairwise tuples from the hashtag list
        4. Build the hashtag graph with the hashtags
        5. Calculate the rolling average degree 
    """
    def average_degree(self):
        try:
            stats_fh = open(self.output_file, 'w')
        except IOError:
            print 'Cannot open', self.output_file

        try:
            tweet_fh = open(self.input_file)
        except IOError:
            print 'Cannot open', self.input_file
        else:
            tweets = tweet_fh.readlines()

            graph = tweet_graph()
        
            for tweet_line in tweets:
                tweet_dec = json.loads(tweet_line)
        
                if "limit" in tweet_dec.keys():
                    continue
        
                cur_tweet = Tweet(tweet_dec)
                hashtags = cur_tweet.get_hashtags()
        
                cur_ts = datetime.strptime(cur_tweet.get_timestamp(), self.format)
        
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
        
#                print "New graph looks like:"
#                graph.print_graph()
#                print "\n"
                stats_fh.write(("%0.2f" % graph.average_degree()) + "\n")
    
            tweet_fh.close()

        if not stats_fh.closed:
            stats_fh.close()


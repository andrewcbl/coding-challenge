# example of program that calculates the average degree of hashtags
import sys
import json
from tweet import Tweet
from tweet_graph import tweet_vertex, tweet_graph

def pairwise(hashtags):
    n = len(hashtags)
    return [(hashtags[i], hashtags[j]) for i in range(n) for j in range(n) if i < j]

def average_degree(tweet_file):
    tweet_fh = open(tweet_file)
    tweets = tweet_fh.readlines()

    graph = tweet_graph()

    for tweet_line in tweets:
        tweet_dec = json.loads(tweet_line)

        if "limit" in tweet_dec.keys():
            continue

        cur_tweet = Tweet(tweet_dec)
        hashtags = cur_tweet.get_hashtags()

#        print hashtags

        if (len(hashtags) >= 2):
            for hashtag in hashtags:
#                print "Adding hashtag " + hashtag
                graph.add_vertex(hashtag)

            edges = pairwise(hashtags)

            for edge in edges:
#                print "adding edge " + str(edge)

                graph.add_edge(graph.get_vertex(edge[0]), 
                               graph.get_vertex(edge[1]), 
                               cur_tweet.get_timestamp())

#            print "New graph looks like:"
#            graph.print_graph()
#            print "\n"

    print graph.average_degree()

    tweet_fh.close()

def main():
    tweet_file = sys.argv[1]
    output_file = sys.argv[2]
    average_degree(tweet_file)

if __name__ == '__main__':
    main()


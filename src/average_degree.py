"""
    This file implements the hashtag degree calculation feature
    It is wrapper over tweet_graph class 
"""
import sys
from hashtag_analyzer import hashtag_analyzer

def main():
    tweet_file = sys.argv[1]
    output_file = sys.argv[2]
    tg = hashtag_analyzer(tweet_file, output_file)

    if len(sys.argv) > 3:
        tg.set_self_check(int(sys.argv[3]))

    tg.average_degree()

if __name__ == '__main__':
    main()


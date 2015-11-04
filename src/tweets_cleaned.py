"""
    This file implements the tweet text cleaning feature
    It is wrapper over tweet class
"""

import sys
import json
import re

from time import strftime
from datetime import datetime

from tweet import Tweet

class tweet_cleaner:
    def __init__(self):
        self.input_file = None
        self.output_file = None
        self.num_unicode_tweets = 0

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.num_unicode_tweets = 0

    """
        Steps of calculating average degree:
        1. Read the tweet file and parse with json library
        2. Extract text from the tweet
        3. Remove non-ascii characters from the tweet text
        4. Update the statistics of tweet cleaning
    """
    def clean_tweets(self):
        self.num_unicode_tweets = 0

        try:
            clean_fh = open(self.output_file, 'w')
        except IOError:
            print 'Cannot open', self.output_file

        try:
            tweet_fh = open(self.input_file)
        except IOError:
            print 'Cannot open', self.input_file
        else:
            tweets = tweet_fh.readlines()

            for tweet_line in tweets:
                tweet_dec = json.loads(tweet_line)

                if "limit" in tweet_dec.keys():
                    continue

                cur_tweet = Tweet(tweet_dec)
                clean_fh.write(cur_tweet.to_str() + "\n")

                if not cur_tweet.is_tweet_ascii():
                    self.num_unicode_tweets += 1

            clean_fh.write("\n%d tweets contained unicode" % (self.num_unicode_tweets))

            tweet_fh.close()

        if not clean_fh.closed:
            clean_fh.close()

def main():
    cleaner = tweet_cleaner(sys.argv[1], sys.argv[2])
    cleaner.clean_tweets()

if __name__ == '__main__':
    main()

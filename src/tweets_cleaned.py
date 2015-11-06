"""
    This file implements the tweet text cleaning feature
    It is wrapper over tweet class

    This also implements one basic self checking scheme to make sure 
    the number of tweets containing unicode is correct
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
        self.self_checking = False

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.num_unicode_tweets = 0
        self.self_checking = False

    def set_self_check(self, enable):
        self.self_checking = enable

    """
        Steps of calculating average degree:
        1. Read the tweet file and parse with json library
        2. Extract text from the tweet
        3. Remove non-ascii characters from the tweet text
        4. Update the statistics of tweet cleaning
    """
    def clean_tweets(self):
        self.num_unicode_tweets = 0
        same_tweets_after_clean_cnt = 0

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

                # This portion of code is for basic self checking mechanism
                if self.self_checking:
                    cur_text = cur_tweet.get_text()
                    if cur_tweet.remove_non_ascii(cur_text) != cur_text:
                        same_tweets_after_clean_cnt += 1

                if not cur_tweet.is_tweet_ascii():
                    self.num_unicode_tweets += 1

            clean_fh.write("\n%d tweets contained unicode" % (self.num_unicode_tweets))

            tweet_fh.close()

        if self.self_checking and (same_tweets_after_clean_cnt != self.num_unicode_tweets):
            print "Self checking failure: " + str(same_tweets_after_clean_cnt) + str(self.num_unicode_tweets)

        if not clean_fh.closed:
            clean_fh.close()

def main():
    cleaner = tweet_cleaner(sys.argv[1], sys.argv[2])

    if len(sys.argv) > 2:
        cleaner.set_self_check(int(sys.argv[3]))

    cleaner.clean_tweets()

if __name__ == '__main__':
    main()

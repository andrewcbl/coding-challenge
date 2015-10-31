# example of program that calculates the number of tweets cleaned
import sys
import json
import re
from time import strftime
from datetime import datetime

class tweet:
    def __init__(self):
        self._tweet = None 

    def __init__(self, t):
        self._tweet = t

    def get_text(self):
        if self._tweet != None:
            if "text" in self._tweet.keys():
                return self._tweet["text"]
            else:
                raise ValueError("Text is empty in the tweet")
        else:
            raise ReferenceError("Tweet is not initialized")

    def get_clean_text(self):
        try:
            text = self.get_text()
        except ValueError as e:
            raise ValueError(e.strerror)
        except ReferenceError as e:
            raise ReferenceError(e.strerror)

        return self.remove_non_ascii2(text)

    def get_timestamp(self):
        if self._tweet != None:
            if "created_at" in self._tweet.keys():
                return self._tweet["created_at"]
            elif "timestamp_ms" in self._tweet.keys():
                timestamp_ms = self._tweet["timestamp_ms"]
                cur_datetime = datetime.fromtimestamp(int(timestamp_ms) / 1000.0)
                return cur_datetime.strftime("%a %b %d %H:%M:%S +0000 %Y") 
            else:
                raise ValueError("Timestamp is empty in the tweet")
        else:
            raise ReferenceError("Tweet is not initialized")

    def is_tweet_ascii(self):
        text = self.get_text()

        try:
            text.decode('ascii')
        except UnicodeEncodeError:
            return False
        else:
            return True

    def remove_non_ascii(self, text):
        return ''.join(i if ord(i) < 128 else '*' for i in text)

    def remove_non_ascii2(self, text):
        return re.sub(r'[^\x00-\x7F]+','', text)

    def to_str(self):
        tweet_msg = self.get_clean_text()
        tweet_ts  = self.get_timestamp()

        return tweet_msg + " (timestamp: " + tweet_ts + ")"
        
    def __repr__(self):
         return self.to_str()

class tweet_cleaner:
    def __init__(self):
        self.input_file = None
        self.output_file = None
        self.num_unicode_tweets = 0

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.num_unicode_tweets = 0

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

            for cur_tweet in tweets:
                tweet_dec = json.loads(cur_tweet)

                if "limit" in tweet_dec.keys():
                    continue

                cur_tweet = tweet(tweet_dec)
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

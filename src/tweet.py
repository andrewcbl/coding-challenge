# example of program that calculates the number of tweets cleaned
import json
import re
from time import strftime
from datetime import datetime

class Tweet:
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

    def extract_tags(self, tags_array):
        result = []

        for tag in tags_array:
            if "text" in tag.keys():
                tag_clean = self.remove_non_ascii2(tag["text"]).strip()
                if len(tag_clean) != 0:
                    result.append(tag["text"].lower())

        return result

    def get_hashtags(self):
        if (self._tweet != None):
            if "entities" in self._tweet.keys():
                entities = self._tweet["entities"]
                if "hashtags" in entities.keys():
                    return self.extract_tags(entities["hashtags"])
                else:
                    return []
            else:
                return []
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

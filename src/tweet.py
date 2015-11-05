# -*- coding: utf-8 -*-
"""
Basic class to parse tweet. 
Supports tweet text cleaning, hashtag extraction functions
"""

import json
import re
from time import strftime
from datetime import datetime

class Tweet:
    def __init__(self):
        self._tweet = None 

    def __init__(self, t):
        self._tweet = t

    """
        Returns original (uncleaned) text in the tweet
    """
    def get_text(self):
        if self._tweet != None:
            if "text" in self._tweet.keys():
                return self._tweet["text"]
            else:
                raise ValueError("Text is empty in the tweet")
        else:
            raise ReferenceError("Tweet is not initialized")


    """
        Returns cleaned text in the tweet
        Text cleaning mechanism: Remove non ascii characters from the text 
    """
    def get_clean_text(self):
        try:
            text = self.get_text()
        except ValueError as e:
            raise ValueError(e.strerror)
        except ReferenceError as e:
            raise ReferenceError(e.strerror)

        text = self.remove_non_ascii(text)
        return self.replace_escape_seq(text)


    """
        Returns timestamp from the tweet
        Supported two ways of timestamp extraction
        - From the "created_at" field
        - From the "timestamp_ms" field
    """
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

    """
        Each tag entry is a dictionary type. Only returns the TAG itself
        This function also cleans the hashtag and convert to lower case
    """
    def extract_tags(self, tags_array):
        result = []

        for tag in tags_array:
            if "text" in tag.keys():
                tag_clean = self.remove_non_ascii(tag["text"]).strip()
                tag_clean = self.replace_escape_seq(tag_clean)
                if len(tag_clean) != 0:
                    result.append(tag_clean.lower())

        return result

    """
        This function extract hashtags from the tweet
    """
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

    """
        This function checks if the tweet text contains only ASCII code
    """
    def is_tweet_ascii(self):
        text = self.get_text()

        try:
            text.decode('ascii')
        except UnicodeEncodeError:
            return False
        else:
            return True

    def remove_non_ascii(self, text):
        return re.sub(r'[^\x00-\x7F]+','', text)

    def replace_escape_seq(self, text):
        str = re.sub(r'\n', ' ', text)
        str = re.sub(r'\t', ' ', str)
        return str

    """
        Reformatting the tweet as "clean text" + "timestamp" layout
    """
    def to_str(self):
        tweet_msg = self.get_clean_text()
        tweet_ts  = self.get_timestamp()

        return tweet_msg + " (timestamp: " + tweet_ts + ")"
        
    def __repr__(self):
         return self.to_str()

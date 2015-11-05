import env
import unittest
import json
from src.tweet import Tweet

class test_tweet(unittest.TestCase):
    def setUp(self):
        self.tweets_json = [
           """
              {"created_at":"Thu Oct 29 17:51:01 +0000 2015",
               "text":"Spark Summit East this week! #Spark #Apache",
               "entities":{"hashtags":[{"text":"Spark","indices":[29,35]},{"text":"Apache","indices":[36,43]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 18:10:49 +0000 2015",
               "text":"I'm at Terminal de Integra\u00e7\u00e3o do Varadouro in Jo\u00e3o Pessoa, \\t\\n\\\PB https:\/\/t.co\/HOl34REL1a",
               "entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[]}}
           """,

           """
              {"created_at":"Thu Oct 29 17:51:01 +0000 2015",
               "text":"Spark Summit East this week! #Spark #Apache",
               "entities":{"hashtags":[{"text":"Spark\\"\\\\\/","indices":[29,35]},{"text":"Apache\\n\\tStorm","indices":[36,43]}],"urls":[],"user_mentions":[],"symbols":[]}}
           """
        ]

        self.tweets = [json.loads(cur_tweet_json) for cur_tweet_json in self.tweets_json]

    # This test covers if the function removes unicode and 
    # keeps all the non-unicode characters
    def test_clean_text(self):
        self.assertEqual(Tweet(self.tweets[0]).get_clean_text(), "Spark Summit East this week! #Spark #Apache")
        self.assertEqual(Tweet(self.tweets[1]).get_clean_text(), "I'm at Terminal de Integrao do Varadouro in Joo Pessoa,   \PB https://t.co/HOl34REL1a")
 
    # This test covers if the function can determine if a 
    # tweet contains only ascii text.
    def test_is_tweet_ascii(self):
        self.assertEqual(Tweet(self.tweets[0]).is_tweet_ascii(), True)
        self.assertEqual(Tweet(self.tweets[1]).is_tweet_ascii(), False)

    # This test covers if the tweet class can extract hashtags properly
    # This test also covers if the hashtags is cleaned
    def test_get_hashtags(self):
        self.assertEqual(Tweet(self.tweets[0]).get_hashtags(), ["spark", "apache"])
        self.assertEqual(Tweet(self.tweets[1]).get_hashtags(), [])
        self.assertEqual(Tweet(self.tweets[2]).get_hashtags(), ["spark\"\/", "apache  storm"])
 
if __name__ == '__main__':
    unittest.main()

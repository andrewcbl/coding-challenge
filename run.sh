#!/usr/bin/env bash

# example of the run script for running the word count

#+----------------------------------------------------------------------+
#|Part1. Feature implementation for the coding challenge requirements   |
#+----------------------------------------------------------------------+
# I'll execute my programs, with the input directory tweet_input and 
# output the files in the directory tweet_output
python ./src/tweets_cleaned.py ./tweet_input/tweets.txt ./tweet_output/ft1.txt
python ./src/average_degree.py ./tweet_input/tweets.txt ./tweet_output/ft2.txt

#+-----------------------------------------------------+
#| Part 2: Visualization of hashtag graph degree trend |
#+-----------------------------------------------------+

# If user want to visuallize the trend of hashtag average and peak degree
# Use this command
# It also prints the statistics of how many times one hashtag be the one
# with most connections

# python ./src/hashtag_plots.py ./tweet_input/tweets.txt ./tweet_output/ft3.txt


#+-----------------------------------------+
#| Part 3: Executing the testing programs  |
#+-----------------------------------------+
# First one is for the tweet cleaning feature
# Second one is for the tweet graph feature

# python -m test.test_tweet
# python -m test.test_tweet_graph

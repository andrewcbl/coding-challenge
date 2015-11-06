"""
    This file visually displays different statistics of hashtags in the tweets
"""
import sys
import matplotlib.pyplot as plt
from hashtag_analyzer import hashtag_analyzer

def hashtag_to_tuples(hashtags):
    hashtag_dict = []
    prev_hashtag =  ""
    cur_hashtag_cnt = 0

    # Count the occurrence of each hashtag
    for i in range(len(hashtags)):
        if hashtags[i] != prev_hashtag:
            if prev_hashtag != "": 
                hashtag_dict.append((prev_hashtag, cur_hashtag_cnt))
                prev_hashtag = hashtags[i]
                cur_hashtag_cnt = 1
            else:
                prev_hashtag = hashtags[i]
                cur_hashtag_cnt = 1
        else:
            cur_hashtag_cnt += 1

    if cur_hashtag_cnt != 0:
        hashtag_dict.append((prev_hashtag, cur_hashtag_cnt))

    return hashtag_dict


def main():
    tweet_file = sys.argv[1]
    output_file = sys.argv[2]
    tg = hashtag_analyzer(tweet_file, output_file)
    tg.set_track(True)
    tg.average_degree()
    ad_hist = tg.get_ad_hist()
    pd_hist = tg.get_pd_hist()
    pn_hist = tg.get_pn_hist()

    hashtags_stats = hashtag_to_tuples(pn_hist)

    print "Peak hashtag count (The number of times some hashtag continuously being peak hashtag): \n" + str(hashtags_stats)

    plt.figure("Average Tweet Hashtag Degree")
    plt.plot(range(len(ad_hist)), ad_hist)
    plt.figure("Peak Tweet Hashtag Degree")
    plt.plot(range(len(pd_hist)), pd_hist)
    plt.show()


if __name__ == '__main__':
    main()


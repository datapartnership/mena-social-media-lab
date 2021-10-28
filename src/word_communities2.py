import psycopg2
import nltk
import pandas as pd
import os

seed_words = ['inequality', 'inequity', 'opportunity', 'american dream',
              'social mobility', 'economic mobility', 'wealth', 'inclusion',
              'elite capture', 'cronyism', 'gilded age', 'top income']
seed_words = ['terrorism']
seed_words = ['inequality']

seed_words = [seed_word.lower() for seed_word in seed_words]
ps = nltk.stem.PorterStemmer()
seed_words = [ps.stem(seed_word) for seed_word in seed_words]

input_dir = '/home/alexei/Dropbox/alexei_roy_shared_folder/twitter_handles_to_track/influencer_iterative_procedure2'
output_dir = '/home/alexei/Dropbox/alexei_roy_shared_folder/scripts/word_communities'
# LOAD TWEETS:
my_dbname = 'alexei_roy_db2'
my_user = 'alexei'
my_host = '127.0.0.1'
my_password = 'Knopfler123!'
conn = psycopg2.connect(dbname=my_dbname, user=my_user, password=my_password, host='127.0.0.1')
cur = conn.cursor()
cur.execute('SELECT * FROM persona_tweets LIMIT 0')
column_metadata = cur.description
column_vars = [x.name for x in column_metadata]
for var in column_vars:
    print(var)

# influencer tweet repository:
cur.execute('SELECT tweet_text, tweet_retweet_id FROM persona_tweets')
tweets = cur.fetchall()

tweets = [x for (x, y) in tweets if y is None]  # remove retweets

# define stopwords:
stop_words = nltk.corpus.stopwords.words('english')
stop_words = stop_words + ['n\'t', '...', '-', '/', '\'', '\\', '.', '…', 'rt', 'i', 'my', '@', ':', '#', 'co', 'http', 'https', ';', '’', '?', '&', '``', '!',
                           '(', ')', ',', 'amp', "''", '“', '”', '%', '$', '\'s']

def tokenize_remove_stopwords_and_stem(tweet):
    tweet = tweet.lower() # lowercase
    tweet = nltk.tokenize.word_tokenize(tweet) # tokenize
    tweet = [word for word in tweet if word not in stop_words] # remove stopwords
    ps = nltk.stem.PorterStemmer() # define stem object
    tweet = [ps.stem(word) for word in tweet] # stem remaining words in tweets
    tweet = ' '.join(tweet) # reconstitute tweet as a string
    return tweet

import time
start_time = time.time()
for i in range(0, len(tweets)):
    tweets[i] = tokenize_remove_stopwords_and_stem(tweets[i])
    if i%10000 == 0:
        print('done ', i, ' so far!')
        print(time.time()-start_time)

# tweets = [tokenize_remove_stopwords_and_stem(tweet) for tweet in tweets] # tokenize

# # remove particular punctuation marks:
# tweets = [doc.replace('…', ' ') for doc in tweets]
# tweets = [doc.replace('.', ' ') for doc in tweets]
# # tweets = [doc.replace('—', ' ') for doc in tweets]
# tweets = [doc.replace('\\', ' ') for doc in tweets]
# tweets = [doc.replace('-', ' ') for doc in tweets]
# tweets = [doc.replace('/', ' ') for doc in tweets]
# tweets = [doc.replace('\'', ' ') for doc in tweets]
# # tweets = [doc.replace('_', ' ') for doc in tweets]

###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
def enough_seed_words_appear_as_substrings_of_tweet(tweet, seed_words, num_seed_words_appearing=1):
    count = 0
    for seed_word in seed_words:
        if not is_bigram(seed_word):
            if tweet.find(seed_word) != -1:
                count += 1
        else: # seed word is a bigram
            seed_sub_words = seed_word.split()
            if tweet.find(seed_sub_words[0]) != -1 and tweet.find(seed_sub_words[1]) != -1:
                count += 1
    if count >= num_seed_words_appearing:
        return True
    else:
        return False

def is_bigram(word):
    if len(word.split()) == 2:
        return True
    else:
        return False

# check if seed words appear:
def enough_seed_words_appear_independently_in_tweet(tweet, seed_words, num_seed_words_appearing=1):
    # tokenize tweets:
    tweet_word_list = nltk.tokenize.word_tokenize(tweet)
    # tweet_word_list = [word for word in tweet_word_list if word not in stop_words]

    # ps = nltk.stem.PorterStemmer()
    # tweet_word_list = [ps.stem(word) for word in tweet_word_list]

    # need a tweet_bigram_list, and to check
    bgs = nltk.bigrams(tweet_word_list)  # extract all bigrams from corpus
    tweet_bigrams_list = []
    for item in bgs:
        tweet_bigrams_list.append(item[0] + " " + item[1])
    del bgs
    tweet_bigrams_list = list(set(tweet_bigrams_list))

    tweet_word_list = list(set(tweet_word_list)) # this line MUST follow bigrams, not precede

    count = 0
    for seed_word in seed_words:
        if not is_bigram(seed_word):
            for word in tweet_word_list:
                if word == seed_word:
                    count += 1
        else: # seed word is a bigram:
            for word in tweet_bigrams_list:
                if word == seed_word:
                    count += 1

    if count >= num_seed_words_appearing:
        return True
    else:
        return False

def enough_seed_words_in_word(word, seed_words, num_seed_words_appearing = 1):
    print('now checking ', word, 'against seed words ', seed_words)
    count = 0
    for seed_word in seed_words:
        if word.find(seed_word) != -1 or word == seed_word:
            count += 1
    if count >= num_seed_words_appearing:
        return True
    else:
        return False

def find_freq_of_particular_word(tweets, seed_words=['inequality']):
    number_of_tweets_on_topic = len(tweets)
    print('number of tweets on topic: ', number_of_tweets_on_topic)
    tweets = [tweet for tweet in tweets if enough_seed_words_appear_independently_in_tweet(tweet, seed_words)]
    number_of_tweets_mentioning_seed_words = len(tweets)
    print('number of on-topic tweets mentioning ', seed_words, ': ', number_of_tweets_mentioning_seed_words)
    if number_of_tweets_on_topic > 0:
        share_of_ontopic_tweets_with_seed_words = 100.0 * number_of_tweets_mentioning_seed_words / number_of_tweets_on_topic
        print('percentage: ', share_of_ontopic_tweets_with_seed_words)
    else:
        share_of_ontopic_tweets_with_seed_words = 0.0
    return share_of_ontopic_tweets_with_seed_words, number_of_tweets_on_topic, number_of_tweets_mentioning_seed_words

# def enough_seed_words_in_tweet2(tweet, seed_words, num_seed_words_appearing=1):
#     count = 0
#     for seed_word in seed_words:
#         if tweet.find(seed_word) != -1:
#             count += 1
#     if count >= num_seed_words_appearing:
#         return True
#     else:
#         return False

def new_common_words_finder(tweets, seed_words, cut_off):
    tweets = [tweet for tweet in tweets if enough_seed_words_appear_independently_in_tweet(tweet, seed_words)]

    # tokenize tweets:
    tweet_word_lists = [nltk.tokenize.word_tokenize(tweet) for tweet in tweets]
    tweet_word_lists = [[word for word in tweet if word not in stop_words] for tweet in tweet_word_lists]

    ps = nltk.stem.PorterStemmer()
    tweet_word_lists = [[ps.stem(word) for word in tweet_word_list] for tweet_word_list in tweet_word_lists]

    # single-word frequencies:
    freq_dictionary = {}
    for tweet_word_list in tweet_word_lists:
        tweet_word_list = list(set(tweet_word_list))
        for word in tweet_word_list:
            if word not in freq_dictionary:
                freq_dictionary[word] = 100.0*1/len(tweets)
            else:
                freq_dictionary[word] += 100.0*1/len(tweets)

    for tweet_word_list in tweet_word_lists:
        bgs = nltk.bigrams(tweet_word_list)  # extract all bigrams from corpus
        bigrams_list = []
        for item in bgs:
            bigrams_list.append(item[0] + " " + item[1])
        del bgs

        bigrams_list = list(set(bigrams_list))
        for word in bigrams_list:
            if word not in freq_dictionary:
                freq_dictionary[word] = 100.0*1/len(tweet_word_lists)
            else:
                freq_dictionary[word] += 100.0*1/len(tweet_word_lists)

    #############################################################

    freq_ordered_list = [(key, freq_dictionary[key]) for key in freq_dictionary]
    freq_ordered_list.sort(reverse=True, key=lambda x: x[1])
    reduced_freq_ordered_list = [(x, y) for (x, y) in freq_ordered_list if y >= cut_off]
    return reduced_freq_ordered_list
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
# import these modules

###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################
###################################################################################

# tweets_df = pd.DataFrame(tweets, columns=['inequality'])
# tweets_df = tweets_df.sample(frac=1)
# tweets_df.to_csv(output_dir + os.sep + 'inequality_tweets.csv', index=False)

cut_off = 0.5  # percent collocation of word with the (union of) seed words
# most_common_words = new_common_words_finder([tweet for tweet in tweets if enough_seed_words_in_tweet(tweet, seed_words, 1)], cut_off)

iteration_number = 0
new_seed_words = seed_words
delta = len(seed_words)
while delta > 0:
    seed_words = new_seed_words
    print('beginning iteration number ', str(iteration_number), ' with ', len(seed_words), ' seed words: ', seed_words)
    # find most common singles & bigrams for seed_word:
    most_common_words = new_common_words_finder([tweet for tweet in tweets if enough_seed_words_appear_as_substrings_of_tweet(tweet, seed_words, 1)], seed_words, cut_off)
    print("number of candidate words for seed words ", seed_words, ": ", len(most_common_words))

    most_common_words = [(x, y) for (x, y) in most_common_words if not enough_seed_words_in_word(x, seed_words)]
    # most_common_words = [(x, y) for (x, y) in most_common_words if y > cut_off]
    print("number of candidate words for seed words ", seed_words, " after removing overlapping words: ",
          len(most_common_words))
    for word in most_common_words:
        print(word)
    results = []

    for word_tuple in most_common_words:
        word = word_tuple[0]
        freq = word_tuple[1]
        print('============================================')
        print('topic word: ', word, '(appears in ', freq, '% of tweets on topic ', seed_words, ')')
        clean_list_of_tweets_mentioning_word = [tweet for tweet in [tweet for tweet in tweets if enough_seed_words_appear_as_substrings_of_tweet(tweet, [word])]
         if enough_seed_words_appear_independently_in_tweet(tweet, [word])]
        share_of_ontopic_tweets_with_search_term, number_of_tweets_on_topic, number_of_tweets_mentioning_search_word = find_freq_of_particular_word(clean_list_of_tweets_mentioning_word, seed_words)
        results.append((word, freq, share_of_ontopic_tweets_with_search_term, number_of_tweets_on_topic, number_of_tweets_mentioning_search_word))
        # break

    # export results to disk:
    results_df = pd.DataFrame(results, columns=['word', 'incidence_of_other_words_conditional_on_seed_words', 'incidence_of_seed_words_conditional_on_other_words', 'number_of_tweets_on_row_topic', 'number_of_tweets_on_row_topic_mentioning_' + '_'.join(seed_words)])
    results_df.to_csv(output_dir + os.sep + 'word_community_table_for_' + '_'.join(seed_words) + '_iteration_' + str(iteration_number) + '_TUESDAY_JULY2ND.csv', index=False)
    ############################################################################
    ############################################################################
    # RESET FOR NEXT ITERATION:
    # based on cutoff, choose which of the related words to add to the seed word list
    keep_results = [result for result in results if result[1]>cut_off and result[2]>cut_off]
    print(keep_results)
    seed_words_to_add = [result[0] for result in keep_results]
    print('adding the following words: ', seed_words_to_add)
    new_seed_words = seed_words + seed_words_to_add
    delta = len(new_seed_words) - len(seed_words)
    iteration_number += 1
    print('======================================================')
    print('======================================================')
    print('======================================================')
    print('======================================================')

for my_word in []:
    print(100.0*len([tweet for tweet in inequality_tweets if tweet.find(my_word)!=-1])/len(inequality_tweets))
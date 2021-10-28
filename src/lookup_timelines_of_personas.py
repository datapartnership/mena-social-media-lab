# input_dir = '/home/ubuntu'
input_dir = '/home/alexei/Dropbox/alexei_roy_shared_folder/scripts'

import sys
sys.path.append(input_dir)
import pika
import threading
import pandas as pd
import os
import tweepy
import new_mylib
import json
import psycopg2
from datetime import datetime, timezone

#############################################################################
my_input_queue = "personas"
my_output_queue = "persona_tweets"
log_dir = input_dir + os.sep + 'logs'
apps_csv = input_dir + os.sep + "twitter_apps.csv"

my_dbname = 'personas_db'
my_user = 'postgres'
my_password = 'Almowt3amlshaa9'
#############################################################################
#############################################################################
crr_dt = datetime.now()
crr_dt_nice = "{}_{}_{}_{}_{}_{}_{}".format(crr_dt.year, crr_dt.month,
                                            crr_dt.day, crr_dt.hour, crr_dt.minute, crr_dt.second, 'local')
logger = new_mylib.define_logger('persona_timeline_downloader_{}'.format(crr_dt_nice), log_dir)
# logger = new_mylib.define_logger(my_output_queue, log_dir)

#############################################################################
# IMPORT TWITTER AUTHENTICATION CREDENTIALS FROM CSV:
app_df = pd.read_csv(apps_csv)
app_list = []
for i in range(0, app_df.shape[0]):
    app_dict = {}
    for var in list(app_df):
        app_dict[var] = app_df.loc[i, var]
    app_list.append(app_dict)
# app_list = [app_list[-1]]
logger.warning('apps available to deploy: ' + str(len(app_list)))
#############################################################################
def worker(app_credentials):
    params = pika.ConnectionParameters('localhost', heartbeat=60*30,
                                       blocked_connection_timeout=60*30)
    connection = pika.BlockingConnection(params)
    input_channel = connection.channel()

    def callback(ch, method, properties, body):
        def publish_results(all_results, my_queue, my_table):
            connection = pika.BlockingConnection(params)
            output_channel = connection.channel()
            output_channel.queue_declare(queue=my_queue, durable=True)
            publish_count = 0
            for tweet_json in all_results:
                if isinstance(tweet_json, dict):
                    tweet_json['stream'] = False
                    tweet_json['query_list'] = ""
                    tweet_json['table_name'] = my_table
                    a_datetime = datetime.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S %z %Y')
                    a_datetime = a_datetime.timestamp()
                    tweet_json['tweet_received_at'] = int(a_datetime)
                    try:
                        output_channel.basic_publish(exchange='', routing_key=my_queue,
                                              body=json.dumps(tweet_json),
                                              properties=pika.BasicProperties(
                                                  delivery_mode=2,  # make message persistent
                                              ))
                        publish_count += 1
                    except Exception as e:
                        logger.error('failed to publish tweet; exception ' + str(e), exc_info=True)
                else:
                    # print('skipping -- ' + str(tweet_json))
                    logger.warning('skipping -- ' + str(tweet_json))
            logger.warning('published: ' + str(publish_count) + ' to ' + my_queue)
        ##########################################################################
        body = body.decode("utf-8")
        body = body.split(',')
        persona_id = int(body[0])
        my_table = body[1]
        logger.warning('processing user_id = ' + str(persona_id) + ' for table ' + my_table)
        # identify most recent tweet in database
        conn = psycopg2.connect(dbname=my_dbname, user=my_user, password=my_password, host='127.0.0.1')
        cur = conn.cursor()
        newest_tweet_id_already_in_db = new_mylib.most_recent_tweet_by_persona_in_db(persona_id, my_table=my_table, cur=cur)
        conn.close()
        # logger.warning('newest_tweet_id: ' + str(newest_tweet_id_already_in_db))
        ####################################################################################
        # this next line potentially takes a long time to execute, because in the course of
        # fetching the user's timeline we may run up against an API rate limit, in which case
        # the function mylib.fetch_user_timeline_from_twitter_api will pause for up to 15 minutes
        # as it waits for the API rate limit to replenish. if such a delay occurs, rabbitmq
        # channels may have timed out in the interim. it is important to re-initiate connections
        # to queues if they have timed out
        all_results = new_mylib.fetch_user_timeline_from_twitter_api(persona_id, api, logger, newest_tweet_id_already_in_db)
        # logger.warning(str(len(all_results)) + ' right before publishing')
        #####################################################################################

        publish_results(all_results, my_output_queue, my_table)
        try:
            ch.basic_ack(delivery_tag=method.delivery_tag) # contact input_queue to
            # acknowledge completion of task
        except:
            logger.warning('rabbitmq channel timed out before worker could send acknowledgment! reopening channel...')
            connection = pika.BlockingConnection(params)
            ch = connection.channel()
            ch.queue_declare(queue=my_input_queue, durable=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.warning('successfully sent acknowledgment!')
        # res = ch.queue_declare(queue=my_input_queue, durable=True, passive=True)
        # messages_in_queue = res.method.message_count
        # if messages_in_queue == 0:
        #     ch.stop_consuming()

    ############################################################
    # input_channel.basic_qos(prefetch_count=1)
    # input_channel.basic_consume(on_message_callback=callback, queue=my_input_queue)

    while True:
        try:
            logger.warning('(re-)initiating this thread with app ' + str(app_credentials['name']))
            # connect to rabbitmq input queue (list of personas to track):
            ############################################################
            # AUTHENTICATE WITH TWITTER API:
            auth = tweepy.OAuthHandler(app_credentials['app_key'], app_credentials['app_secret'])
            auth.set_access_token(app_credentials['oauth_token'], app_credentials['oauth_token_secret'])
            api = tweepy.API(auth)
            #############################################################
            # START RECEIVING PERSONAS FROM THE LOCAL MESSAGE-QUEUE:
            res = input_channel.queue_declare(queue=my_input_queue, durable=True)
            messages_in_queue = res.method.message_count
            logger.warning('Messages in input_queue: %d' % messages_in_queue)
            #############################################################
            # print('[*] Waiting for messages from Twitter listeners...')
            logger.warning('[*] Waiting for messages from Twitter listeners...')
            input_channel.basic_qos(prefetch_count=1)
            input_channel.basic_consume(on_message_callback=callback, queue=my_input_queue)
            input_channel.start_consuming()
            print('executed input_channel.start_consuming()!')
        except Exception as e:
            logger.error('input_channel.start_consuming() error on {}: {}'.format(app_credentials['name'], str(e)), exc_info=True)
            connection = pika.BlockingConnection(params)
            input_channel = connection.channel()

for i in range(0, len(app_list)):
    thread = threading.Thread(target=worker, args=(app_list[i],))
    thread.start()

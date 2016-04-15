#!/usr/bin/python
# -*- coding: utf-8 -*-

""" test script to fetch tweets based on hashtags or keywords -- using python-twitter library.
    The script requires that a target DATABASE and SCHEMA already exist.
    In this case information found using the 'pizza' keyword is stored into the
    'twitter' schema, 'pizza' table of the GISDATA database on ninsrv16
"""

import twitter
import psycopg2
import datetime
import logging
from creds import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET
from creds import ninserv16_host, ninserv16_dbname, ninserv16_user, ninserv16_password, ninserv16_port

# Trondheim coordinates
lat = 63.446827
lon = 10.421906
# fetch only tweets at a range distance from given location
rang = 2500

keyword = 'trondheim'
#keyword = "gjedde"
#keyword = 'gjeddefiske'
#keyword = 'lemen'
#keyword = 'elgjakt'
#keyword = 'stisykling'
#keyword = 'ulv'
logging.basicConfig(filename='/home/matteo.destefano/.scripts/twitter_logging.txt', level=logging.INFO)

logging.info('***********************************************************')
logging.info('starting new tweet recording on: ' + str(datetime.datetime.now()))
# Connect to postgres
try:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(ninserv16_host, ninserv16_dbname, ninserv16_user, ninserv16_password, ninserv16_port))
    print "perfectly connected!"
    logging.info('perfectly connected!')
except:
    logging.info("Connection Error")

cur = conn.cursor()
try:
    # create a new table pizza in twitter schema
    cur.execute('CREATE TABLE twitter.trondheim(tid bigint PRIMARY KEY, text text, lang text, x double precision, y double precision, geom geometry);')
    conn.commit()
    cur.close()

except:
    logging.info('skipped new table creation on: ' + str(datetime.datetime.now()))
    cur.close()
    conn.rollback()

# Let's get authorized access to twitter:
try:
    api = twitter.Api(access_token_secret=TOKEN_SECRET, access_token_key=ACCESS_TOKEN, consumer_secret=CONSUMER_SECRET, consumer_key=CONSUMER_KEY)
    logging.info('twitter api parameters passed')
except:
    logging.info('some issues with authorized access to twitter api')

# The correct syntax for the geocode parameter with GetSearch is geocode=(lat, lon, 'range')
try:
    logging.info('starting tweet search')
    tweets = api.GetSearch(term=keyword, lang="no", geocode=(lat, lon, "{}km".format(rang)), count=100)
    logging.info('there are this number of tweets: ' + str(tweets.count()))
except:
    logging.info('some issues with using GetSearch. Count = ' + str(tweets.count()))

for t in tweets:
    # store tweets only if associated to the geographical coordinates
    if t.geo:
        logging.info('in the tweet loop...' + str(t.text))
        #print t.text
        tweetlon = t.geo['coordinates'][1]
        tweetlat = t.geo['coordinates'][0]

        # makepoint = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)".format(tweetlon, tweetlat)

        # this is a subset f all information tags available, just as an example.
        # The amount of information stored could be extended:
        data = (t.id, t.text, t.lang, tweetlon, tweetlat, tweetlon, tweetlat, t.id)
        cur = conn.cursor()
        cur.execute("INSERT INTO twitter.pizza(tid, text, lang, x, y, geom) SELECT %s, %s, %s, %s, %s, "
                    "ST_SetSRID(ST_MakePoint(%s, %s), 4326) WHERE NOT EXISTS "
                    "(SELECT 1 FROM twitter.pizza WHERE tid = %s)", data)
        conn.commit()
        cur.close()

        try:
            logging.info('tweet fetched: '+str(t.text))
        except:
            logging.info('tweet not fetched!')

conn.close()



import os
import datetime
import re
import logging
import ldig
import simplejson as json
from optparse import OptionParser
from pymongo import Connection
from ssl import SSLError
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.api import API

logger = logging.getLogger('TwitterStreamLog')


def get_parser():
    parser = OptionParser()
    parser.add_option("-s", "--server", dest="server", default="localhost", help="mongodb host")
    parser.add_option("-d", "--database", dest="database", default="TwitterStream", help="mongodb database name")
    parser.add_option("-c", "--colelction", dest="collection", default="Tweets", help="mongodb collection name")
    parser.add_option("-u", "--username", dest="username", default="", help="username for mongodb")
    parser.add_option("-p", "--passwd", dest="passwd", default="", help="passwd for mongodb")  
    parser.add_option("-o", "--oauth", dest="oauthfile", default="oauth.json", help="file with oauth options")
    parser.add_option("-w", "--wordfile", dest="querywords", default="query.txt", help="file with query words")
    parser.add_option("-l", "--language", dest="language", default="en", help="language of tweets we want to collect")
    parser.usage = "bad parametres"
    return parser


def get_query(options):
    f = file(options.querywords, 'r')
    query_words = []
    for line in f.readlines():
        word = line.strip() 
        query_words.append(word)
    return query_words


def run(stream, query):
    now = datetime.datetime.now()
    print "Twitter Stream with started at: %s" % now
    print "Query words are: %s" % query
    for q in query:
        print q
    logger.info("Twitter Stream with started at: %s" % now)
    logger.info("Query words are: %s" % query)
    connected = False
    while True:
        try:
            if not connected:
                connected = True
                stream.filter(track=query)
        except SSLError, e:
            print e
            logger.error(e)
            connected = False


def set_logger():
    if not os.path.exists("Log"):
        os.makedirs("Log")
    #set logger for this project
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('Log/TwitterStreamLog.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)


def prettyPrintStatus(status):
    """This funcution pretty print the created time of the tweets, the description
    and the text to the terminal.
    """
    text = status["text"]
    description = status['user']['screen_name']
    tweet_id = str(status['_id'])

    if "retweeted_status" in status:
        description = ("%s RT by %s") % (status["retweeted_status"]["user"]["screen_name"], status['user']['screen_name'])
        text = status["retweeted_status"]["text"]

    try:
        return '[%s][%s][%-36s]: %s' % (tweet_id, status['created_at'], description, text)
    except UnicodeEncodeError:
        return "Can't decode UNICODE tweet"
    except:
        return "Error printing status"


class MongoDBCoordinator:
    """This class will set up connection with MongoDB and store tweet
    that matches the query word and in reuqired language"""
    def __init__(self, host, database, collection, username, passwd, language, query):
        self.host = host
        self.database = database
        self.collection = collection
        self.username = username
        self.passwd = passwd
        self.language = language
        self.query = query
        try:
            self.mongo = Connection(host,port=27017)
        except:
            print "Error starting MongoDB"
            logger.error("Error starting MongoDB")
            raise
        self.db = self.mongo[database]

        if self.username and self.passwd:
            self.db.authenticate(self.username, self.passwd)

    def addTweet(self, tweet):
        mycollection = self.collection
        patterns = "|".join(query)

        content = tweet['text']
        if "retweeted_status" in tweet:
            content = tweet["retweeted_status"]["text"]

        strre = re.compile(patterns, re.IGNORECASE)
        match = strre.search(content)
        if match and detector.detect(tweet, self.language):
            if not mycollection in self.db.collection_names():
                self.db.create_collection(mycollection)
            collection = self.db[mycollection]
            collection.save(tweet)
            try:
                print "%s" % (prettyPrintStatus(tweet))
            except Exception as (e):
                print "Error %s" % e.message
                logger.error("Error %s" % e.message)


class MongoDBListener(StreamListener):
    """ A listener handles tweets that received from the stream.
    Call addTweet method to store in MongoDB
    """
    def __init__(self, api=None):
        self.api = api or API()

    def on_data(self, data):
        """Called when raw data is received from connection.
        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """

        if 'in_reply_to_status_id' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False

    def on_status(self, status):
        jstatus = json.loads(status)
        mongo.addTweet(jstatus)

    def on_error(self, status):
        logger.error("StreamListener.on_error: %r" % status)
        return False

    def on_delete(self, status_id, user_id):
        logger.warn("StreamListener.on_delete: " + str(status_id) + str(user_id))
        return

    def on_limit(self, track):
        logger.warn("StreamListener.on_limit: " + str(track))
        return

    def on_timeout(self):
        logger.warn("StreamLister.on_timeout")


if __name__ == "__main__":

    set_logger()
    parser = get_parser()
    (options, args) = parser.parse_args()

    query = get_query(options)
    detector = ldig.ldig("./models")
    mongo = MongoDBCoordinator(options.server, options.database, options.collection, 
                                            options.username, options.passwd, options.language, query)
    listener = MongoDBListener()
    oauth = json.loads(open(options.oauthfile, 'r').read())
    auth = OAuthHandler(oauth['consumer_key'], oauth['consumer_secret'])
    auth.set_access_token(oauth['access_token'], oauth['access_token_secret'])
    stream = Stream(auth, listener)
    run(stream, query)



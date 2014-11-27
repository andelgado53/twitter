import tweepy
import pprint
import time
import requests
import json
import resources
from pymongo import MongoClient


access_token = resources.access_token
access_token_secret = resources.access_token_secret
consumer_key = resources.consumer_key
consumer_secret = resources.consumer_secret
datumbox_key = resources.datumbox_key

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
#spam = 'Amazon Music with Prime Music [PC] [Download]'
#api.update_status('hello world')

# public_tweets = api.home_timeline()
# for tweet in public_tweets:
# 	print tweet.text.encode('utf-8')

		
def get_ends_tweet_saved(collection, sort_order = -1):
	"""sort_order = -1 when trying to get the last tweet saved.
		sort_order = 1 when trying to get first tweet saved.
	"""
	if sort_order:
		tweet_id = collection.aggregate([{'$sort': {'id': sort_order}}, {'$limit': 1}])['result'][0]['id']

	else:
 		tweet_id = collection.aggregate([{'$sort': {'id': -1}}, {'$limit': 1}])['result'][0]['id']

 	
 	return tweet_id


def get_newer_tweets(total_needed, key_words, last ):

	tweets = []
	last_id = last

	while len(tweets) < total_needed:

		try:
			search_tweets = api.search(key_words, count = 100, since_id=str(last_id )) 
			if len(search_tweets) > 0:
				last_id = search_tweets[0].id
				for t in search_tweets:
					tweet = { }
					hashtags, user_name, created_at, twit, t_id, followers, following, country = get_twit_attr(t)
					tweet = { 
						  	'id': t_id,
					  		'user': { 'user_name': user_name, 'location': country, 'followers': followers, 'following': following },
					  		'hashtags': hashtags,
				  	  		'date_created': created_at,
				      		'twit_text': twit
							}

					tweets.append(tweet)
		except tweepy.error.TweepError:
			print('>>>Rate limit exceeded. Waiting for 16 minutes before fetching more twits. ' + str(len(tweets)) + ' fetched so far. '  + str(total_needed - len(tweets)) + ' to go')
			time.sleep(60*16)
			print('>>>Waiting time reached. Starting to fetch more twits')
			continue
		except: 
		  	print('>>>Unknown error. Try again later. Goodbye')
		  	break
				
	#pprint.pprint(tweets)
	return tweets




def get_twits(total_needed, key_words, last = 1):
	tweets = []
	last_id = last
	

	while len(tweets) < total_needed:

		try:
			search_tweets = api.search(key_words, count = 100, max_id=str(last_id - 1)) 
			if len(search_tweets) > 0:
				last_id = search_tweets[-1].id
				for t in search_tweets:
					tweet = { }
					hashtags, user_name, created_at, twit, t_id, followers, following, country, profile_pic = get_twit_attr(t)
			
					tweet = { 
						  	'id': t_id,
					  		'user': { 'user_name': user_name, 'location': country, 'followers': followers, 'following': following },
					  		'hashtags': hashtags,
				  	  		'date_created': created_at,
				      		'twit_text': twit,
				      		'profile_pic': profile_pic
							}

					tweets.append(tweet)				
				
		except tweepy.error.TweepError:
			
			print('>>>Rate limit exceeded. Waiting for 16 minutes before fetching more twits. ' + str(len(tweets)) + ' fetched so far. '  + str(total_needed - len(tweets)) + ' to go')
			time.sleep(60*16)
			print('>>>Waiting time reached. Starting to fetch more twits')
			continue
		except: 
		 	print('>>>Unknown error. Try again later. Goodbye')
		 	break
				
	#pprint.pprint(tweets)
	return tweets

def get_most_active(tweets, MAX):
	user_count = {}
	for tweet in tweets:
		user_count[tweet['user']['user_name']] = user_count.get(tweet['user']['user_name'], 0) + 1
		#most_active = max([(int(user_count[user]), user ) for user in user_count ]) # most active 
	active_custs = [(int(user_count[user]), user ) for user in user_count ] # top 10 most active 
	active_custs.sort(reverse= True)
	top_most_active = active_custs[0:MAX]
	return active_custs[0:MAX]
	#pprint.pprint(top_most_active)

def get_twit_text(tweets, filter_user =None):
	
	if not filter_user:
		for tweet in tweets:
			print(tweet['user']['user_name'], tweet['twit_text'])
	else:
		for tweet in tweets:
			if tweet['user']['user_name'] == filter_user:
				print(tweet['user']['user_name'], tweet['twit_text'])

def get_hashtags(twit):
	list_of_hashtags = []
	for e in twit.entities['hashtags']:
		list_of_hashtags.append(e['text'])
	return list_of_hashtags


def get_twit_attr(t):

	list_hashtags = get_hashtags(t)
	user_name = t.author.screen_name.encode('utf-8')
	created_at = str(t.created_at)
	twit =  t.text.encode('utf-8')
	t_id = t.id
	followers = int(t.user.followers_count)
	following = int(t.user.friends_count) 
	profile_pic = t.author.profile_image_url_https
	if t.place:
		country = t.place.country
	else:
		country = None
	return list_hashtags, user_name, created_at, twit, t_id, followers, following, country, profile_pic

def get_current_id(collection):
	current_ids = set()
	for doc in collection.find():
		current_ids.add(doc['id'])
	return current_ids


def insert_tweets_into_MongoDB(list_of_tweets, collection):	
	
	current_ids = get_current_id(collection)
	to_insert = [ e for e in list_of_tweets if e['id'] not in current_ids]
	try:
		collection.insert(to_insert) # inserts a list of dictionaries
		print(str(len(list_of_tweets)) + ' tweets were pulled. ' + str(len(to_insert)) + ' tweets not already saved were inserted')
		print(tc.count(), 'tweets in tweets collection')
	except :
		print(str(len(list_of_tweets)) +' tweets were fecthed - all already in the collection. ' + str(len(to_insert)) + ' were inserted')

def get_hashtags_count(collection):
	""" produces a descending list of hastags used in the tweets ordered desendingly according to times used.
		Takes as input the collection where the tweets are stored.
	"""

	pprint.pprint(collection.aggregate([
										{'$unwind': '$hashtags'},
										{'$group': {'_id': '$hashtags', 'count': {'$sum': 1}}},
										{'$sort': {'count': -1}}
 										]
 									   ))

def get_sentiment(text):
    
    url= 'http://api.datumbox.com/1.0/TwitterSentimentAnalysis.json'
    para = {'api_key': datumbox_key, 'text': text }
    r = requests.post(url, para)
    json_r = json.loads(r.text)
    return json_r['output']['result']



client = MongoClient() # connects to server
db = client.test # connects to db
tc = db.twits_collection # access to table in db, or collection in MongoBD
#get_hashtags_count(tc)
# pprint.pprint(tc.aggregate([
# 	{'$group': {'_id': '$user.user_name', 'count': {'$sum': 1}, 'tweets': {'$push': '$twit_text'}}},
# 	{'$project': {'_id': 0,'user_name': '$_id', 'num_of_tweets': '$count', 'tweets': '$tweets'}},
# 	{'$limit': 25}
# ]))


#last_tweet_saved = get_ends_tweet_saved(tc, -1)
#first_tweet_saved = get_ends_tweet_saved(tc, 1)
tweets_list = get_twits(200 , 'amazon prime music')

insert_tweets_into_MongoDB(tweets_list, tc)

#t = tc.find()
# for tweet in t:
# 	text = tweet['twit_text']
	#print(text.encode('utf-8'))
	
# 	try:
# 		print(get_sentiment(text.encode('utf-8')))
# 	except:
# 		continue

		



#T = get_twits(100, 'amazon prime music', get_ends_tweet_saved(tc, 1)) # gets older tweets
#nt = get_newer_tweets(3, 'amazon prime music', get_ends_tweet_saved(tc, -1) ) # get newer tweets 
#for e in nt:
	#pprint.pprint(e)





#print(tc.find({'user.user_name': 'ashleigh915'}).count())
#for e in tc.find({'user.user_name': 'consuelocostin'}):
    # pprint.pprint(e)

#pprint.pprint(tc.aggregate( [{'$match': {'user.followers':{'$gt': 10000} }},{'$group': {'_id': '$user.user_name', 'total_twits' : {'$sum' : 1}}}, {'$sort': {'total_twits': -1}} ]))
#pprint.pprint(tc.aggregate([{'$match': {'user.followers':{'$gt': 10000} }}, {'$project': {'name': '$user.user_name', 'foll': '$user.followers' }}, {'$limit': 1}]))
#pprint.pprint(tc.aggregate([{'$sort': {'id': 1}}, {'$limit': 1}])['result'][0]['id'])

#pprint.pprint(tc.aggregate([{'$sort': {'id': 1}}, {'$limit': 1}]))
#pprint.pprint(tc.aggregate([{'$sort': {'id': -1}}, {'$limit': 1}]))


#tc.drop() # drops the collection 

#pprint.pprint(tc.aggregate([{'$group': {'_id': '$id', 'total': {'$sum': 1}}}, {'$match': {'total': {'$gt': 1}}}, {'$sort': {'total': -1}}])) #looks for duplicates

#print(get_ends_tweet_saved(tc, -1))


#print(get_ends_tweet_saved(tc, -1))
#print(get_ends_tweet_saved(tc))









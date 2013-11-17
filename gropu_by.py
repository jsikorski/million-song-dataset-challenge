from pymongo import *
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017

def one_by_one():
    songs = db.most_popular.find(),
    print '100 most popular songs selected'
    i = 0
    
    while(i < 100):
        key = [ 'user_index' ]
        condition = { 'song_index': songs[0][i]['_id'] }
        initial = { 'count': 0, 'sum': 0 }
        reduce = 'function(doc, out) {  }'
        songsGroup = db.triplets.group(key, condition, initial, reduce)

        print 'Groupby song: ',songs[0][i]
        print 'Groupby song _id: ',songs[0][i]['_id']
        print 'Group: ',songsGroup
        print 'Group length: ',len(songsGroup)
        print i+1,' percents'

        i += 1

with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local
    invoke_measurable_task(
        lambda: one_by_one(),
        'Group by most popular song one by one')
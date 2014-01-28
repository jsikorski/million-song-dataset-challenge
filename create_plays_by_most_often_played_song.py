from pymongo import MongoClient
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017


def create_plays_by_most_often_played_song(db, collections_suffix):
    plays_by_user_filtered_simple = db['plays_by_user_filtered_simple' + collections_suffix].find()
    print "plays_by_user_filtered_simple ready"

    buckets = {}

    for plays_by_user in plays_by_user_filtered_simple:
        user_id = plays_by_user['_id']
        plays = plays_by_user['value']

        most_often_played_song = plays[0]

        if most_often_played_song in buckets:
            bucket = buckets[most_often_played_song]
        else:
            bucket = []
            buckets[most_often_played_song] = bucket

        bucket.append({'_id': user_id, 'value': plays})

    target_collection = db['plays_by_most_often_played_song' + collections_suffix]
    batch = [{'_id': bucket_id, 'value': value} for bucket_id, value in buckets.iteritems()]
    insert_batch(batch, target_collection)


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    invoke_measurable_task(
        lambda: create_plays_by_most_often_played_song(db, '_t'),
        'Create plays_by_most_often_played_song collection for train set')
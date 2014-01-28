from pymongo import MongoClient
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
BATCH_SIZE = 1500


def create_plays_by_most_often_played_song(db, source_collection_name, target_collection_name):
    buckets = {}

    i = 1
    for plays_by_user in db[source_collection_name].find():
        if i == 1 or i % 100 == 0:
            print "Looking for bucket for %d. user" % i

        user_id = plays_by_user['_id']
        plays = plays_by_user['value']

        most_often_played_song = plays[0]

        if most_often_played_song in buckets:
            bucket = buckets[most_often_played_song]
        else:
            bucket = []
            buckets[most_often_played_song] = bucket

        bucket.append({'_id': user_id, 'value': plays})
        i += 1

    print "Number of buckets: %d" % len(buckets)

    target_collection = db[target_collection_name]
    batch = [{'_id': bucket_id, 'value': value} for bucket_id, value in buckets.iteritems()]
    print "Buckets batch ready"

    start = 0
    end = BATCH_SIZE
    while start < len(batch):
        insert_batch(batch[start:end], target_collection)
        start += BATCH_SIZE
        end += BATCH_SIZE


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    invoke_measurable_task(
        lambda: create_plays_by_most_often_played_song(db, 'plays_by_user_simple_t', 'plays_by_most_often_played_song_t'),
        'Create plays_by_most_often_played_song collection for train set')
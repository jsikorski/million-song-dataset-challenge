from pymongo import MongoClient
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
BATCH_SIZE = 1


def create_user_plays_by_most_often_played_song(db, collections_suffix):
    plays_by_user_simple_filtered = db['plays_by_user_simple_filtered' + collections_suffix].find()
    print "plays_by_user_simple_filtered ready"
    plays_by_user_simple_filtered_map = {int(x['_id']): x['value'] for x in plays_by_user_simple_filtered}
    print "plays_by_user_simple_filtered_map ready"
    users_by_most_often_played_song = db['users_by_most_often_played_song' + collections_suffix].find()
    print "users_by_most_often_played_song ready"

    target_collection = db['plays_by_most_often_played_song' + collections_suffix]
    batch = []

    for users_by_song in users_by_most_often_played_song:
        entry = {'_id': users_by_song['_id'], 'value': []}

        for user_id in users_by_song['value']:
            user_index = int(user_id)
            user_plays = plays_by_user_simple_filtered_map[user_index]
            entry['value'].append({'_id': user_index, 'value': user_plays})

        batch.append(entry)

        if len(batch) > 0 and len(batch) % BATCH_SIZE == 0:
            insert_batch(batch, target_collection)
            batch = []

    if len(batch) > 0:
        insert_batch(batch, target_collection)


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    invoke_measurable_task(
        lambda: create_user_plays_by_most_often_played_song(db, '_t'),
        'Create user_plays_by_most_often_played_song collection for train set')

    invoke_measurable_task(
        lambda: create_user_plays_by_most_often_played_song(db, '_v'),
        'Create user_plays_by_most_often_played_song collection for validation set')
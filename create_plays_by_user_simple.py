from pymongo import MongoClient
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
BATCH_SIZE = 1500


def create_plays_by_user_simple(plays_by_user, target_collection):
    plays_batch = []

    for plays_by_user in plays_by_user.find():
        entry = {
            '_id': plays_by_user['_id'],
            'value': [song_id for song_id in plays_by_user['value']]
        }

        # sort by plays count
        entry['value'].sort(key=lambda x: plays_by_user['value'][x], reverse=True)

        plays_batch.append(entry)

        if len(plays_batch) > 0 and len(plays_batch) % BATCH_SIZE == 0:
            insert_batch(plays_batch, target_collection)
            plays_batch = []

    if len(plays_batch) > 0:
        insert_batch(plays_batch, target_collection)


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    invoke_measurable_task(
        lambda: create_plays_by_user_simple(db.plays_by_user_t, db.plays_by_user_simple_t),
        "Create plays_by_user_simple_filtered collection for train set")

    invoke_measurable_task(
        lambda: create_plays_by_user_simple(db.plays_by_user_v, db.plays_by_user_simple_v),
        "Create plays_by_user_simple_filtered collection for validation set")
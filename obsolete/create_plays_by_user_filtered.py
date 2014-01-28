from pymongo import MongoClient, DESCENDING
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
NUMBER_OF_MOST_POPULAR_SONGS = 500
BATCH_SIZE = 1500


def create_plays_by_user_filtered(plays_by_user, most_popular_songs, target_collection, disable_filtering=False):
    plays_batch = []

    most_popular_song_ids = map(lambda s: int(s['_id']), most_popular_songs)

    for user_play_map in plays_by_user.find():
        entry = {'_id': user_play_map['_id'], 'value': {}}

        for song_id, play_count in user_play_map['value'].iteritems():
            song_index = int(song_id)
            if song_index in most_popular_song_ids:
                entry['value'][song_id] = play_count

        if disable_filtering or len(entry['value'].keys()) > 1:
            plays_batch.append(entry)

        if len(plays_batch) > 0 and len(plays_batch) % BATCH_SIZE == 0:
            insert_batch(plays_batch, target_collection)
            plays_batch = []

    if len(plays_batch) > 0:
        insert_batch(plays_batch, target_collection)


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    most_popular_songs = [1]

    def load_most_popular_songs():
        most_popular_songs[0] = list(
            db.play_count_by_song_t.find()
            .sort('value', DESCENDING)
            .limit(NUMBER_OF_MOST_POPULAR_SONGS)
        )

    invoke_measurable_task(load_most_popular_songs, 'Load %d most popular songs' % NUMBER_OF_MOST_POPULAR_SONGS)
    most_popular_songs = most_popular_songs[0]

    invoke_measurable_task(
        lambda: create_plays_by_user_filtered(db.plays_by_user_t, most_popular_songs, db.plays_by_user_filtered_t),
        "Create plays_by_user_filtered collection for train set")

    invoke_measurable_task(
        lambda: create_plays_by_user_filtered(db.plays_by_user_v, most_popular_songs, db.plays_by_user_filtered_v, True),
        "Create plays_by_user_filtered collection for validation set")
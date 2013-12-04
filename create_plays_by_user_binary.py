from pymongo import MongoClient, DESCENDING
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
NUMBER_OF_MOST_POPULAR_SONGS = 100
BATCH_SIZE = 2500


def create_plays_by_user_binary(plays_by_user, most_popular_songs, target_collection):
    plays_batch = []

    for user_play_map in plays_by_user.find():
        entry = {'_id': user_play_map['_id']}
        for song in most_popular_songs:
            song_index = str(int(song['_id']))
            entry[song_index] = song_index in user_play_map['value']

        plays_batch.append(entry)

        if len(plays_batch) % BATCH_SIZE == 0:
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
        lambda: create_plays_by_user_binary(db.plays_by_user_t, most_popular_songs, db.plays_by_user_binary_t),
        "Create plays_by_user_binary collection for train set")

    invoke_measurable_task(
        lambda: create_plays_by_user_binary(db.plays_by_user_v, most_popular_songs, db.plays_by_user_binary_v),
        "Create plays_by_user_binary collection for validation set")
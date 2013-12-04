from pymongo import MongoClient, DESCENDING
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
NUMBER_OF_MOST_POPULAR_SONGS = 63
BATCH_SIZE = 2500

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

    def create_indexes_for_collection(db_collection):
        for song in most_popular_songs:
            db_collection.ensure_index([(str(int(song['_id'])), 1)])

    invoke_measurable_task(
        lambda: create_indexes_for_collection(db.plays_by_user_binary_t),
        'Create indexes on plays_by_user_binary for training set')

    invoke_measurable_task(
        lambda: create_indexes_for_collection(db.plays_by_user_binary_v),
        'Create indexes on plays_by_user_binary for validation set')
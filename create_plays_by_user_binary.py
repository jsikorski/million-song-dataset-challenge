from pymongo import MongoClient, DESCENDING
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
NUMBER_OF_MOST_POPULAR_SONGS = 100
BATCH_SIZE = 2500


def create_plays_by_user_binary_collection(db, most_popular_songs):
    plays_batch = []

    for play_map in db.plays_by_user.find():
        plays_for_user = {'_id': play_map['_id'], 'value': {}}
        for song in most_popular_songs:
            song_index = str(int(song['_id']))
            plays_for_user['value'][song_index] = song_index in play_map['value']

        plays_batch.append(plays_for_user)

        if len(plays_batch) % BATCH_SIZE == 0:
            insert_batch(plays_batch, db.plays_by_user_binary)
            plays_batch = []

    if len(plays_batch) > 0:
        insert_batch(plays_batch, db.plays_by_user_binary)


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    most_popular_songs = [1]

    def load_most_popular_songs():
        most_popular_songs[0] = list(
            db.play_count_by_song.find()
            .sort('value', DESCENDING)
            .limit(NUMBER_OF_MOST_POPULAR_SONGS)
        )

    invoke_measurable_task(load_most_popular_songs, 'Load %d most popular songs' % NUMBER_OF_MOST_POPULAR_SONGS)
    most_popular_songs = most_popular_songs[0]

    invoke_measurable_task(lambda: create_plays_by_user_binary_collection(db, most_popular_songs),
                           "Create plays_by_user_binary collection")
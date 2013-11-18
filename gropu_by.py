from pymongo import *
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
NUMBER_OF_MOST_POPULAR_SONGS = 100
NUMBER_OF_SONGS_AT_ONE = 20


def one_by_one(most_popular_songs, plays_by_user_binary_t):
    for song in most_popular_songs:
        song_index = str(int(song['_id']))
        fixed_song_index = '$value.' + song_index

        songs_group = plays_by_user_binary_t.aggregate([
            {'$group':{'_id': fixed_song_index, 'res': {'$addToSet': fixed_song_index}}}
        ])


def pairs(most_popular_songs, plays_by_user_binary_t):
    for song in most_popular_songs:
        for second_song in most_popular_songs:
            song_index = str(int(song['_id']))
            fixed_song_index = '$value.' + song_index

            second_song_index = str(int(second_song['_id']))
            fixed_second_song_index = '$value.' + second_song_index

            songs_group = plays_by_user_binary_t.aggregate([
                {'$group':{'_id': fixed_song_index, 'res': {'$addToSet': fixed_song_index}}},
                {'$group':{'_id': fixed_second_song_index, 'res': {'$addToSet': fixed_second_song_index}}}
            ])


def all_at_once(most_popular_songs, plays_by_user_binary_t):
    pipeline = [20]

    for num in range(0,20):
        song_index = str(int(most_popular_songs[num]['_id']))
        fixed_song_index = '$value.' + song_index
        pipeline[0] = ({'$group':{'_id': fixed_song_index, 'res': {'$addToSet': fixed_song_index}}})

    songs_group = plays_by_user_binary_t.aggregate(pipeline)


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
    print '100 most popular songs selected\n'

    invoke_measurable_task(
        lambda: one_by_one(most_popular_songs, db.plays_by_user_binary_t),
        'Group by most popular song one by one')

    invoke_measurable_task(
        lambda: pairs(most_popular_songs, db.plays_by_user_binary_t),
        'Group by pairs of most popular songs')

    invoke_measurable_task(load_most_popular_songs, 'Load %d most popular songs' % NUMBER_OF_SONGS_AT_ONE)
    most_popular_songs = most_popular_songs[0]
    print '20 most popular songs selected\n'

    invoke_measurable_task(
        lambda: all_at_once(most_popular_songs, db.plays_by_user_binary_t),
        'Group by 20 most popular songs ot once')

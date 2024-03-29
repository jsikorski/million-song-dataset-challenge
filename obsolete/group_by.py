from pymongo import *
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
NUMBER_OF_MOST_POPULAR_SONGS = 100
NUMBER_OF_SONGS_AT_ONE = 20


def one_by_one(most_popular, plays_by_user_binary_t):
    for song in most_popular:
        song_index = str(int(song['_id']))
        fixed_song_index = '$value.' + song_index

        songs_group = plays_by_user_binary_t.aggregate([
            {'$group': {'_id': fixed_song_index}}
        ])


def pairs(most_popular, plays_by_user_binary_t):
    for song in most_popular:
        for second_song in most_popular:
            song_index = int(song['_id'])
            second_song_index = int(second_song['_id'])

            if song_index < second_song_index:
                fixed_song_index = '$value.' + str(song_index)
                fixed_second_song_index = '$value.' + str(second_song_index)

                songs_group = plays_by_user_binary_t.aggregate([
                    {'$group': {'_id': fixed_song_index}},
                    {'$group': {'_id': fixed_second_song_index}}
                ])


def all_at_once(most_popular, plays_by_user_binary_t):
    pipeline = [NUMBER_OF_SONGS_AT_ONE]

    for num in range(0, NUMBER_OF_SONGS_AT_ONE):
        song_index = str(int(most_popular[num]['_id']))
        fixed_song_index = '$value.' + song_index
        pipeline[0] = ({'$group': {'_id': fixed_song_index}})

    print 'Query created'
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
    print NUMBER_OF_MOST_POPULAR_SONGS, ' most popular songs selected\n'

    invoke_measurable_task(
        lambda: one_by_one(most_popular_songs, db.plays_by_user_binary_t),
        'Group by most popular song one by one')

    invoke_measurable_task(
        lambda: pairs(most_popular_songs, db.plays_by_user_binary_t),
        'Group by pairs of most popular songs')

    invoke_measurable_task(load_most_popular_songs, 'Load %d most popular songs' % NUMBER_OF_SONGS_AT_ONE)
    most_popular_songs = most_popular_songs[0]
    print NUMBER_OF_SONGS_AT_ONE, 'most popular songs selected\n'

    invoke_measurable_task(
        lambda: all_at_once(most_popular_songs, db.plays_by_user_binary_t),
        'Group by 20 most popular songs ot once')
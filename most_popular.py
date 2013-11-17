from pymongo import *
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017


def hundred_songs():
    songs = db.train_play_count_by_song.find().sort('value', DESCENDING).limit(100)
    db.most_popular.insert(songs, w=0,
                           j=False, fsync=False,
                           continue_on_error=True)


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local
    invoke_measurable_task(
        lambda: hundred_songs(),
        "Find 100 most often played songs")
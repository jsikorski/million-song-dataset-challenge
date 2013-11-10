from pymongo import *
from measurements import invoke_measurable_task

MONGODB_PORT = 27017


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local
    invoke_measurable_task(
        lambda: list(db.play_count_by_song.find().sort('value', DESCENDING).limit(100)),
        "Find 100 most often played songs")
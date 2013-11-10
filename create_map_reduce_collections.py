from pymongo import MongoClient
from measurements import invoke_measurable_task
import os

MONGODB_PORT = 27017

for directory in os.listdir('map_reduce'):
    with open('map_reduce/%s/map.js' % directory) as map_file:
        map = map_file.read()

    with open('map_reduce/%s/reduce.js' % directory) as reduce_file:
        reduce = reduce_file.read()

    with MongoClient('localhost', MONGODB_PORT) as client:
        db = client.local
        invoke_measurable_task(
            lambda: db.train_triplets.map_reduce(map, reduce, directory),
            "Create map reduce collection %s" % directory)
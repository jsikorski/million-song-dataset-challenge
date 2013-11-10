from genericpath import exists, isdir
from operator import contains
from pymongo import MongoClient
from measurements import invoke_measurable_task
import os

MONGODB_PORT = 27017
CREATED_COLLECTIONS_FILE_PATH = 'map_reduce/created_collections.txt'

if not exists(CREATED_COLLECTIONS_FILE_PATH):
    open(CREATED_COLLECTIONS_FILE_PATH, 'a').close()

created_collections = []
with open(CREATED_COLLECTIONS_FILE_PATH) as file:
    created_collections = map(lambda x: x.strip(), file.readlines())

should_be_created = lambda x: isdir('map_reduce/%s' % x) and not contains(created_collections, x)
for directory in filter(should_be_created, os.listdir('map_reduce')):
    with open('map_reduce/%s/map.js' % directory) as map_file:
        map = map_file.read()

    with open('map_reduce/%s/reduce.js' % directory) as reduce_file:
        reduce = reduce_file.read()

    with MongoClient('localhost', MONGODB_PORT) as client:
        db = client.local
        invoke_measurable_task(
            lambda: db.train_triplets.map_reduce(map, reduce, directory),
            "Create map reduce collection %s" % directory)

    with open(CREATED_COLLECTIONS_FILE_PATH, 'a') as file:
        file.write('%s\n' % directory)
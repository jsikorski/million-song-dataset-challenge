from genericpath import exists, isdir
from operator import contains
from pymongo import MongoClient
from utils.measurements import invoke_measurable_task
import os


MONGODB_PORT = 27017
CREATED_COLLECTIONS_FILE_PATH = 'created_collections.txt'

if not exists(CREATED_COLLECTIONS_FILE_PATH):
    open(CREATED_COLLECTIONS_FILE_PATH, 'a').close()

created_collections = []
with open(CREATED_COLLECTIONS_FILE_PATH) as file:
    created_collections = map(lambda x: x.strip(), file.readlines())

should_be_created = lambda x: isdir('%s' % x) and not contains(created_collections, x)
for directory in filter(should_be_created, os.listdir('.')):
    with open('%s/map.js' % directory) as map_file:
        map = map_file.read()

    with open('%s/reduce.js' % directory) as reduce_file:
        reduce = reduce_file.read()

    finalize_path = '%s/finalize.js' % directory
    if exists(finalize_path):
        with open(finalize_path) as finalize_file:
            finalize = finalize_file.read()
    else:
        finalize = None

    collection_path = '%s/source_collection.txt' % directory
    if exists(collection_path):
        with open(collection_path) as collection_file:
            source_collection_name = collection_file.read()
    else:
        source_collection_name = 'triplets'

    with MongoClient('localhost', MONGODB_PORT) as client:
        db = client.local

        invoke_measurable_task(
            lambda: db[source_collection_name + '_t'].map_reduce(map, reduce, directory + '_t', finalize=finalize),
            "Create map reduce collection %s for train set" % directory)

        invoke_measurable_task(
            lambda: db[source_collection_name + '_v'].map_reduce(map, reduce, directory + '_v', finalize=finalize),
            "Create map reduce collection %s for validation set" % directory)

    with open(CREATED_COLLECTIONS_FILE_PATH, 'a') as file:
        file.write('%s\n' % directory)
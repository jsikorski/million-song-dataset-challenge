from utils.measurements import invoke_measurable_task
import os


def import_triplets_to_mongo_db():
    os.system('python import_triplets_to_mongo_db.py')


def create_map_reduce_collections():
    os.system('python ./map_reduce/create_map_reduce_collections.py')


def plays_by_user_simple():
    os.system('python create_plays_by_user_simple.py')


def plays_by_user_binary():
    os.system('python create_plays_by_user_binary.py')


def plays_by_user_filtered():
    os.system('python create_plays_by_user_filtered.py')

print 'INITIALIZING DATABASE'
invoke_measurable_task(
    lambda: import_triplets_to_mongo_db(), 'Import triplets to mongo db')
invoke_measurable_task(
    lambda: create_map_reduce_collections(), 'Create Map Reduce collections')
invoke_measurable_task(
    lambda: plays_by_user_simple(), 'Create collection plays_by_user_simple')
invoke_measurable_task(
    lambda: plays_by_user_binary(), 'Create collection plays_by_user_binary')
invoke_measurable_task(
    lambda: plays_by_user_filtered(), 'Create collection plays_by_user_filtered')
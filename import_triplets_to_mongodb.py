from pymongo import MongoClient
from utils.database_inserter import insert_batch
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
BATCH_SIZE = 1500
KAGGLE_USERS_MAPPING_FILE_PATH = 'resources/private/inclass_kaggle_users.txt'
KAGGLE_SONGS_MAPPING_FILE_PATH = 'resources/private/inclass_kaggle_songs.txt'
TRAIN_TRIPLETS_FILE_PATH = 'resources/private/train_triplets.txt'
TEST_TRIPLETS_FILE_PATH = 'resources/private/inclass_kaggle_visible_evaluation_triplets.txt'

users_map = {}
songs_map = {}

# Python closure hack
last_index = [0]


def load_kaggle_users_mapping():
    with open(KAGGLE_USERS_MAPPING_FILE_PATH) as kaggle_users:
        for index, user_id in enumerate(kaggle_users):
            users_map[user_id.strip()] = index + 1
            last_index[0] += 1


def load_kaggle_songs_mapping():
    with open(KAGGLE_SONGS_MAPPING_FILE_PATH) as kaggle_songs:
        for line in kaggle_songs:
            parts = line.strip().split()
            song_id = parts[0]
            song_index = parts[1]
            songs_map[song_id] = int(song_index)


def import_triplets_from_file(filename, db_collection):
    with open(filename) as file:
        triplets_batch = []
        for line in file:
            triplet = get_triplet_from_fileline(line)
            triplets_batch.append(triplet)

            if len(triplets_batch) % BATCH_SIZE == 0:
                insert_batch(triplets_batch, db_collection)
                triplets_batch = []

        if len(triplets_batch) > 0:
            insert_batch(triplets_batch, db_collection)


def get_triplet_from_fileline(line):
    parts = line.strip().split()

    user_id = parts[0]

    if user_id in users_map:
        user_index = users_map[parts[0]]
    else:
        last_index[0] += 1
        user_index = last_index[0]
        users_map[user_id] = user_index

    return {
        'user_index': user_index,
        'song_index': songs_map[parts[1]],
        'play_count': int(parts[2])
    }


invoke_measurable_task(load_kaggle_users_mapping, 'Load Kaggle users mapping')
invoke_measurable_task(load_kaggle_songs_mapping, 'Load Kaggle songs mapping')

with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local
    invoke_measurable_task(
        lambda: import_triplets_from_file(TRAIN_TRIPLETS_FILE_PATH, db.triplets_t),
        'Import train triplets')
    invoke_measurable_task(
        lambda: import_triplets_from_file(TEST_TRIPLETS_FILE_PATH, db.triplets_v),
        'Import validation triplets')
from utils.naivebayes.bayes import naive_bayes
from pymongo import *
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
COUNT = 1000
WORK_FILE = './resources/private/tmp_file.txt'

def learn_bayes(collection):
    arguments = ["", "learn" , "true", collection, COUNT]
    naive_bayes(arguments)


def save_to_file(collection):
    f = open(WORK_FILE, 'a')
    f.truncate()
    for song in collection:
        for song_id in song[u'value']:
            val = (song[u'value'][song_id])
            f.seek(0)
            f.write(str(val))
            f.write(' ')
        f.write('\n')

    f.close()


with MongoClient('localhost', MONGODB_PORT) as client:
    collection = [1]

    def load_collection():
        collection[0] = list(
            db.plays_by_user_binary_t.find().limit(COUNT)
        )

    db = client.local

    invoke_measurable_task(
        lambda: load_collection(), 'Get collection')

    invoke_measurable_task(
        lambda: save_to_file(collection[0]), 'Save collection to file')

    invoke_measurable_task(
        lambda: learn_bayes(WORK_FILE),
        'Teach Naive Bayes')
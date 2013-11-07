import numpy
import hdf5_getters
import os
import glob
import time
from pymongo import MongoClient

MONGODB_PORT = 27017
SONGS_PATH = "C:\Million Songs Data\MillionSongSubset\data"
SONGS_BATCH_SIZE = 1000

def get_song_from_file(song_file):
    getters_names = filter(lambda x: x[:3] == 'get', hdf5_getters.__dict__.keys())

    song = {}

    for getterName in getters_names:
        attribute_name = getterName[4:]
        getter = getattr(hdf5_getters, getterName)
        attribute_value = getter(song_file)

        attribute_value_type = type(attribute_value)

        if attribute_value_type is numpy.int64:
            attribute_value = long(attribute_value)

        if attribute_value_type is numpy.ndarray:
            attribute_value = attribute_value.tolist()

        song[attribute_name] = attribute_value

    return song


def export_all_songs(basedir, db):
    start = time.time()
    songs_batch = []

    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root, '*.h5'))
        for f in files:
            song_file = hdf5_getters.open_h5_file_read(f)
            song = get_song_from_file(song_file)
            songs_batch.append(song)

            if len(songs_batch) % SONGS_BATCH_SIZE == 0:
                db.songs.insert(songs_batch, w=0, j=False, fsync=False, continue_on_error=True)
                stop = time.time()
                print stop - start
                start = stop
                songs_batch = []

            song_file.close()


client = MongoClient('localhost', MONGODB_PORT)
db = client.local
export_all_songs(SONGS_PATH, db)
client.close()
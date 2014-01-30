from pymongo import MongoClient, DESCENDING
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
KNN_RESULTS_PATH = 'users_knn_250_results.txt'
KNN_CONVERTED_PATH = 'users_knn_250_results.csv'
KAGGLE_USERS_MAPPING_FILE_PATH = 'resources/private/inclass_kaggle_users.txt'
NUMBER_OF_MOST_POPULAR_SONGS = 500

knn_map = []
last_index = [0]


def get_data_from_fileline(line):
    parts = line.strip().split()

    user_index = parts[0]
    song_id = []

    number_of_songs = 0
    k = 0
    for part in parts:
        if number_of_songs >= 500:
            break

        if k > 0:
            song_id.append(int(part))
            number_of_songs += 1
        k += 1

    ret = []
    ret.append(int(user_index)),
    ret.append(song_id),
    ret.append(int(number_of_songs))

    return ret


def load_knn_results():
    with open(KNN_RESULTS_PATH) as knn_results:
        i = 1
        for line in knn_results:
            print i
            knn_map.append(get_data_from_fileline(line))
            i += 1

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

    load_knn_results()

    with open(KAGGLE_USERS_MAPPING_FILE_PATH) as kaggle_users:
        with open(KNN_CONVERTED_PATH, 'w') as my_file:
            my_file.write('Id,Expected\n')
            j = 1
            for index, user_id in enumerate(kaggle_users):
                my_file.write(user_id.strip() + ',')
                result = knn_map[index]
                index += 1

                predicted_songs = 0
                for song in result[1]:
                    if predicted_songs < 500:
                        my_file.write(str(song) + ' ')
                        predicted_songs += 1
                to_add = 500 - predicted_songs

                l = 0
                while to_add > 0:
                    my_file.write(str(int(most_popular_songs[l]['_id'])) + ' ')
                    l += 1
                    to_add -= 1
                my_file.write("\n")
                print j
                j += 1

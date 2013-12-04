from pymongo import MongoClient
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
KNN_K = 50
NUMBER_OF_USERS = 1000


class JaccardBasedKnn(object):
    def __init__(self, plays_for_all_users, k):
        self.plays_for_all_users = plays_for_all_users
        self.k = k

    def compute_jaccard_index(self, set_1, set_2):
        if not set_1 and not set_2:
            return None

        n = len(set_1.intersection(set_2))
        return n / float(len(set_1) + len(set_2) - n)

    def find_for_user(self, user_plays):
        scores = []

        for other_user_plays in self.plays_for_all_users:
            if other_user_plays['value'][0] != iter(user_plays).next():
                continue

            jaccard_index = self.compute_jaccard_index(user_plays, set(other_user_plays['value']))
            scores.append({'user_id': other_user_plays['_id'], 'score': jaccard_index})

        scores.sort(key=lambda x: x['score'], reverse=True)
        scores = scores[0:self.k]
        return scores

    def find_for_users(self, users_plays):
        scores = []

        current_user_number = 1
        for user_play in users_plays:
            print 'Looking for knn for %s. user' % current_user_number
            scores.append({'user_id': user_play['_id'], 'scores': self.find_for_user(set(user_play['value']))})
            current_user_number += 1

        return scores


class LshOptimizedJaccardBasedKnn(object):
    def __init__(self, plays_by_most_often_played_song, most_often_played_song_by_user, k):
        self.plays_by_most_often_played_song = plays_by_most_often_played_song
        self.most_often_played_song_by_user = most_often_played_song_by_user
        self.k = k

    def compute_jaccard_index(self, set_1, set_2):
        n = len(set_1.intersection(set_2))
        return n / float(len(set_1) + len(set_2) - n)

    def find_for_user(self, user_plays):
        scores = []

        lsh_bucket_key = self.most_often_played_song_by_user[user_plays['_id']]
        lsh_bucket = self.plays_by_most_often_played_song[lsh_bucket_key]

        for other_user_plays in lsh_bucket:
            jaccard_index = self.compute_jaccard_index(set(user_plays['value']), set(other_user_plays['value']))
            scores.append({'user_id': other_user_plays['_id'], 'score': jaccard_index})

        scores.sort(key=lambda x: x['score'], reverse=True)
        scores = scores[0:self.k]
        return scores

    def find_for_users(self, users_plays):
        scores = []

        current_user_number = 1
        for user_plays in users_plays:
            print 'Looking for knn for %s. user' % current_user_number
            scores.append({'user_id': user_plays['_id'], 'scores': self.find_for_user(user_plays)})
            current_user_number += 1

        return scores


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    plays_by_most_often_played_song = [1]
    def load_plays_by_most_often_played_song(): plays_by_most_often_played_song[0] = \
        {x['_id']: x['value'] for x in list(db.plays_by_most_often_played_song_t.find())}
    invoke_measurable_task(load_plays_by_most_often_played_song, 'Load plays by most often played song')
    plays_by_most_often_played_song = plays_by_most_often_played_song[0]

    most_often_played_song_by_user = [1]
    def load_most_often_played_song_by_user(): most_often_played_song_by_user[0] = \
        {x['_id']: x['value'] for x in list(db.most_often_played_song_by_user_v.find())}
    invoke_measurable_task(load_most_often_played_song_by_user, 'Load most often played song by user')
    most_often_played_song_by_user = most_often_played_song_by_user[0]

    plays_for_validated_users = [1]
    def load_plays_for_all_users(): plays_for_validated_users[0] = list(
        db.plays_by_user_simple_filtered_v.find().limit(NUMBER_OF_USERS))
    invoke_measurable_task(load_plays_for_all_users, 'Load plays for validated users')
    plays_for_validated_users = plays_for_validated_users[0]

    knn = LshOptimizedJaccardBasedKnn(plays_by_most_often_played_song, most_often_played_song_by_user, KNN_K)
    invoke_measurable_task(lambda: knn.find_for_users(plays_for_validated_users),
                           'Find knn for %d users' % NUMBER_OF_USERS)
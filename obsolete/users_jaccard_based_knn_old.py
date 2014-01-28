from pymongo import MongoClient
from utils.measurements import invoke_measurable_task

MONGODB_PORT = 27017
KNN_K = 50
NUMBER_OF_USERS = 1


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

        with open('../results.txt', 'w') as file:
            file.write(str(scores[0]['user_id']))

            for score in scores[0]['scores']:
                file.write(' ' + str(score['user_id']))

        return scores


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    plays_for_all_users = [1]
    def load_plays_for_all_users(): plays_for_all_users[0] = list(db.plays_by_user_filtered_t.find())
    invoke_measurable_task(load_plays_for_all_users, 'Load plays for all users')
    plays_for_all_users = plays_for_all_users[0]

    plays_for_validated_users = [1]
    def load_plays_for_all_users(): plays_for_validated_users[0] = list(db.plays_by_user_filtered_v.find().limit(NUMBER_OF_USERS))
    invoke_measurable_task(load_plays_for_all_users, 'Load plays for validated users')
    plays_for_validated_users = plays_for_validated_users[0]

    knn = JaccardBasedKnn(plays_for_all_users, KNN_K)
    invoke_measurable_task(lambda: knn.find_for_users(plays_for_validated_users),
                           'Find knn for %d users' % NUMBER_OF_USERS)
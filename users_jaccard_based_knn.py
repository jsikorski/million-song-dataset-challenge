from heapq import heappush, nsmallest
from pymongo import MongoClient
from utils.measurements import invoke_measurable_task
from collections import OrderedDict

MONGODB_PORT = 27017
KNN_K = 50
NUMBER_OF_USERS = 110000
MIN_SIMILARITY = 0.15
MAX_SIZE_OF_BUCKET = 50000


class LshOptimizedJaccardBasedKnn(object):
    def __init__(self, buckets, k):
        self.buckets = buckets
        self.k = k

    def compute_jaccard_index(self, set_1, set_2):
        n = len(set_1.intersection(set_2))
        return n / float(len(set_1) + len(set_2) - n)

    def find_for_user(self, user_plays):
        if not user_plays['value']:
            return []

        scores = []

        song_ids = user_plays['value']
        lsh_bucket = []

        for i in range(1, len(song_ids)):
            if len(lsh_bucket) > MAX_SIZE_OF_BUCKET:
                break

            lsh_bucket_key = song_ids[i]
            if lsh_bucket_key in self.buckets:
                candidate_bucket = self.buckets[lsh_bucket_key]
                if len(candidate_bucket) <= MAX_SIZE_OF_BUCKET - len(lsh_bucket):
                    lsh_bucket += list(self.buckets[lsh_bucket_key])

        for other_user_plays in lsh_bucket:
            jaccard_index = self.compute_jaccard_index(set(user_plays['value']), set(other_user_plays['value']))
            heappush(scores, (1 - jaccard_index, {'user_id': other_user_plays['_id'], 'score': jaccard_index, 'songs': other_user_plays['value']}))

        return nsmallest(self.k, scores)

    def find_for_users(self, users_plays):
        scores = []

        current_user_number = 1
        for user_plays in users_plays:
            if current_user_number == 0 or current_user_number % 100 == 0:
                print 'Looking for knn for %s. user' % current_user_number
            scores.append({'user_id': user_plays['_id'], 'scores': self.find_for_user(user_plays)})
            current_user_number += 1

        return scores


with MongoClient('localhost', MONGODB_PORT) as client:
    db = client.local

    buckets = [1]
    def load_buckets(): buckets[0] = {x['_id']: x['value'] for x in list(db.plays_by_most_often_played_song_t.find())}
    invoke_measurable_task(load_buckets, 'Load buckets')
    buckets = buckets[0]

    plays_for_validated_users = [1]
    def load_plays_for_all_users(): plays_for_validated_users[0] = list(
        db.plays_by_user_simple_v.find().limit(NUMBER_OF_USERS))
    invoke_measurable_task(load_plays_for_all_users, 'Load plays for validated users')
    plays_for_validated_users = plays_for_validated_users[0]

    scores = [1]
    def find_knn_scores():
        knn = LshOptimizedJaccardBasedKnn(buckets, KNN_K)
        scores[0] = knn.find_for_users(plays_for_validated_users)

    invoke_measurable_task(find_knn_scores, 'Find knn for %d users' % NUMBER_OF_USERS)
    scores = scores[0]

    with open('users_knn_%s_results.txt' % KNN_K, 'w') as file:
        for score in scores:
            file.write(str(int(score['user_id'])))

            songs = OrderedDict()
            for scoreInt in score['scores']:
                if scoreInt[1]['score'] >= MIN_SIMILARITY:
                    for song_id in scoreInt[1]['songs']:
                        songs[song_id] = True

            for song_id in songs.iterkeys():
                file.write(str(' ' + str(song_id)))

            file.write('\n')
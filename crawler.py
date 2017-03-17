from bs4 import BeautifulSoup
import logging
import os
import time
from queue import Queue
import soundcloud
import sqlite3
import sys
import threading
from urllib.request import urlopen
import json

'''
TODO:
    - Fix issue where reposts aren't being added to DB
    - Fix "connection reset by peer" error
    - Signal handling for a cleaner way to stop crawling
    - Pick up crawling from a previous run
'''

CLIENT_ID = '2t9loNQH90kzJcsFCODdigxfp325aq4z' #'fDoItMDbsbZz8dY16ZzARCZmzgHBPotA' #'49009eb8904b11a2a5d2c6bdc162dd32'

USAGE = 'Usage: python3 crawler.py [-i <initial_user_id>]\n\t-i\tThe user ID to begin the crawl with. Defaults to Diplo (16730).'

# TABLE CREATION
CREATE_USER_TABLE = '''CREATE TABLE IF NOT EXISTS users (
    id integer PRIMARY KEY, username text, url text, avatar_url text, 
    country text, city text,
    total_play_count integer, total_like_count integer, total_comment_count integer )'''
CREATE_FOLLOWING_TABLE = '''CREATE TABLE IF NOT EXISTS following (
    unique_id text PRIMARY KEY, id integer, following_id integer )'''
CREATE_COMMENTS_TABLE = '''CREATE TABLE IF NOT EXISTS comments (
    unique_id integer PRIMARY KEY, id integer, target_id integer )'''
CREATE_LIKES_TABLE = '''CREATE TABLE IF NOT EXISTS likes (
    unique_id text PRIMARY KEY, id integer, target_id integer )'''
CREATE_REPOSTS_TABLE = '''CREATE TABLE IF NOT EXISTS reposts (
    unique_id text PRIMARY KEY, id integer, target_id integer )'''

# INSERTIONS
INSERT_USER = '''INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?,?,?,?)'''
INSERT_FOLLOWING = '''INSERT OR REPLACE INTO following VALUES (?,?,?)'''
INSERT_COMMENT = '''INSERT OR REPLACE INTO comments VALUES (?,?,?)'''
INSERT_LIKE = '''INSERT OR REPLACE INTO likes VALUES (?,?,?)'''
INSERT_REPOST = '''INSERT OR REPLACE INTO reposts VALUES (?,?,?)'''

logging.basicConfig(level=logging.WARNING, format='%(message)s')

def main():
    if len(sys.argv) == 1:
        with Crawler() as c:
            c.crawl()
    elif len(sys.argv) == 2:
        url = sys.argv[1]
        with Crawler(url) as c:
            c.crawl()
    else:
        print(USAGE)

class Crawler:
    def __init__(self, first_user=16730, db='sc_graph.sqlite3'):
        self.visited_users_lock = threading.Lock()
        self.visited_users = set()

        self.user_id_queue = Queue()
        self.user_id_queue.put(first_user)
        self.user_data_queue = Queue()

        self.setup_db(db)

    def setup_db(self, filename):
        self.conn = sqlite3.connect(filename)
        self.conn.execute(CREATE_USER_TABLE)
        self.conn.execute(CREATE_FOLLOWING_TABLE)
        self.conn.execute(CREATE_COMMENTS_TABLE)
        self.conn.execute(CREATE_LIKES_TABLE)
        self.conn.execute(CREATE_REPOSTS_TABLE)
        self.conn.commit()

        # TODO: Load IDs from `users` table into `self.visited_users`

    def saver(self):
        while True:
            user_data = self.user_data_queue.get()
            try:
                user = user_data['user']
                user_cols = (
                    user['id'],
                    user['username'],
                    user['permalink_url'],
                    user['avatar_url'],
                    user['country'] if 'country' in user else user['country_code'],
                    user['city'],
                    user_data['total_play_count'],
                    user_data['total_like_count'],
                    user_data['total_comment_count']
                )
                self.conn.execute(INSERT_USER, user_cols)

                followings = user_data['followings']
                self.conn.executemany(INSERT_FOLLOWING, [(str(user['id']) + '-' + str(following['id']), user['id'], following['id']) for following in followings])

                comments = user_data['comments']
                ids_for_comments = user_data['ids_for_comments']
                self.conn.executemany(INSERT_COMMENT, [(comment['self']['urn'].split(':')[-1], user['id'], ids_for_comments[idx]) for idx, comment in enumerate(comments)])

                likes = user_data['likes']
                self.conn.executemany(INSERT_LIKE, [(str(user['id']) + '-' + str(like['track']['id']), user['id'], like['track']['user']['id']) for like in likes])

                reposts = user_data['reposts']
                self.conn.executemany(INSERT_REPOST, [(str(user['id']) + '-' + str(repost['track']['id']), user['id'], repost['track']['user']['id']) for repost in reposts])

                self.conn.commit()
                logging.warning("[INFO] Saved \"{}\"".format(user['username']))
            except Exception as e:
                logging.error("[ERROR] Failed to save \"{}\" ({})".format(user['username'], e))
            self.user_data_queue.task_done()

    def get_collection(self, user_id, resource_name, max_pages=100000000):    # maximum = 200 * 100mil = 20bil objects
        url = 'https://api-v2.soundcloud.com/'
        if resource_name == 'reposts': # reposts need '/stream' in the url
            url += 'stream/'
        url += ('users/'
            + str(user_id)
            + '/' + resource_name
            + '?client_id=' + CLIENT_ID
            + '&limit=200&linked_partitioning=1')

        response = json.loads(urlopen(url).read().decode('utf-8'))
        collection = response['collection']
        max_pages -= 1

        while 'next_href' in response and response['next_href'] and max_pages > 0:
            next_href = response['next_href'] + '&client_id=' + CLIENT_ID
            response = json.loads(urlopen(next_href).read().decode('utf-8'))
            collection += response['collection']
            max_pages -= 1

        return collection

    def get_user(self, user_id):
        url = ('https://api.soundcloud.com/users/' + str(user_id)
            + '?client_id=' + CLIENT_ID)
        response = json.loads(urlopen(url).read().decode('utf-8'))
        return response

    def get_track(self, track_id):
        url = ('https://api-v2.soundcloud.com/tracks/' + str(track_id)
            + '?client_id=' + CLIENT_ID)
        response = json.loads(urlopen(url).read().decode('utf-8'))
        return response

    def get_user_ids_for_tracks(self, track_ids):
        url = ('https://api-v2.soundcloud.com/tracks?ids=' + ','.join(track_ids)
            + '&client_id=' + CLIENT_ID)
        response = json.loads(urlopen(url).read().decode('utf-8'))
        user_ids = []
        for track_id in track_ids:
            found = False
            for r in response:
                if r['id'] == int(track_id):
                    user_ids.append(r['user_id'])
                    found = True
                    break
            if not found:
                user_ids.append(0)
        return user_ids

    def scraper(self, id=-1):
        while True:
            try:
                start = time.time()
                user = {'username': 'Unknown'}
                user_id = self.user_id_queue.get()
                logging.warning('[INFO] Queue has grown to length: {}'.format(self.user_id_queue.qsize()))
                with self.visited_users_lock:
                    if user_id in self.visited_users:
                        self.user_id_queue.task_done()
                        continue
                    self.visited_users.add(user_id)

                user_start = time.time()
                user = self.get_user(user_id)
                user_time = time.time() - user_start

                followings_start = time.time()
                followings = self.get_collection(user_id, 'followings')
                followings_time = time.time() - followings_start

                likes_start = time.time()
                likes = self.get_collection(user_id, 'likes')
                track_likes = []
                for l in likes:
                    if 'track' in l:    # some likes are playlists (ignore these for now)
                        track_likes.append(l)
                likes = track_likes
                likes_time = time.time() - likes_start

                comments_start = time.time()
                comments = self.get_collection(user_id, 'comments')
                ids_for_comments = []
                try:
                    track_ids = [comment['track'].split(':')[-1] for comment in comments]
                    ids_for_comments = [user for user in self.get_user_ids_for_tracks(track_ids)]
                except Exception as e:
                    logging.info('Failed to scrape comments for user {}, continuing...'.format(user['username']))

                if len(ids_for_comments) > 0:
                    ids_for_comments, comments = zip(*[(i, comment) for (i, comment) in zip(ids_for_comments, comments) if i != 0])
                else:
                    ids_for_comments, comments = [], []
                comments_time = time.time() - comments_start
                
                reposts_start = time.time()
                reposts = self.get_collection(user_id, 'reposts')
                track_reposts = []
                for r in reposts:
                    if 'track' in r:    # some reposts are playlists (ignore these for now)
                        track_reposts.append(r)
                reposts = track_reposts
                reposts_time = time.time() - reposts_start

                tracks_start = time.time()
                tracks = self.get_collection(user_id, 'tracks')
                play_count, like_count, comment_count = 0, 0, 0
                for track in tracks:
                    if 'playback_count' in track:
                        play_count += track['playback_count'] if track['playback_count'] else 0
                    if 'likes_count' in track:
                        like_count += track['likes_count'] if track['likes_count'] else 0
                    if 'comment_count' in track:
                        comment_count += track['comment_count'] if track['comment_count'] else 0
                tracks_time = time.time() - tracks_start

                user_data = {
                    'user': user,
                    'total_like_count': like_count,
                    'total_comment_count': comment_count,
                    'total_play_count': play_count,
                    'followings': followings,
                    'likes': likes,
                    'comments': comments,
                    'ids_for_comments': ids_for_comments,
                    'reposts': reposts 
                }
                
                self.user_data_queue.put(user_data)

                for following in followings:
                    with self.visited_users_lock:
                        if following['id'] not in self.visited_users:
                            self.user_id_queue.put(following['id'])

                followers_start = time.time()
                followers = self.get_collection(user_id, 'followers', max_pages=5)  # Get 1000 followers at most
                for follower in followers:
                    with self.visited_users_lock:
                        if follower['id'] not in self.visited_users:
                            self.user_id_queue.put(follower['id'])
                followers_time = time.time() - followers_start

                total_time = time.time() - start
                logging.warning('[INFO] Thread {} scraped {} ({}) in {:.2f}s\n\tUser info in {:.2f}s\n\t{} followings in {:.2f}s\n\t{} likes {:.2f}s\n\t{} comments: {:.2f}s\n\t{} reposts in {:.2f}s\n\t{} tracks in {:.2f}s\n\t{} followers in {:.2f}s'.format(id, user['username'], user_id, total_time, user_time, len(followings), followings_time, len(likes), likes_time, len(comments), comments_time, len(reposts), reposts_time, len(tracks), tracks_time, len(followers), followers_time))

                self.user_id_queue.task_done()
            except Exception as e:
                logging.error('[ERROR] Failed to scrape {} ({}) due to error: {}'.format(user['username'], user_id, e))

    def crawl(self):
        scraper_threads = []
        for i in range(10):
            t = threading.Thread(target=self.scraper, args=(i,))
            scraper_threads.append(t)
            t.start()

        self.saver()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.conn.commit()
        self.conn.close()

if __name__ == '__main__':
    main()

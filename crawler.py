from bs4 import BeautifulSoup
import logging
import os
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

CLIENT_ID = 'fDoItMDbsbZz8dY16ZzARCZmzgHBPotA' #'49009eb8904b11a2a5d2c6bdc162dd32'

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

    def get_collection(self, user_id, resource_name):
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

        while 'next_href' in response and response['next_href']:
            next_href = response['next_href'] + '&client_id=' + CLIENT_ID
            response = json.loads(urlopen(next_href).read().decode('utf-8'))
            collection += response['collection']

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

    def scraper(self):
        while True:
            try:
                user = {'username': 'Unknown'}
                user_id = self.user_id_queue.get()
                with self.visited_users_lock:
                    if user_id in self.visited_users:
                        self.user_id_queue.task_done()
                        return
                    self.visited_users.add(user_id)

                user = self.get_user(user_id)

                followings = self.get_collection(user_id, 'followings')

                likes = self.get_collection(user_id, 'likes')
                track_likes = []
                for l in likes:
                    if 'track' in l:    # some likes are playlists (ignore these for now)
                        track_likes.append(l)
                likes = track_likes

                comments = self.get_collection(user_id, 'comments')
                ids_for_comments = []
                for comment in comments:
                    try:
                        track = self.get_track(comment['track'].split(':')[-1])
                        ids_for_comments.append(track['user']['id'])
                    except Exception as e:
                        ids_for_comments.append(0)
                        logging.info('Failed to scrape comment {} for user {}, continuing...'.format(comment['self']['urn'], user['username']))
                
                reposts = self.get_collection(user_id, 'reposts')
                track_reposts = []
                for r in reposts:
                    if 'track' in r:    # some reposts are playlists (ignore these for now)
                        track_reposts.append(r)
                reposts = track_reposts

                tracks = self.get_collection(user_id, 'tracks')
                play_count, like_count, comment_count = 0, 0, 0
                for track in tracks:
                    if 'playback_count' in track:
                        play_count += track['playback_count'] if track['playback_count'] else 0
                    if 'likes_count' in track:
                        like_count += track['likes_count'] if track['likes_count'] else 0
                    if 'comment_count' in track:
                        comment_count += track['comment_count'] if track['comment_count'] else 0

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
                    self.user_id_queue.put(following['id'])

                followers = self.get_collection(user_id, 'followers')
                for follower in followers:
                    self.user_id_queue.put(follower['id'])

                self.user_id_queue.task_done()
            except Exception as e:
                logging.error('[ERROR] Failed to scrape \"{}\" ({}) because: {}'.format(user['username'], user_id, e))

    def crawl(self):
        scraper_threads = []
        for i in range(200):
            t = threading.Thread(target=self.scraper)
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

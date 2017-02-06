from bs4 import BeautifulSoup
import logging
import os
from queue import Queue
import soundcloud
import sqlite3
import sys
import threading
from urllib.request import urlopen

'''
TODO:
    - Signal handling for a cleaner way to stop crawling
    - Pick up crawling from a previous run
'''

CLIENT_ID = 'fDoItMDbsbZz8dY16ZzARCZmzgHBPotA' #'49009eb8904b11a2a5d2c6bdc162dd32'

USAGE = 'Usage: python3 crawler.py <initial_track_url> [-l <traversal_limit>]'

# TABLE CREATION
CREATE_USER_TABLE = '''CREATE TABLE IF NOT EXISTS users (
    id integer PRIMARY KEY, username text, url text, avatar_url text, 
    country text, city text, description text, 
    track_count integer, followers_count integer, followings_count integer,
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
INSERT_USER = '''INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'''
INSERT_FOLLOWING = '''INSERT INTO following VALUES (?,?,?)'''
INSERT_COMMENT = '''INSERT INTO comments VALUES (?,?,?)'''
INSERT_LIKE = '''INSERT INTO likes VALUES (?,?,?)'''
INSERT_REPOST = '''INSERT INTO reposts VALUES (?,?,?)'''

logging.basicConfig(level=logging.WARNING, format='%(message)s')

def main():
    if len(sys.argv) == 1:
        with Crawler() as c:
            c.crawl()
    if len(sys.argv) == 2:
        url = sys.argv[1]
        with Crawler(url) as c:
            c.crawl()
    elif len(sys.argv) == 4:
        url = sys.argv[1]
        limit = int(sys.argv[3])
        with Crawler(url, limit=limit) as c:
            c.crawl()

class Crawler:
    def __init__(self, first_user=16730, limit=float('Inf'), db='sc_graph.sqlite3'):
        self.visited_users_lock = threading.Lock()
        self.visited_users = set()
        self.limit = limit

        self.user_id_queue = Queue()
        self.user_id_queue.put(first_user)
        self.user_data_queue = Queue()

        self.client = soundcloud.Client(client_id=CLIENT_ID)

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
                    user.id,
                    user.username,
                    user.permalink_url,
                    user.avatar_url,
                    user.country,
                    user.city,
                    user.description,
                    user.track_count,
                    user.followers_count,
                    user.followings_count,
                    user_data['total_play_count'],
                    user_data['total_like_count'],
                    user_data['total_comment_count']
                )
                self.conn.execute(INSERT_USER, user_cols)

                followings = user_data['followings']
                self.conn.executemany(INSERT_FOLLOWING, [(str(user.id) + '-' + str(following.id), user.id, following.id) for following in followings])

                comments = user_data['comments']
                ids_for_comments = user_data['ids_for_comments']
                self.conn.executemany(INSERT_COMMENT, [(comment.id, user.id, ids_for_comments[idx]) for idx, comment in enumerate(comments)])

                likes = user_data['likes']
                self.conn.executemany(INSERT_LIKE, [(str(user.id) + '-' + str(like.id), user.id, like.user['id']) for like in likes])

                reposts = user_data['reposts']
                self.conn.executemany(INSERT_REPOST, [(str(user.id) + '-' + str(repost.track['id']), user.id, repost.track['user']['id']) for repost in reposts])
                
                self.conn.commit()
                logging.warning("[INFO] Saved \"{}\"".format(user.username))
            except Exception as e:
                logging.error("[ERROR] Failed to save \"{}\" ({})".format(user.username, e))
            self.user_data_queue.task_done()

    def get_collection(self, path):
        resource = self.client.get(path, linked_partitioning=1)
        collection = resource.collection

        while hasattr(resource, 'next_href'):
            resource = self.client.get(resource.next_href)
            collection += resource.collection
            if hasattr(resource, 'next_href') and resource.next_href == None:
                break

        return collection

    def scraper(self):
        while True:
            try:
                user_id = self.user_id_queue.get()
                with self.visited_users_lock:
                    if user_id in self.visited_users:
                        self.user_id_queue.task_done()
                        return
                    self.visited_users.add(user_id)

                path = '/users/' + str(user_id)

                user = self.client.get(path)
                followings = self.get_collection(path + '/followings')
                likes = self.get_collection(path + '/favorites')
                comments = self.get_collection(path + '/comments')
                ids_for_comments = []
                for comment in comments:
                    track = self.client.get('/tracks/' + str(comment.track_id))
                    ids_for_comments.append(track.user['id'])
                reposts = self.client.get('/e1' + path + '/track_reposts')

                tracks = self.client.get(path + '/tracks')
                play_count, like_count, comment_count = 0, 0, 0
                for track in tracks:
                    if hasattr(track, 'playback_count'):
                        play_count += track.playback_count
                    if hasattr(track, 'favoritings_count'):
                        like_count += track.favoritings_count
                    if hasattr(track, 'comment_count'):
                        comment_count += track.comment_count

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
                    self.user_id_queue.put(following.id)

                followers = self.client.get(path + '/followers').collection
                for follower in followers:
                    self.user_id_queue.put(follower.id)

                self.user_id_queue.task_done()
            except Exception as e:
                print('[ERROR] Failed to scrape \"{}\" ({}) because: {}'.format(user.username if user else 'Unknown', user_id, e))

    def crawl(self):
        scraper_threads = []
        for i in range(500):
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

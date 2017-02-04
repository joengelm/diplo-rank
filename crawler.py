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

CLIENT_ID = '49009eb8904b11a2a5d2c6bdc162dd32'

USAGE = 'Usage: python3 crawler.py <initial_track_url> [-l <traversal_limit>]'

CREATE_USER_TABLE = '''CREATE TABLE IF NOT EXISTS users (
    id integer PRIMARY KEY, username text, url text, avatar_url text, 
    country text, city text, description text, 
    track_count integer, follow_count integer, following_count integer )'''

CREATE_FOLLOWING_TABLE = '''CREATE TABLE IF NOT EXISTS following (
    id integer, following_id integer )'''

CREATE_COMMENTS_TABLE = '''CREATE TABLE IF NOT EXISTS comments (
    id integer, target_id integer )'''

CREATE_LIKES_TABLE = '''CREATE TABLE IF NOT EXISTS likes (
    id integer, target_id integer )'''

CREATE_REPOSTS_TABLE = '''CREATE TABLE IF NOT EXISTS reposts (
    id integer, target_id integer )'''

logging.basicConfig(level=logging.WARNING, format='%(message)s')

def main():
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
    def __init__(self, first_user, limit=float('Inf'), db='sc_users.sqlite3'):
        self.future_users = Queue()
        self.future_users.put(first_user)

        self.visited_users_lock = threading.Lock()
        self.visited_urls = set()
        self.limit = limit

        self.user_id_queue = Queue()
        self.user_data_queue = Queue()

        self.client = soundcloud.Client(client_id=CLIENT_ID)

        self.setup_db(filename)

    def setup_db(self, filename):
        self.conn = sqlite3.connect(db)
        self.conn.execute(CREATE_USER_TABLE)
        self.conn.execute(CREATE_FOLLOWING_TABLE)
        self.conn.execute(CREATE_COMMENTS_TABLE)
        self.conn.execute(CREATE_LIKES_TABLE)
        self.conn.execute(CREATE_REPOSTS_TABLE)
        self.conn.commit()

    def saver(self):
        while True:
            user_data = self.user_data_queue.get()
            try:
                user = user_data['user']
                following = user_data['following']
                comments = user_data['comments']
                likes = user_data['likes']
                reposts = user_data['reposts']
                self.conn.execute("INSERT OR REPLACE INTO tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", track_details)
                self.conn.commit()
                logging.warning("{:1.3f}  {} ({})".format(track.favoritings_count / track.playback_count, track.title, track.permalink_url))
            except:
                logging.error("Failed to save: {0}".format(track.title))
            self.user_data_queue.task_done()

    def scraper(self):
        try:

            user_id = self.user_id_queue.get()
            path = '/users/' + str(user_id)

            user = client.get(path)
            followings = client.get(path + '/followings').collection
            likes = client.get(path + '/favorites')
            comments = client.get(path + '/comments')
            reposts = client.get('/e1' + path + '/track_reposts')
            #play_count = get_play_count(user_id)
            obj = {
                'user': user,
                'followings': followings,
                'likes': likes,
                'comments': comments,
                'reposts': reposts 
            }
            
            self.user_data_queue.put(obj)

        except:
            print('Error')

    def crawl(self):
        scraper_threads = []
        for i in range(20):
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

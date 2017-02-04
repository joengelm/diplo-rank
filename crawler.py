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

CREATE_TABLE_IF_NEEDED = '''CREATE TABLE IF NOT EXISTS tracks (
    id integer PRIMARY KEY, title text, user text, url text,
    released timestamp, description text, artwork_url text, 
    duration integer, genre text, 
    plays integer, likes integer, comments integer )'''

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
    def __init__(self, first_url, limit=float('Inf'), db='tracks.sqlite3'):
        self.future_urls = Queue()
        self.future_urls.put(first_url)

        self.visited_urls = set()
        self.limit = limit

        self.url_queue = Queue()
        self.track_queue = Queue()

        self.client = soundcloud.Client(client_id=CLIENT_ID)

        self.conn = sqlite3.connect(db)
        self.conn.execute(CREATE_TABLE_IF_NEEDED)
        self.conn.commit()

    def saver(self):
        while True:
            track = self.track_queue.get()
            try:
                track_details = (
                    track.id,
                    track.title, 
                    track.user['username'],
                    track.permalink_url,
                    track.created_at,
                    track.description,
                    track.artwork_url,
                    track.duration,
                    track.genre,
                    track.playback_count, 
                    track.favoritings_count, 
                    track.comment_count
                )
                self.conn.execute("INSERT OR REPLACE INTO tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", track_details)
                self.conn.commit()
                logging.warning("{:1.3f}  {} ({})".format(track.favoritings_count / track.playback_count, track.title, track.permalink_url))
            except:
                logging.error("Failed to save: {0}".format(track.title))
            self.track_queue.task_done()
        
    def processor(self):
        while True:
            url = self.url_queue.get()
            try:
                resolved_url = self.client.get('/resolve', url=url);
                if resolved_url.kind == 'track':
                    track = self.client.get('/tracks/' + str(resolved_url.id))
                    self.track_queue.put(track)
            except:
                logging.error('Failed to save: {0}'.format(url))
            self.url_queue.task_done()

    def generator(self):
        while len(self.visited_urls) < self.limit:
            url = self.future_urls.get()
            if url not in self.visited_urls:
                self.url_queue.put(url)
                self.visited_urls.add(url)
                try:
                    soup = BeautifulSoup(urlopen(url + "/recommended"), "html.parser")
                    for related in soup.find_all(itemprop="url"):
                        self.future_urls.put("https://soundcloud.com" + related['href'])
                except:
                    logging.error('Failed to parse links on page: {0}/recommended'.format(url))

    def crawl(self):
        processor_threads = []
        for i in range(10):
            t = threading.Thread(target=self.processor)
            processor_threads.append(t)
            t.start()

        generator_threads = []
        for i in range(5):
            t = threading.Thread(target=self.generator)
            generator_threads.append(t)
            t.start()

        self.saver()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.conn.commit()
        self.conn.close()

if __name__ == '__main__':
    main()

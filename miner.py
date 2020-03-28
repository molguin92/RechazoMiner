from __future__ import annotations

import json
import threading
import time
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import List, NamedTuple

import tweepy as tp
from loguru import logger


def setup_API() -> tp.API:
    with open('./auth.json', 'r') as fp:
        keys = json.load(fp)
    auth = tp.OAuthHandler(keys['api_key'], keys['api_secret'])
    auth.set_access_token(keys['access_token'], keys['access_token_secret'])
    return tp.API(auth)


class TweetRecord(NamedTuple):
    tweet_id: int
    full_text: str
    user_id: int
    date: datetime
    hashtags: List[str]


class AsyncDiskWriteListener(AbstractContextManager, tp.StreamListener):
    def __init__(self, save_path: Path, *args, **kwargs):
        super(AsyncDiskWriteListener, self).__init__(*args, **kwargs)
        self._path = save_path

        self._running = threading.Event()
        self._running.clear()

        self._write_q = Queue()

        self._write_thread = threading.Thread(
            target=AsyncDiskWriteListener._write_loop, args=(self,))

    def start(self):
        self._running.set()
        self._write_thread.start()

    def stop(self):
        self._running.clear()
        try:
            self._write_thread.join(timeout=1)
        except TimeoutError:
            logger.warning('Timed out while joining write thread.')

    def __enter__(self) -> AsyncDiskWriteListener:
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def _write_loop(self):
        while self._running.is_set():
            time.sleep(.5)
            try:
                tweet = self._write_q.get(block=False)
                print(tweet)
            except Empty:
                continue

    def parse_tweet(self, tweet: tp.Status):
        if hasattr(tweet, 'extended_tweet'):
            text = tweet.extended_tweet['full_text']
        else:
            text = tweet.text

        self._write_q.put(
            TweetRecord(int(tweet.id_str),
                        text,
                        int(tweet.user.id_str),
                        tweet.created_at,
                        tweet.entities.get('hashtags', []))
        )

    def on_status(self, tweet: tp.Status):
        if hasattr(tweet, 'retweeted_status'):
            self.parse_tweet(tweet.retweeted_status)
        elif hasattr(tweet, 'quoted_status'):
            self.parse_tweet(tweet)
            self.parse_tweet(tweet.quoted_status)
        else:
            self.parse_tweet(tweet)

    def on_error(self, status_code: int):
        print(status_code)
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False
        return True
        # returning non-False reconnects the stream, with backoff.


if __name__ == '__main__':
    api = setup_API()

    with AsyncDiskWriteListener(Path('/tmp')) as listener:
        stream = tp.Stream(auth=api.auth,
                           listener=listener)
        stream.filter(track=['#rechazo',
                             '#rechazotuoportunismo',
                             '#kramermiserable'],
                      languages=['es'],
                      locations=[-109.7, -56.7, -66.1, -17.5], )
    # is_async=True)
    # time.sleep(10)
    # stream.disconnect()

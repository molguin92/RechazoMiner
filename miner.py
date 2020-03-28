from __future__ import annotations

import json
import signal
import threading
import time
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import List, Literal, NamedTuple

import pandas as pd
import tweepy as tp
from loguru import logger

_shutdown_event = threading.Event()
_shutdown_event.clear()


def catch_signal(*args, **kwargs):
    logger.warning('Got shutdown signal!')
    _shutdown_event.set()


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
    def __init__(self, save_path: Path, *args,
                 mode: Literal['append', 'overwrite'] = 'append',
                 backlog_sz: int = 100, **kwargs):
        super(AsyncDiskWriteListener, self).__init__(*args, **kwargs)
        self._path = save_path.resolve()
        logger.info('Writing to {}', save_path)

        self._running = threading.Event()
        self._running.clear()

        self._write_q = Queue()

        self._write_thread = threading.Thread(
            target=AsyncDiskWriteListener._write_loop, args=(self,))

        self._backlog_sz = backlog_sz
        if self._path.exists():
            assert not self._path.is_dir()
            logger.warning('Path {} already exists.', self._path)
            if mode == 'append':
                self._saved_data = pd.read_parquet(save_path)
            elif mode == 'overwrite':
                logger.warning('Setting mode to overwrite!')
                self._saved_data = pd.DataFrame(columns=TweetRecord._fields) \
                    .set_index('tweet_id', drop=True)
            else:
                logger.error('Unrecognized mode: {}', mode)
                raise RuntimeError()

            logger.info('Set mode: {}', mode)
        else:
            logger.info('Specified path {} does not exist. Creating it.',
                        self._path)
            self._saved_data = pd.DataFrame(columns=TweetRecord._fields) \
                .set_index('tweet_id', drop=True)

    def start(self):
        logger.info('Starting write thread.')
        self._running.set()
        self._write_thread.start()

    def stop(self):
        logger.info('Stopping write thread.')
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

    @logger.catch
    def _write_loop(self):
        backlog = []
        while self._running.is_set():
            time.sleep(.5)
            try:
                backlog.append(self._write_q.get(block=False))
                if len(backlog) >= self._backlog_sz:
                    # write chunk to disk
                    logger.info('Backlog full - writing data to disk.')
                    chunk = pd.DataFrame(backlog).set_index('tweet_id',
                                                            drop=True)
                    backlog.clear()

                    self._saved_data = pd.concat((self._saved_data, chunk),
                                                 ignore_index=False)
                    self._saved_data.to_parquet(str(self._path))
            except Empty:
                continue

        logger.warning('Writing remaining items in backlog to disk.')
        chunk = pd.DataFrame(backlog).set_index('tweet_id',
                                                drop=True)
        backlog.clear()

        self._saved_data = pd.concat((self._saved_data, chunk),
                                     ignore_index=False)
        self._saved_data.to_parquet(str(self._path))
        logger.info('Exiting write loop.')

    def parse_tweet(self, tweet: tp.Status):
        tweet_id = int(tweet.id_str)
        if hasattr(tweet, 'extended_tweet'):
            text = tweet.extended_tweet['full_text']
        else:
            text = tweet.text

        hashtags = [h['text'] for h in tweet.entities.get('hashtags', [])]

        logger.info('Got tweet: {}', tweet_id)
        logger.info('Tweet preview: {}', text[:80])
        if len(hashtags) > 0:
            logger.info('Hashtags: {}', hashtags)

        self._write_q.put(TweetRecord(tweet_id,
                                      text,
                                      int(tweet.user.id_str),
                                      tweet.created_at,
                                      hashtags))

    def on_status(self, tweet: tp.Status):
        if hasattr(tweet, 'retweeted_status'):
            self.parse_tweet(tweet.retweeted_status)
        elif hasattr(tweet, 'quoted_status'):
            self.parse_tweet(tweet)
            self.parse_tweet(tweet.quoted_status)
        else:
            self.parse_tweet(tweet)

    def on_error(self, status_code: int):
        logger.warning('Got error code from Twitter API: {}', status_code)
        if status_code == 420:
            # returning False in on_error disconnects the stream
            logger.error('Disconnecting!')
            return False

        logger.warning('Reconnecting, with backoff.')
        return True
        # returning non-False reconnects the stream, with backoff.


if __name__ == '__main__':
    # register shutdown
    signal.signal(signal.SIGINT, catch_signal)

    api = setup_API()
    with AsyncDiskWriteListener(Path('./tweets.parquet')) as listener:
        stream = tp.Stream(auth=api.auth,
                           listener=listener)
        stream.filter(track=['#rechazo',
                             '#rechazotuoportunismo',
                             '#kramermiserable'],
                      languages=['es'],
                      locations=[-109.7, -56.7, -66.1, -17.5],
                      is_async=True)

        while not _shutdown_event.is_set():
            time.sleep(.5)
        stream.disconnect()

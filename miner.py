#!/usr/bin/env python3
from __future__ import annotations

import json
import signal
import threading
import time
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import List, Literal, NamedTuple, Optional, Tuple

import click
import pandas as pd
import tweepy as tp
from loguru import logger
from urllib3.exceptions import ProtocolError

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
                 backlog_sz: int = 500, **kwargs):
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

                # fix duplicates
                self._saved_data = self._saved_data.reset_index() \
                    .drop_duplicates(subset='tweet_id') \
                    .set_index('tweet_id')

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

    # noinspection DuplicatedCode
    @logger.catch
    def _write_loop(self):
        backlog = []
        while self._running.is_set():
            try:
                backlog.append(self._write_q.get(block=True, timeout=0.1))
                if len(backlog) >= self._backlog_sz:
                    # write chunk to disk
                    logger.info('Backlog full - writing data to disk.')
                    chunk = pd.DataFrame(backlog) \
                        .drop_duplicates(subset='tweet_id') \
                        .set_index('tweet_id', drop=True)
                    backlog.clear()

                    self._saved_data = pd.concat((self._saved_data, chunk),
                                                 ignore_index=False)
                    self._saved_data.to_parquet(str(self._path))
            except Empty:
                continue

        logger.warning('Writing remaining items in backlog to disk.')
        chunk = pd.DataFrame(backlog) \
            .drop_duplicates(subset='tweet_id') \
            .set_index('tweet_id', drop=True)
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


@click.command()
@click.argument('save_path', type=click.Path(exists=False,
                                             file_okay=True,
                                             dir_okay=False,
                                             writable=True,
                                             resolve_path=True))
@click.argument('track_terms', type=str, nargs=-1)
@click.option('-m', '--mode',
              type=click.Choice(['append', 'overwrite'],
                                case_sensitive=False),
              default='append', show_default=True)
@click.option('-b', '--backlog', 'backlog_sz', type=int, default=500,
              show_default=True)
@click.option('--location', 'locations', multiple=True,
              type=click.Tuple(types=[float, float, float, float]))
@click.option('--language', 'languages', multiple=True,
              type=str)
def main(save_path: str,
         track_terms: Tuple['str'],
         mode: Literal['append', 'overwrite'] = 'append',
         backlog_sz: int = 1000,
         locations: Optional[Tuple[Tuple[float]]] = None,
         languages: Optional[Tuple[str]] = None):
    if locations is not None:
        locations = []
        for loc in locations:
            locations = locations + list(loc)

    api = setup_API()
    logger.warning('Tracking terms: {}', list(track_terms))
    with AsyncDiskWriteListener(
            save_path=Path(save_path),
            mode=mode,
            backlog_sz=backlog_sz) as listener:
        while True:
            try:
                stream = tp.Stream(auth=api.auth,
                                   listener=listener)
                stream.filter(track=track_terms,
                              languages=list(languages),
                              locations=locations,
                              is_async=True)

                while not _shutdown_event.is_set():
                    time.sleep(.5)
                stream.disconnect()
                break
            except ProtocolError:
                continue
    pass


if __name__ == '__main__':
    # register shutdown
    signal.signal(signal.SIGINT, catch_signal)
    main()

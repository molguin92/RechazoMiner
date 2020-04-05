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
from typing import Collection, List, Literal, NamedTuple, Optional, Tuple

import click
import pandas as pd
import tweepy as tp
from loguru import logger
from urllib3.exceptions import ProtocolError

_shutdown_event = threading.Event()
_shutdown_event.clear()


# noinspection PyUnusedLocal
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
    retweets: int
    likes: int
    replies: int


class UserRecord(NamedTuple):
    user_id: int
    name: str
    screen_name: str
    location: str
    url: str
    verified: bool
    description: str
    following: int
    followers: int
    tweets: int
    created: datetime


class AsyncDiskWriteListener(AbstractContextManager, tp.StreamListener):
    def __init__(self,
                 save_path: Path,
                 *args,
                 mode: Literal['append', 'overwrite'] = 'append',
                 backlog_sz: int = 500,
                 **kwargs):
        super(AsyncDiskWriteListener, self).__init__(*args, **kwargs)
        save_path.mkdir(exist_ok=True, parents=True)
        self._tweet_path = save_path.resolve() / 'tweets.parquet'
        self._user_path = save_path.resolve() / 'users.parquet'
        logger.info('Writing tweets to {}', self._tweet_path)
        logger.info('Writing users to {}', self._user_path)

        self._running = threading.Event()
        self._running.clear()

        self._write_q = Queue()

        self._write_thread = threading.Thread(
            target=AsyncDiskWriteListener._process_loop, args=(self,))

        self._backlog_sz = backlog_sz

        if mode == 'overwrite':
            logger.warning('Setting mode to overwrite!')
            self._saved_tweet_data = pd.DataFrame(columns=TweetRecord._fields) \
                .set_index('tweet_id', drop=True)
            self._saved_user_data = pd.DataFrame(columns=UserRecord._fields) \
                .set_index('user_id', drop=True)
        elif mode == 'append':
            if self._tweet_path.exists():
                assert self._tweet_path.is_file()
                self._saved_tweet_data = pd.read_parquet(self._tweet_path)
            else:
                self._saved_tweet_data = pd.DataFrame(
                    columns=TweetRecord._fields) \
                    .set_index('tweet_id', drop=True)

            if self._user_path.exists():
                assert self._user_path.is_file()
                self._saved_user_data = pd.read_parquet(self._user_path)
            else:
                self._saved_user_data = pd.DataFrame(
                    columns=UserRecord._fields) \
                    .set_index('user_id', drop=True)

        else:
            raise RuntimeError(f'Unrecognized mode: {mode}')

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

    def _write_backlog_to_disk(self,
                               tweets: Collection[TweetRecord],
                               users: Collection[UserRecord]) -> None:
        chunk_tweets = pd.DataFrame(tweets).set_index('tweet_id', drop=True)
        chunk_users = pd.DataFrame(users).set_index('user_id', drop=True)

        self._saved_tweet_data = pd.concat(
            objs=(self._saved_tweet_data, chunk_tweets),
            ignore_index=False) \
            .reset_index() \
            .drop_duplicates(subset='tweet_id') \
            .set_index('tweet_id', verify_integrity=True, drop=True)

        self._saved_tweet_data.to_parquet(str(self._tweet_path))

        self._saved_user_data = pd.concat(
            objs=(self._saved_user_data, chunk_users),
            ignore_index=False) \
            .reset_index() \
            .drop_duplicates(subset='user_id') \
            .set_index('user_id', verify_integrity=True, drop=True)

        self._saved_user_data.to_parquet(str(self._user_path))

    @logger.catch
    def _process_loop(self):
        backlog_tweets = {}
        backlog_users = {}
        while self._running.is_set():
            try:
                tweet, user = AsyncDiskWriteListener. \
                    _parse_tweet(self._write_q.get(block=True, timeout=0.1))

                backlog_tweets[tweet.tweet_id] = tweet
                backlog_users[user.user_id] = user

                if max(len(backlog_tweets), len(backlog_users)) >= \
                        self._backlog_sz:
                    # write chunk to disk
                    logger.info('Backlog full - writing data to disk.')
                    self._write_backlog_to_disk(
                        tweets=list(backlog_tweets.values()),
                        users=list(backlog_users.values())
                    )
                    backlog_tweets.clear()
                    backlog_users.clear()
            except Empty:
                continue

        if max(len(backlog_tweets), len(backlog_users)) > 0:
            logger.warning('Writing remaining items in backlog to disk.')
            self._write_backlog_to_disk(
                tweets=list(backlog_tweets.values()),
                users=list(backlog_users.values())
            )
            backlog_tweets.clear()
            backlog_users.clear()
        logger.info('Exiting write loop.')

    @staticmethod
    def _parse_tweet(tweet: tp.Status) -> Tuple[TweetRecord, UserRecord]:
        user = tweet.user
        tweet_r = TweetRecord(
            tweet_id=tweet.id,
            full_text=(tweet.extended_tweet['full_text']
                       if hasattr(tweet, 'extended_tweet') else tweet.text),
            user_id=user.id,
            date=tweet.created_at,
            hashtags=[h['text'] for h in tweet.entities.get('hashtags', [])],
            retweets=tweet.retweet_count,
            likes=tweet.favorite_count,
            replies=tweet.reply_count
        )

        user_r = UserRecord(
            user_id=user.id,
            name=user.name,
            screen_name=user.screen_name,
            location=user.location if hasattr(user, 'location') else None,
            url=user.url if hasattr(user, 'url') else None,
            verified=user.verified,
            description=(user.description
                         if hasattr(user, 'description') else None),
            following=user.friends_count,
            followers=user.followers_count,
            tweets=user.statuses_count,
            created=user.created_at
        )

        logger.info('Got tweet: {}', tweet_r.tweet_id)
        logger.info('By user: {}', user_r.screen_name)
        logger.info('Tweet preview: {}', tweet_r.full_text[:80])
        if len(tweet_r.hashtags) > 0:
            logger.info('Hashtags: {}', tweet_r.hashtags)

        return tweet_r, user_r

    def on_status(self, tweet: tp.Status):
        # just put it in the queue
        if hasattr(tweet, 'retweeted_status'):
            self._write_q.put(tweet.retweeted_status)
        elif hasattr(tweet, 'quoted_status'):
            self._write_q.put(tweet)
            self._write_q.put(tweet.quoted_status)
        else:
            self._write_q.put(tweet)

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
                                             file_okay=False,
                                             dir_okay=True,
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
    # noinspection PyTypeChecker
    signal.signal(signal.SIGINT, catch_signal)
    main()

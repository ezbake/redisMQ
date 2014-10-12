#   Copyright (C) 2013-2014 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

__all__ = ['RedisProducer', 'RedisConsumer', 'RedisMessage']

from redis import StrictRedis
from redis.exceptions import WatchError
import time
import logging

log = logging.getLogger(__name__)

SEPARATOR = ":"
NEXT_INDEX = "nextIndex"
MESSAGES = "messages"
TRIES_LIMIT = 3

def get_next_index_for_topic_key(topic):
    return get_key(topic, NEXT_INDEX)

def get_next_index_for_group_id_key(topic, group_id):
    return get_key(topic, group_id, NEXT_INDEX)

def get_message_key(topic, index):
    return get_key(topic, MESSAGES, str(index))

def get_key(prefix, suffix, *additional_suffixes):
    result = [prefix, SEPARATOR, suffix]
    result.extend(SEPARATOR + additional for additional in additional_suffixes)
    return "".join(result)


class RedisMessage(object):
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


class RedisProducer(object):
    def __init__(self, hostname = 'localhost', port = 6379):
        log.debug("Initializing RedisProducer with hostname of %s and port %s" % (hostname, port))
        self.r = StrictRedis(host = hostname, port = port)

    def send(self, message):
        tries = 0
        next_index_key = get_next_index_for_topic_key(message.topic)
        next_index = 1
        result = None
        log.debug("Sending message on topic %s" % message.topic)

        while result is None and tries < TRIES_LIMIT:
            if self.r.exists(next_index_key):
                next_index = long(self.r.get(next_index_key)) + 1

            message_key = get_message_key(message.topic, next_index)

            try:
                pl = self.r.pipeline()
                pl.watch(next_index_key, message_key)
                pl.multi()
                pl.incr(next_index_key).set(message_key, message.payload)
                result = pl.execute()
            except WatchError:
                # Should probably log something here, but all it means is we're
                # retrying
                pass

        if result is None:
            log.error("Could not send message, retry amount exceeded")
            raise RuntimeError("Attempted to send message %s times and failed" % TRIES_LIMIT)


class RedisConsumer(object):
    def __init__(self, timeout, group_id, hostname = 'localhost', port = 6379):
        self.group_id = group_id
        self.timeout = timeout
        log.debug("Initializing RedisConsumer with hostname of %s and port %s" % (hostname, port))
        self.r = StrictRedis(host = hostname, port = port)

    def poll(self, topic):
        result = None
        current_index_key = get_next_index_for_topic_key(topic)
        end_millis = time.time() * 1000 + self.timeout
        log.debug("Polling topic %s" % topic)

        while time.time() * 1000 < end_millis:
            if self.r.exists(current_index_key):
                current_index = long(self.r.get(current_index_key))
                consumer_index_key = get_next_index_for_group_id_key(topic, self.group_id)
                pl = self.r.pipeline()

                pl.watch(consumer_index_key)
                consumer_index = 0
                if self.r.exists(consumer_index_key):
                    consumer_index = long(self.r.get(consumer_index_key))

                if current_index > consumer_index:
                    try:
                        pl.multi()
                        pl.incr(consumer_index_key)

                        incr_result = pl.execute()

                        if not incr_result is None and len(incr_result) > 0:
                            consumer_index = long(incr_result[0])
                            key = get_message_key(topic, consumer_index)
                            if self.r.exists(key):
                                result = self.r.get(key)
                                break
                    except WatchError:
                        log.debug("Redis keys changed for topic %s and group %s, trying again" % (topic, self.group_id))
                        pass

        return result

    def unsubscribe_from_topic(self, topic):
        self.r.delete(get_next_index_for_group_id_key(topic, self.group_id))




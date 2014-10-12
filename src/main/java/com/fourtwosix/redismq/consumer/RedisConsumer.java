/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package com.fourtwosix.redismq.consumer;

import com.fourtwosix.redismq.utils.RedisMQUtils;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Consumer class for use with Redis instance. Each topic corresponds to keys in Redis. The following
 * are descriptions of all the consumer specific keys for a topic (words in all caps are constants):
 *
 *      o <topic>:<groupId>:NEXT_INDEX - Similar to the NEXT_INDEX key in the Producer, this key stores the current
 *                                       index for the topic in the given group ID. This means that for a consumer
 *                                       with the given groupId, the index stored here denotes the next message index
 *                                       to retrieve. This allows the consumer to replay all messages from a topic
 *                                       (if the user unsubscribes from the topic, their index returns to nil).
 *
 * With that key, it is easy to determine the next index to poll. Simply check the key against the current message
 * index for the given topic, and if that index is higher than the group ID index for the topic (or the group ID index
 * is nil) then increment the group ID index and get the value from the corresponding message key.
 */
public class RedisConsumer implements Closeable {
    private static Logger log = LoggerFactory.getLogger(RedisConsumer.class);
    private Jedis jedis;
    private String groupId;
    private int timeout;

    public RedisConsumer(int timeout, String groupId) {
        this(timeout, groupId, RedisMQUtils.getDefaultHostname(), RedisMQUtils.getDefaultPort());
    }

    public RedisConsumer(int timeout, String groupId, String hostname, int port) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hostname), "hostname cannot be null or empty!");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "groupId cannot be null or empty!");
        this.jedis = new Jedis(hostname, port);
        this.timeout = timeout;
        this.groupId = groupId;

        short tries = 0;
        while(!jedis.isConnected() && tries < 3) {
            log.info("----- Retrying redis consumer connection in 3s -----");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                // It's a sleep I don't care
            }
            jedis.connect();
            tries++;
        }

        if(!jedis.isConnected()) {
            throw new RuntimeException("Unable to connect to Redis after 4 tries. If you're sure it's " +
                    "running something bad is probably happening.");
        }

        log.info("----- Succesfully connected consumer to {}:{} -----", hostname, port,  jedis.isConnected());
    }

    /**
     * This method polls the given topic for a message. Optional.absent() is returned when no new message is found.
     *
     * @param topic the topic to poll for a message
     * @return the retrieved message
     */
    public Optional<byte[]> poll(String topic) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        Optional<byte[]> result = Optional.absent();
        String currentIndexKey = RedisMQUtils.getNextIndexForTopicKey(topic);

        long endMillis = System.currentTimeMillis() + timeout;

        // Continue to poll the queue until the timeout period ends
        while (System.currentTimeMillis() < endMillis) {
            // If the current index does not exist, then the topic does not exist
            if (jedis.exists(currentIndexKey)) {
                long currentIndex = Long.parseLong(RedisMQUtils.jedisGetString(jedis, currentIndexKey));
                String consumerIndexKey = RedisMQUtils.getNextIndexForGroupIdKey(topic, groupId);

                // Watch the consumer index to make sure that we don't increment twice when only looking for one
                // message
                jedis.watch(consumerIndexKey);
                long consumerIndex = 0;
                if (jedis.exists(consumerIndexKey)) {
                    consumerIndex = Long.parseLong(RedisMQUtils.jedisGetString(jedis, consumerIndexKey));
                }

                // Check to make sure the current message index is higher than the group index, signifying that there
                // are new messages on the queue
                if (currentIndex > consumerIndex) {
                    Transaction t = jedis.multi();
                    t.incr(consumerIndexKey);
                    List<Object> execResult = t.exec();

                    // If the transaction succeeded it means that the key was incremented and we can consume a message
                    if (execResult != null && execResult.size() > 0) {
                        consumerIndex = (Long)execResult.get(0);
                        String key = RedisMQUtils.getMessageKey(topic, Long.toString(consumerIndex));
                        byte[] keyBytes = key.getBytes();
                        if (jedis.exists(keyBytes)) {
                            byte[] payload = RedisMQUtils.jedisGetBytes(jedis, keyBytes);
                            result = Optional.of(payload);
                            break;
                        }
                    }
                }
            }
        }

        return result;
    }

    /**
     * This method unsubscribes the consumer from the given topic. This will reset the consumer's message index for
     * the given topic and force the consumer to replay all of the messages on the queue for that topic.
     *
     * @param topic the topic to unsubscribe from
     */
    public void unsubscribeFromTopic(String topic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        String topicKey = RedisMQUtils.getNextIndexForGroupIdKey(topic, groupId);
        jedis.del(topicKey);
    }

    @Override
    public void close() throws IOException {
        String rc = jedis.quit();
        log.info("Closed consumer with return code of " + rc);
    }
}

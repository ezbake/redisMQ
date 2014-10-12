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

package com.fourtwosix.redismq.producer;

import com.fourtwosix.redismq.message.RedisMessage;
import com.fourtwosix.redismq.utils.RedisMQUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Producer class for use with Redis instance. Each topic corresponds to several keys in Redis. The following
 * are descriptions of all the producer specific keys for a topic (words in all caps are constants):
 *
 *      o <topic>:MESSAGES:<number> - This key represents each message on the queue for the topic, with <number>
 *                                    denoting the ID of the message.
 *      o <topic>:NEXT_INDEX - This key represents the current message ID for this topic. For instance, a value of
 *                             4 would denote that <topic>:MESSAGES:4 exists. By keeping track of this, we can simply
 *                             increment this key and generate a new key for storing the next message in the queue.
 *
 * With those two facts in mind, a message in RedisMQ is sent simply by obtaining the next available message key,
 * and setting that key to the value of the given message.
 */
public class RedisProducer implements Closeable {
    private static Logger log = LoggerFactory.getLogger(RedisProducer.class);
    private Jedis jedis;

    public RedisProducer() {
        this(RedisMQUtils.getDefaultHostname(), RedisMQUtils.getDefaultPort());
    }

    public RedisProducer(String hostname, int port) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hostname), "hostname cannot be null or empty!");
        jedis = new Jedis(hostname, port);

        short tries = 0;
        while(!jedis.isConnected() && tries < 3) {
            log.info("----- Retrying redis producer connection in 3s -----");
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

        log.info("----- Succesfully connected producer to {}:{} -----", hostname, port,  jedis.isConnected());
    }

    /**
     * This method takes a RedisMessage and pushes it onto the topic specified by the message.
     *
     * @param message message to push onto RedisMQ
     */
    public void send(RedisMessage message) throws IOException {
        Preconditions.checkNotNull(message);
        List<Object> result = null;
        int tries = 0;
        String topic = message.getTopic();
        String nextIndexKey = RedisMQUtils.getNextIndexForTopicKey(topic);
        long nextIndex = 1;

        // Only try sending three times. The result object will not be null if the exec() command
        // succeeds.
        while ((result == null || result.size() == 0) && tries < 3) {
            if (jedis.exists(nextIndexKey)) {
                nextIndex = Long.parseLong(RedisMQUtils.jedisGetString(jedis, nextIndexKey)) + 1;
            }
            String messageKey = RedisMQUtils.getMessageKey(topic, Long.toString(nextIndex));

            // If the nextIndexKey or messageKey change then the transaction should fail
            jedis.watch(nextIndexKey, messageKey);
            Transaction t = jedis.multi();
            t.incr(nextIndexKey);
            t.set(messageKey.getBytes(), message.getPayload());
            result = t.exec();
            tries++;
        }

        if (result == null || result.size() == 0) {
            throw new RuntimeException("Attempted to send 3 times. Could not obtain lock.");
        }
    }

    @Override
    public void close() throws IOException {
        String rc = jedis.quit();
        log.info("Closed producer with return code of " + rc);
    }
}

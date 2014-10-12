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

package com.fourtwosix.redismq.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;

/**
 * Utility class for RedisMQ.
 */
public class RedisMQUtils {
    private static final Logger log = LoggerFactory.getLogger(RedisMQUtils.class);
    private static final char SEPARATOR = ':';
    private static final String NEXT_INDEX = "nextIndex";
    private static final String MESSAGES = "messages";
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int DEFAULT_PORT = 6379;
    private static final int MAX_TRIES = 3;

    public static String getNextIndexForTopicKey(String topic) {
        return getKey(topic, NEXT_INDEX);
    }

    public static String getNextIndexForGroupIdKey(String topic, String groupId) {
        return getKey(topic, groupId, NEXT_INDEX);
    }

    public static String getMessageKey(String topic, String index) {
        return getKey(topic, MESSAGES, index);
    }

    public static String getDefaultHostname() {
        return DEFAULT_HOSTNAME;
    }

    public static int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public static String jedisGetString(Jedis jedis, String key) throws IOException {
        String result = null;
        for (int i = 0; i < MAX_TRIES && result == null; i++) {
            try {
                result = jedis.get(key);
            } catch (JedisConnectionException e) {
                log.warn("Connection error occurred when getting {}, retrying", key);
                result = null;
            }
        }

        if (result == null) {
            throw new IOException(String.format("Could not retrieve key %s, message has been dropped", key));
        }

        return result;
    }

    public static byte[] jedisGetBytes(Jedis jedis, byte[] key) throws IOException {
        byte[] result = null;
        for (int i = 0; i < MAX_TRIES && result == null; i++) {
            try {
                result = jedis.get(key);
            } catch (JedisConnectionException e) {
                log.warn("Connection error occurred when getting {}, retrying", new String(key));
                result = null;
            }
        }

        if (result == null) {
            throw new IOException(String.format("Could not retrieve key %s, message has been dropped", new String(key)));
        }

        return result;
    }

    private static String getKey(String prefix, String suffix, String... suffixes) {
        StringBuffer result = new StringBuffer(prefix);
        result.append(SEPARATOR).append(suffix);

        for (String additional : suffixes) {
            result.append(SEPARATOR).append(additional);
        }
        return result.toString();
    }
}

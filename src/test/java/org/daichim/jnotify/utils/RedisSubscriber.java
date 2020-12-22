package org.daichim.jnotify.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.daichim.jnotify.impl.RedisNotifierClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.daichim.jnotify.utils.TestUtils.redisUsername;

@Slf4j
public class RedisSubscriber {

    private final Jedis jedis;

    private ObjectMapper objectMapper;

    private ExecutorService subscriber;

    public RedisSubscriber(String host, int port) {
        this.jedis = new Jedis(host, port);
        this.objectMapper = new ObjectMapper();
        this.subscriber = Executors.newCachedThreadPool();
    }

    public Subscription set(String user,
                            Wrapper<Boolean> subscrFlag,
                            CountDownLatch subscriberLatch) throws Exception {
        JedisPubSub pubsub = new JedisPubSub() {
            @SneakyThrows
            @Override
            public void onMessage(String channel, String message) {
                log.debug("Callback received: {} -> {}", channel, message);
                String[] users = objectMapper.readerFor(String[].class).readValue(message);
                if (channel.equals(RedisNotifierClient.CALLBACK_CHANNEL) &&
                    ArrayUtils.contains(users, redisUsername(user))) {
                    subscrFlag.set(Boolean.TRUE);
                }
                subscriberLatch.countDown();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        Future<?> fut = subscriber.submit(() -> {
            latch.countDown();
            jedis.subscribe(pubsub, RedisNotifierClient.CALLBACK_CHANNEL);

        });
        latch.await();
        return new Subscription(fut, pubsub);

    }

    @AllArgsConstructor
    public static class Subscription implements AutoCloseable {
        private Future<?> future;
        private JedisPubSub pubSub;

        @Override
        public void close() throws Exception {
             this.pubSub.unsubscribe(RedisNotifierClient.CALLBACK_CHANNEL);
            this.future.cancel(true);
        }
    }
}

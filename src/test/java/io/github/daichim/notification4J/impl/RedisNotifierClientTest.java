package io.github.daichim.notification4J.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMap;
import io.github.daichim.notification4J.ErrorHandler;
import io.github.daichim.notification4J.model.Notification;
import io.github.daichim.notification4J.model.NotificationConfiguration;
import io.github.daichim.notification4J.mybatis.UserDataMapper;
import io.github.daichim.notification4J.utils.TestUtils;
import io.github.daichim.notification4J.utils.Wrapper;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.stubbing.Answer;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.embedded.RedisServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.daichim.notification4J.utils.TestUtils.randomNotification;
import static io.github.daichim.notification4J.utils.TestUtils.randomUsername;
import static io.github.daichim.notification4J.utils.TestUtils.redisUsername;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Slf4j
public class RedisNotifierClientTest {

    public static final int MAX_ATTEMPTS = 3;
    public static final int MAX_DELAY = 10_000;

    @Spy
    NotificationConfiguration configuration;
    @Mock
    UserDataMapper userDataMapper;
    @Mock
    JedisFactory jedisFactory;
    @Mock
    Scheduler quartzScheduler;
    @Mock
    NotificationSerDe serde;
    @Spy
    ObjectMapper objectMapper;

    @InjectMocks
    RedisNotifierClient client;

    private RedisServer redisServer;
    private Jedis jedis;

    @SneakyThrows
    private void initRedis() {
        this.redisServer = RedisServer.builder()
            .port(ThreadLocalRandom.current().nextInt(49152, 65535))
            .build();
        this.redisServer.start();
        log.info("Mock redis server started at {}:{}",
            "localhost", redisServer.ports().get(0));
        this.configuration = new NotificationConfiguration()
            .setRedisHost("localhost")
            .setRedisPort(redisServer.ports().get(0))
            .setRedisDatabase(0)
            .setRedisConnectionTimeout(1000)
            .setIdleRedisConnections(3)
            .setMaxRedisConnections(6)
            .setBackOffDelayMillis(1000)
            .setMaxBackOffDelayMillis(MAX_DELAY)
            .setMaxAttempts(MAX_ATTEMPTS)
            .setUseSsl(false)
            .setDefaultExpiry(Duration.ofDays(7))
            .setDefaultSource("TEST")
            .setDefaultSeverity(Notification.Severity.INFO);
    }

    @SneakyThrows
    public void initMocks() {
        doNothing().when(quartzScheduler).start();
        doReturn(new Date()).when(quartzScheduler)
            .scheduleJob(any(JobDetail.class), any(Trigger.class));
        doAnswer((Answer<Optional<String>>) inv -> {
            Notification n = inv.getArgument(0);
            return Optional.ofNullable(objectMapper.writeValueAsString(n));
        }).when(serde).safeSerialize(any(Notification.class));
        doAnswer((Answer<Optional<Notification>>) inv -> {
            String json = inv.getArgument(0);
            ObjectReader reader = objectMapper.readerFor(Notification.class);
            return Optional.ofNullable(reader.readValue(json));
        }).when(serde).safeDeserialize(anyString());
    }

    @BeforeClass
    @SneakyThrows
    private void initializeMocks() {
        initRedis();
        this.objectMapper = new ObjectMapper();
        RedisServer redisServer = this.redisServer;
        this.jedis = new Jedis("localhost", redisServer.ports().get(0));
        MockitoAnnotations.initMocks(this);
        this.initMocks();
        client.init();
    }

    private ErrorHandler exceptionHandler(Wrapper<Boolean> exFlag) {
        return ex -> {
            log.error("Exception faced", ex);
            exFlag.set(true);
        };
    }

    private void verify(Map<Function<Jedis, Object>, Object> cmds) {
        for (Map.Entry<Function<Jedis, Object>, Object> v : cmds.entrySet()) {
            Object res = v.getKey().apply(jedis);
            assertEquals(res, v.getValue(), "Mismatch in redis verification");
        }
    }

    private Subscription createSubscribers(String[] users,
                                           Wrapper<Boolean> subscrFlag,
                                           CountDownLatch subscriberLatch) throws Exception {
        JedisPubSub pubsub = new JedisPubSub() {
            @SneakyThrows
            @Override
            public void onMessage(String channel, String message) {
                log.debug("Callback received: {} -> {}", channel, message);
                log.debug("Users to check: {}", Arrays.toString(users));
                String[] recdUsers = objectMapper.readerFor(String[].class).readValue(message);
                if (channel.equals(RedisNotifierClient.CALLBACK_CHANNEL)) {
                    subscrFlag.set(true);
                    for (String u : users) {
                        if (!ArrayUtils.contains(recdUsers, redisUsername(u))) {
                            subscrFlag.set(false);
                        }
                    }
                }
                subscriberLatch.countDown();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        final Jedis jedis = new Jedis("localhost", redisServer.ports().get(0));
        Future<?> fut = ForkJoinPool.commonPool().submit(() -> {
            latch.countDown();
            jedis.subscribe(pubsub, RedisNotifierClient.CALLBACK_CHANNEL);
        });
        latch.await(3000, TimeUnit.MILLISECONDS);
        return new Subscription(fut, pubsub, jedis);
    }

    private Subscription createSubscribers(String user,
                                           Wrapper<Boolean> subscrFlag,
                                           CountDownLatch subscriberLatch) throws Exception {
        return createSubscribers(new String[]{user}, subscrFlag, subscriberLatch);
    }


    @Test
    public void testNotifyUsers_Success() throws Exception {
        Notification notification = randomNotification();
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subscrLatch = new CountDownLatch(1);


        try (Jedis testJedis = new Jedis("localhost", redisServer.ports().get(0));
             Subscription subscription = createSubscribers(user, subscrFlag, subscrLatch)) {
            doReturn(testJedis).when(jedisFactory).get();

            client.notifyUsers(exceptionHandler(exFlag), notification, user);
            subscrLatch.await(1000, TimeUnit.MILLISECONDS);

            assertNotNull(notification.getId());
            String notfnJson = objectMapper.writeValueAsString(notification);
            verify(new HashMap<Function<Jedis, Object>, Object>() {{
                put(jedis -> jedis.hget(redisUsername(user), notification.getId()), notfnJson);
            }});

            assertFalse(exFlag.get());
            assertTrue(subscrFlag.get());
        }
    }

    @Test
    public void testNotifyUsers_SuccessAfterRetry() throws Exception {
        Notification notification = randomNotification();
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subscrLatch = new CountDownLatch(1);

        try (Jedis testJedis = new Jedis("localhost", redisServer.ports().get(0));
             Subscription subscription = createSubscribers(user, subscrFlag, subscrLatch)) {
            doThrow(JedisConnectionException.class).doReturn(testJedis).when(jedisFactory).get();

            client.notifyUsers(exceptionHandler(exFlag), notification, user);
            subscrLatch.await(1000, TimeUnit.MILLISECONDS);

            assertNotNull(notification.getId());
            String notfnJson = objectMapper.writeValueAsString(notification);
            verify(new HashMap<Function<Jedis, Object>, Object>() {{
                put(jedis -> jedis.hget(redisUsername(user), notification.getId()), notfnJson);
            }});

            assertFalse(exFlag.get());
            assertTrue(subscrFlag.get());
        }
    }

    @Test
    public void testNotifyUsers_FailureAfterRetry() throws Exception {
        Notification notification = randomNotification();
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(Boolean.FALSE);
        doThrow(JedisConnectionException.class).when(jedisFactory).get();

        client.notifyUsers(exceptionHandler(exFlag), notification, user);

        assertNull(notification.getId());
        assertTrue(exFlag.get());
        assertFalse(subscrFlag.get());
    }

    @Test
    public void testNotifyUserGroups() throws Exception {
        Notification notification = randomNotification();
        List<String> users = IntStream.range(0, 10)
            .mapToObj(i -> randomUsername())
            .collect(Collectors.toList());
        String group = "group";
        String query = "User Query";
        Map<String, Object> paramMap = ImmutableMap.<String, Object>builder()
            .put("foo", "bar")
            .build();
        Wrapper<Boolean> exFlag = new Wrapper<>(false);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(false);
        CountDownLatch subscrLatch = new CountDownLatch(1);

        try (Jedis testJedis = new Jedis("localhost", redisServer.ports().get(0));
             Subscription subscription =
                 createSubscribers(users.toArray(new String[0]), subscrFlag, subscrLatch)) {

            doReturn(query).when(userDataMapper).getUserGroupQuery(eq(group));
            doReturn(users).when(userDataMapper).getUsers(eq(query), eq(paramMap));
            doReturn(testJedis).when(jedisFactory).get();

            client.notifyGroup(exceptionHandler(exFlag), notification, group, paramMap);
            subscrLatch.await(1000, TimeUnit.MILLISECONDS);

            String notfnJson = objectMapper.writeValueAsString(notification);
            Map<Function<Jedis, Object>, Object> verificationMap = new HashMap<>();
            for (String u : users) {
                verificationMap.put(jedis -> jedis.hget(redisUsername(u), notification.getId()),
                    notfnJson);
            }
            verify(verificationMap);
            assertFalse(exFlag.get());
            assertTrue(subscrFlag.get());
        }
    }

    @Test
    public void testUpdateStatus_Success() throws Exception {
        Notification notification = randomNotification();
        notification.setId(String.valueOf(jedis.incr(RedisNotifierClient.ID_KEY)));
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subscrLatch = new CountDownLatch(1);

        try (Jedis testJedis = new Jedis("localhost", redisServer.ports().get(0));
             Subscription subscription = createSubscribers(user, subscrFlag, subscrLatch)) {
            doReturn(testJedis).when(jedisFactory).get();
            jedis.hset(redisUsername(user), notification.getId(),
                objectMapper.writeValueAsString(notification));

            client.updateStatus(exceptionHandler(exFlag),
                new String[]{notification.getId()},
                user, Notification.Status.ACKNOWLEDGED);
            subscrLatch.await(1000, TimeUnit.MILLISECONDS);

            Notification expect = TestUtils.clone(notification)
                .setStatus(Notification.Status.ACKNOWLEDGED);
            String expectJson = objectMapper.writeValueAsString(expect);
            verify(new HashMap<Function<Jedis, Object>, Object>() {{
                put(jedis -> jedis.hget(redisUsername(user), notification.getId()), expectJson);
            }});
            assertFalse(exFlag.get());
            assertTrue(subscrFlag.get());
        }
    }

    @Test
    public void testUpdateStatus_SuccessAfterRetry() throws Exception {
        Notification notification = randomNotification();
        notification.setId(String.valueOf(jedis.incr(RedisNotifierClient.ID_KEY)));
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subLatch = new CountDownLatch(1);

        try (Jedis testJedis = new Jedis("localhost", redisServer.ports().get(0));
             Subscription subscription = createSubscribers(user, subFlag, subLatch)) {
            doThrow(JedisConnectionException.class).doReturn(testJedis).when(jedisFactory).get();
            jedis.hset(redisUsername(user), notification.getId(),
                objectMapper.writeValueAsString(notification));

            client.updateStatus(exceptionHandler(exFlag),
                new String[]{notification.getId()},
                user, Notification.Status.ACKNOWLEDGED);
            subLatch.await(1000, TimeUnit.MILLISECONDS);

            Notification expect = TestUtils.clone(notification)
                .setStatus(Notification.Status.ACKNOWLEDGED);
            String expectJson = objectMapper.writeValueAsString(expect);
            verify(new HashMap<Function<Jedis, Object>, Object>() {{
                put(jedis -> jedis.hget(redisUsername(user), notification.getId()), expectJson);
            }});
            assertFalse(exFlag.get());
            assertTrue(subFlag.get());
        }
    }

    @Test
    public void testUpdateStatus_FailureAfterRetry() throws Exception {
        Notification notification = randomNotification();
        notification.setId(String.valueOf(jedis.incr(RedisNotifierClient.ID_KEY)));
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        doThrow(JedisConnectionException.class).when(jedisFactory).get();
        jedis.hset(redisUsername(user), notification.getId(),
            objectMapper.writeValueAsString(notification));

        client.updateStatus(exceptionHandler(exFlag),
            new String[]{notification.getId()},
            user, Notification.Status.ACKNOWLEDGED);


        String expectJson = objectMapper.writeValueAsString(notification);
        verify(new HashMap<Function<Jedis, Object>, Object>() {{
            put(jedis -> jedis.hget(redisUsername(user), notification.getId()), expectJson);
        }});
        assertTrue(exFlag.get());
    }

    @Test
    public void testGetAllNotifications_Success() throws Exception {
        String user = randomUsername();
        List<Notification> expect = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Notification n = randomNotification();
            long id = jedis.incr(RedisNotifierClient.ID_KEY);
            expect.add(n);
            n.setId(String.valueOf(id));
            String json = objectMapper.writeValueAsString(n);
            jedis.hset(redisUsername(user), String.valueOf(id), json);
        }
        Wrapper<Boolean> exFlag = new Wrapper<>(false);

        try (Jedis testJedis = new Jedis("localhost", redisServer.ports().get(0))) {
            doReturn(testJedis).when(jedisFactory).get();
            CompletableFuture<Collection<Notification>> cf =
                client.getNotifications(exceptionHandler(exFlag), user);
            Collection<Notification> returned = cf.get();

            for (Notification x : returned) {
                assertTrue(expect.contains(x));
            }
            assertFalse(exFlag.get());
        }
    }

    @Test
    public void testGetAllNotifications_SuccessAfterRetry() throws Exception {
        String user = randomUsername();
        List<Notification> expect = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Notification n = randomNotification();
            long id = jedis.incr(RedisNotifierClient.ID_KEY);
            expect.add(n);
            n.setId(String.valueOf(id));
            String json = objectMapper.writeValueAsString(n);
            jedis.hset(redisUsername(user), String.valueOf(id), json);
        }
        Wrapper<Boolean> exFlag = new Wrapper<>(false);

        try (Jedis testJedis = new Jedis("localhost", redisServer.ports().get(0))) {
            doThrow(JedisConnectionException.class).doReturn(testJedis).when(jedisFactory).get();
            CompletableFuture<Collection<Notification>> cf =
                client.getNotifications(exceptionHandler(exFlag), user);
            Collection<Notification> returned = cf.get();

            for (Notification x : returned) {
                assertTrue(expect.contains(x));
            }
            assertFalse(exFlag.get());
        }
    }


    @Test
    public void testGetAllNotifications_Failure() throws Exception {
        String user = randomUsername();
        List<Notification> expect = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Notification n = randomNotification();
            long id = jedis.incr(RedisNotifierClient.ID_KEY);
            expect.add(n);
            n.setId(String.valueOf(id));
            String json = objectMapper.writeValueAsString(n);
            jedis.hset(redisUsername(user), String.valueOf(id), json);
        }
        Wrapper<Boolean> exFlag = new Wrapper<>(false);

        doThrow(JedisConnectionException.class).when(jedisFactory).get();
        CompletableFuture<Collection<Notification>> cf =
            client.getNotifications(exceptionHandler(exFlag), user);
        try {
            cf.get();
            fail("Should have failed in get");
        } catch (ExecutionException ex) {
            assertTrue(true);
        }
        assertTrue(exFlag.get());
    }

    @AfterClass
    public void tearDown(ITestContext context) {
        this.jedis.close();
        if (this.redisServer.isActive()) {
            this.redisServer.stop();
        }
        log.info("Mock redis server has been stopped");
    }


    @AllArgsConstructor
    public static class Subscription implements AutoCloseable {
        private Future<?> future;
        private JedisPubSub pubSub;
        private Jedis jedis;

        @Override
        public void close() throws Exception {
            this.pubSub.unsubscribe(RedisNotifierClient.CALLBACK_CHANNEL);
            this.future.cancel(true);
            this.jedis.close();
        }
    }
}

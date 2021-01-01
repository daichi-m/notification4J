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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.daichim.notification4J.utils.TestUtils.randomNotification;
import static io.github.daichim.notification4J.utils.TestUtils.randomUsername;
import static io.github.daichim.notification4J.utils.TestUtils.redisUsername;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
    //    @Mock
//    Jedis jedis;
    @Spy
    ObjectMapper objectMapper;

//    @Inject
//    RedisInitializer redisInitializer;

    @InjectMocks
    RedisNotifierClient client;

//    private RedisServer redisServer;
//    private Jedis jedis;

    private AtomicLong notfnIdGen;

    private void initializeConfiguration() {
        this.configuration = new NotificationConfiguration()
            .setRedisHost("localhost")
            .setRedisPort(6379)
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

    @BeforeClass
    @SneakyThrows
    private void initializeMocks() {
        initializeConfiguration();
        this.objectMapper = new ObjectMapper();
        this.notfnIdGen = new AtomicLong(0L);

        MockitoAnnotations.initMocks(this);
        doNothing().when(quartzScheduler).start();
        doReturn(new Date()).when(quartzScheduler)
            .scheduleJob(any(JobDetail.class), any(Trigger.class));

        // SerDe mocks
        doAnswer((Answer<Optional<String>>) inv -> {
            Notification n = inv.getArgument(0);
            return Optional.ofNullable(objectMapper.writeValueAsString(n));
        }).when(serde).safeSerialize(any(Notification.class));
        doAnswer((Answer<Optional<Notification>>) inv -> {
            String json = inv.getArgument(0);
            ObjectReader reader = objectMapper.readerFor(Notification.class);
            return Optional.ofNullable(reader.readValue(json));
        }).when(serde).safeDeserialize(anyString());

        // PostConstruct
        client.init();
    }


    private ErrorHandler exceptionHandler(AtomicBoolean exFlag) {
        return ex -> {
            log.error("Exception faced", ex);
            exFlag.set(true);
        };
    }

    /*
    private void verify(Map<Function<Jedis, Object>, Object> cmds) {
        for (Map.Entry<Function<Jedis, Object>, Object> v : cmds.entrySet()) {
            Object res = v.getKey().apply(jedis);
            assertEquals(res, v.getValue(), "Mismatch in redis verification");
        }
    }*/

    /*
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
    }*/

    private <T> T deserCallbackJson(String json, Class<T> clazz) {
        try {
            T users = objectMapper.readerFor(clazz).readValue(json);
            return users;
        } catch (Exception ex) {
            log.warn("Could not deserialize callback json {}", json);
            return null;
        }
    }

    private <T> String serialize(T data) {
        try {
            String json = objectMapper.writeValueAsString(data);
            return json;
        } catch (Exception ex) {
            return "";
        }
    }

    @DataProvider(name = "retryProvider", parallel = false)
    public Object[][] retryMatrix() {
        return new Object[][]{
            {false},
            {true}
        };

    }

    @Test(dataProvider = "retryProvider")
    public void testNotifyUsersSuccess(boolean doRetry) throws Exception {
        Notification notification = randomNotification();
        String user = randomUsername();
        AtomicBoolean exFlag = new AtomicBoolean(false);
        Jedis jedis = mock(Jedis.class);
        if (doRetry) {
            doThrow(JedisConnectionException.class).doReturn(jedis).when(jedisFactory).get();
        } else {
            doReturn(jedis).when(jedisFactory).get();
        }
        doReturn(notfnIdGen.incrementAndGet()).when(jedis).incr(RedisNotifierClient.ID_KEY);
        doReturn(1L).when(jedis).hset(anyString(), anyMap());
        doReturn(1L).when(jedis)
            .publish(eq(RedisNotifierClient.CALLBACK_CHANNEL), anyString());
        doNothing().when(jedis).close();

        CompletableFuture<Boolean> completion =
            client.notifyUsers(exceptionHandler(exFlag), notification, user);
        completion.get();

        assertNotNull(notification.getId());
        String notfnJson = objectMapper.writeValueAsString(notification);
        String id = notification.getId();
        verify(jedis, atLeastOnce()).incr(RedisNotifierClient.ID_KEY);
        verify(jedis, atLeastOnce())
            .hset(eq(redisUsername(user)), argThat(
                arg -> {
                    String key = id;
                    String json = arg.get(key);
                    return notfnJson.equals(json);
                })
            );
        verify(jedis, atLeastOnce())
            .publish(eq(RedisNotifierClient.CALLBACK_CHANNEL),
                argThat(arg -> {
                    String[] users = deserCallbackJson(arg, String[].class);
                    return ArrayUtils.contains(users, redisUsername(user));
                })
            );
        assertFalse(exFlag.get());
    }


    /*
    @Test
    public void testNotifyUsers_SuccessAfterRetry() throws Exception {
        Notification notification = randomNotification();
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subscrLatch = new CountDownLatch(1);

        doThrow(JedisConnectionException.class).doReturn(jedis).when(jedisFactory).get();
        doReturn(notfnIdGen.incrementAndGet()).when(jedis).incr(RedisNotifierClient.ID_KEY);
        doReturn(1L).when(jedis).hset(anyString(), anyMap());
        doReturn(1L).when(jedis)
            .publish(eq(RedisNotifierClient.CALLBACK_CHANNEL), anyString());
        doNothing().when(jedis).close();

        client.notifyUsers(exceptionHandler(exFlag), notification, user);

        assertNotNull(notification.getId());
        String notfnJson = objectMapper.writeValueAsString(notification);
        String id = notification.getId();
        verify(jedis, atLeastOnce()).incr(RedisNotifierClient.ID_KEY);
        verify(jedis, atLeastOnce()).hset(eq(redisUsername(user)), Mockito.argThat(
            argument -> {
                String key = id;
                String json = argument.get(key);
                return notfnJson.equals(json);
            })
        );
        assertFalse(exFlag.get());
    }
}*/


    @Test(expectedExceptions = ExecutionException.class)
    public void testNotifyUsersFailure()
        throws ExecutionException, InterruptedException {
        Notification notification = randomNotification();
        String user = randomUsername();
        AtomicBoolean exFlag = new AtomicBoolean(false);
        Jedis jedis = mock(Jedis.class);
        doThrow(JedisConnectionException.class).when(jedisFactory).get();


        CompletableFuture<Boolean> completion =
            client.notifyUsers(exceptionHandler(exFlag), notification, user);

        assertNull(notification.getId());
        assertTrue(exFlag.get());
        verify(jedis, never()).publish(eq(RedisNotifierClient.CALLBACK_CHANNEL), anyString());
        completion.get();
    }


    @Test
    public void testNotifyUserGroups() throws Exception {
        Notification notification = randomNotification();
        List<String> users = IntStream.range(0, 5)
            .mapToObj(i -> randomUsername())
            .collect(Collectors.toList());
        String group = "group";
        String query = "User Query";
        Map<String, Object> paramMap = ImmutableMap.<String, Object>builder()
            .put("foo", "bar")
            .build();
        AtomicBoolean exFlag = new AtomicBoolean(false);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(false);

        Jedis jedis = mock(Jedis.class);

        doReturn(query).when(userDataMapper).getUserGroupQuery(eq(group));
        doReturn(users).when(userDataMapper).getUsers(eq(query), eq(paramMap));
        doReturn(jedis).when(jedisFactory).get();
        doReturn(notfnIdGen.incrementAndGet()).when(jedis).incr(RedisNotifierClient.ID_KEY);
        doReturn(1L).when(jedis).hset(anyString(), anyMap());
        doReturn(1L).when(jedis)
            .publish(eq(RedisNotifierClient.CALLBACK_CHANNEL), anyString());
        doNothing().when(jedis).close();

        client.notifyGroup(exceptionHandler(exFlag), notification, group, paramMap);

        assertNotNull(notification.getId());
        String id = notification.getId();
        String notfnJson = objectMapper.writeValueAsString(notification);
        verify(jedis, atLeastOnce()).incr(RedisNotifierClient.ID_KEY);
        for (String user : users) {
            verify(jedis, atLeastOnce())
                .hset(eq(redisUsername(user)), argThat(
                    arg -> {
                        String key = id;
                        String json = arg.get(key);
                        return notfnJson.equals(json);
                    })
                );
            verify(jedis, atLeastOnce())
                .publish(eq(RedisNotifierClient.CALLBACK_CHANNEL),
                    argThat(arg -> {
                        String[] pubUsers = deserCallbackJson(arg, String[].class);
                        return ArrayUtils.contains(pubUsers, redisUsername(user));
                    })
                );
        }
        assertFalse(exFlag.get());
    }


    @Test(dataProvider = "retryProvider")
    public void testUpdateStatusSuccess(boolean retry) throws Exception {
        Jedis jedis = mock(Jedis.class);

        Notification notification = randomNotification();
        String user = randomUsername();
        Long id = notfnIdGen.incrementAndGet();
        notification.setId(String.valueOf(id));
        String notfnJSON = objectMapper.writeValueAsString(notification);
        Notification cloned = TestUtils.clone(notification);
        cloned.setStatus(Notification.Status.ACKNOWLEDGED);
        String clonedJSON = objectMapper.writeValueAsString(cloned);

        AtomicBoolean exFlag = new AtomicBoolean(false);

        if (retry) {
            doThrow(JedisConnectionException.class).doReturn(jedis).when(jedisFactory).get();
        } else {
            doReturn(jedis).when(jedisFactory).get();
        }
        doReturn(Arrays.asList(notfnJSON))
            .when(jedis).hmget(anyString(), eq(String.valueOf(id)));
        doReturn("OK")
            .when(jedis).hmset(anyString(), anyMap());

        CompletableFuture<Boolean> completion =
            client.updateStatus(exceptionHandler(exFlag), new String[]{String.valueOf(id)},
                user, Notification.Status.ACKNOWLEDGED);
        completion.get();

        verify(jedis, atLeastOnce()).hmget(eq(redisUsername(user)), eq(String.valueOf(id)));
        verify(jedis, atLeastOnce()).hmset(eq(redisUsername(user)),
            argThat(arg -> {
                String updatedJson = arg.get(String.valueOf(id));
                return updatedJson.equals(clonedJSON);
            })
        );
        verify(jedis, atLeastOnce()).publish(eq(RedisNotifierClient.CALLBACK_CHANNEL),
            argThat(arg -> {
                String[] pubUsers = deserCallbackJson(arg, String[].class);
                return ArrayUtils.contains(pubUsers, redisUsername(user));
            })
        );
        assertFalse(exFlag.get());
    }

    /*
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
    }*/

    @Test(expectedExceptions = ExecutionException.class)
    public void testUpdateStatusFailure() throws Exception {
        Notification notification = randomNotification();
        Long id = notfnIdGen.incrementAndGet();
        notification.setId(String.valueOf(id));
        String user = randomUsername();
        AtomicBoolean exFlag = new AtomicBoolean(false);
        Jedis jedis = mock(Jedis.class);

        doThrow(JedisConnectionException.class).when(jedisFactory).get();
        jedis.hset(redisUsername(user), notification.getId(),
            objectMapper.writeValueAsString(notification));

        CompletableFuture<Boolean> completion =
            client.updateStatus(exceptionHandler(exFlag), new String[]{String.valueOf(id)},
                user, Notification.Status.ACKNOWLEDGED);
        verify(jedis, never()).publish(eq(RedisNotifierClient.CALLBACK_CHANNEL), anyString());
        assertTrue(exFlag.get());
        completion.get();
    }


    @Test(dataProvider = "retryProvider")
    public void testGetAllNotificationsSuccess(boolean retry) throws Exception {
        String user = randomUsername();
        List<Notification> expected = IntStream.range(0, 10)
            .mapToObj(i -> randomNotification())
            .peek(n -> n.setId(String.valueOf(notfnIdGen.incrementAndGet())))
            .collect(Collectors.toList());
        Map<String, String> expectJSON = expected.stream()
            .collect(Collectors.toMap(Notification::getId, this::serialize));
        AtomicBoolean exFlag = new AtomicBoolean(false);
        Jedis jedis = mock(Jedis.class);

        if (retry) {
            doThrow(JedisConnectionException.class).doReturn(jedis).when(jedisFactory).get();
        } else {
            doReturn(jedis).when(jedisFactory).get();
        }
        ScanResult<Map.Entry<String, String>> scanResult = mock(ScanResult.class);
        doReturn("5").doReturn("0").when(scanResult).getCursor();
        List<Map.Entry<String, String>> batch1 = expectJSON.entrySet()
            .stream().limit(5).collect(Collectors.toList());
        List<Map.Entry<String, String>> batch2 = expectJSON.entrySet()
            .stream().filter(x -> !batch1.contains(x)).collect(Collectors.toList());
        doReturn(batch1).doReturn(batch2).when(scanResult).getResult();
        doReturn(scanResult).when(jedis)
            .hscan(eq(redisUsername(user)), anyString(), any(ScanParams.class));

        CompletableFuture<Collection<Notification>> completion =
            client.getNotifications(exceptionHandler(exFlag), user);

        Collection<Notification> result = completion.get();
        for (Notification n : result) {
            assertTrue(expected.contains(n));
        }
        assertFalse(exFlag.get());
    }

    /*
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
        AtomicBoolean exFlag = new AtomicBoolean(false);

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
    }*/


    @Test(expectedExceptions = ExecutionException.class)
    public void testGetAllNotificationsFailure() throws Exception {
        String user = randomUsername();
        AtomicBoolean exFlag = new AtomicBoolean(false);
        doThrow(JedisConnectionException.class).when(jedisFactory).get();
        CompletableFuture<Collection<Notification>> completion =
            client.getNotifications(exceptionHandler(exFlag), user);
        assertTrue(exFlag.get());
        completion.get();
    }
/*
    @AfterClass
    public void tearDown() throws Exception {
        SECONDS.sleep(2);
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
    } */
}

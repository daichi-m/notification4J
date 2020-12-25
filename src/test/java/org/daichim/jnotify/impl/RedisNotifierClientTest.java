package org.daichim.jnotify.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fppt.jedismock.RedisServer;
import lombok.extern.slf4j.Slf4j;
import org.daichim.jnotify.ErrorHandler;
import org.daichim.jnotify.model.Notification;
import org.daichim.jnotify.model.NotificationConfiguration;
import org.daichim.jnotify.mybatis.UserDataMapper;
import org.daichim.jnotify.utils.RedisSubscriber;
import org.daichim.jnotify.utils.RedisSubscriber.Subscription;
import org.daichim.jnotify.utils.RedisVerification;
import org.daichim.jnotify.utils.RedisVerification.Verification;
import org.daichim.jnotify.utils.TestUtils;
import org.daichim.jnotify.utils.Wrapper;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.stubbing.Answer;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.daichim.jnotify.utils.TestUtils.randomNotification;
import static org.daichim.jnotify.utils.TestUtils.randomUsername;
import static org.daichim.jnotify.utils.TestUtils.redisUsername;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
    @Spy
    ObjectMapper objectMapper;

    @InjectMocks
    RedisNotifierClient client;


    private RedisServer redisServer;
    private RedisSubscriber redisSubscriber;
    private RedisVerification verifier;
    private Jedis jedis;


    private void initRedis() throws IOException {
        this.redisServer = RedisServer.newRedisServer();
        this.redisServer.start();
        log.info("Mock redis server started at {}:{}",
            redisServer.getHost(), redisServer.getBindPort());
        this.configuration = new NotificationConfiguration()
            .setRedisHost(redisServer.getHost())
            .setRedisPort(redisServer.getBindPort())
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
    private void initializeMocks() throws Exception {
        initRedis();
        this.objectMapper = new ObjectMapper();
        this.jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort());
        this.redisSubscriber =
            new RedisSubscriber(redisServer.getHost(), redisServer.getBindPort());
        this.verifier = new RedisVerification(redisServer.getHost(), redisServer.getBindPort());
        MockitoAnnotations.initMocks(this);
        doNothing().when(quartzScheduler).start();
        doReturn(new Date())
            .when(quartzScheduler).scheduleJob(any(JobDetail.class), any(Trigger.class));
        doAnswer(
            (Answer<Optional<String>>) inv ->
                Optional.ofNullable(objectMapper.writeValueAsString(inv.getArgument(0))))
            .when(serde).safeSerialize(any(Notification.class));
        doAnswer((Answer<Optional<Notification>>) inv ->
            Optional.ofNullable(objectMapper.readerFor(Notification.class)
                .readValue(inv.getArgument(0).toString())))
            .when(serde).safeDeserialize(anyString());
        client.init();
    }

    private ErrorHandler exceptionHandler(Wrapper<Boolean> exFlag) {
        return ex -> {
            log.error("Exception faced", ex);
            exFlag.set(true);
        };
    }


    @Test
    public void testNotifyUsers_Success() throws Exception {
        Notification notification = randomNotification();
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subscrLatch = new CountDownLatch(1);
        Subscription subscription = redisSubscriber.set(user, subscrFlag, subscrLatch);
        doReturn(new Jedis(redisServer.getHost(), redisServer.getBindPort()))
            .when(jedisFactory).get();

        client.notifyUsers(exceptionHandler(exFlag), notification, user);
        subscrLatch.await(100, TimeUnit.MILLISECONDS);

        assertNotNull(notification.getId());
        String notfnJson = objectMapper.writeValueAsString(notification);
        verifier.verify(Arrays.asList(Verification.of(
            jedis -> jedis.hget(redisUsername(user), notification.getId()), notfnJson)
        ));

        assertFalse(exFlag.get());
        assertTrue(subscrFlag.get());
        subscription.close();
    }

    @Test
    public void testNotifyUsers_SuccessAfterRetry() throws Exception {
        Notification notification = randomNotification();
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subscrFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subscrLatch = new CountDownLatch(1);
        Subscription subscription = redisSubscriber.set(user, subscrFlag, subscrLatch);
        doThrow(JedisConnectionException.class).doReturn(jedis)
            .when(jedisFactory).get();

        client.notifyUsers(exceptionHandler(exFlag), notification, user);
        subscrLatch.await(100, TimeUnit.MILLISECONDS);

        assertNotNull(notification.getId());
        String notfnJson = objectMapper.writeValueAsString(notification);
        verifier.verify(Arrays.asList(Verification.of(
            jedis -> jedis.hget(redisUsername(user), notification.getId()), notfnJson)
        ));

        assertFalse(exFlag.get());
        assertTrue(subscrFlag.get());
        subscription.close();
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
    public void testUpdateStatus_Success() throws Exception {
        Notification notification = randomNotification();
        notification.setId(String.valueOf(jedis.incr(RedisNotifierClient.ID_KEY)));
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subLatch = new CountDownLatch(1);
        doReturn(jedis).when(jedisFactory).get();
        Subscription subscription = redisSubscriber.set(user, subFlag, subLatch);
        jedis.hset(redisUsername(user), notification.getId(),
            objectMapper.writeValueAsString(notification));

        client.updateStatus(exceptionHandler(exFlag),
            new String[]{notification.getId()},
            user, Notification.Status.ACKNOWLEDGED);
        subLatch.await(100, TimeUnit.MILLISECONDS);

        Notification expect = TestUtils.clone(notification)
            .setStatus(Notification.Status.ACKNOWLEDGED);
        String expectJson = objectMapper.writeValueAsString(expect);
        verifier.verify(Arrays.asList(Verification.of(
            jedis -> jedis.hget(redisUsername(user), notification.getId()), expectJson)
        ));
        assertFalse(exFlag.get());
        assertTrue(subFlag.get());
        subscription.close();
    }

    @Test
    public void testUpdateStatus_SuccessAfterRetry() throws Exception {
        Notification notification = randomNotification();
        notification.setId(String.valueOf(jedis.incr(RedisNotifierClient.ID_KEY)));
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subLatch = new CountDownLatch(1);
        doThrow(JedisConnectionException.class).doReturn(jedis).when(jedisFactory).get();
        Subscription subscription = redisSubscriber.set(user, subFlag, subLatch);
        jedis.hset(redisUsername(user), notification.getId(),
            objectMapper.writeValueAsString(notification));

        client.updateStatus(exceptionHandler(exFlag),
            new String[]{notification.getId()},
            user, Notification.Status.ACKNOWLEDGED);
        subLatch.await(100, TimeUnit.MILLISECONDS);

        Notification expect = TestUtils.clone(notification)
            .setStatus(Notification.Status.ACKNOWLEDGED);
        String expectJson = objectMapper.writeValueAsString(expect);
        verifier.verify(Arrays.asList(Verification.of(
            jedis -> jedis.hget(redisUsername(user), notification.getId()), expectJson)
        ));
        assertFalse(exFlag.get());
        assertTrue(subFlag.get());
        subscription.close();
    }

    @Test
    public void testUpdateStatus_FailureAfterRetry() throws Exception {
        Notification notification = randomNotification();
        notification.setId(String.valueOf(jedis.incr(RedisNotifierClient.ID_KEY)));
        String user = randomUsername();
        Wrapper<Boolean> exFlag = new Wrapper<>(Boolean.FALSE);
        Wrapper<Boolean> subFlag = new Wrapper<>(Boolean.FALSE);
        CountDownLatch subLatch = new CountDownLatch(1);
        doThrow(JedisConnectionException.class).when(jedisFactory).get();
        jedis.hset(redisUsername(user), notification.getId(),
            objectMapper.writeValueAsString(notification));

        client.updateStatus(exceptionHandler(exFlag),
            new String[]{notification.getId()},
            user, Notification.Status.ACKNOWLEDGED);
        subLatch.await(100, TimeUnit.MILLISECONDS);


        String expectJson = objectMapper.writeValueAsString(notification);
        verifier.verify(Arrays.asList(Verification.of(
            jedis -> jedis.hget(redisUsername(user), notification.getId()), expectJson)
        ));
        assertTrue(exFlag.get());
        assertFalse(subFlag.get());
    }

    @AfterClass
    public void tearDown() {
        this.redisServer.stop();
        log.info("Mock redis server has been stopped");
    }


}

package io.github.daichim.notification4J.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.github.daichim.notification4J.model.Notification;
import io.github.daichim.notification4J.utils.TestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.embedded.RedisServer;

import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Slf4j
public class NotificationPurgeJobTest {

    @Mock
    JedisFactory jedisFactory;
    @Mock
    RedisLock redisLock;
    @Mock
    NotificationSerDe serde;
    @InjectMocks
    NotificationPurgeJob purgeJob;

    private RedisServer redisServer;
    private Jedis jedis;
    private ObjectMapper objectMapper;

    @SneakyThrows
    @BeforeClass
    private void initRedis() {
        this.redisServer = RedisServer.builder()
            .port(ThreadLocalRandom.current().nextInt(49152, 65535))
            .build();
        this.redisServer.start();
        log.info("Mock redis server started at {}:{}", "localhost", redisServer.ports().get(0));
        this.jedis = new Jedis("localhost", redisServer.ports().get(0));
        this.objectMapper = new ObjectMapper();

        MockitoAnnotations.initMocks(this);
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


    public void insertNotificationsForUsers( String[] users,
                                             ZonedDateTime expiry,
                                             Notification.Status status)
        throws Exception {

        for (int i = 0; i < 50; i++) {
            Notification n = TestUtils.randomNotification();
            long id = jedis.incr(RedisNotifierClient.ID_KEY);
            n.setStatus(status);
            n.setId(String.valueOf(id));
            Field f = FieldUtils.getDeclaredField(Notification.class, "expiryAt", true);
            f.setAccessible(true);
            f.set(n, expiry);
            String json = objectMapper.writeValueAsString(n);
            jedis.hset(TestUtils.redisUsername(users[i%5]), String.valueOf(id), json);
        }
    }

    @Test
    public void purgeTestExpired() throws Exception {
        String[] users = IntStream.range(0, 5)
            .mapToObj(i -> TestUtils.randomUsername())
            .toArray(String[]::new);

        JobExecutionContext ctxt = mock(JobExecutionContext.class);
        doReturn(new Date()).when(ctxt).getScheduledFireTime();
        doAnswer((Answer<Void>) inv -> {
            Callable<Void> callable = inv.getArgument(0);
            return callable.call();
        }).when(redisLock).executeUnderLock(any(), anyLong());
        doReturn(jedis).when(jedisFactory).get();
        insertNotificationsForUsers(users,
            ZonedDateTime.now().minus(10, ChronoUnit.MINUTES),
            Notification.Status.ACKNOWLEDGED);
        purgeJob.execute(ctxt);

        for (String u : users) {
            Map<String, String> notfn = jedis.hgetAll(TestUtils.redisUsername(u));
            assertTrue(MapUtils.isEmpty(notfn));
        }
    }

    @Test
    public void purgeTestDeleted() throws Exception {
        String[] users = IntStream.range(0, 5)
            .mapToObj(i -> TestUtils.randomUsername())
            .toArray(String[]::new);

        JobExecutionContext ctxt = mock(JobExecutionContext.class);
        doReturn(new Date()).when(ctxt).getScheduledFireTime();
        doAnswer((Answer<Void>) inv -> {
            Callable<Void> callable = inv.getArgument(0);
            return callable.call();
        }).when(redisLock).executeUnderLock(any(), anyLong());
        doReturn(jedis).when(jedisFactory).get();
        insertNotificationsForUsers(users,
            ZonedDateTime.now().plus(10, ChronoUnit.MINUTES),
            Notification.Status.DELETED);
        purgeJob.execute(ctxt);

        for (String u : users) {
            Map<String, String> notfn = jedis.hgetAll(TestUtils.redisUsername(u));
            assertTrue(MapUtils.isEmpty(notfn));
        }
    }

    @Test
    public void purgeTestNoPurge() throws Exception {
        String[] users = IntStream.range(0, 5)
            .mapToObj(i -> TestUtils.randomUsername())
            .toArray(String[]::new);

        JobExecutionContext ctxt = mock(JobExecutionContext.class);
        doReturn(new Date()).when(ctxt).getScheduledFireTime();
        doAnswer((Answer<Void>) inv -> {
            Callable<Void> callable = inv.getArgument(0);
            return callable.call();
        }).when(redisLock).executeUnderLock(any(), anyLong());
        doReturn(jedis).when(jedisFactory).get();
        insertNotificationsForUsers(users,
            ZonedDateTime.now().plus(10, ChronoUnit.MINUTES),
            Notification.Status.ACKNOWLEDGED);
        purgeJob.execute(ctxt);

        for (String u : users) {
            Map<String, String> notfn = jedis.hgetAll(TestUtils.redisUsername(u));
            assertTrue(MapUtils.isNotEmpty(notfn));
            assertEquals(notfn.size(), 10);
        }
    }

    @Test(expectedExceptions = JobExecutionException.class)
    public void purgeTestFailed() throws Exception {
        JobExecutionContext ctxt = mock(JobExecutionContext.class);
        doReturn(new Date()).when(ctxt).getFireTime();
        doThrow(JedisException.class).when(jedisFactory).get();
        doAnswer((Answer<Void>) inv -> {
            Callable<Void> callable = inv.getArgument(0);
            return callable.call();
        }).when(redisLock).executeUnderLock(any(), anyLong());
        purgeJob.execute(ctxt);
    }

    @AfterClass
    public void terminate() {
        if (redisServer.isActive()) {
            redisServer.stop();
        }
    }
}

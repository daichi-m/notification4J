package io.github.daichim.notification4J.impl;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import io.github.daichim.notification4J.exception.RedisLockException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static io.github.daichim.notification4J.impl.RedisLock.LOCK_KEY;
import static io.github.daichim.notification4J.impl.RedisLock.LOCK_VAL;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Slf4j
public class RedisLockTest {

    @Mock
    JedisFactory jedisFactory;
    @InjectMocks
    RedisLock redisLock;

    private RedisServer redisServer;
    private Jedis jedis;

    @SneakyThrows
    @BeforeClass
    private void initRedis() {
        this.redisServer = RedisServer.builder()
            .port(ThreadLocalRandom.current().nextInt(49152, 65535))
            .build();
        this.redisServer.start();
        log.info("Mock redis server started at {}:{}", "localhost", redisServer.ports().get(0));
        jedis = new Jedis("localhost", redisServer.ports().get(0));

        MockitoAnnotations.initMocks(this);
        redisLock.init();
    }

    private void forceLock() {
        jedis.set(LOCK_KEY, LOCK_VAL);
    }

    private void forceUnlock() {
        jedis.del(LOCK_KEY);
    }

    private void assertUnlocked() {
        String val = jedis.get(LOCK_KEY);
        assertTrue(StringUtils.isEmpty(val));
    }

    @Test
    public void tryLockSuccessTest() {
        try {
            doReturn(jedis).when(jedisFactory).get();
            boolean locked = redisLock.tryLock(1000);
            assertTrue(locked);
            String value = jedis.get(LOCK_KEY);
            assertEquals(LOCK_VAL, value);
        } finally {
            forceUnlock();
        }
    }

    @Test
    public void tryLockFailedTest() {
        try {
            forceLock();
            doReturn(jedis).when(jedisFactory).get();
            boolean locked = redisLock.tryLock(1000);
            assertFalse(locked);
        } finally {
            forceUnlock();
        }
    }


    @Test
    public void tryUnlockSuccessTest() {
        try {
            forceLock();
            boolean unlocked = redisLock.tryUnlock();
            assertTrue(unlocked);
        } finally {
            forceUnlock();
        }
    }

    @Test
    public void tryUnlockFailedTest() {
        try {
            boolean unlocked = redisLock.tryUnlock();
            assertFalse(unlocked);
        } finally {
            forceUnlock();
        }
    }

    @Test
    public void testExecuteUnderLockSuccess() throws Exception {
        doReturn(jedis).when(jedisFactory).get();
        String value = "Hello, world";
        Callable<String> callable = () -> value;
        String result = redisLock.executeUnderLock(callable, 1000);
        assertEquals(result, value);
        assertUnlocked();
    }

    @Test(expectedExceptions = RedisLockException.class)
    public void testExecuteUnderLockFailed() throws Exception {
        try {
            doReturn(jedis).when(jedisFactory).get();
            forceLock();
            String value = "Hello, world";
            Callable<String> callable = () -> value;
            String result = redisLock.executeUnderLock(callable, 1000);
            assertNull(result);
        } finally {
            forceUnlock();
        }
    }

    @Test(expectedExceptions = {TimeoutException.class, InterruptedException.class})
    public void testExecuteUnderLockTimeout() throws Exception {
        try {
            doReturn(jedis).when(jedisFactory).get();
            String value = "Hello, world";
            Callable<String> callable = () -> {
                Thread.sleep(2000);
                return value;
            };
            String result = redisLock.executeUnderLock(callable, 1000);
            assertNull(result);
        } finally {
            assertUnlocked();
        }
    }

    @AfterClass
    public void tearDown(ITestContext context) {
        this.jedis.close();
        if (this.redisServer.isActive()) {
            this.redisServer.stop();
        }
        log.info("Mock redis server has been stopped");
    }
}

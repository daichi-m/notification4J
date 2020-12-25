package org.daichim.jnotify.impl;

import org.daichim.jnotify.exception.RedisLockException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.SetParams;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Named
public class RedisLock {

    public static final String LOCK_KEY = "REDIS_LOCK";
    public static final String LOCK_VAL = "locked";
    public static final String REDIS_OK = "OK";

    @Inject
    private JedisFactory jedisFactory;

    private ScheduledExecutorService threadPool;

    @PostConstruct
    public void init() {
        this.threadPool = Executors.newScheduledThreadPool(10);
    }

    /**
     * Try acquiring the lock in Redis with a given timeout for the locks expiry.
     *
     * @param timeout The expiry of the lock in milliseconds
     *
     * @return {@literal true} is the lock was acquired, {@literal false} otherwise.
     *
     * @throws JedisException In case of issues with Redis.
     */
    public boolean tryLock(long timeout) throws JedisException {
        try (Jedis jedis = jedisFactory.get()) {
            SetParams params = new SetParams().nx().px(timeout);
            String res = jedis.set(LOCK_KEY, LOCK_VAL, params);
            if (!res.equals(REDIS_OK)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Try releasing the lock in Redis.
     *
     * @return {@literal true} is the lock was released successfully, {@literal false} otherwise.
     *
     * @throws JedisException In case of issues with Redis.
     */
    public boolean tryUnlock() throws JedisException {
        try (Jedis jedis = jedisFactory.get()) {
            String res = jedis.get(LOCK_KEY);
            if (!res.equals(LOCK_VAL)) {
                return false;
            }
            jedis.del(LOCK_KEY);
        }
        return true;
    }

    /**
     * Execute a given {@link Callable} task under a lock acquired in Redis. If the task is not
     * completed within the specified timeout, it is force fully interrupted and the task would
     * receive an {@link InterruptedException} which can be handled for cleanup.
     *
     * @param task    The {@link Callable} task to run under the redis lock.
     * @param timeout The timeout in milliseconds for the task to complete.
     * @param <V>     The result of the {@link Callable}
     *
     * @return The result of the {@link Callable}
     *
     * @throws RedisLockException   In case there was an issue with acquiring or releasing the Redis
     *                              lock.
     * @throws ExecutionException   In case of issues in the {@link Callable} code.
     * @throws InterruptedException If the {@link Callable} task got interrupted before it could
     *                              complete it's execution.
     */
    public <V> V executeUnderLock(Callable<V> task, long timeout)
        throws RedisLockException, ExecutionException, InterruptedException {
        boolean lock = tryLock(timeout);
        if (!lock) {
            throw new RedisLockException("Could not acquire redis lock");
        }
        Future<V> future = threadPool.submit(task);
        threadPool.schedule(() -> {
            if (!future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }, timeout, TimeUnit.MILLISECONDS);
        V res = future.get();
        boolean unlock = tryUnlock();
        if (!unlock) {
            throw new RedisLockException("Could not release redis lock");
        }
        return res;
    }

}

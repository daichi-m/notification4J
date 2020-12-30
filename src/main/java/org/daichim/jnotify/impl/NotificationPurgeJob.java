package org.daichim.jnotify.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.daichim.jnotify.model.Notification;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import javax.inject.Inject;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class NotificationPurgeJob implements Job {

    public static final int SCAN_BATCH_SIZE = 100;
    public static final String ID_KEY = "LAST_NOTIFICATION_ID";
    public static final String CALLBACK_CHANNEL = "KEYS_CHANGED";
    public static final String NOTIFICATION_PREFIX = "NOTFN_";
    public static final String NIL = "nil";

    @Inject
    private JedisFactory jedisFactory;

    @Inject
    private NotificationSerDe serde;

    @Inject
    private RedisLock redisLock;

    /**
     * Utility method to cleanup all expired notifications from Redis. This can be run under a cron
     * job from a Java service.
     */
    public void purgeNotifications() {

        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams params = new ScanParams()
            .count(SCAN_BATCH_SIZE)
            .match(NOTIFICATION_PREFIX + "*");

        ScanResult<String> scanResult;
        List<String> notificationKeys;
        try (Jedis jedis = jedisFactory.get()) {
            do {
                scanResult = jedis.scan(cursor, params);
                notificationKeys = scanResult.getResult();
                cursor = scanResult.getCursor();
                log.debug("Cleanup of notification for cursor: {} -> {} keys",
                    cursor, notificationKeys.size());

                notificationKeys.forEach(k -> {
                    Map<String, String> allNotfn = jedis.hgetAll(k);
                    String[] idsToExpire = allNotfn.values().stream()
                        .map(serde::safeDeserialize)
                        .filter(notfn -> notfn.isPresent() && shouldDelete(notfn.get()))
                        .map(notfn -> notfn.get().getId())
                        .toArray(String[]::new);

                    long delCount = 0;
                    if (ArrayUtils.isNotEmpty(idsToExpire)) {
                        delCount = jedis.hdel(k, idsToExpire);
                    }
                    log.debug("Cleanup {} notification for key: {}", delCount, k);
                });
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        }
    }

    private boolean shouldDelete(Notification notification) {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        return notification.getExpiryAt().compareTo(now) <= 0
            || notification.getStatus() == Notification.Status.DELETED;
    }


    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Date fire = context.getFireTime();
        log.debug("Purge job fired at {}", fire);
        try {
            Callable<Void> purger = () -> {
                purgeNotifications();
                return null;
            };
            long timeout = MILLISECONDS.convert(10, SECONDS);
            redisLock.executeUnderLock(purger, timeout);
            log.debug("Notifications has been purged, finishing job");
        } catch (Exception ex) {
            log.warn("Exception while purging notifications, will try again in next run: {}",
                ex.getMessage());
            throw new JobExecutionException(ex);
        }
    }
}

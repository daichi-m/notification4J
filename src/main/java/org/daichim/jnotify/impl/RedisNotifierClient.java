package org.daichim.jnotify.impl;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.vavr.CheckedRunnable;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.daichim.jnotify.ErrorHandler;
import org.daichim.jnotify.NotificationGroupClient;
import org.daichim.jnotify.NotifierClient;
import org.daichim.jnotify.exception.NotificationException;
import org.daichim.jnotify.model.Notification;
import org.daichim.jnotify.model.NotificationConfiguration;
import org.daichim.jnotify.mybatis.UserDataMapper;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * An instance of {@link NotifierClient} that users Redis as a backing store.
 * <p>
 * TODO: Add a retry queue in case of {@link JedisConnectionException}
 */
@Slf4j
@Named
public class RedisNotifierClient implements NotifierClient, NotificationGroupClient {

    public static final String ID_KEY = "LAST_NOTIFICATION_ID";
    public static final String CALLBACK_CHANNEL = "KEYS_CHANGED";
    public static final String NOTIFICATION_PREFIX = "NOTFN_";
    public static final String NIL = "nil";

    private ErrorHandler defaultErrorHandler;

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private UserDataMapper userDataMapper;

    @Inject
    private NotificationConfiguration configuration;

    @Inject
    private JedisFactory jedisFactory;

    @Inject
    private Scheduler quartzScheduler;

    @Inject
    private NotificationSerDe serde;

    @PostConstruct
    public void init() throws Exception {
        this.defaultErrorHandler = ex -> log.error("Error in handling request", ex);
        this.quartzScheduler.start();
        this.initializePurgeJob();
    }

    @PreDestroy
    public void cleanup() throws Exception {
        this.quartzScheduler.shutdown(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyUsers(final ErrorHandler onError, final Notification notification,
                            String... users) {

        List<String> redisUserKeys = Arrays.stream(users)
            .map(this::redisSafeUsername)
            .collect(Collectors.toList());
        CheckedRunnable writeRedis = Retry.decorateCheckedRunnable(createRetryFromConfig(),
            () -> writeToRedis(notification, redisUserKeys));
        Try.run(writeRedis).recover(recoveryFunction(onError));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyGroup(ErrorHandler onError, Notification notification, String userGroup,
                            Map<String, Object> queryParams) {

        CheckedRunnable runnable = () -> {
            String queryTemplate = userDataMapper.getUserGroupQuery(userGroup);
            List<String> users = userDataMapper.getUsers(queryTemplate, queryParams);
            List<String> redisUserKeys = users.stream()
                .map(this::redisSafeUsername)
                .collect(Collectors.toList());
            log.debug("User-List that notification will be sent to: {}", users);
            if (!CollectionUtils.isEmpty(users)) {
                writeToRedis(notification, redisUserKeys);
            }
        };

        CheckedRunnable retriedRunnable =
            Retry.decorateCheckedRunnable(createRetryFromConfig(), runnable);
        Try.run(retriedRunnable).recover(recoveryFunction(onError));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateStatus(ErrorHandler onError, String[] notificationIds,
                             String user, Notification.Status updatedStatus) {

        NotificationException wrappedException = null;
        CheckedRunnable runnable =
            () -> updateStatusWithException(notificationIds, user, updatedStatus);
        CheckedRunnable retriedRunnable = Retry.decorateCheckedRunnable(createRetryFromConfig(),
            runnable);
        Try.run(retriedRunnable)
            .recover(recoveryFunction(onError));
    }

    @Override
    public CompletableFuture<Collection<Notification>> getNotifications(
        ErrorHandler onError, String user) {

        CompletableFuture<Collection<Notification>> completion = new CompletableFuture<>();
        Callable<Collection<Notification>> callable = () -> getNotificationsSync(user);
        Callable<Collection<Notification>> retriedCallable = Retry.decorateCallable(
            createRetryFromConfig(), callable);
        Try.ofCallable(retriedCallable)
            .onSuccess(completion::complete)
            .onFailure(completion::completeExceptionally)
            .recover(recoveryFunction(onError));
        return completion;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return this.defaultErrorHandler;
    }

    /**
     * Create a {@link Retry} instance from the {@link NotificationConfiguration}
     */
    private Retry createRetryFromConfig() {
        RetryConfig config = RetryConfig.custom()
            .maxAttempts(configuration.getMaxAttempts())
            .retryExceptions(JedisConnectionException.class, JedisException.class)
            .intervalFunction(IntervalFunction.ofExponentialBackoff(
                configuration.getBackOffDelayMillis(),
                IntervalFunction.DEFAULT_MULTIPLIER,
                configuration.getMaxBackOffDelayMillis()))
            .build();
        return Retry.of("retry", config);
    }

    /**
     * Create a {@link Function} to handle recovery in case of retry errors.
     */
    private <T> Function<? super Throwable, ? extends T> recoveryFunction(ErrorHandler onError) {
        return ex -> {
            NotificationException nex = ex instanceof NotificationException
                ? (NotificationException) ex
                : new NotificationException(ex);
            onError.accept(nex);
            return null;
        };
    }

    /**
     * Write a notification to Redis for the give list of users.
     *
     * @param notification The {@link Notification} object to convert to JSON
     * @param users        The list of users to which this notification is to be sent.
     *
     * @throws JedisException In case of issues in connecting to Redis
     * @throws Exception      In case of unexpected failures.
     */
    private void writeToRedis(Notification notification, List<String> users)
        throws JedisException, Exception {

        try (Jedis jedis = jedisFactory.get()) {
            Long val = jedis.incr(ID_KEY);
            notification.setId(Long.toString(val));
            setDefaults(notification);

            Optional<String> json = serde.safeSerialize(notification);
            if (!json.isPresent()) {
                throw new NotificationException("Failed to convert to JSON");
            }

            users.forEach(u -> {
                jedis.hset(u, ImmutableMap.<String, String>builder()
                    .put(notification.getId(), json.get())
                    .build()
                );
                log.debug("Inserted to redis {} -> {}", u, json);
            });
            String callbackJson = objectMapper.writeValueAsString(users);
            jedis.publish(CALLBACK_CHANNEL, callbackJson);
            log.debug("Inserted callback into Redis: {} -> {}", CALLBACK_CHANNEL, callbackJson);
            log.info("Notified {} users of event {}", users.size(), json);
        }
    }

    /**
     * Update the status of the given notificationIds for the corresponding user to status
     * provided.
     *
     * @param notificationIds The list of notificationIds to change the status
     * @param user            The user for whose notifications should be changed
     * @param status          The updated status of the notifications.
     *
     * @throws NotificationException In case of issue with the notifications.
     * @throws JedisException        If there is any issue with Redis.
     */
    private void updateStatusWithException(String[] notificationIds, String user,
                                           Notification.Status status)
        throws NotificationException, JedisException {
        try (Jedis jedis = jedisFactory.get()) {
            String redisUser = redisSafeUsername(user);
            List<String> notfnJSONs = jedis.hmget(redisUser, notificationIds);
            if (CollectionUtils.isEmpty(notfnJSONs)) {
                return;
            }

            Map<String, String> updatedMap = new HashMap<>();
            for (String json : notfnJSONs) {
                if (StringUtils.isEmpty(json) || json.equals(NIL)) {
                    return;
                }
                Optional<Notification> notification = serde.safeDeserialize(json);
                Optional<String> updatedJson = notification.map(n -> {
                    Notification.Status prevStatus = n.getStatus();
                    n.setStatus(status);
                    Optional<String> tmp = serde.safeSerialize(n);
                    log.debug("Update status for {} from {} to {}", n.getId(), prevStatus, status);
                    return tmp.orElse(null);
                });
                if (notification.isPresent() && updatedJson.isPresent()) {
                    updatedMap.put(notification.get().getId(), updatedJson.get());
                }
            }
            jedis.hmset(redisUser, updatedMap);
            String callbackJson =
                objectMapper.writeValueAsString(Collections.singletonList(redisUser));
            jedis.publish(CALLBACK_CHANNEL, callbackJson);
        } catch (IOException ex) {
            throw new NotificationException("Unable to update notification status", ex);
        } catch (JedisException | NotificationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new NotificationException("Unexpected error in updating notification", ex);
        }
    }

    /**
     * Gets the list of notifications for a userId. This method synchronously gets the notifications
     * from Redis.
     *
     * @param userId The userId for which notifications are retrieved.
     *
     * @return The list of notifications for the userId which are not deleted or expired.
     *
     * @throws JedisException        In case of issues with Redis.
     * @throws NotificationException In case of issues with notifications serialization or
     *                               deserialization.
     */
    private Collection<Notification> getNotificationsSync(String userId)
        throws JedisException, NotificationException {

        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams batchSize = new ScanParams().count(100);
        List<Notification> notifications = new ArrayList<>();
        try (Jedis jedis = jedisFactory.get()) {
            String redisKey = redisSafeUsername(userId);
            do {
                ScanResult<Entry<String, String>> result =
                    jedis.hscan(redisKey, cursor, batchSize);
                cursor = result.getCursor();
                List<Entry<String, String>> resultList = result.getResult();
                notifications.addAll(resultList.stream()
                    .map(e -> serde.safeDeserialize(e.getValue()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .filter(n -> (!n.isExpired() && !n.isDeleted()))
                    .collect(Collectors.toList()));
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            return notifications;
        } catch (JedisException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new NotificationException(
                "Unexpected exception while retrieving notifications", ex);
        }
    }

    private void initializePurgeJob() throws SchedulerException {
        JobDetail purgeJob = JobBuilder.newJob()
            .withIdentity("notification-purge")
            .withDescription("Scheduled job to purge expired and deleted notifications from store")
            .ofType(NotificationPurgeJob.class)
            .requestRecovery(false)
            .build();
        Trigger purgeTrigger = TriggerBuilder.newTrigger()
            .forJob(purgeJob)
            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withIntervalInSeconds(configuration.getPurgeIntervalSeconds()).repeatForever())
            .startNow()
            .build();
        this.quartzScheduler.scheduleJob(purgeJob, purgeTrigger);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createGroup(String userGroup, String query, String creator)
        throws NotificationException {
        userDataMapper.createUserGroup(userGroup, query, creator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void editGroup(String oldGroupName, String userGroup, String query, String editor)
        throws NotificationException {
        userDataMapper.updateUserGroup(oldGroupName, userGroup, query, editor);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteGroup(String userGroup, String deleter) throws NotificationException {
        userDataMapper.deleteUserGroup(userGroup, deleter);
    }

    /**
     * Replace all non alphanumeric characters with "_" and prepends the notification prefix
     */
    private String redisSafeUsername(String username) {
        return NOTIFICATION_PREFIX + username.replaceAll("\\W", "_");
    }

    private void setDefaults(final Notification notification) {
        if (notification.getSeverity() == null) {
            notification.setSeverity(configuration.getDefaultSeverity());
        }
        if (StringUtils.isBlank(notification.getSource())) {
            notification.setSource(configuration.getDefaultSource());
        }
        if (notification.getCreatedAt() == null) {
            notification.createdAt(ZonedDateTime.now(ZoneOffset.UTC));
        }
        if (notification.getExpiryAt() == null) {
            notification.expireAfter(configuration.getDefaultExpiry());
        }
    }
}

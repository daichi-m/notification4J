package org.daichim.jnotify.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import org.daichim.jnotify.model.Notification;
import org.daichim.jnotify.model.Notification.Severity;
import org.daichim.jnotify.model.Notification.Status;
import org.daichim.jnotify.model.NotificationConfiguration;
import org.daichim.jnotify.mybatis.UserDataMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Slf4j
public class RedisNotifierClientTest {

    @InjectMocks
    private RedisNotifierClient notifierClient;

    @Mock
    private JedisPool jedisPool;

    @Mock
    private UserDataMapper userDataMapper;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @Spy
    private NotificationConfiguration configuration;

    @Mock
    private JedisProducer jedisProducer;

    private Lorem lorem = LoremIpsum.getInstance();

    private Map<String, Map<String, Notification>> redisNotificationMap;

    private List<String> redisChannel;

    private AtomicLong redisKeyId;

    @BeforeClass
    @SneakyThrows
    public void setup() {
        MockitoAnnotations.initMocks(this);
        notifierClient.init();
        redisNotificationMap = Collections.synchronizedMap(new HashMap<>());
        redisChannel = Collections.synchronizedList(new ArrayList<>());
        redisKeyId = new AtomicLong(0L);
        configuration = new NotificationConfiguration()
            .setRedisHost("localhost")
            .setRedisPort(6479)
            .setRedisDatabase(0)
            .setRedisConnectionTimeout(1000)
            .setIdleRedisConnections(1)
            .setMaxRedisConnections(1)
            .setUseSsl(false)
            .setDefaultExpiry(Duration.ofDays(7))
            .setDefaultSource("TEST")
            .setDefaultSeverity(Severity.INFO)
            .setMaxAttempts(3)
            .setBackOffDelayMillis(10)
            .setMaxBackOffDelayMillis(1000);
        doReturn(jedisPool).when(jedisProducer).get();
        FieldUtils.writeField(notifierClient, "jedisPool", this.jedisPool, true);
    }


    @Test
    @SneakyThrows
    public void testSingleNotification() {
        Jedis jedis = mock(Jedis.class);
        doReturn(jedis).when(jedisPool).getResource();
        Notification notification = randomNotification();
        String user = randomUsername();
        String safeUser = redisUsername(user);
        redisInsertMocks(jedis);

        notifierClient.notifyUsers(notification, user);

        assertNotNull(notification.getId());
        assertTrue(redisChannel.contains(safeUser));
        Map<String, Notification> map = redisNotificationMap.get(safeUser);
        assertTrue(map.containsKey(notification.getId()));
        assertEquals(map.get(notification.getId()), notification);
    }

    @SneakyThrows
    @Test
    public void testMultiUserNotifications() {
        Jedis jedis = mock(Jedis.class);
        doReturn(jedis).when(jedisPool).getResource();
        Notification notification = randomNotification();
        String[] users = IntStream.range(0, 5)
            .mapToObj(i -> randomUsername())
            .toArray(String[]::new);
        String[] safeUsers = Arrays.stream(users)
            .map(this::redisUsername)
            .toArray(String[]::new);
        redisInsertMocks(jedis);

        notifierClient.notifyUsers(notification, users);

        assertNotNull(notification.getId());
        for (String u : safeUsers) {
            assertTrue(redisChannel.contains(u));
            Map<String, Notification> map = redisNotificationMap.get(u);
            assertTrue(map.containsKey(notification.getId()));
            assertEquals(map.get(notification.getId()), notification);
        }
    }

    @SneakyThrows
    @Test
    public void testMultiNotifications() {
        Jedis jedis = mock(Jedis.class);
        doReturn(jedis).when(jedisPool).getResource();
        String user = randomUsername();
        String safeUser = redisUsername(user);
        List<Notification> notificationList = IntStream.range(0, 5)
            .mapToObj(i -> randomNotification())
            .collect(Collectors.toList());
        redisInsertMocks(jedis);

        notificationList.forEach(n -> notifierClient.notifyUsers(n, user));

        notificationList.forEach(n -> assertNotNull(n.getId()));
        Map<String, Notification> mappedNotfn = redisNotificationMap.get(safeUser);
        assertTrue(MapUtils.isNotEmpty(mappedNotfn));
        notificationList.forEach(n -> {
            String id = n.getId();
            Notification n2 = mappedNotfn.get(id);
            assertEquals(n, n2);
        });
    }

    @SneakyThrows
    @Test
    public void testUserGroupNotifications() {

        Jedis jedis = mock(Jedis.class);
        doReturn(jedis).when(jedisPool).getResource();
        Notification notification = randomNotification();
        String[] users = IntStream.range(0, 5)
            .mapToObj(i -> randomUsername())
            .toArray(String[]::new);
        String[] safeUsers = Arrays.stream(users)
            .map(this::redisUsername)
            .toArray(String[]::new);
        redisInsertMocks(jedis);
        doReturn("dummyQuery").when(userDataMapper).getUserGroupQuery(anyString());
        doReturn(Arrays.asList(users)).when(userDataMapper).getUsers(anyString(), anyMap());

        notifierClient.notifyGroup(notification, "dummyGroup", new HashMap<>());

        assertNotNull(notification.getId());
        for (String u : safeUsers) {
            assertTrue(redisChannel.contains(u));
            Map<String, Notification> map = redisNotificationMap.get(u);
            assertTrue(map.containsKey(notification.getId()));
            assertEquals(map.get(notification.getId()), notification);
        }
    }

    @Test(dataProvider = "statusProvider")
    public void testUpdateStatus(Status status) {
        Jedis jedis = mock(Jedis.class);
        doReturn(jedis).when(jedisPool).getResource();
        Notification[] notfnList = IntStream.range(0, 5)
            .mapToObj(i -> randomNotification())
            .peek(n -> n.setId(Long.toString(redisKeyId.incrementAndGet())))
            .toArray(Notification[]::new);
        String[] idList = Arrays.stream(notfnList)
            .map(Notification::getId)
            .toArray(String[]::new);

        String user = randomUsername();
        String redisUser = redisUsername(user);

        AtomicBoolean setCalled = new AtomicBoolean(false);
        AtomicBoolean publishCalled = new AtomicBoolean(false);
        Consumer<Notification> onSet = notification -> {
            assertEquals(notification.getStatus(), status);
            setCalled.set(true);
        };
        Consumer<List<String>> onPublish = strings -> {
            assertTrue(strings.contains(redisUser));
            publishCalled.set(true);
        };
        redisInsertMocks(jedis);
        redisGetAndSetMocks(jedis, notfnList, redisUser, onSet, onPublish);

        notifierClient.updateStatus(idList, user, status);

        assertTrue(setCalled.get());
        assertTrue(publishCalled.get());
    }

    @DataProvider(name = "statusProvider")
    public Object[][] getStatuses() {
        return new Object[][]{
            {Status.ACKNOWLEDGED},
            {Status.DELETED}
        };
    }

    private void redisGetAndSetMocks(Jedis jedis, Notification[] notificationList, String userKey,
                                     Consumer<Notification> verifyOnSet,
                                     Consumer<List<String>> verifyOnPublish) {
        Map<String, Notification> notfnMap = Arrays.stream(notificationList)
            .collect(Collectors.toMap(Notification::getId, Function.identity()));
        doAnswer((Answer<String>) inv -> {
            String id = inv.getArgument(1);
            return notfnMap.containsKey(id)
                ? toJson(notfnMap.get(id), Notification.class)
                : null;
        }).when(jedis).hget(eq(userKey), anyString());

        doAnswer((Answer<List<String>>) inv -> {
            String[] idList = inv.getArgument(1);
            return Arrays.stream(idList)
                .map(id -> {
                    return notfnMap.containsKey(id)
                        ? toJson(notfnMap.get(id), Notification.class)
                        : null;
                })
                .collect(Collectors.toList());
        }).when(jedis).hmget(eq(userKey), any(String[].class));

        doAnswer((Answer<Long>) inv -> {
            String json = inv.getArgument(2);
            Notification notfn = toObject(json, Notification.class);
            verifyOnSet.accept(notfn);
            return 1L;
        }).when(jedis).hset(eq(userKey), anyString(), anyString());

        doAnswer((Answer<String>) inv -> {
            Map<String, String> json = inv.getArgument(2);
            json.values().forEach(n -> {
                Notification notfn = toObject(n, Notification.class);
                verifyOnSet.accept(notfn);
            });
            return "OK";
        }).when(jedis).hmset(eq(userKey), anyMap());

        doAnswer((Answer<Long>) inv -> {
            String json = inv.getArgument(1);
            List<String> userList = toObject(json, List.class);
            verifyOnPublish.accept(userList);
            return (long) userList.size();
        }).when(jedis).publish(eq(RedisNotifierClient.CALLBACK_CHANNEL), anyString());

    }

    @SuppressWarnings("unchecked")
    private void redisInsertMocks(Jedis jedis) {

        doAnswer((Answer<Long>) inv -> redisKeyId.incrementAndGet())
            .when(jedis).incr(RedisNotifierClient.ID_KEY);

        doAnswer((Answer<Long>) inv -> {
            String user = inv.getArgument(0, String.class);
            Map<String, String> map = inv.getArgument(1, Map.class);
            Map<String, Notification> map2 = map.entrySet().stream()
                .collect(Collectors.toMap(
                    Entry::getKey, e -> toObject(e.getValue(), Notification.class)
                ));
            if (redisNotificationMap.get(user) != null) {
                redisNotificationMap.get(user).putAll(map2);
            } else {
                redisNotificationMap.put(user, map2);
            }
            return 1L;
        }).when(jedis).hset(anyString(), anyMap());

        doAnswer((Answer<Long>) inv -> {
            String channel = inv.getArgument(0, String.class);
            String message = inv.getArgument(1, String.class);
            List<String> userList = toObject(message, List.class);
            redisChannel.addAll(userList);
            return 1L;
        }).when(jedis).publish(eq(RedisNotifierClient.CALLBACK_CHANNEL), anyString());

    }

    @SneakyThrows
    private <T> T toObject(String json, Class<T> clazz) {
        ObjectReader reader = objectMapper.readerFor(clazz);
        return reader.readValue(json);
    }

    @SneakyThrows
    private <T> String toJson(T object, Class<T> clazz) {
        ObjectWriter writer = objectMapper.writerFor(clazz);
        return writer.writeValueAsString(object);
    }


    private Notification randomNotification() {
        try {
            return new Notification()
                .setMesage(lorem.getWords(5))
                .setDescription(lorem.getWords(8, 10))
                .putRedirectURL("here", new URL("https://www.google.com/"))
                .setSeverity(Severity.INFO)
                .setSource("testservice")
                .setStatus(Status.NOT_ACKNOWLEDGED)
                .expireAfter(Duration.ofMinutes(10));
        } catch (MalformedURLException ex) {
            // Ignore
            throw new RuntimeException();
        }
    }

    private String randomUsername() {
        return "homeoffice\\" + lorem.getFirstName().toLowerCase();
    }

    private String redisUsername(String user) {
        return RedisNotifierClient.NOTIFICATION_PREFIX + user.replaceAll("\\W", "_");
    }

    @Test
    public void notificationJson() {
        Notification nfn = randomNotification();
        nfn.setId(Long.toString(redisKeyId.incrementAndGet()));
        String json = toJson(nfn, Notification.class);
        log.debug("Json: {}", json);
    }

}

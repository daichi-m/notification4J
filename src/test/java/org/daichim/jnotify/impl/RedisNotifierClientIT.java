package org.daichim.jnotify.impl;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.github.fppt.jedismock.RedisServer;
import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import org.daichim.jnotify.model.Notification;
import org.daichim.jnotify.model.Notification.Severity;
import org.daichim.jnotify.model.Notification.Status;
import org.daichim.jnotify.model.NotificationConfiguration;
import org.daichim.jnotify.mybatis.UserDataMapper;
import org.daichim.jnotify.utils.Wrapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RedisNotifierClientIT {

    @InjectMocks
    RedisNotifierClient notifierClient;

    @Spy JedisPool jedisPool;

    @Mock
    UserDataMapper userDataMapper;

    @Spy ObjectMapper objectMapper = new ObjectMapper();

    @Spy NotificationConfiguration configuration;

    private ThreadLocal<ObjectReader> notificationReader = ThreadLocal.withInitial(
        () -> objectMapper.readerFor(Notification.class));

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    private RedisServer redisServer;

    private Lorem lorem = LoremIpsum.getInstance();

    @BeforeClass
    public void setup() throws Exception {
        this.redisServer = new RedisServer();
        redisServer.start();
        System.out.println(redisServer.getHost());
        System.out.println(redisServer.getBindPort());
        configuration = new NotificationConfiguration()
            .setRedisHost(redisServer.getHost())
            .setRedisPort(redisServer.getBindPort())
            .setRedisDatabase(0)
            .setRedisConnectionTimeout(1000)
            .setIdleRedisConnections(3)
            .setMaxRedisConnections(3)
            .setUseSsl(false)
            .setDefaultExpiry(Duration.ofDays(7))
            .setDefaultSource("TEST")
            .setDefaultSeverity(Severity.INFO);
        this.jedisPool = new JedisPool(redisServer.getHost(), redisServer.getBindPort());
        MockitoAnnotations.initMocks(this);
        notifierClient.init();
        FieldUtils.writeField(notifierClient, "jedisPool", this.jedisPool, true);

    }

    @Test
    public void testSingleUserNotification(ITestContext context) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            Notification notification = randomNotification();
            String user = randomUsername();
            final String redisKey = redisUsername(user);


            Wrapper<Boolean> pubsubInvoked = subscribeToPubSub(jedis,
                (channel, message) ->
                    assertPubSubMessage(channel, message, Arrays.asList(redisKey)),
                RedisNotifierClient.CALLBACK_CHANNEL);

            notifierClient.notifyUsers(notification, user);
            Map<String, String> alldata = jedis.hgetAll(redisKey);
            log.debug("Received reply: {}", alldata);
            assertNotification(alldata, notification);
            Assert.assertTrue(pubsubInvoked.get());
            log.info("Test {} Done", context.getCurrentXmlTest().getName());
        } catch (Exception ex) {
            log.error("Exception occured", ex);
            Assert.fail();
        }
    }

    @Test(enabled = true)
    public void testMultiUserNotification(ITestContext context) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            Notification notification = randomNotification();
            List<String> userIds = new ArrayList<>();
            List<String> redisKeys = IntStream.range(0, 5)
                .mapToObj(i -> randomUsername())
                .peek(userIds::add)
                .map(this::redisUsername)
                .collect(Collectors.toList());
            Wrapper<Boolean> pubsubInvoked = subscribeToPubSub(jedis,
                (channel, message) -> {
                    assertPubSubMessage(channel, message, redisKeys);
                }, RedisNotifierClient.CALLBACK_CHANNEL
            );

            notifierClient.notifyUsers(notification, userIds.toArray(new String[0]));

            redisKeys.forEach(key -> {
                Map<String, String> alldata = jedis.hgetAll(key);
                assertNotification(alldata, notification);
            });
            log.debug("Verified notification delivered to {} users", userIds.size());
            log.debug("Test {} Done!!", context.getCurrentXmlTest().getName());
            Assert.assertTrue(pubsubInvoked.get());
        }
    }

    private Wrapper<Boolean> subscribeToPubSub(Jedis jedis,
                                               BiConsumer<String, String> onMessage,
                                               String channel) {
        Wrapper<Boolean> pubsubWrapper = new Wrapper<>(Boolean.TRUE);
        JedisPubSub jedisPubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                onMessage.accept(channel, message);
                pubsubWrapper.set(true);
            }
        };
        subscribeToPubSub(jedis, jedisPubSub, channel);
        return pubsubWrapper;
    }

    private void subscribeToPubSub(Jedis jedis, JedisPubSub jedisPubSub, String channel) {
        threadPool.submit(() -> jedis.subscribe(jedisPubSub, channel));
    }

    @SneakyThrows
    private void assertPubSubMessage(String pubsubChannel, String pubsubMsg,
                                     Collection<String> expectedKeys) {
        Assert.assertEquals(pubsubChannel, RedisNotifierClient.CALLBACK_CHANNEL);
        List<String> actualKeys = objectMapper.readValue(pubsubMsg,
            new TypeReference<List<String>>() {
            });
        for (String key : actualKeys) {
            Assert.assertTrue(expectedKeys.contains(key));
        }
    }

    @SneakyThrows
    private void assertNotification(Map<String, String> allData,
                                    Notification expectedNotification) {

        Assert.assertNotNull(expectedNotification.getId());
        String json = allData.get(expectedNotification.getId());
        Assert.assertNotNull(json);
        Notification actualNotification = notificationReader.get().readValue(json);
        Assert.assertEquals(actualNotification, expectedNotification);
    }


    private Notification randomNotification() {
        try {
            Notification notification = new Notification()
                .setMesage(lorem.getWords(5))
                .setDescription(lorem.getWords(8, 10))
                .putRedirectURL("here", new URL("https://www.google.com/"))
                .setSeverity(Severity.INFO)
                .setStatus(Status.NOT_ACKNOWLEDGED)
                .expireAfter(Duration.ofMinutes(10));
            return notification;
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

    @AfterClass
    public void tearDown() throws Exception {
        redisServer.stop();
        threadPool.shutdownNow();
    }

}

package org.daichim.jnotify.utils;

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.daichim.jnotify.impl.RedisNotifierClient;
import org.daichim.jnotify.model.Notification;

import java.lang.reflect.Field;
import java.net.*;
import java.time.Duration;

public class TestUtils {

    private static final Lorem lorem;

    static {
        lorem = LoremIpsum.getInstance();
    }

    public static Notification randomNotification() {
        try {
            return new Notification()
                .setMesage(lorem.getWords(5))
                .setDescription(lorem.getWords(8, 10))
                .putRedirectURL("here", new URL("https://www.google.com/"))
                .setSeverity(Notification.Severity.INFO)
                .setSource("testservice")
                .setStatus(Notification.Status.NOT_ACKNOWLEDGED)
                .expireAfter(Duration.ofMinutes(10));
        } catch (MalformedURLException ex) {
            // Ignore
            throw new RuntimeException();
        }
    }

    public static Notification clone(Notification other) throws Exception {
        Notification cloned = new Notification()
            .setId(other.getId())
            .setMesage(other.getMesage())
            .setDescription(other.getDescription())
            .setRedirectUrl(other.getRedirectUrl())
            .setSeverity(other.getSeverity())
            .setSource(other.getSource())
            .setStatus(other.getStatus());
        Field exp = FieldUtils.getDeclaredField(
            Notification.class, "expiryAt", true);
        Field cr = FieldUtils.getDeclaredField(
            Notification.class, "createdAt", true);
        exp.setAccessible(true);
        cr.setAccessible(true);
        exp.set(cloned, other.getExpiryAt());
        cr.set(cloned, other.getCreatedAt());
        return  cloned;
    }

    public static String randomUsername() {
        return "homeoffice\\" + lorem.getFirstName().toLowerCase();
    }

    public static String redisUsername(String user) {
        return RedisNotifierClient.NOTIFICATION_PREFIX + user.replaceAll("\\W", "_");
    }

}

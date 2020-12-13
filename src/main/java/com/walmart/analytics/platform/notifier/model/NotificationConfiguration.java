package com.walmart.analytics.platform.notifier.model;

import com.walmart.analytics.platform.notifier.model.Notification.Severity;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Getter
@Setter
@ToString(exclude = "redisPassword")
@Accessors(chain = true)
public class NotificationConfiguration {

    private String redisHost;

    private int redisPort;

    private String redisPassword;

    private int redisDatabase = 0;

    private int redisConnectionTimeout = 30_000;

    private boolean useSsl = false;

    private int maxRedisConnections = 10;

    private int idleRedisConnections = 3;

    private Duration defaultExpiry = Duration.of(7, ChronoUnit.DAYS);

    private Severity defaultSeverity = Severity.INFO;

    private String defaultSource = Notification.SOURCE_UNKNOWN;

    private long backOffDelayMillis = 1000;

    private long maxBackOffDelayMillis = 10_000;

    private int maxAttempts = 5;

}

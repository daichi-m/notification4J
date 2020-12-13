package com.walmart.analytics.platform.notifier.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static java.time.ZonedDateTime.ofInstant;
import static java.time.temporal.ChronoUnit.SECONDS;

@Data
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Notification {

    public static final String SOURCE_UNKNOWN = "UNKNOWN";

    @JsonProperty("id")
    private String id;

    @JsonProperty("message")
    private String mesage;

    @JsonProperty("description")
    private String description;

    @JsonProperty("status")
    private Status status = Status.NEW;

    @JsonProperty("severity")
    private Severity severity;

    @JsonProperty("source")
    private String source;

    @JsonProperty("displayType")
    private DisplayType displayType = DisplayType.INBOX;

    @JsonIgnore
    @Setter(AccessLevel.NONE)
    private ZonedDateTime createdAt = now(UTC).truncatedTo(SECONDS);

    @JsonIgnore
    @Setter(AccessLevel.NONE)
    private ZonedDateTime expiryAt;

    @JsonProperty("redirectUrl")
    private Map<String, URL> redirectUrl;

    @JsonProperty("creationTS")
    public Long getCreationTimestamp() {
        return this.createdAt.toEpochSecond() * 1000L;
    }

    @JsonProperty("creationTS")
    void setCreationTimestamp(long epochMillis) {
        this.createdAt = ofInstant(Instant.ofEpochMilli(epochMillis), UTC);
    }

    @JsonProperty("expiryTS")
    public Long getExpiryTimestamp() {
        return this.expiryAt.toEpochSecond() * 1000L;
    }

    @JsonProperty("expiryTS")
    void setExpiryAt(long epochMillis) {
        this.expiryAt = ofInstant(Instant.ofEpochMilli(epochMillis), UTC);
    }

    @JsonIgnore
    public boolean isExpired() {
        ZonedDateTime now = ZonedDateTime.now(UTC);
        return this.expiryAt.isBefore(now);
    }

    @JsonIgnore
    public Notification createdAt(ZonedDateTime ts) {
        this.createdAt = ts;
        return this;
    }

    @JsonIgnore
    public boolean isDeleted() {
        return this.getStatus() != Status.DELETED;
    }

    public Notification putRedirectURL(String label, URL url) {
        if (this.redirectUrl == null) {
            this.redirectUrl = new HashMap<>();
        }
        this.redirectUrl.put(label, url);
        return this;
    }

    public Notification expireAfter(Duration duration) {
        this.expiryAt = this.createdAt.plus(duration);
        return this;
    }

    public enum Status {
        NEW, NOT_ACKNOWLEDGED, ACKNOWLEDGED, DELETED;
    }

    public enum Severity {
        INFO, SUCCESS, WARNING, ERROR;
    }

    public enum DisplayType {
        INBOX, BANNER_DISMISS, BANNER_NONDISMISS, PAGE, POPUP;
    }


}

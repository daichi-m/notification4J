package com.walmart.analytics.platform.notifier.exception;

public class RedisLockException extends NotificationException {
    public RedisLockException(Throwable cause) {
        super(cause);
    }

    public RedisLockException(String message) {
        super(message);
    }

    public RedisLockException(String message, Throwable cause) {
        super(message, cause);
    }
}

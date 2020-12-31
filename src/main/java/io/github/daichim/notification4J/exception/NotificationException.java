package io.github.daichim.notification4J.exception;

public class NotificationException extends RuntimeException {

    public NotificationException(Throwable cause) {
        super(cause);
    }

    public NotificationException(String message) {
        super(message);
    }

    public NotificationException(String message, Throwable cause) {
        super(message, cause);
    }
}

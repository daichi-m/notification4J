package io.github.daichim.notification4J.exception;

public class NotificationStoreException extends NotificationException {

    public NotificationStoreException(String message) {
        super(message);
    }

    public NotificationStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}

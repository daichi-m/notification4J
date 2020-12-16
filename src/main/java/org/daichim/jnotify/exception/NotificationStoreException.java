package org.daichim.jnotify.exception;

public class NotificationStoreException extends NotificationException {

    public NotificationStoreException(String message) {
        super(message);
    }

    public NotificationStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}

package org.daichim.jnotify.exception;

public class NotificationNotFound extends NotificationException {

    public NotificationNotFound(Throwable cause) {
        super(cause);
    }

    public NotificationNotFound(String message) {
        super(message);
    }

    public NotificationNotFound(String message, Throwable cause) {
        super(message, cause);
    }
}

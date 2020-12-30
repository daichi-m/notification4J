package org.daichim.jnotify;

import org.daichim.jnotify.exception.NotificationException;

import java.util.function.Consumer;

@FunctionalInterface
public interface ErrorHandler extends Consumer<NotificationException> {
}

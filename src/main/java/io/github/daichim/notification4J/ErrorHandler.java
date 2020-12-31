package io.github.daichim.notification4J;

import io.github.daichim.notification4J.exception.NotificationException;

import java.util.function.Consumer;

@FunctionalInterface
public interface ErrorHandler extends Consumer<NotificationException> {
}

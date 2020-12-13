package com.walmart.analytics.platform.notifier;

import com.walmart.analytics.platform.notifier.exception.NotificationException;

import java.util.function.Consumer;

@FunctionalInterface
public interface ErrorHandler extends Consumer<NotificationException> {
}

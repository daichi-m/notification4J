package org.daichim.jnotify;

import org.daichim.jnotify.exception.NotificationException;
import org.daichim.jnotify.model.Notification;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * The primary client for the notifier. Systems using the library needs to create an instance of
 * {@link NotifierClient} to communicate with the notification system. This interface can be
 * implemented for different backend systems.
 */

public interface NotifierClient {

    /**
     * Notify all the users given in the list about the notification. It uses the default {@link
     * ErrorHandler} returned by {@link #getErrorHandler()} to handle any errors faced during
     * publishing the notification.
     *
     * @param notification The notification to send to all users
     * @param users        List of users to send the notification to
     */
    default void notifyUsers(Notification notification, String... users) {
        notifyUsers(getErrorHandler(), notification, users);
    }

    /**
     * Notify all the users given in the list about the notification. If an error occurs during
     * publishing the notification, the {@link ErrorHandler} is invoked with the appropriate {@link
     * NotificationException} for the invoker to cleanup.
     *
     * @param onError      The {@link ErrorHandler} to handle errors during publishing.
     * @param notification The notification to send to all users
     * @param users        List of users to send the notification to
     */
    void notifyUsers(ErrorHandler onError, Notification notification, String... users);

    /**
     * Notify a group of users about the notification. The notification group is used to identify
     * the query. Any error in publishing in handler via the default {@link ErrorHandler} returned
     * by {@link #getErrorHandler()}.
     *
     * @param notification The notification to send
     * @param userGroup    Name of the user group
     * @param queryParams  A {@link Map} of parameters to replace in the query for the user group
     *                     which is retrieved from the database.
     */
    default void notifyGroup(Notification notification, String userGroup,
                             Map<String, Object> queryParams) {
        notifyGroup(getErrorHandler(), notification, userGroup, queryParams);
    }

    /**
     * Notify a group of users about the notification. The notification group is used to identify
     * the query. Any error in publishing is handled by the {@link ErrorHandler} provided.
     *
     * @param onError      The {@link ErrorHandler} to handle errors during publishing.
     * @param notification The notification to send
     * @param userGroup    Name of the user group
     * @param queryParams  A {@link Map} of parameters to replace in the query for the user group
     *                     which is retrieved from the database.
     */
    void notifyGroup(ErrorHandler onError, Notification notification, String userGroup,
                     Map<String, Object> queryParams);

    /**
     * Updates the status of the notification for a user.
     *
     * @param notificationIds The list of id's of the notification to update the status
     * @param user            The user whose notification status is updated
     * @param status          The updated status of the notification
     */
    default void updateStatus(String[] notificationIds, String user, Notification.Status status) {
        updateStatus(getErrorHandler(), notificationIds, user, status);
    }

    /**
     * Updates the status of the notification for a user.
     *
     * @param onError         The {@link ErrorHandler} to handle errors during publishing.
     * @param notificationIds The list of id's of the notification to update the status
     * @param user            The user whose notification status is updated
     * @param updatedStatus   The updated status of the notification
     */
    void updateStatus(ErrorHandler onError, String[] notificationIds, String user,
                      Notification.Status updatedStatus);

    /**
     * Gets the list of all the notifications for the user. Based on implementation detail, this
     * method might not always return proper results.
     *
     * @param user The user whose notifications are required.
     *
     * @return The list of {@link Notification} object for the user which are not deleted and not
     *     expired.
     *
     * @throws UnsupportedOperationException If this method cannot be implemented for a specific
     *                                       backend.
     */
    default CompletionStage<Collection<Notification>> getNotifications(String user)
        throws UnsupportedOperationException {
        return getNotifications(getErrorHandler(), user);
    }

    /**
     * Gets the list of all the notifications for the user. Based on implementation detail, this
     * method might not always return proper results.
     *
     * @param onError The {@link ErrorHandler} to handle errors during retrieving the notifications
     *                for the user.
     * @param user    The user whose notifications are required.
     *
     * @return A {@link CompletionStage} which when complete returns a {@link Collection} of {@link
     *     Notification} for the user which are not expired and not deleted.
     *
     * @throws UnsupportedOperationException If this method cannot be implemented for a specific
     *                                       backend.
     */
    CompletionStage<Collection<Notification>> getNotifications(ErrorHandler onError, String user)
        throws UnsupportedOperationException;


    /**
     * Get the {@link ErrorHandler} for this client, in case of errors in publishing notifications.
     *
     * @return Returns the {@link ErrorHandler} for this client. In case no {@link ErrorHandler} is
     *     specified, it returns {@literal null}.
     */
    ErrorHandler getErrorHandler();

}

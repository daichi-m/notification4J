package io.github.daichim.notification4J;

import io.github.daichim.notification4J.exception.NotificationException;

public interface NotificationGroupClient {

    /**
     * Creates a new user group.
     *
     * @param userGroup The name of the user group
     * @param query     The query template that will be used to figure out the group
     * @param editedBy  The user who is creating this group
     */
    void createGroup(String userGroup, String query, String editedBy)
        throws NotificationException;

    /**
     * Creates a new user group.
     *
     * @param oldGroupName The older group name which to edit
     * @param userGroup    The name of the user group
     * @param query        The query template that will be used to figure out the group
     * @param editedBy     The user who is editing this group
     */
    void editGroup(String oldGroupName, String userGroup, String query, String editedBy)
        throws NotificationException;

    /**
     * Creates a new user group.
     *
     * @param userGroup The name of the user group
     * @param editedBy  The user who is deleting this group
     */
    void deleteGroup(String userGroup, String editedBy) throws NotificationException;

}

package com.cassandraplayground.notifications;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

@Accessor
public interface AllQueries {

  @Query("SELECT * FROM notifications.notifications")
  public Result<Notification> getAllNotifications();

  @Query("SELECT * FROM notifications.notifications WHERE recipient_id = :recipient_id")
  public Result<Notification> getNotifications(@Param("recipient_id") String recipientId);
}
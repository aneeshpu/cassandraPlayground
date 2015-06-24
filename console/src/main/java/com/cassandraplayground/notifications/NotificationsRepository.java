package com.cassandraplayground.notifications;

import com.cassandraplayground.blog.CassandraConnection;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

public class NotificationsRepository {
  private final CassandraConnection connection;

  public NotificationsRepository() {
    connection = new CassandraConnection();
  }

  public void saveNotification(Notification notification) {
    Session session = null;

    try {
      session = connection.getSession();

      Insert insertStatement = QueryBuilder.insertInto("notifications", "notifications")
          .value("recipient_id", notification.getRecipientId())
          .value("type", notification.getNotificationType())
          .value("banner", notification.getBanner())
          .value("created_date", notification.getCreatedDate())
          .value("message", notification.getMessage())
          .value("expiry_date", notification.getExpiryDate())
          .value("correlation_id", notification.getCorrelationId());

      session.execute(insertStatement.using(ttl(notification.lifetime())));

    } finally {
      session.close();
    }
  }
}

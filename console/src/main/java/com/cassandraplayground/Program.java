package com.cassandraplayground;

import com.cassandraplayground.notifications.CassandraConnection;
import com.cassandraplayground.notifications.Notification;
import com.cassandraplayground.notifications.NotificationsRepository;

import java.util.*;

public class Program {
  public static void main(String[] args) {

    saveNotification(randomNotification());

    final CassandraConnection.SessionWrapper wrapper = new CassandraConnection.SessionWrapper();

    try {
      final List<Notification> notifications = printNotifications("frys", wrapper);

      notifications.forEach(notification -> System.out.println(notification));

      final Optional<Notification> optional = notifications
          .stream()
          .filter(notification -> notification.getBanner().equals("frys"))
          .findFirst();

      if(!optional.isPresent()){

        System.out.println("Could not find a frys notification to delete");
        return;
      }
      final Notification notification = optional.get();
      System.out.printf("============= Deleting %s ============", notification);
      wrapper.getMapper(Notification.class).delete(notification);

    } finally {
      wrapper.close();

      //hack to kill all threads.
      System.exit(0);
    }


  }

  private static Notification randomNotification() {

    Map<String,String> message = new HashMap<>();
    message.put("foo", "bar");
    message.put("body", "Save your money!");

    Calendar c = Calendar.getInstance();
    c.setTime(new Date());
    c.add(Calendar.DATE, 30);
    Date expiryDate = c.getTime();

    return new Notification("frys", "expiring_coupon", "frys", UUID.randomUUID(), message, new Date(), expiryDate);
  }

  private static void saveNotification(Notification notification) {
    new NotificationsRepository().saveNotification(notification);
    System.out.println("========== com.cassandraplayground.blog.Notification Saved! =============");
  }

  private static List<Notification> printNotifications(String recipient, CassandraConnection.SessionWrapper wrapper) {

    try {

      final List<Notification> notifications = wrapper
          .getAllQueries()
          .getNotifications(recipient)
          .all();

      return notifications;



    } finally {
/*
      System.out.println("Executing finally");
      wrapper.close();

      //Threads seem to keep the process alive. Not sure how to stop all the worker threads that are spawned.
      //This is a hack
      System.exit(0);
*/
    }
  }
}

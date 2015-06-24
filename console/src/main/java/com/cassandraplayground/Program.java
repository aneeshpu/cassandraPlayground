package com.cassandraplayground;

import com.cassandraplayground.notifications.CassandraConnection;
import com.cassandraplayground.notifications.Notification;
import com.cassandraplayground.notifications.NotificationsRepository;

import java.util.*;

public class Program {
  public static void main(String[] args) {

    try {
      final String firstArg = args[0];

      if (firstArg.equals("-c")) {

        saveNotification(randomNotification("frys", "kroger").toArray(new Notification[randomNotification("frys").size()]));
      } else if (firstArg.equals("-r")) {

        printNotifications();

      } else if (firstArg.equals("-d")) {
        deleteNotification(args[1]);
      }
    } finally {
      //hack to kill all threads
      System.exit(0);
    }
  }

  private static void deleteNotification(String banner) {
    final CassandraConnection.SessionWrapper wrapper = new CassandraConnection.SessionWrapper();
    final List<Notification> notifications = getNotifications();
    try {

      final Optional<Notification> optional = notifications
          .stream()
          .filter(notification -> notification.getBanner().equals(banner))
          .findFirst();

      if (!optional.isPresent()) {

        System.out.printf("Could not find a %s notification to delete", banner);
        return;
      }

      final Notification notification = optional.get();
      System.out.printf("============= Deleting %s ============", notification.getCorrelationId());
      wrapper.getMapper(Notification.class).delete(notification);

    } finally {
      wrapper.close();
    }
    ;
  }

  private static void printNotifications() {
    final List<Notification> notifications = getNotifications();
    notifications.forEach(System.out::println);
  }

  private static List<Notification> getNotifications() {
    final CassandraConnection.SessionWrapper wrapper = new CassandraConnection.SessionWrapper();
    final List<Notification> notifications;
    try {
      notifications = new ArrayList<>();

      Arrays.asList("frys", "kroger").forEach(banner -> {
        notifications.addAll(getNotifications(wrapper, banner));
      });
    } finally {
      wrapper.close();
    }


    return notifications;
  }

  private static List<Notification> randomNotification(String... banners) {

    Map<String, String> message = new HashMap<>();
    message.put("foo", "bar");
    message.put("body", "Save your money!");

    Calendar c = Calendar.getInstance();
    c.setTime(new Date());
    c.add(Calendar.DATE, 30);
    Date expiryDate = c.getTime();

    final List<Notification> notifications = new ArrayList<>();
    for (String banner : banners) {

      final Notification notification = new Notification(banner, "expiring_coupon", "frys", UUID.randomUUID(), message, new Date(), expiryDate);
      notifications.add(notification);
    }


    return notifications;

  }

  private static void saveNotification(Notification... notifications) {
    final NotificationsRepository notificationsRepository = new NotificationsRepository();

    for (Notification notification : notifications) {
      notificationsRepository.saveNotification(notification);
    }

    System.out.printf("========== Saved! %d notifications=============\n", notifications.length);
  }

  private static List<Notification> getNotifications(CassandraConnection.SessionWrapper wrapper, String recipient) {

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

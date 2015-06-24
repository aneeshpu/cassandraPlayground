package com.cassandraplayground;

import com.cassandraplayground.notifications.CassandraConnection;
import com.cassandraplayground.notifications.Notification;
import com.cassandraplayground.notifications.NotificationsRepository;

import java.time.LocalTime;
import java.util.*;

public class Program {
  public static void main(String[] args) {

    try {
      final String firstArg = args[0];

      if (firstArg.equals("-c")) {

        final List<Notification> notifications = randomNotification(args[1]);
        saveNotification(notifications.toArray(new Notification[notifications.size()]));
      } else if (firstArg.equals("-r")) {

        printNotifications(args[1]);

      } else if (firstArg.equals("-d")) {
        deleteNotification(args[1]);
      } else if (firstArg.equals("-u")) {
        updateNotification(args[1]);
      }
    } finally {
      //hack to kill all threads
      System.exit(0);
    }
  }

  private static void updateNotification(String banner) {

    final List<Notification> notifications = getNotifications(banner);
    final Optional<Notification> optional = notifications
        .stream()
        .filter(notification -> notification.getRecipientId()
            .equals(banner))
        .findFirst();

    if (!optional.isPresent()) {
      System.out.printf("No notifications present for %s\n", banner);
      return;
    }

    final Notification notification = optional.get();
    final HashMap<String, String> message = new HashMap<>();
    message.put("updatedMessage", "This new notification got added at " + LocalTime.now());
    notification.getMessage().add(message);

    final CassandraConnection.SessionWrapper wrapper = new CassandraConnection.SessionWrapper();
    wrapper.getMapper(Notification.class).save(notification);

    System.out.printf("======Updated %s======", notification.getCorrelationId());

  }

  private static void deleteNotification(String banner) {
    final CassandraConnection.SessionWrapper wrapper = new CassandraConnection.SessionWrapper();
    final List<Notification> notifications = getNotifications(banner);
    try {

      final Optional<Notification> optional = notifications
          .stream()
          .filter(notification -> notification.getRecipientId().equals(banner))
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

  private static void printNotifications(String... banners) {
    final List<Notification> notifications = getNotifications(banners);
    notifications.forEach(System.out::println);
  }

  private static List<Notification> getNotifications(String... banners) {
    final CassandraConnection.SessionWrapper wrapper = new CassandraConnection.SessionWrapper();
    final List<Notification> notifications;
    try {
      notifications = new ArrayList<>();

      Arrays.asList(banners).forEach(banner -> {
        notifications.addAll(getNotifications(wrapper, banner));
      });
    } finally {
      wrapper.close();
    }


    return notifications;
  }

  private static List<Notification> randomNotification(String... banners) {

    final HashMap<String, String> message = new HashMap<>();
    message.put("foo", "bar");
    message.put("body", "Save your money!");

    final List<Map<String,String>> messages = new ArrayList<>();
    messages.add(message);

    Calendar c = Calendar.getInstance();
    c.setTime(new Date());
    c.add(Calendar.DATE, 30);
    Date expiryDate = c.getTime();

    final List<Notification> notifications = new ArrayList<>();
    for (String banner : banners) {

      final Notification notification = new Notification(banner, "expiring_coupon", "frys", UUID.randomUUID(), messages, new Date(), expiryDate);
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

    final Notification notification = wrapper.getMapper(Notification.class).get(recipient);
/*
    final List<Notification> notifications = wrapper
        .getAllQueries()
        .getNotifications(recipient)
        .all();
*/

    final ArrayList<Notification> notifications = new ArrayList<>();
    notifications.add(notification);
    return notifications;


  }
}

package com.cassandraplayground;

import com.cassandraplayground.notifications.CassandraConnection;
import com.cassandraplayground.notifications.Notification;
import com.cassandraplayground.notifications.NotificationsRepository;

import java.util.*;

public class Program {
  public static void main(String[] args) {

    saveNotification(randomNotification("frys", "kroger").toArray(new Notification[randomNotification("frys").size()]));

    final CassandraConnection.SessionWrapper wrapper = new CassandraConnection.SessionWrapper();

    try {

      final List<Notification> notifications = new ArrayList<>();

      Arrays.asList("frys", "kroger").forEach(banner -> {
        notifications.addAll(getNotifications(wrapper, banner));
      });

      notifications.forEach(System.out::println);


      final Optional<Notification> optional = notifications
          .stream()
          .filter(notification -> notification.getBanner().equals("frys"))
          .findFirst();

      if(!optional.isPresent()){

        System.out.println("Could not find a frys notification to delete");
        return;
      }
      final Notification notification = optional.get();
      System.out.printf("============= Deleting %s ============", notification.getCorrelationId());
      wrapper.getMapper(Notification.class).delete(notification);

    } finally {
      wrapper.close();

      //hack to kill all threads.
      System.exit(0);
    }


  }

  private static List<Notification> randomNotification(String... banners) {

    Map<String,String> message = new HashMap<>();
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

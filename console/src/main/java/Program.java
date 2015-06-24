import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Program {
  public static void main(String[] args) {

//    getNotifications("kroger");

    Notification notification = randomNotification();
    saveNotification(notification);
  }

  private static Notification randomNotification() {

    Map message = new HashMap<>();
    message.put("foo", "bar");
    message.put("body", "Save your money!");

//    Map<String, String> message = Collections.map(Stream.of(
//        new AbstractMap.SimpleEntry<>("body", "blah blah"),
//        new AbstractMap.SimpleEntry<>("somethingElse", "blah blah"))
//        .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())
//        ));

    Calendar c = Calendar.getInstance();
    c.setTime(new Date());
    c.add(Calendar.DATE, 30);
    Date expiryDate = c.getTime();

    return new Notification("frys", "expiring_coupon", "frys", UUID.randomUUID(), message, new Date(), expiryDate);
  }

  private static void saveNotification(Notification notification) {
    new NotificationsRepository().saveNotification(notification);
    System.out.println("Notification Saved!");
  }

  private static void getNotifications(String recipient) {
    CassandraConnection.SessionWrapper wrapper = null;

    try {
      wrapper = new CassandraConnection.SessionWrapper();

      List<Notification> notifications = wrapper.getAllQueries().getNotifications(recipient).all();

      notifications.parallelStream().forEach(note -> System.out.println(note));
    } finally {
      wrapper.close();
    }
  }
}

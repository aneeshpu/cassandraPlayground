import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import lombok.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Table(keyspace = "notifications", name = "notifications")
public class Notification extends AbstractVO<Notification> {
  @PartitionKey
  @Column(name = "recipient_id")
  private String recipientId;

  @Column(name = "type")
  private String notificationType;
  private String banner;

  @Column(name = "correlation_id")
  private UUID correlationId;

  @Column(name="message")
  Map message;

  @Column(name = "created_date")
  private Date createdDate;

  @Transient
  private Date expiryDate;

  public int lifetime() {
    LocalDateTime start = LocalDateTime.ofInstant(createdDate.toInstant(), ZoneId.systemDefault());
    LocalDateTime finish = LocalDateTime.ofInstant(expiryDate.toInstant(), ZoneId.systemDefault());
    return (int)Duration.between(start, finish).toMinutes();
  }

  @Override
  protected Notification getInstance() {
    return this;
  }

  @Override
  protected Class<Notification> getType() {
    return Notification.class;
  }

  @Override
  public String toString() {
    return "Notification{" +
        "recipientId='" + recipientId + '\'' +
        ", notificationType='" + notificationType + '\'' +
        ", banner='" + banner + '\'' +
        ", message='" + message + '\'' +
        ", correlationId=" + correlationId +
        ", createdDate=" + createdDate +
        '}';
  }
}

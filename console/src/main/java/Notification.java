import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import lombok.EqualsAndHashCode;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

//@Data
//@AllArgsConstructor
//@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Table(keyspace = "notifications", name = "notifications")
public class Notification extends AbstractVO<Notification> {
  public String getRecipientId() {
    return recipientId;
  }

  public void setRecipientId(String recipientId) {
    this.recipientId = recipientId;
  }

  public String getNotificationType() {
    return notificationType;
  }

  public void setNotificationType(String notificationType) {
    this.notificationType = notificationType;
  }

  public String getBanner() {
    return banner;
  }

  public void setBanner(String banner) {
    this.banner = banner;
  }

  public UUID getCorrelationId() {
    return correlationId;
  }

  public void setCorrelationId(UUID correlationId) {
    this.correlationId = correlationId;
  }

  public Map getMessage() {
    return message;
  }

  public void setMessage(Map message) {
    this.message = message;
  }

  public Date getCreatedDate() {
    return createdDate;
  }

  public void setCreatedDate(Date createdDate) {
    this.createdDate = createdDate;
  }

  public Date getExpiryDate() {
    return expiryDate;
  }

  public void setExpiryDate(Date expiryDate) {
    this.expiryDate = expiryDate;
  }

  @PartitionKey
  @Column(name = "recipient_id")
  private String recipientId;

  @Column(name = "type")
  private String notificationType;
  private String banner;

  @Column(name = "correlation_id")
  private UUID correlationId;

  @Column(name="message")
  private Map<String,String> message;

  @Column(name = "created_date")
  private Date createdDate;

  @Transient
  private Date expiryDate;

  public Notification(String recipientId, String notificationType, String banner, UUID correlationId, Map message, Date createdDate, Date expiryDate){

    this.recipientId = recipientId;
    this.notificationType = notificationType;
    this.banner = banner;
    this.correlationId = correlationId;
    this.message = message;
    this.createdDate = createdDate;
    this.expiryDate = expiryDate;
  }

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

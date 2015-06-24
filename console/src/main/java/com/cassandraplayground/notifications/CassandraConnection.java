package com.cassandraplayground.notifications;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CassandraConnection {
  private static final Logger log = Logger.getLogger(CassandraConnection.class);
  private static Cluster cluster;

  private static final int cassandraPort = 9042;
  private static final String cassandraUser = "";
  private static final String cassandraPassword = "";
  private static final int cassandraLocalCoreConnectionsPerHost = 1;
  private static final int cassandraRemoteCoreConnectionsPerHost = 2;
  private static final int cassandraLocalMaxConnectionsPerHost = 2;
  private static final int cassandraRemoteMaxConnectionsPerHost = 8;

  public static final Session getSession() {
    if (cluster == null) {
      cluster = cassandraCluster();
    }

    return cluster.connect();
  }

  private static Cluster cassandraCluster() {
    PoolingOptions poolingOptions = new PoolingOptions();
    poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, cassandraLocalCoreConnectionsPerHost);
    poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, cassandraRemoteCoreConnectionsPerHost);
    poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, cassandraLocalMaxConnectionsPerHost);
    poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, cassandraRemoteMaxConnectionsPerHost);

    Cluster.Builder builder = Cluster.builder()
        .addContactPoint("localhost") // TODO
        .withPoolingOptions(poolingOptions)
        .withPort(cassandraPort)
        .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
        .withSocketOptions(new SocketOptions().setKeepAlive(true));

    if (cassandraUser != null && cassandraUser.length() > 0 && cassandraPassword != null) {
      builder.withCredentials(cassandraUser, cassandraPassword);
    }

    return builder.build();
  }

  public static final void closeClusterConnection() {
    if (cluster != null && !cluster.isClosed()) {
      try {
        cluster.close();
      } catch (Exception e) {
                /*ignore*/
        log.warn("Error closing cluster connection", e);
      }
    }
  }

  /**
   * This is just an utility class to make sure that I do not create
   * Mapper again and again (which recreates same PreparedStatement)
   * causing the driver library to throw ugly warning about inefficiency
   * in recreating statement.
   * <p>
   * This class makes sure for a given session and class, you create mappers
   * just once.
   *
   * @author naishe
   */
  public static class SessionWrapper implements Closeable {
    private Session session;
    private Map<Class<?>, Mapper<? extends AbstractVO<?>>> mapperMap = new ConcurrentHashMap<>();
    private AllQueries allQueries;

    public SessionWrapper() {
      this.session = CassandraConnection.getSession();
    }

    @SuppressWarnings("unchecked")
    public <T extends AbstractVO<T>> Mapper<T> getMapper(Class<T> klass) {
      if (!mapperMap.containsKey(klass)) {
        mapperMap.put(klass, new MappingManager(session).mapper(klass));
      }

      return (Mapper<T>) mapperMap.get(klass);
    }

    public boolean isClosed() {
      return session.isClosed();
    }

    @Override
    public void close() {
      session.close();
    }

    public Session getSession() {
      return this.session;
    }

    public AllQueries getAllQueries() {
      if (allQueries == null) {
        allQueries = new MappingManager(getSession()).createAccessor(AllQueries.class);
      }
      return allQueries;
    }
  }
}
package com.aiven.test.commons.service;

import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.model.Metric;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PostgresService {

    private static final Logger logger = Logger.getLogger(PostgresService.class.getName());

    static DataSource dataSource;

    public PostgresService()  {

        if (!checkMetricTableExists()) {
            createMetricTable();
        }
    }

    public static DataSource getDataSource() {

        if (dataSource == null) {
            String url = ContextProvider.contextProperties.getProperty("pgsql.url");
            String user = ContextProvider.contextProperties.getProperty("pgsql.user");
            String password = ContextProvider.contextProperties.getProperty("pgsql.password");
            ConnectionFactory connectionFactory =
                    new DriverManagerConnectionFactory(url, user, password);
            PoolableConnectionFactory poolableConnectionFactory =
                    new PoolableConnectionFactory(connectionFactory, null);
            ObjectPool<PoolableConnection> connectionPool =
                    new GenericObjectPool<>(poolableConnectionFactory);
            ((GenericObjectPool) connectionPool).setMaxIdle(5);
            ((GenericObjectPool) connectionPool).setMaxTotal(5);
            AbandonedConfig abandonedConfig = new AbandonedConfig();
            abandonedConfig.setRemoveAbandonedOnBorrow(true);
            abandonedConfig.setLogAbandoned(true);
            abandonedConfig.setRemoveAbandonedTimeout(3);
            ((GenericObjectPool) connectionPool).setAbandonedConfig(abandonedConfig);

            poolableConnectionFactory.setPool(connectionPool);

            dataSource =
                    new PoolingDataSource<>(connectionPool);

        }
        return dataSource;
    }

    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    public synchronized Connection connect() throws SQLException {
        return getDataSource().getConnection();
    }

    private boolean checkMetricTableExists() {
        String check = "SELECT EXISTS (" +
                "   SELECT FROM pg_tables" +
                "   WHERE  schemaname = 'public'" +
                "   AND    tablename  = 'metric'" +
                "   );";
        boolean exists = false;
        try (
                Connection conn = connect();
                Statement s = conn.createStatement()) {

            try (ResultSet rs = s.executeQuery(check)) {
                while (rs.next()) {
                    exists = rs.getBoolean(1);
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return exists;
    }

    private void createMetricTable() {
        String createSql = "CREATE TABLE public.metric (" +
                "system_id varchar(20) NOT NULL," +
                "temperature numeric(8,2) NOT NULL," +
                "ts timestamp NOT NULL DEFAULT now()," +
                "id uuid NOT NULL DEFAULT uuid_generate_v4()" +
                ");";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createSql);

            logger.info("Table created");

        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public String insertMetric(Metric metric, Semaphore semaphore) {
        String SQL = "INSERT INTO metric(system_id,temperature,ts) "
                + "VALUES(?,?,?)";

        String id = null;
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL,
                     Statement.RETURN_GENERATED_KEYS)) {

            pstmt.setString(1, metric.getSystemId());
            pstmt.setBigDecimal(2, metric.getTemperature().setScale(8, 2));
            pstmt.setTimestamp(3, new Timestamp(metric.getTs()));

            int num = pstmt.executeUpdate();
            if (num > 0) {
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    while (rs.next()) {
                        id = rs.getString(4);
                        logger.info("ID " + id);
                    }
                }
            }

        } catch (SQLException ex) {
            logger.log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            semaphore.release();
        }
        return id;
    }


}

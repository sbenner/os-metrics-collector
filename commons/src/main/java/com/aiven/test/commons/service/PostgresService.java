package com.aiven.test.commons.service;

import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.model.Metric;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PostgresService {

    private static final Logger logger = Logger.getLogger(PostgresService.class.getName());
    private static Connection connection;

    public PostgresService() throws SQLException {
        connect();
        if (!checkMetricTableExists()) {
            createMetricTable();
        }
    }


    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    public static synchronized Connection connect() throws SQLException {

        if (connection == null ||
                connection.isClosed()) {
            String url = ContextProvider.contextProperties.getProperty("pgsql.url");
            String user = ContextProvider.contextProperties.getProperty("pgsql.user");
            String password = ContextProvider.contextProperties.getProperty("pgsql.password");
            connection = DriverManager.getConnection(url, user, password);
        }
        return connection;
    }

    private boolean checkMetricTableExists() throws SQLException {
        String check = "SELECT EXISTS (" +
                "   SELECT FROM pg_tables" +
                "   WHERE  schemaname = 'public'" +
                "   AND    tablename  = 'metric'" +
                "   );";
        boolean exists = false;
        Statement s = null;
        try {
            s = connect().createStatement();
            try (ResultSet rs = s.executeQuery(check)) {
                while (rs.next()) {
                    exists = rs.getBoolean(1);
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            if (s != null) {
                s.close();
            }
        }
        return exists;
    }

    private void createMetricTable() throws SQLException {
        String createSql = "CREATE TABLE public.metric (" +
                "system_id varchar(20) NOT NULL," +
                "temperature numeric(8,2) NOT NULL," +
                "ts timestamp NOT NULL DEFAULT now()," +
                "id uuid NOT NULL DEFAULT uuid_generate_v4()" +
                ");";

        Statement stmt = null;
        try {
            stmt = connect().createStatement();
            stmt.executeUpdate(createSql);

            logger.info("Table created");

        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        } finally {

            // Close connection
            if (stmt != null) {
                stmt.close();
            }


        }

    }

    public String insertMetric(Metric metric) throws SQLException {
        String SQL = "INSERT INTO metric(system_id,temperature,ts) "
                + "VALUES(?,?,?)";

        String id = "";

        PreparedStatement pstmt = null;
        try {
            pstmt = connect().prepareStatement(SQL,
                    Statement.RETURN_GENERATED_KEYS);

            pstmt.setString(1, metric.getSystemId());
            pstmt.setBigDecimal(2, metric.getTemperature().setScale(8, 2));
            pstmt.setTimestamp(3, new Timestamp(metric.getTs()));

            int affectedRows = pstmt.executeUpdate();
            // check the affected rows
            if (affectedRows > 0) {
                // get the ID back
                return id;

            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            if (pstmt != null) pstmt.close();

        }
        return id;
    }


}

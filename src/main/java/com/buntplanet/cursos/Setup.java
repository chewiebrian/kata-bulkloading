package com.buntplanet.cursos;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

class Setup {

  private static final Logger logger = LoggerFactory.getLogger(Setup.class);

  private static final String JDBC_URL = "jdbc:sqlite:bulkloading.db";
  private static final String JDBC_USER = "bunt";
  private static final String JDBC_PASS = "bunt";


  static Connection getSingleConnection() throws SQLException {
    return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
  }

  static DataSource getDataSource() {
    HikariDataSource dataSource = new HikariDataSource();
    dataSource.setDriverClassName(org.sqlite.JDBC.class.getName());
    dataSource.setJdbcUrl(JDBC_URL);
    dataSource.setUsername(JDBC_USER);
    dataSource.setPassword(JDBC_PASS);
    dataSource.setTransactionIsolation("TRANSACTION_READ_UNCOMMITTED");
    dataSource.setConnectionTestQuery("SELECT 1");
    dataSource.setIdleTimeout(0);
    return dataSource;
  }

  private static void executeSql(Connection conn, String sql) throws SQLException {
    try (final Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  static void prepareDB(Connection conn) throws SQLException {
    executeSql(conn, "DROP TABLE IF EXISTS trips");
    logger.info("Tabla 'trips' borrada si existe");
    executeSql(conn, "" +
        "CREATE TABLE trips (" +
        "   duration_ms VARCHAR(250), " +
        "   start_time VARCHAR(250), " +
        "   start_terminal VARCHAR(250), " +
        "   end_time VARCHAR(250), " +
        "   end_terminal VARCHAR(250), " +
        "   bike_number VARCHAR(20), " +
        "   subscription_type VARCHAR(20)" +
        ")");

    logger.info("Tabla 'trips' creada");
  }

  /**
   * Devuelve el número actual de filas en la tabla trips.
   *
   * @return
   */
  static int getTripsTableLineCount() {
    try (Connection conn = getSingleConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM trips")) {
      return rs.next() ? rs.getInt(1) : 0;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
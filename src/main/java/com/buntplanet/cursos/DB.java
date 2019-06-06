package com.buntplanet.cursos;

import java.sql.*;

class DB {

  private static final String JDBC_URL = "jdbc:sqlite:bulkloading.db";
  private static final String JDBC_USER = "bunt";
  private static final String JDBC_PASS = "bunt";


  static Connection createConnection() throws SQLException {
    return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
  }

  static void executeSql(Connection conn, String sql) throws SQLException {
    try (final Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  static int executeSqlScalar(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
      return rs.next() ? rs.getInt(1) : 0;
    }
  }

  static void prepare(Connection conn) throws SQLException {
    executeSql(conn, "DROP TABLE IF EXISTS trips");
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
  }

  /**
   * Devuelve el número actual de filas en la tabla trips.
   *
   * @return
   */
  static int getTripsTableLineCount() {
    try (Connection conn = createConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM trips")) {
      return rs.next() ? rs.getInt(1) : 0;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
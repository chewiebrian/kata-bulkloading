package com.buntplanet.cursos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AppTest {

  @Before
  public void setUp() throws Exception {
    Metrics.start();
    try (final Connection conn = Setup.getSingleConnection()) {
      Setup.prepareDBNoIndexes(conn);
    }
  }

  @After
  public void tearDown() throws Exception {
    Metrics.stop();
  }

  @Test
  public void canConnectToEmbeddedSqlite() throws SQLException {
    final Connection conn = Setup.getSingleConnection();
    Setup.prepareDBNoIndexes(conn);

    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(rs.getString(1), "1");
    }
  }

  private long getCsvLineCount(Path csvPath) throws IOException {
    return Files.lines(csvPath).skip(1).count();
  }

  @Test
  public void ej0_count_total_csv_lines() throws IOException {
    assertEquals(Setup.getCSVLineCount(), getCsvLineCount(Setup.getCSVPath()));
  }

  private void insertValuesOneByOneCollecting(Connection conn) throws IOException {
    final List<String> lines = Files.lines(Setup.getCSVPath()).skip(1).limit(100000).collect(Collectors.toList());
    lines.stream().map(line -> {
      String[] cols = lines.get(0).split(",");
      return String.format(
          "INSERT INTO trips VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
          cols[0],
          cols[1],
          cols[2],
          cols[3],
          cols[4],
          cols[5],
          cols[6]);
    })
        .forEach(sql -> {
          try {
            conn.createStatement().execute(sql);
          } catch (SQLException e) {
            e.printStackTrace();
          }
        });
  }

  private void insertValuesOneByOneStreaming(Connection conn) throws IOException {
    Files.lines(Setup.getCSVPath()).skip(1).limit(100000).map(line -> {
      String[] cols = line.replaceAll("'", "_").split(",");
      return String.format(
          "INSERT INTO trips VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
          cols[0],
          cols[1],
          cols[2],
          cols[3],
          cols[4],
          cols[5],
          cols[6]);
    })
        .forEach(sql -> {
          try {
            conn.createStatement().execute(sql);
          } catch (SQLException e) {
            e.printStackTrace();
          }
        });
  }

  @Test
  public void ej1_insert_one_by_one() throws Exception {
    //Leer todas las líneas del CSV e ir creando una INSERT para cada. Ejecutarla tal cual.

    final Connection conn = Setup.getSingleConnection();

    //insertValuesOneByOneCollecting(conn);

    insertValuesOneByOneStreaming(conn);
  }

  private static void feedLine(PreparedStatement preparedStatement, String line) throws SQLException {
    String[] cols = line.split(",");
    preparedStatement.setString(1, cols[0]);
    preparedStatement.setString(2, cols[1]);
    preparedStatement.setString(3, cols[2]);
    preparedStatement.setString(4, cols[3]);
    preparedStatement.setString(5, cols[4]);
    preparedStatement.setString(6, cols[5]);
    preparedStatement.setString(7, cols[6]);
  }

  @Test
  public void ej2_insert_batch() throws Exception {
//Crear una PreparedStatement, leer todas las lineas e ir añadiendo las INSERT al batch. Ejecutar todo al final.
    final Connection conn = Setup.getSingleConnection();
    PreparedStatement ps = conn.prepareStatement("INSERT INTO trips VALUES(?, ?, ?, ?, ?, ?, ?)");

    Files.lines(Setup.getCSVPath()).skip(1).limit(100000)
        .forEach(line -> {
          try {
            feedLine(ps, line);
          } catch (SQLException e) {
            e.printStackTrace();
          }
        });

    ps.executeBatch();
  }


  @Test
  public void ej_insert_multiple_values() throws Exception {
//Crear una única INSERT con múltiples valores (collectors.join)
    final Connection conn = Setup.getSingleConnection();

    String sql = "INSERT INTO trips VALUES" + Files.lines(Setup.getCSVPath()).skip(1).map(
        line -> {
          String[] cols = line.replaceAll("'", "_").split(",");
          return String.format(
              "('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
              cols[0],
              cols[1],
              cols[2],
              cols[3],
              cols[4],
              cols[5],
              cols[6]);
        }
    ).collect(Collectors.joining(","));

    conn.createStatement().execute(sql);
  }

  @Test
  public void ej_paralelizar_multiple_values() {
//(parallel streams) + crear streams a pelo
  }

}

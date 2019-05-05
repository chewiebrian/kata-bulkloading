package com.buntplanet.cursos;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.collections4.ListUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

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
    final List<String> lines = Files.lines(Setup.getCSVPath()).skip(1).limit(100000).collect(toList());
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

    assertThat(Setup.getTripsTableLineCount(), is(100000));
  }

  @Test
  public void ej2_insert_batch() throws Exception {
//Crear una PreparedStatement, leer todas las lineas e ir añadiendo las INSERT al batch. Ejecutar todo al final.

    try (final Connection conn = Setup.getSingleConnection();
         PreparedStatement ps = conn.prepareStatement("INSERT INTO trips VALUES(?, ?, ?, ?, ?, ?, ?)");
    ) {
      Files.lines(Setup.getCSVPath()).skip(1).limit(100000)
          .map(line -> line.split(","))
          .forEach(cols -> {
            try {
              ps.setString(1, cols[0]);
              ps.setString(2, cols[1]);
              ps.setString(3, cols[2]);
              ps.setString(4, cols[3]);
              ps.setString(5, cols[4]);
              ps.setString(6, cols[5]);
              ps.setString(7, cols[6]);
              ps.addBatch();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          });

      ps.executeBatch();
    }

    assertThat(Setup.getTripsTableLineCount(), is(100000));
  }


  @Test
  public void ej_insert_multiple_values() throws Exception {
//Crear una única INSERT con múltiples valores (collectors.join)
    String values = Files.lines(Setup.getCSVPath()).skip(1).map(
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

    try (Connection conn = Setup.getSingleConnection(); Statement st = conn.createStatement()) {
      st.execute("INSERT INTO trips VALUES " + values);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    assertThat(Setup.getTripsTableLineCount(), is(Setup.getCSVLineCount()));
  }

  @Test
  public void ej_paralelizar_multiple_values() throws IOException {
    //parallel streams + crear streams a pelo
    DataSource ds = Setup.getDataSource();

    //Stream<List<String>> chunks = buildChunkStreamCollecting();
    Stream<List<String>> chunks = buildChunkStreamIterating();
    AtomicInteger chunkCount = new AtomicInteger();
    chunks.parallel()
        .forEach(
            lines -> {
              System.out.println("chunk " + chunkCount.getAndIncrement());
              String values = lines.stream()
                  .map(line -> {
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
                  }).collect(Collectors.joining(","));

              try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
                st.execute("INSERT INTO trips VALUES " + values);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
        );

    assertThat(Setup.getTripsTableLineCount(), is(Setup.getCSVLineCount()));
  }

  private Stream<List<String>> buildChunkStreamCollecting() throws IOException {
    List<String> csvLines = Files.lines(Setup.getCSVPath()).skip(1).collect(toList());
    List<List<String>> partitions = ListUtils.partition(csvLines, 10000);

    /*
    Otra forma:
    final AtomicInteger counter = new AtomicInteger();

    Map<Object, List<String>> csvLines = Files.lines(Setup.getCSVPath()).skip(1).collect(
        Collectors.groupingBy( it -> counter.getAndIncrement() / 10000)
    );
     */


    Stream.Builder<List<String>> builder = Stream.builder();
    partitions.forEach(builder::add);
    return builder.build();
  }

  private Stream<List<String>> buildChunkStreamIterating() throws IOException {
    long numOfChunks = getCsvLineCount(Setup.getCSVPath()) / 10000;
    final Iterator<String> lineIterator = Files.lines(Setup.getCSVPath()).iterator();

    return Stream.generate(() -> {
      List<String> chunk = new ArrayList<>();

      synchronized (lineIterator) {
        while (lineIterator.hasNext() && chunk.size() < 10000) {
          chunk.add(lineIterator.next());
        }
      }

      return chunk;
    }).limit(numOfChunks).filter(chunk -> !chunk.isEmpty());
  }

}

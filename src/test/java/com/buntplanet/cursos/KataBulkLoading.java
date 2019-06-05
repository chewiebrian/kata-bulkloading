package com.buntplanet.cursos;

import static com.buntplanet.cursos.LambdaUtils.retry;
import static com.buntplanet.cursos.LambdaUtils.unchecked;
import static com.buntplanet.cursos.Setup.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.sql.DataSource;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class KataBulkLoading {

  /**
   * Ruta al recurso que vamos a usar como CSV.
   */
  private static final String CSV_RESOURCE_NAME = "/2015-Q2-Trips-History-Data.csv";

  /**
   * Número de lineas que contiene el CSV, sin contar la cabecera.
   */
  private static final int CSV_LINE_COUNT = 999070;

  @Before
  public void setUp() throws Exception {
    Metrics.start();

    try (final Connection conn = getSingleConnection()) {
      prepareDB(conn);
    }
  }

  @After
  public void tearDown() {
    Metrics.stop();
  }


  @Test
  public void canConnectToEmbeddedSqlite() throws SQLException {
    final Connection conn = getSingleConnection();

    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(rs.getString(1), "1");
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 1: contar el número total de líneas del CSV (excluyendo la cabecera).
   * <p>
   * Extraer a una función estática getCSVPath la ruta al CSV (la usaremos en todos los tests).
   * <p>
   * Hint: Setup.class.getResource(CSV_RESOURCE_NAME).toURI()
   * Hint: Usar el api de Files (https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html)
   * Hint: Echar un vistazo a https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html
   *
   * @throws IOException
   */
  @Test
  public void ej1_count_total_csv_lines() throws IOException {
    final int csvLineCount = (int) Files.lines(getCSVPath()).skip(1).count();

    assertThat(csvLineCount, is(CSV_LINE_COUNT));
  }

  static Path getCSVPath() {
    try {
      return Paths.get(Setup.class.getResource(CSV_RESOURCE_NAME).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 2: insertar los datos en Sqlite de uno en uno.
   * <p>
   * Habrá que transformar cada fila del CSV en una INSERT INTO trips VALUES ...
   * Como éste es el método más lento, procesar sólamente las 100000 primeras líneas.
   *
   * Hint: Stream.limit()
   *
   * @throws Exception
   */
  @Test
  public void ej2_insert_one_by_one() throws Exception {
    //Se ve Stream.map()
    //Extra: curryficacion, extraer funciones
    //Soluciones con collect() o directamente con el stream.

    try (final Connection conn = getSingleConnection()) {
      insertValuesOneByOneCollecting(conn);

      //menos consumo memoria, igual tiempo
      //insertValuesOneByOneStreaming(conn);
    }

    assertThat(getTripsTableLineCount(), is(100000));
  }

  private void insertValuesOneByOneCollecting(Connection conn) throws IOException {
    final List<String> lines = Files.lines(getCSVPath()).skip(1).limit(100000).collect(toList());

    lines.stream().map(this::mapCsvLineToInsertStatement)
        .forEach(unchecked(sql -> executeSql(conn, sql)));
  }

  private void insertValuesOneByOneStreaming(Connection conn) throws IOException {
    Files.lines(getCSVPath()).skip(1).limit(100000)
        .map(this::mapCsvLineToInsertStatement)
        .forEach(executeSqlWithConnection(conn));   //Ejemplo curryficacion
  }

  private void executeSql(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  private Consumer<String> executeSqlWithConnection(Connection conn) {
    return unchecked(sql -> executeSql(conn, sql));
  }

  String mapCsvLineToInsertStatement(String line) {
    return "INSERT INTO trips VALUES " + mapCsvLineToInsertValues(line);
  }

  String mapCsvLineToInsertValues(String line) {
    final String[] cols = line.replaceAll("'", "_").split(",");
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

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 3: de nuevo insertar las primeras 100000 líneas de la tabla.
   * <p>
   * Esta vez usar el API PreparedStatement de JDBC (https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html),
   * creando un batch de inserciones en vez de ir una a una y ejecutarlo una única vez al final (addBatch y executeBatch).
   * <p>
   * El rendimiento tiene que ser un poco mejor que en el ejercicio 2.
   *
   * Hint: Stream.onClose()
   *
   * @throws Exception
   */
  @Test
  public void ej3_insert_batch() throws Exception {
    //se ve: Stream.onClose

    try (final Connection conn = getSingleConnection();
         PreparedStatement ps = conn.prepareStatement("INSERT INTO trips VALUES(?, ?, ?, ?, ?, ?, ?)");
         Stream<String> lines = Files.lines(getCSVPath()).skip(1).limit(100000)
    ) {
      lines.onClose(unchecked(ps::executeBatch));

      lines
          .map(line -> line.split(","))
          .forEach(unchecked(cols -> {
            ps.setString(1, cols[0]);
            ps.setString(2, cols[1]);
            ps.setString(3, cols[2]);
            ps.setString(4, cols[3]);
            ps.setString(5, cols[4]);
            ps.setString(6, cols[5]);
            ps.setString(7, cols[6]);

            ps.addBatch();
          }));
    }

    assertThat(getTripsTableLineCount(), is(100000));
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final String TEST_DAY = "6/30/2015";
  private static final int TRIPS_ON_TEST_DAY = 11970;

  /**
   * Ejercicio 4: insertar sólo las líneas del día 6/30/2015.
   * <p>
   * Consejo: no partir de la implementación de un ejercicio anterior.
   *
   * Hint: Stream.filter()
   *
   * @throws Exception
   */
  @Test
  public void ej4_insert_filtered() throws Exception {
    //Se ve: Stream.filter()

    try (final Connection conn = Setup.getSingleConnection()) {
      final List<String> lines = Files.lines(getCSVPath()).skip(1).limit(100000).collect(toList());

      lines.stream()
          .map(line -> line.replaceAll("'", "_").split(","))
          .filter(cols -> StringUtils.startsWithIgnoreCase(cols[1], TEST_DAY))
          .map(cols -> "INSERT INTO trips VALUES " + mapCsvLineToInsertValues(cols))
          .forEach(executeSqlWithConnection(conn));
    }

    assertThat(getTripsTableLineCount(), is(TRIPS_ON_TEST_DAY));
  }

  String mapCsvLineToInsertValues(String[] cols) {
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

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 5: en vez de hacer una INSERT por fila, hacer una INSERT con múltiples valores.
   * <p>
   * Es decir INSERT INTO trips VALUES (...),(...),  ...
   * <p>
   * Usar el CSV completo.
   * <p>
   * Hint: usar Collectors.xxx() (https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collectors.html)
   *
   * @throws Exception
   */
  @Test
  public void ej5_insert_multiple_values() throws Exception {
    //Se ve: Collectors.join()

    final String insertWithMultipleValues = Files.lines(getCSVPath())
        .skip(1)
        .map(this::mapCsvLineToInsertValues)
        .collect(Collectors.joining(","));

    try (Connection conn = getSingleConnection()) {
      executeSql(conn, "INSERT INTO trips VALUES " + insertWithMultipleValues);
    }

    assertThat(getTripsTableLineCount(), is(CSV_LINE_COUNT));
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 6: repetir el ejercicio 2, pero paralelizando la ejecución de las INSERT.
   * <p>
   * Hint: Sqlite no maneja bien múltiples hilos, usar LambdaUtils.retry() o usar una única conexión a BD.
   */
  @Test
  public void ej6_insert_values_parallel() throws Exception {
    final DataSource ds = Setup.getDataSource();

    Files.lines(getCSVPath())
        .skip(1)
        .limit(100000)
        .parallel()
        .map(this::mapCsvLineToInsertStatement)
        .forEach(
            retry(3, sql -> {
              try (final Connection conn = ds.getConnection()) {
                executeSql(conn, sql);
              }
            })
        );

    assertThat(getTripsTableLineCount(), is(100000));
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final int CHUNK_SIZE = 10000;

  /**
   * Ejercicio 7: repetir el ejercicio 5, pero troceando el CSV para insertar en paralelo paquetes (chunks) de 10000.
   * <p>
   * Ojo: usar un DataSource para sacar las conexiones!
   * <p>
   * Hint: Stream.Builder (https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.Builder.html)
   * Hint: ListUtils.partition() (https://commons.apache.org/proper/commons-collections/apidocs/org/apache/commons/collections4/ListUtils.html)
   *
   * @throws IOException
   */
  @Test
  public void ej7_insert_multiple_values_parallel() throws IOException {
    //Se ve: parallel streams + crear streams a pelo
    DataSource ds = getDataSource();

    Stream<List<String>> chunks = buildChunkStream();

    AtomicInteger chunkCount = new AtomicInteger();
    chunks.parallel()
        .map(chunk -> "INSERT INTO trips VALUES " + chunk.stream().map(this::mapCsvLineToInsertValues).collect(joining(",")))
        .forEach(
            sql -> {
              System.out.println("chunk " + chunkCount.getAndIncrement() + " (hilo " + Thread.currentThread().getName() + ")");

              try (Connection conn = ds.getConnection()) {
                executeSql(conn, sql);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
        );

    assertThat(getTripsTableLineCount(), is(CSV_LINE_COUNT));
  }

  private Stream<List<String>> buildChunkStream() throws IOException {
    List<String> csvLines = Files.lines(getCSVPath()).skip(1).collect(toList());
    List<List<String>> partitions = ListUtils.partition(csvLines, CHUNK_SIZE);

    /*
    Otra forma:
    final AtomicInteger counter = new AtomicInteger();

    Map<Object, List<String>> csvLines = Files.lines(Setup.getCSVPath()).skip(1).collect(
        Collectors.groupingBy( it -> counter.getAndIncrement() / CHUNK_SIZE)
    );
     */

    Stream.Builder<List<String>> builder = Stream.builder();
    partitions.forEach(builder::add);
    return builder.build();
  }


}

package com.buntplanet.cursos;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.buntplanet.cursos.LambdaUtils.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class KataBulkLoading {

  private static final String CSV_RESOURCE_NAME = "/2015-Q2-Trips-History-Data.csv";
  private static final int CSV_LINE_COUNT = 999070;
  private static final int TEN_THOUSAND = 10000;

  @Rule
  public TestName runningTestName = new TestName();

  @Before
  public void setUp() throws Exception {
    System.out.println(runningTestName.getMethodName());
    Metrics.start();

    try (final Connection conn = DB.createConnection()) {
      DB.prepare(conn);
    }
  }

  @After
  public void tearDown() {
    Metrics.stop();
  }


  @Test
  public void canConnectToEmbeddedSqlite() throws SQLException {
    try (final Connection conn = DB.createConnection()) {
      assertThat(DB.executeSqlScalar(conn, "SELECT 1"), is(1));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 1: contar el número total de líneas del CSV (excluyendo la cabecera).
   * <p>
   * Hint: Usar getCSVPath() para obtener la ruta del CSV.
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
      return Paths.get(DB.class.getResource(CSV_RESOURCE_NAME).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 2: insertar los datos en Sqlite de uno en uno.
   * <p>
   * Habrá que transformar cada fila del CSV en una INSERT INTO trips VALUES ...
   * Como éste es el método más lento, procesar sólamente las 10000 primeras líneas.
   * <p>
   * Hint: Stream.limit()
   *
   * @throws Exception
   */
  @Test
  public void ej2_insert_one_by_one() throws Exception {
    //Se ve Stream.map()
    //Extra: curryficacion, extraer funciones
    //Soluciones con collect() o directamente con el stream.
    final String sql = "INSERT INTO trips VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')";

    try (final Connection conn = DB.createConnection()) {
      insertValuesOneByOneCollecting(conn);

      //menos consumo memoria, igual tiempo
      //insertValuesOneByOneStreaming(conn);
    }

    assertThat(DB.getTripsTableLineCount(), is(TEN_THOUSAND));
  }

  private void insertValuesOneByOneCollecting(Connection conn) throws IOException {
    final List<String> lines = Files.lines(getCSVPath()).skip(1).limit(TEN_THOUSAND).collect(toList());

    lines.stream().map(this::mapCsvLineToInsertStatement)
        .forEach(LambdaUtils.uncheckedConsumer(sql -> DB.executeSql(conn, sql)));
  }

  private void insertValuesOneByOneStreaming(Connection conn) throws IOException {
    Files.lines(getCSVPath()).skip(1).limit(TEN_THOUSAND)
        .map(this::mapCsvLineToInsertStatement)
        .forEach(executeSqlWithConnection(conn));   //Ejemplo curryficacion
  }

  private Consumer<String> executeSqlWithConnection(Connection conn) {
    return LambdaUtils.uncheckedConsumer(sql -> DB.executeSql(conn, sql));
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
   * Ejercicio 3: de nuevo insertar las primeras 10000 líneas de la tabla.
   * <p>
   * Esta vez usar el API PreparedStatement de JDBC (https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html),
   * creando un batch de inserciones en vez de ir una a una y ejecutarlo una única vez al final (addBatch y executeBatch).
   * <p>
   * El rendimiento tiene que ser un poco mejor que en el ejercicio 2.
   * <p>
   * Hint: Stream.onClose()
   *
   * @throws Exception
   */
  @Test
  public void ej3_insert_batch() throws Exception {
    //se ve: Stream.onClose
    final String sql = "INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?)";

    try (final Connection conn = DB.createConnection();
         PreparedStatement ps = conn.prepareStatement(sql);
         Stream<String> lines = Files.lines(getCSVPath()).skip(1).limit(TEN_THOUSAND)
    ) {
      lines.onClose(uncheckedRunnable(ps::executeBatch));

      lines
          .map(line -> line.split(","))
          .forEach(LambdaUtils.uncheckedConsumer(cols -> {
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

    assertThat(DB.getTripsTableLineCount(), is(TEN_THOUSAND));
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final String TEST_DAY = "6/30/2015";
  private static final int TRIPS_ON_TEST_DAY = 11970;

  /**
   * Ejercicio 4: insertar sólo las líneas del día 6/30/2015.
   * <p>
   * Consejo: no partir de la implementación de un ejercicio anterior.
   * <p>
   * Hint: Stream.filter()
   *
   * @throws Exception
   */
  @Test
  public void ej4_insert_filtered() throws Exception {
    //Se ve: Stream.filter()

    try (final Connection conn = DB.createConnection()) {
      final List<String> lines = Files.lines(getCSVPath()).skip(1).collect(toList());

      lines.stream()
          .map(line -> line.replaceAll("'", "_").split(","))
          .filter(cols -> StringUtils.startsWithIgnoreCase(cols[1], TEST_DAY))
          .map(cols -> "INSERT INTO trips VALUES " + mapCsvLineToInsertValues(cols))
          .forEach(executeSqlWithConnection(conn));
    }

    assertThat(DB.getTripsTableLineCount(), is(TRIPS_ON_TEST_DAY));
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
   * Ejercicio 5: en vez de hacer una INSERT por fila, hacer una INSERT con múltiples valores. Trabajar con todo el CSV.
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

    try (Connection conn = DB.createConnection()) {
      DB.executeSql(conn, "INSERT INTO trips VALUES " + insertWithMultipleValues);
    }

    assertThat(DB.getTripsTableLineCount(), is(CSV_LINE_COUNT));
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 6: repetir el ejercicio 2, pero paralelizando la ejecución de las INSERT. Sólo las 10000 primeras.
   * <p>
   * Hint: Sqlite no maneja bien múltiples hilos, usar una única conexión a BD.
   */
  @Test
  public void ej6_insert_values_parallel() throws Exception {
    try (final Connection conn = DB.createConnection()) {
      Files.lines(getCSVPath())
          .skip(1)
          .limit(TEN_THOUSAND)
          .parallel()
          .map(this::mapCsvLineToInsertStatement)
          .forEach(executeSqlWithConnection(conn));
    }

    assertThat(DB.getTripsTableLineCount(), is(TEN_THOUSAND));
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final int CHUNK_SIZE = 10000;

  /**
   * Ejercicio 7: repetir el ejercicio 5, pero troceando el CSV para insertar en paralelo paquetes (chunks) de 10000.
   * <p>
   * Hint: Stream.Builder (https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.Builder.html)
   * Hint: ListUtils.partition() (https://commons.apache.org/proper/commons-collections/apidocs/org/apache/commons/collections4/ListUtils.html)
   * Hint: Sqlite no maneja bien múltiples hilos, usar una única conexión a BD.
   *
   * @throws IOException
   */
  @Test
  public void ej7_insert_multiple_values_parallel() throws Exception {
    //Se ve: parallel streams + crear streams a pelo

    try (final Connection conn = DB.createConnection()) {
      final Stream<List<String>> chunks = buildChunkStream();
      final AtomicInteger chunkCount = new AtomicInteger();

      chunks.parallel()
          .map(chunk -> "INSERT INTO trips VALUES " + chunk.stream().map(this::mapCsvLineToInsertValues).collect(joining(",")))
          .forEach(
              sql -> {
                System.out.println("chunk " + chunkCount.getAndIncrement() + " (hilo " + Thread.currentThread().getName() + ")");

                try {
                  DB.executeSql(conn, sql);
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
          );
    }

    assertThat(DB.getTripsTableLineCount(), is(CSV_LINE_COUNT));
  }

  private Stream<List<String>> buildChunkStream() throws IOException {
    List<String> csvLines = Files.lines(getCSVPath()).skip(1).collect(toList());
    List<List<String>> partitions = ListUtils.partition(csvLines, CHUNK_SIZE);

    /*
    Otra forma:
    final AtomicInteger counter = new AtomicInteger();

    Map<Object, List<String>> csvLines = Files.lines(DB.getCSVPath()).skip(1).collect(
        Collectors.groupingBy( it -> counter.getAndIncrement() / CHUNK_SIZE)
    );
     */

    Stream.Builder<List<String>> builder = Stream.builder();
    partitions.forEach(builder::add);
    return builder.build();
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final String TEST_DAY_1 = "6/25/2015";
  private static final String TEST_DAY_2 = "6/28/2015";
  private static final String TEST_DAY_3 = "6/30/2015";
  private static final int TOTAL_DURATION_ON_TEST_DAYS_MINUTES = 667150;

  /**
   * Ejercicio 8: partiendo de la tabla completa de viajes, calcular la suma de las duraciones de los viajes en los tres días indicados.
   * <p>
   * Calcular cada día por separado y en paralelo. La suma la puede hacer la siguiente SQL:
   * SELECT sum(cast(duration_ms as numeric)) / 60000 FROM trips WHERE start_time like '*dia*%'
   * <p>
   * Hint: CompletableFuture.supplyAsync() (https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html)
   *
   * @throws Exception
   */
  @Test
  public void ej8_accumulate_duration_of_three_days() throws Exception {
    //usaremos la implementación del ejercicio anterior para rellenar la tabla de viajes
    ej7_insert_multiple_values_parallel();

    final int completeDuration;

    try (final Connection conn = DB.createConnection()) {
      CompletableFuture<Integer> futDay1 = CompletableFuture.supplyAsync(uncheckedSupplier(() -> calculateDayDuration(conn, TEST_DAY_1)));
      CompletableFuture<Integer> futDay2 = CompletableFuture.supplyAsync(uncheckedSupplier(() -> calculateDayDuration(conn, TEST_DAY_2)));
      CompletableFuture<Integer> futDay3 = CompletableFuture.supplyAsync(uncheckedSupplier(() -> calculateDayDuration(conn, TEST_DAY_3)));

      //realmente no es necesario; get() es bloqueante
      CompletableFuture.allOf(futDay1, futDay2, futDay3).join();

      completeDuration = futDay1.get() + futDay2.get() + futDay3.get();
    }

    assertThat(completeDuration, is(TOTAL_DURATION_ON_TEST_DAYS_MINUTES));
  }

  int calculateDayDuration(Connection conn, String day) throws SQLException {
    return DB.executeSqlScalar(conn, "SELECT sum(cast(duration_ms as numeric)) / 60000 FROM trips WHERE start_time like '" + day + "%'");
  }
}

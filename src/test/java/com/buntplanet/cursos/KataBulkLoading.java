package com.buntplanet.cursos;

import org.apache.commons.collections4.Closure;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.Predicate;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.BoundedIterator;
import org.apache.commons.collections4.iterators.SkippingIterator;
import org.apache.commons.io.FileUtils;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
    final int csvLineCount;

    try (CloseableLineIterator allLines = new CloseableLineIterator(FileUtils.lineIterator(getCSVFile()))) {
      final SkippingIterator linesWithoutHeader = IteratorUtils.skippingIterator(allLines, 1);
      csvLineCount = IteratorUtils.size(linesWithoutHeader);
    }

    assertThat(csvLineCount, is(CSV_LINE_COUNT));
  }

  static Path getCSVPath() {
    try {
      return Paths.get(DB.class.getResource(CSV_RESOURCE_NAME).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  static File getCSVFile() {
    return new File(DB.class.getResource(CSV_RESOURCE_NAME).getFile());
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
    final String sql = "INSERT INTO trips VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')";

    try (Connection conn = DB.createConnection();
         CloseableLineIterator allLines = new CloseableLineIterator(FileUtils.lineIterator(getCSVFile()))) {
      final SkippingIterator linesWithoutHeader = IteratorUtils.skippingIterator(allLines, 1);
      final BoundedIterator tenThousandLinesWithoutHeader = IteratorUtils.boundedIterator(linesWithoutHeader, TEN_THOUSAND);

      final Iterator columnas = IteratorUtils.transformedIterator(tenThousandLinesWithoutHeader, new Transformer<String, String[]>() {
        @Override
        public String[] transform(String linea) {
          return linea.replaceAll("'", "_").split(",");
        }
      });

      final Iterator sqlInserts = IteratorUtils.transformedIterator(columnas, new Transformer<String[], String>() {
        @Override
        public String transform(String[] columnas) {
          return String.format(sql, columnas);
        }
      });

      IteratorUtils.forEach(sqlInserts, new Closure<String>() {
        @Override
        public void execute(String sqlInsert) {
          try {
            DB.executeSql(conn, sqlInsert);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      });

    }

    assertThat(DB.getTripsTableLineCount(), is(TEN_THOUSAND));
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
    final String sql = "INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?)";

    try (Connection conn = DB.createConnection();
         PreparedStatement ps = conn.prepareStatement(sql);
         CloseableLineIterator allLines = new CloseableLineIterator(FileUtils.lineIterator(getCSVFile()))) {

      final SkippingIterator linesWithoutHeader = IteratorUtils.skippingIterator(allLines, 1);
      final BoundedIterator tenThousandLinesWithoutHeader = IteratorUtils.boundedIterator(linesWithoutHeader, TEN_THOUSAND);

      final Iterator<String[]> lineColumns = IteratorUtils.transformedIterator(tenThousandLinesWithoutHeader, new Transformer<String, String[]>() {
        @Override
        public String[] transform(String linea) {
          return linea.replaceAll("'", "_").split(",");
        }
      });

      for (String[] cols : IteratorUtils.asIterable(lineColumns)
      ) {
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
      }

/*
      //Alternativa:
      IteratorUtils.forEach(lineColumns, new Closure<String[]>() {
        @Override
        public void execute(String[] cols) {
          ...
        }
      });
*/

      ps.executeBatch();
    }

    assertThat(DB.getTripsTableLineCount(), is(TEN_THOUSAND));
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final LocalDate TEST_DAY = new LocalDate(2015, 6, 28);
  private static final int TRIPS_ON_EJ4 = 24161;

  /**
   * Ejercicio 4: insertar sólo las líneas posteriores al día 6/28/2015.
   * <p>
   * Consejo: no partir de la implementación de un ejercicio anterior.
   * <p>
   * Hint: Stream.filter()
   * Hint: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html#parse-java.lang.CharSequence-java.time.format.DateTimeFormatter-
   *
   * @throws Exception
   */
  @Test
  public void ej4_insert_filtered() throws Exception {
    //TODO: implementar
    final String sql = "INSERT INTO trips VALUES (?, ?, ?, ?, ?, ?, ?)";
    final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("M/d/yyyy H:m").toFormatter();

    try (Connection conn = DB.createConnection();
         PreparedStatement ps = conn.prepareStatement(sql);
         CloseableLineIterator allLines = new CloseableLineIterator(FileUtils.lineIterator(getCSVFile()))) {

      final SkippingIterator linesWithoutHeader = IteratorUtils.skippingIterator(allLines, 1);

      final Iterator<String[]> lineColumns = IteratorUtils.transformedIterator(linesWithoutHeader, new Transformer<String, String[]>() {
        @Override
        public String[] transform(String linea) {
          return linea.replaceAll("'", "_").split(",");
        }
      });

      final Iterator<String[]> filteredLines = IteratorUtils.filteredIterator(lineColumns, new Predicate<String[]>() {
        @Override
        public boolean evaluate(String[] cols) {
          final LocalDateTime dateTime = LocalDateTime.parse(cols[1], dateTimeFormatter);
          return dateTime.toLocalDate().isAfter(TEST_DAY);
        }
      });

      for (String[] cols : IteratorUtils.asIterable(filteredLines)
      ) {
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
      }

      ps.executeBatch();
    }

    assertThat(DB.getTripsTableLineCount(), is(TRIPS_ON_EJ4));
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 5: en vez de hacer una INSERT por fila, hacer una INSERT con múltiples valores. Sólo las 10000 primeras filas.
   * <p>
   * Es decir INSERT INTO trips VALUES (...),(...),  ...
   * <p>
   * Hint: usar Collectors.xxx() (https://docs.oracle.com/javase/8/docs/api/java/util/stream/Collectors.html)
   *
   * @throws Exception
   */
  @Test
  public void ej5_insert_multiple_values() throws Exception {
    //TODO: implementar

    assertThat(DB.getTripsTableLineCount(), is(TEN_THOUSAND));
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 6: repetir el ejercicio 2, pero paralelizando la ejecución de las INSERT. Sólo las 10000 primeras.
   * <p>
   * Hint: Sqlite no maneja bien múltiples hilos, usar una única conexión a BD.
   */
  @Test
  public void ej6_insert_values_parallel() throws Exception {
    //TODO: implementar

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
    //TODO: implementar

    assertThat(DB.getTripsTableLineCount(), is(CSV_LINE_COUNT));
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

    final int completeDuration = 0;  //TODO: implementar

    assertThat(completeDuration, is(TOTAL_DURATION_ON_TEST_DAYS_MINUTES));
  }

}

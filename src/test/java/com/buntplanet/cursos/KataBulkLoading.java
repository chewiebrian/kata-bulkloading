package com.buntplanet.cursos;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.buntplanet.cursos.Setup.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

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
    final int csvLineCount = 0; //TODO: contar correctamente

    assertThat(csvLineCount, is(CSV_LINE_COUNT));
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 2: insertar los datos en Sqlite de uno en uno.
   * <p>
   * Habrá que transformar cada fila del CSV en una INSERT INTO trips VALUES ...
   * Como éste es el método más lento, procesar sólamente las 100000 primeras líneas.
   * <p>
   * Hint: Stream.limit()
   *
   * @throws Exception
   */
  @Test
  public void ej2_insert_one_by_one() throws Exception {
    //TODO: implementar

    assertThat(getTripsTableLineCount(), is(100000));
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Ejercicio 3: de nuevo insertar las primeras 100000 líneas de la tabla.
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
    //TODO: implementar

    assertThat(getTripsTableLineCount(), is(100000));
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
    //TODO: implementar

    assertThat(getTripsTableLineCount(), is(TRIPS_ON_TEST_DAY));
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
    //TODO: implementar

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
    //TODO: implementar

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
    //TODO: implementar

    assertThat(getTripsTableLineCount(), is(CSV_LINE_COUNT));
  }

}

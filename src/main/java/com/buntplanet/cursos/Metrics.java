package com.buntplanet.cursos;

import org.apache.commons.lang3.time.StopWatch;

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

public class Metrics {

  private static StopWatch sw;
  private static long startMem;
  private static long stopMem;

  public static void start() {
    Runtime.getRuntime().gc();

    sw = new StopWatch();
    sw.start();

    startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  public static void stop() {
    sw.stop();
    stopMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

    final int tripsTableLineCount = Setup.getTripsTableLineCount();
    if (tripsTableLineCount > 0) {
      final long tps =  tripsTableLineCount > 0 ? tripsTableLineCount / sw.getTime(TimeUnit.SECONDS) : 0;

      System.out.println(MessageFormat.format("Tiempo total: {0} -> {1} tps", sw.toString(), tps));
      System.out.println("Memoria usada: " + (stopMem - startMem) / 1048576 + " MB");
    }
  }

}

package com.buntplanet.cursos;

import org.apache.commons.lang3.time.StopWatch;

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

    System.out.println("Tiempo total: " + sw.toString());
    System.out.println("Memoria usada: " + (stopMem - startMem) / 1048576 + " M");
  }

}

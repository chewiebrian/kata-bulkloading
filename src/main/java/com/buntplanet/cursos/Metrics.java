package com.buntplanet.cursos;

import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.lang3.time.StopWatch;

import java.lang.management.ManagementFactory;

public class Metrics {

  private static final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

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
    System.out.println(String.format("Carga CPU: %f%%" , osBean.getProcessCpuLoad()*100.0));
  }

}

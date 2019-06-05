package com.buntplanet.cursos;

import java.util.function.Consumer;

public class LambdaUtils {

  static <T> Consumer<T> unchecked(CheckedConsumer<T> consumer) {
    return obj -> {
      try {
        consumer.accept(obj);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }

  static Runnable unchecked(CheckedRunnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }

  static <T> Consumer<T> retry(int numTimes, CheckedConsumer<T> consumer) {
    return obj -> {
      int times = numTimes;

      while (times-- >= 0) {
        try {
          consumer.accept(obj);
          return;
        } catch (Throwable e) {
          System.out.println("Retrying due to: " + e.toString());
          //ignore
        }
      }
    };
  }

  @FunctionalInterface
  public interface CheckedConsumer<T>  {
    void accept(T t) throws Throwable;
  }

  @FunctionalInterface
  public interface CheckedRunnable {
    void run() throws Throwable;
  }
}

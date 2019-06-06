package com.buntplanet.cursos;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class LambdaUtils {

  static <T> Consumer<T> uncheckedConsumer(CheckedConsumer<T> consumer) {
    return obj -> {
      try {
        consumer.accept(obj);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }

  static Runnable uncheckedRunnable(CheckedRunnable runnable) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }

  static <T> Supplier<T> uncheckedSupplier(CheckedSupplier<T> supplier) {
    return () -> {
      try {
        return supplier.get();
      } catch (Throwable throwable) {
        throw new RuntimeException();
      }
    };
  }

  @FunctionalInterface
  public interface CheckedConsumer<T> {
    void accept(T t) throws Throwable;
  }

  @FunctionalInterface
  public interface CheckedRunnable {
    void run() throws Throwable;
  }

  @FunctionalInterface
  public interface CheckedSupplier<T> {
    T get() throws Throwable;
  }

}

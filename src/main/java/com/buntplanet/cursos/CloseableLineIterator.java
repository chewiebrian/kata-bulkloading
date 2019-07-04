package com.buntplanet.cursos;

import org.apache.commons.io.LineIterator;

import java.util.Iterator;

public class CloseableLineIterator implements Iterator<String>, AutoCloseable {

  private final LineIterator lineIterator;

  public CloseableLineIterator(LineIterator lineIterator) {
    this.lineIterator = lineIterator;
  }

  @Override
  public void close() {
    lineIterator.close();
  }

  @Override
  public boolean hasNext() {
    return lineIterator.hasNext();
  }

  @Override
  public String next() {
    return lineIterator.nextLine();
  }

  @Override
  public void remove() {
    lineIterator.remove();
  }
}

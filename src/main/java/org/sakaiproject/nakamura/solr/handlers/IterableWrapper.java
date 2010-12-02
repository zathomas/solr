package org.sakaiproject.nakamura.solr.handlers;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class IterableWrapper<T> implements Iterable<T> {

  private Object[] array;

  public IterableWrapper(Object[] v) {
    this.array = v;
  }

  public Iterator<T> iterator() {
    return new Iterator<T>() {
      int p = 0;

      public boolean hasNext() {
        return array != null && p < array.length;
      }

      public T next() {
        try {
          return getValue(array[p++]);
        } catch (ArrayIndexOutOfBoundsException e) {
          throw new NoSuchElementException();
        }
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  protected abstract T getValue(Object object);
}

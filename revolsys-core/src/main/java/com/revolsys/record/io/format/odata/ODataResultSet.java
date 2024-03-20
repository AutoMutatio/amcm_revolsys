package com.revolsys.record.io.format.odata;

import java.util.Iterator;
import java.util.function.Consumer;

import com.revolsys.util.Cancellable;

public class ODataResultSet<T> implements Iterable<T> {

  public int forEach(final Cancellable cancellable, final Consumer<? super T> action) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void forEach(final Consumer<? super T> action) {
    throw new UnsupportedOperationException();
  }

  public int getCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    // TODO Auto-generated method stub
    return null;
  }
}

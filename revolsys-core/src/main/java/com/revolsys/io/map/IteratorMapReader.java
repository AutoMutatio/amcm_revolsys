package com.revolsys.io.map;

import java.util.Iterator;

import com.revolsys.collection.iterator.IteratorReader;
import com.revolsys.collection.map.MapEx;

public class IteratorMapReader extends IteratorReader<MapEx> implements MapReader {
  public IteratorMapReader(final Iterator<MapEx> iterator) {
    super(iterator);
  }
}

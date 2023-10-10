package com.revolsys.io.map;

import java.util.Iterator;

import org.jeometry.common.collection.iterator.IteratorReader;
import org.jeometry.common.collection.map.MapEx;

public class IteratorMapReader extends IteratorReader<MapEx> implements MapReader {
  public IteratorMapReader(final Iterator<MapEx> iterator) {
    super(iterator);
  }
}

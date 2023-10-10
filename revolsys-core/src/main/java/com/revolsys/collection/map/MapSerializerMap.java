package com.revolsys.collection.map;

import java.util.Set;

import org.jeometry.common.collection.map.MapEx;
import org.jeometry.common.io.MapSerializer;

public class MapSerializerMap implements MapEx {

  private final MapSerializer serializer;

  public MapSerializerMap(final MapSerializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public MapEx clone() {
    return this;
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    return this.serializer.toMap().entrySet();
  }

}

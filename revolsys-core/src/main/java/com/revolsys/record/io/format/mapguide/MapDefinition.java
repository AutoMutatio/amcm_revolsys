package com.revolsys.record.io.format.mapguide;

import org.jeometry.common.collection.map.MapEx;

public class MapDefinition extends ResourceDocument {
  public MapDefinition(final MapEx properties) {
    setProperties(properties);
  }

  @Override
  public String getIconName() {
    return "map";
  }
}

package com.revolsys.connection;

import java.nio.file.Path;

import org.jeometry.common.collection.map.MapEx;
import org.jeometry.common.io.MapSerializer;

import com.revolsys.collection.NameProxy;
import com.revolsys.util.IconNameProxy;

public interface Connection extends MapSerializer, NameProxy, IconNameProxy {
  void deleteConnection();

  boolean equalsConfig(Connection connection);

  MapEx getConfig();

  Path getConnectionFile();

  ConnectionRegistry<?> getRegistry();

  default boolean isEditable() {
    return !isReadOnly();
  }

  default boolean isExists() {
    return true;
  }

  boolean isReadOnly();
}

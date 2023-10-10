package com.revolsys.geometry.io;

import org.jeometry.common.collection.map.MapEx;
import org.jeometry.common.json.JsonObject;

import com.revolsys.io.FileIoFactory;
import com.revolsys.io.ReadIoFactory;
import com.revolsys.spring.resource.Resource;

public interface PointReaderFactory extends FileIoFactory, ReadIoFactory {
  default PointReader newPointReader(final Object source) {
    final Resource resource = Resource.getResource(source);
    return newPointReader(resource);
  }

  default PointReader newPointReader(final Resource resource) {
    return newPointReader(resource, JsonObject.EMPTY);
  }

  PointReader newPointReader(final Resource resource, MapEx properties);
}

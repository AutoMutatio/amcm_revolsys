package com.revolsys.record.io.format.saif.util;

import java.io.IOException;

public interface OsnConverter {
  Object read(final OsnIterator iterator);

  void write(final OsnSerializer serializer, Object object) throws IOException;

  default void writeAttribute(final OsnSerializer serializer, final Object object,
    final String name) throws IOException {
    final Object value = com.revolsys.util.Property.getSimple(object, name);
    if (value != null) {
      serializer.endLine();
      serializer.attribute(name, value, false);
    }
  }

  default void writeAttributeEnum(final OsnSerializer serializer, final Object object,
    final String name) throws IOException {
    final String value = com.revolsys.util.Property.getSimple(object, name);
    if (value != null) {
      serializer.endLine();
      serializer.attributeEnum(name, value, false);
    }
  }
}

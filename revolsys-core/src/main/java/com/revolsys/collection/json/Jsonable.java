package com.revolsys.collection.json;

import java.io.IOException;
import java.io.Writer;

import com.revolsys.exception.Exceptions;
import com.revolsys.io.JavaIo;

public interface Jsonable {
  default Appendable appendJson(final Appendable appendable) {
    final JsonType json = asJson();
    if (json != null) {
      json.appendJson(appendable);
    }
    return appendable;
  }

  default Appendable appendJsonFormatted(final Appendable appendable) {
    final JsonType json = asJson();
    if (json != null) {
      json.appendJsonFormatted(appendable);
    }
    return appendable;
  }

  /**
   * Cast this object as a {@link JsonType} instance representing the data in this
   * instance. If
   * this class is a {@link JsonType} it maybe returned instead of a copy. This
   * method should only
   * be used if the changes to the returned instance are expected to update this
   * instance or to
   * serialize the result.
   *
   * @return The instance.
   */
  default JsonType asJson() {
    return toJson();
  }

  /**
   * Create a new {@link JsonType} instance representing the data in this
   * instance.
   *
   * @return The new instance.
   */
  JsonType toJson();

  default String toJsonString() {
    final StringBuilder string = new StringBuilder();
    appendJson(string);
    return string.toString();
  }

  default String toJsonString(final boolean indent) {
    final JsonType json = asJson();
    if (json == null) {
      return null;
    } else {
      return json.toJsonString(indent);
    }
  }

  default void writeTo(final Object target) {
    try (
      Writer writer = JavaIo.createWriter(target)) {
      appendJson(writer);
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }

  }

  default void writeToFormatted(final Object target) {
    try (
      Writer writer = JavaIo.createWriter(target)) {
      appendJsonFormatted(writer);
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }

  }
}

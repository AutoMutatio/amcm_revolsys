package com.revolsys.record.io.format.json;

import java.math.BigDecimal;

public interface JsonProcessor {
  default void after() {
  }

  default void before() {
  }

  default void beforeArrayValue(final JsonStatus status) {
  }

  default void endArray(final JsonStatus status) {
  }

  default void endDocument(final JsonStatus status) {
  }

  default void endObject(final JsonStatus status) {
  }

  default void label(final JsonStatus status, final String label) {
  }

  default void nullValue(final JsonStatus status) {
  }

  default void onCancel() {
  }

  default void onComplete() {
  }

  default void startArray(final JsonStatus status) {
  }

  default void startDocument(final JsonStatus status) {
  }

  default void startObject(final JsonStatus status) {
  }

  default void value(final JsonStatus status, final BigDecimal value) {
  }

  default void value(final JsonStatus status, final boolean value) {
  }

  default void value(final JsonStatus status, final String value) {
  }
}

package com.revolsys.record.io.format.json;

import java.math.BigDecimal;

import com.revolsys.reactive.chars.Processor;

public interface JsonProcessor extends Processor {
  default void after() {
  }

  default void before() {
  }

  default void beforeArrayValue(JsonStatus status) {
  }

  default void endArray(JsonStatus status) {
  }

  default void endDocument(JsonStatus status) {
  }

  default void endObject(JsonStatus status) {
  }

  default void label(JsonStatus status, String label) {
  }

  default void nullValue(JsonStatus status) {
  }

  default void startArray(JsonStatus status) {
  }

  default void startDocument(JsonStatus status) {
  }

  default void startObject(JsonStatus status) {
  }

  default void value(JsonStatus status, BigDecimal value) {
  }

  default void value(JsonStatus status, boolean value) {
  }

  default void value(JsonStatus status, String value) {
  }
}

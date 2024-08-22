package com.revolsys.record.io.format.odata;

public interface ODataTypeDefinition {

  String getODataName();

  default void odataInitialize() {
  }

}

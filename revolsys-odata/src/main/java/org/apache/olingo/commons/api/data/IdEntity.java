package org.apache.olingo.commons.api.data;

import java.net.URI;

public record IdEntity(URI id) implements ODataEntity {

  @Override
  public URI getId() {
    return this.id;
  }

}

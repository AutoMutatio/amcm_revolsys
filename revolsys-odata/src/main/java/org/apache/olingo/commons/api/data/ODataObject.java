package org.apache.olingo.commons.api.data;

import java.net.URI;

public interface ODataObject {

  /**
   * Gets base URI.
   * @return base URI
   */
  default URI getBaseURI() {
    return null;
  }

  /**
   * Gets ID.
   * @return ID.
   */
  default URI getId() {
    return null;
  }

  /**
   * Gets title.
   * @return title
   */
  default String getTitle() {
    return null;
  }

  default void setId(final URI uri) {
  }

}

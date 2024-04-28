package org.apache.olingo.commons.api.data;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public interface ODataPropertyMap {

  default ListEx<String> getFieldNames() {
    return Lists.empty();
  }

  /**
   * Gets property with given name.
   *
   * @param name property name
   * @return property with given name if found, null otherwise
   */
  Property getProperty(String name);

  default Object getValue(final String name) {
    return null;
  }
}

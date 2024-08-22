package org.apache.olingo.commons.api.data;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public interface ODataPropertyMap {

  default ListEx<Annotation> getAnnotations(final String name) {
    return null;
  }

  default ListEx<String> getFieldNames() {
    return Lists.empty();
  }

  default <V> V getValue(final String name) {
    return null;
  }
}

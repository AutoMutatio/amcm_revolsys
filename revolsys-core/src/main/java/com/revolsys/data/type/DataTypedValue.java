package com.revolsys.data.type;

import java.util.Collection;

public interface DataTypedValue {

  boolean equals(Object object, Collection<? extends CharSequence> excludeFieldNames);

}

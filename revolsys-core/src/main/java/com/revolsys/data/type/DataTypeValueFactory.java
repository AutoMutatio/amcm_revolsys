package com.revolsys.data.type;

import java.util.function.Supplier;

public interface DataTypeValueFactory<V> extends DataTypeProxy, Supplier<V> {

}

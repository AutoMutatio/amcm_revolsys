package com.revolsys.data.type;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.revolsys.function.Function3;

public class FunctionDataType<O> extends AbstractDataType {

  public static class Builder<O2> {

    private Function3<Object, Object, Collection<? extends CharSequence>, Boolean> equalsExcludesFunction;

    private BiFunction<Object, Object, Boolean> equalsFunction;

    private Class<?> javaClass;

    private String name;

    private boolean requiresQuotes = true;

    private Function<Object, ?> toObjectFunction;

    private Function<Object, String> toStringFunction = Object::toString;

    private BiFunction<O2, Number, O2> plusFunction;

    public FunctionDataType<O2> build() {
      return new FunctionDataType<>(this.name, this.javaClass, this.requiresQuotes,
        this.equalsExcludesFunction, this.equalsFunction, this.toObjectFunction,
        this.toStringFunction, this.plusFunction);
    }

    public Builder<O2> equalsExcludesFunction(
      final Function3<Object, Object, Collection<? extends CharSequence>, Boolean> equalsExcludesFunction) {
      this.equalsExcludesFunction = Objects.requireNonNull(equalsExcludesFunction);
      return this;
    }

    public Builder<O2> equalsFunction(final BiFunction<Object, Object, Boolean> equalsFunction) {
      this.equalsFunction = Objects.requireNonNull(equalsFunction);
      return this;
    }

    public Builder<O2> javaClass(final Class<?> javaClass) {
      this.javaClass = Objects.requireNonNull(javaClass);
      return this;
    }

    public Builder<O2> name(final String name) {
      this.name = Objects.requireNonNull(name);
      return this;
    }

    public Builder<O2> plusFunction(final BiFunction<O2, Number, O2> plusFunction) {
      this.plusFunction = Objects.requireNonNull(plusFunction);
      return this;
    }

    public Builder<O2> requiresQuotes(final boolean requiresQuotes) {
      this.requiresQuotes = requiresQuotes;
      return this;
    }

    public Builder<O2> toObjectFunction(final Function<Object, ?> toObjectFunction) {
      this.toObjectFunction = Objects.requireNonNull(toObjectFunction);
      return this;
    }

    public Builder<O2> toStringFunction(final Function<Object, String> toStringFunction) {
      this.toStringFunction = Objects.requireNonNull(toStringFunction);
      return this;
    }
  }

  @SuppressWarnings("unchecked")
  public static BiFunction<Object, Object, Boolean> MAP_EQUALS = (object1, object2) -> {
    final Map<Object, Object> map1 = (Map<Object, Object>)object1;
    final Map<Object, Object> map2 = (Map<Object, Object>)object2;
    if (map1.size() == map2.size()) {
      final Set<Object> keys1 = map1.keySet();
      final Set<Object> keys2 = map2.keySet();
      if (keys1.equals(keys2)) {
        for (final Object key : keys1) {
          final Object value1 = map1.get(key);
          final Object value2 = map2.get(key);
          if (!DataType.equal(value1, value2)) {
            return false;
          }
        }
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  };

  @SuppressWarnings("unchecked")
  public static final Function3<Object, Object, Collection<? extends CharSequence>, Boolean> MAP_EQUALS_EXCLUDES = (
    object1, object2, exclude) -> {
    final Map<Object, Object> map1 = (Map<Object, Object>)object1;
    final Map<Object, Object> map2 = (Map<Object, Object>)object2;
    final Set<Object> keys = new TreeSet<>();
    keys.addAll(map1.keySet());
    keys.addAll(map2.keySet());
    keys.removeAll(exclude);

    for (final Object key : keys) {
      final Object value1 = map1.get(key);
      final Object value2 = map2.get(key);
      if (!DataType.equal(value1, value2, exclude)) {
        return false;
      }
    }
    return true;
  };

  public static <O3> Builder<O3> builder(final String name, final Class<O3> javaClass) {
    return new Builder<O3>().name(name)
      .javaClass(javaClass);
  }

  private final BiFunction<O, Number, O> plusFunction;

  private final Function3<Object, Object, Collection<? extends CharSequence>, Boolean> equalsExcludesFunction;

  private final BiFunction<Object, Object, Boolean> equalsFunction;

  private final Function<Object, ?> toObjectFunction;

  private final Function<Object, String> toStringFunction;

  public FunctionDataType(final String name, final Class<?> javaClass, final boolean requiresQuotes,
    final Function3<Object, Object, Collection<? extends CharSequence>, Boolean> equalsExcludesFunction,
    final BiFunction<Object, Object, Boolean> equalsFunction,
    final Function<Object, ?> toObjectFunction, final Function<Object, String> toStringFunction,
    final BiFunction<O, Number, O> plusFunction) {
    super(name, javaClass, requiresQuotes);
    this.toObjectFunction = toObjectFunction;
    if (toStringFunction == null) {
      this.toStringFunction = Object::toString;
    } else {
      this.toStringFunction = toStringFunction;
    }
    if (equalsFunction == null) {
      if (equalsExcludesFunction == null) {
        this.equalsFunction = Object::equals;
      } else {
        this.equalsFunction = (value1, value2) -> equalsNotNull(value1, value2,
          Collections.emptySet());
      }
    } else {
      this.equalsFunction = equalsFunction;
    }
    if (equalsExcludesFunction == null) {
      if (equalsFunction == null) {
        this.equalsExcludesFunction = (value1, value2, _) -> value1.equals(value2);
      } else {
        this.equalsExcludesFunction = (value1, value2, _) -> this.equalsFunction.apply(value1,
          value2);
      }
    } else {
      this.equalsExcludesFunction = equalsExcludesFunction;
    }
    this.plusFunction = plusFunction;
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    return this.equalsFunction.apply(value1, value2);
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2,
    final Collection<? extends CharSequence> excludeFieldNames) {
    return this.equalsExcludesFunction.apply(value1, value2, excludeFieldNames);
  }

  @Override
  public boolean isMathSupported() {
    return this.plusFunction != null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V plus(final Object value, final Number number) {
    final O n = toObject(value);
    return (V)this.plusFunction.apply(n, number);
  }

  @Override
  protected Object toObjectDo(final Object value) {
    return this.toObjectFunction.apply(value);
  }

  @Override
  public String toStringDo(final Object value) {
    return this.toStringFunction.apply(value);
  }
}

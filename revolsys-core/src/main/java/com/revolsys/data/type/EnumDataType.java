package com.revolsys.data.type;

public class EnumDataType<E extends Enum<E>> extends SimpleDataType {

  private final Class<E> enumClass;

  public EnumDataType(final Class<E> enumClass) {
    super(enumClass.getSimpleName(), enumClass);
    this.enumClass = enumClass;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V toObject(final Object value) {
    if (value == null) {
      return null;
    }
    return (V)Enum.valueOf(this.enumClass, value.toString());
  }

}

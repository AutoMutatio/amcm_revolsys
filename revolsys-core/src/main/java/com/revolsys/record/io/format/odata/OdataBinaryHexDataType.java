package com.revolsys.record.io.format.odata;

import com.revolsys.data.type.AbstractDataType;

public class OdataBinaryHexDataType extends AbstractDataType {

  public static final OdataBinaryHexDataType INSTANCE = new OdataBinaryHexDataType();

  private OdataBinaryHexDataType() {
    super("hexBinary", Byte.TYPE, true);
  }

  @Override
  public boolean equals(final Object obj) {
    // TODO Auto-generated method stub
    return super.equals(obj);
  }

  private int fromHex(final char c) {
    if ('0' <= c && c <= '9') {
      return c - '0';
    } else if ('a' <= c && c <= 'f') {
      return 10 + c - 'a';
    } else if ('A' <= c && c <= 'F') {
      return 10 + c - 'A';
    } else {
      throw new IllegalArgumentException("Not a hex char: " + c);
    }
  }

  private byte[] toBytes(final CharSequence string) {
    final int length = string.length();
    final byte[] bytes = new byte[length / 2];
    int j = 0;
    for (int i = 0; i < length;) {
      bytes[j++] = (byte)(fromHex(string.charAt(i++)) << 4 + fromHex(string.charAt(i++)));
    }
    return bytes;
  }

  @Override
  public <V> V toObject(final Object value) {
    if (value instanceof final byte[] bytes) {
      return (V)bytes;
    } else if (value instanceof final CharSequence chars) {
      return (V)toBytes(chars);
    }
    return null;
  }

  @Override
  public <V> V toObject(final String string) {
    return (V)toBytes(string);
  }
}

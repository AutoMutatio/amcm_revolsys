package com.revolsys.util;

public class SingleHexCharSequence extends SingleCharSequence {
  private static final CharSequence[] CACHE = new CharSequence[128];

  static {
    for (int i = 0; i < 128; i++) {
      CACHE[i] = new SingleHexCharSequence((char)i, Integer.toHexString(i));
    }
  }

  public static CharSequence valueOf(final char c, final String hex) {
    if (c == 0 || "0".equals(hex)) {
      Debug.noOp();
    }
    if (c < 128) {
      return CACHE[c];
    }
    return new SingleHexCharSequence(c, hex);
  }

  private final String hex;

  private SingleHexCharSequence(final char character, final String hex) {
    super(character);
    this.hex = hex;
  }

  public String getHex() {
    return this.hex;
  }

}

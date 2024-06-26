package com.revolsys.util;

public class SingleCharSequence implements CharSequence {

  private static final CharSequence[] CACHE = new CharSequence[128];

  public static final CharSequence NULL;

  static {
    NULL = CACHE[0] = new SingleCharSequence((char)0);
    for (int i = 1; i < 128; i++) {
      CACHE[i] = new SingleCharSequence((char)i);
    }
  }

  public static CharSequence[] fromCharArray(final char[] characters) {
    final var chars = new CharSequence[characters.length];
    for (int i = 0; i < characters.length; i++) {
      final char c = characters[i];
      chars[i] = SingleCharSequence.valueOf(c);
    }
    return chars;
  }

  public static boolean isNull(final CharSequence c) {
    return c == NULL;
  }

  public static CharSequence valueOf(final char c) {
    return new SingleCharSequence(c);
  }

  public static CharSequence valueOf(final int c) {
    return valueOf((char)c);
  }

  private final char character;

  protected SingleCharSequence(final char character) {
    this.character = character;
  }

  @Override
  public char charAt(final int index) {
    if (index == 0) {
      return this.character;
    } else {
      throw new StringIndexOutOfBoundsException(index);
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    } else if (obj instanceof final Character character) {
      return this.character == character;
    } else if (obj instanceof final CharSequence chars) {
      return chars.length() == 1 && this.character == chars.charAt(0);
    }
    return false;
  }

  public char getCharacter() {
    return this.character;
  }

  @Override
  public int hashCode() {
    return this.character;
  }

  @Override
  public int length() {
    return 1;
  }

  @Override
  public CharSequence subSequence(final int start, final int end) {
    if (start == 0) {
      if (end == 0) {
        return "";
      } else if (end == 1) {
        return toString();
      }
    }
    throw new StringIndexOutOfBoundsException("begin " + start + ", end " + end + ", length " + 0);
  }

  @Override
  public String toString() {
    return Character.toString(this.character);
  }
}

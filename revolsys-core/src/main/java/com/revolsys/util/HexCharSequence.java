package com.revolsys.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;

public class HexCharSequence implements CharSequence {

  public record Builder(Charset charset) {
    public HexCharSequence create(final CharSequence hex) {
      final var hexString = hex.toString();
      return create(hexString);
    }

    public CharSequence createOrSingle(final CharSequence hex) {
      final var hexString = hex.toString();
      return createOrSingle(hexString);
    }

    public HexCharSequence create(final String hex) {
      final var bytes = HexFormat.of()
        .parseHex(hex);
      final var text = new String(bytes, this.charset);
      return new HexCharSequence(text, hex);
    }

    public CharSequence createOrSingle(final String hex) {
      final var bytes = HexFormat.of()
        .parseHex(hex);
      final var text = new String(bytes, this.charset);
      if (text.length() == 1) {
        return SingleHexCharSequence.valueOf(text.charAt(0), hex);
      } else {
        return new HexCharSequence(text, hex);
      }
    }
  }

  public static Builder UTF16 = new Builder(StandardCharsets.UTF_16BE);

  private final String text;

  private final String hex;

  private HexCharSequence(final String text, final String hex) {
    this.text = Objects.requireNonNull(text);
    this.hex = Objects.requireNonNull(hex);
  }

  @Override
  public char charAt(final int index) {
    return this.text.charAt(index);
  }

  public HexCharSequence create(final CharSequence hex) {
    final var hexString = hex.toString();
    return create(hexString);
  }

  public HexCharSequence create(final String hex) {
    final var bytes = HexFormat.of()
      .parseHex(hex);
    final var text = new String(bytes, StandardCharsets.UTF_16BE);
    return new HexCharSequence(text, hex);
  }

  public CharSequence createOrSingle(final CharSequence hex) {
    final var bytes = HexFormat.of()
      .parseHex(hex.toString());
    final var text = new String(bytes, StandardCharsets.UTF_16BE);
    if (text.length() == 1) {
      return SingleCharSequence.valueOf(text.charAt(0));
    } else {
      return text.toString();
    }
  }

  @Override
  public boolean equals(final Object obj) {
    return this.text.equals(obj);
  }

  public String getHex() {
    return this.hex;
  }

  @Override
  public int hashCode() {
    return this.text.hashCode();
  }

  @Override
  public int length() {
    return this.text.length();
  }

  @Override
  public CharSequence subSequence(final int start, final int end) {
    return this.text.subSequence(start, end);
  }

  @Override
  public String toString() {
    return this.text.toString();
  }
}

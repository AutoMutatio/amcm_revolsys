package com.revolsys.reactive.chars;

import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;

import com.revolsys.exception.Exceptions;

public interface CharacterProcessor {

  public static CharacterProcessor from(final Appendable string) {
    return c -> {
      try {
        string.append(c);
      } catch (final IOException e) {
        throw Exceptions.toRuntimeException(e);
      }
      return true;
    };
  }

  public static CharacterProcessor from(final Writer writer) {
    return new ClosableCharacterProcessor(() -> {
      writer.flush();
      writer.close();
    }, c -> {
      try {
        writer.write(c);
        return true;
      } catch (final IOException e) {
        throw Exceptions.toRuntimeException(e);
      }
    });
  }

  default void onCancel() {
  }

  default void onComplete() {
  }

  boolean process(char c);

  default boolean process(final CharBuffer chars) {
    try {
      while (chars.hasRemaining()) {
        final char c = chars.get();
        if (!process(c)) {
          return false;
        }
      }
      return true;
    } catch (final Exception e) {
      return Exceptions.throwUncheckedException(e);
    }
  }

  default void process(final CharSequence sequence) {
    final int count = sequence.length();
    process(sequence, 0, count);

  }

  default void process(final CharSequence sequence, final int offset, final int count) {
    for (int i = offset; i < count; i++) {
      final char c = sequence.charAt(i);
      process(c);
    }
  }
}

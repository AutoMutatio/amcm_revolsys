package com.revolsys.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.number.Doubles;

public class UuidBuilder {

  private final MessageDigest digester;

  private final int type;

  UuidBuilder(final int type, final MessageDigest digester) {
    this.type = type;
    this.digester = digester;
  }

  public UuidBuilder append(final byte[] bytes) {
    if (bytes != null) {
      this.digester.update(bytes);
    }
    return this;
  }

  public UuidBuilder append(final CharSequence string) {
    if (string != null) {
      append(string.toString());
    }
    return this;
  }

  public UuidBuilder append(final Iterable<String> values) {
    if (values != null) {
      boolean first = true;
      for (final var string : values) {
        if (first) {
          first = false;
        } else {
          separator();
        }
        append(string);
      }
    }
    return this;
  }

  public UuidBuilder append(final Object value) {
    if (value == null) {
      append("null");
    } else if (value instanceof final String string) {
      append(string);
    } else if (value instanceof final Float number) {
      append(Doubles.toString(number));
    } else if (value instanceof final Double number) {
      append(Doubles.toString(number));
    } else if (value != null) {
      append(value.toString());
    }
    return this;
  }

  public UuidBuilder append(final String string) {
    if (string != null) {
      final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
      append(bytes);
    }
    return this;
  }

  public UUID build() {
    final byte[] digest = this.digester.digest();
    return UuidNamespace.toUuid(this.type, digest);
  }

  public Identifier newStringIdentifier() {
    final String string = toString();
    return Identifier.newIdentifier(string);
  }

  public UuidBuilder separator() {
    return append(""); // UNICODE Group Separator
                        // https://unicode-table.com/en/001D/
  }

  @Override
  public String toString() {
    final UUID uuid = build();
    return uuid.toString();
  }
}

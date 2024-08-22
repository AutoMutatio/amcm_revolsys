package com.revolsys.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Base64;

public interface MessageDigestProxy {
  default byte[] getDigest() {
    return getMessageDigest().digest();
  }

  default String getDigestBase64() {
    final byte[] digest = getDigest();
    return Base64.getEncoder()
      .encodeToString(digest);
  }

  default String getDigestHex() {
    final byte[] digest = getDigest();
    return Hex.toHex(digest);
  }

  MessageDigest getMessageDigest();

  default void update(final byte input) {
    getMessageDigest().update(input);
  }

  default void update(final byte[] input) {
    getMessageDigest().update(input);
  }

  default void update(final byte[] input, final int offset, final int len) {
    getMessageDigest().update(input, offset, len);
  }

  default void update(final ByteBuffer input) {
    getMessageDigest().update(input);
  }
}

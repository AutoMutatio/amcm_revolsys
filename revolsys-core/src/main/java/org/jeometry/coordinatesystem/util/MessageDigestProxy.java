package org.jeometry.coordinatesystem.util;

import java.security.MessageDigest;
import java.util.Base64;

public interface MessageDigestProxy {
  default byte[] getDigest() {
    return getMessageDigest().digest();
  }

  default String getDigestBase64() {
    final byte[] digest = getDigest();
    return Base64.getEncoder().encodeToString(digest);
  }

  default String getDigestHex() {
    final byte[] digest = getDigest();
    return Hex.toHex(digest);
  }

  MessageDigest getMessageDigest();
}

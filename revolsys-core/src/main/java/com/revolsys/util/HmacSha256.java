package com.revolsys.util;

import java.util.Base64;

import javax.crypto.spec.SecretKeySpec;

public class HmacSha256 {
  private static final String ALGORITHM = "HmacSHA256";

  public static MacBuilder newMac(final SecretKeySpec secretKey) {
    return MacBuilder.create(secretKey);
  }

  public static SecretKeySpec newSecretKeySpec(final byte[] bytes) {
    return new SecretKeySpec(bytes, ALGORITHM);
  }

  public static SecretKeySpec newSecretKeySpecBase64(final String key) {
    final var bytes = Base64.getDecoder()
      .decode(key);
    return newSecretKeySpec(bytes);
  }
}

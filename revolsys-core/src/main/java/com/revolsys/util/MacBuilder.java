package com.revolsys.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.SecretKey;

import com.revolsys.exception.Exceptions;

public record MacBuilder(Mac mac) {

  public static MacBuilder create(final SecretKey secretKey) {
    try {
      final var algorithm = secretKey.getAlgorithm();
      final var mac = Mac.getInstance(algorithm);
      mac.init(secretKey);
      return new MacBuilder(mac);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public byte[] build() {
    return this.mac.doFinal();
  }

  public MacBuilder update(final Object data) {
    if (data != null) {
      if (data instanceof final byte[] bytes) {
        this.mac.update(bytes);
      } else {
        final var s = data.toString();
        update(s);
      }
    }
    return this;
  }

  public MacBuilder update(final String data) {
    this.mac.update(data.getBytes(StandardCharsets.UTF_8));
    return this;
  }

  public String buildBase64() {
    final byte[] signedBytes = build();
    return Base64.getEncoder()
      .encodeToString(signedBytes);
  }
}

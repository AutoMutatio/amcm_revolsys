package com.revolsys.util;

import java.security.MessageDigest;

public record MessageDigestEx(MessageDigest digest) implements MessageDigestProxy {

  @Override
  public MessageDigest getMessageDigest() {
    return digest();
  }

}

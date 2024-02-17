package com.revolsys.io.stream;

import java.io.InputStream;
import java.security.MessageDigest;

import com.revolsys.util.MessageDigestProxy;

public class DigestInputStream extends java.security.DigestInputStream
  implements MessageDigestProxy {

  public DigestInputStream(final InputStream stream, final MessageDigest digest) {
    super(stream, digest);
  }

}

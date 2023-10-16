package org.jeometry.coordinatesystem.util;

import java.io.InputStream;
import java.security.MessageDigest;

public class DigestInputStream extends java.security.DigestInputStream
  implements MessageDigestProxy {

  public DigestInputStream(final InputStream stream, final MessageDigest digest) {
    super(stream, digest);
  }

}

package org.jeometry.coordinatesystem.util;

import java.io.OutputStream;
import java.security.MessageDigest;

public class DigestOutputStream extends java.security.DigestOutputStream
  implements MessageDigestProxy {

  public DigestOutputStream(final OutputStream stream, final MessageDigest digest) {
    super(stream, digest);
  }

}

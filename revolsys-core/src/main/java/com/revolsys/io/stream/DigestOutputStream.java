package com.revolsys.io.stream;

import java.io.OutputStream;
import java.security.MessageDigest;

import com.revolsys.util.MessageDigestProxy;

public class DigestOutputStream extends java.security.DigestOutputStream
  implements MessageDigestProxy {

  public DigestOutputStream(final OutputStream stream, final MessageDigest digest) {
    super(stream, digest);
  }

}

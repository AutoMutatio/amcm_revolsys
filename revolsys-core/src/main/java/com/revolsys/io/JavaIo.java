package com.revolsys.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

import com.revolsys.spring.resource.Resource;

public class JavaIo extends org.jeometry.common.io.JavaIo {
  @Override
  public OutputStream newOutputStream(final Object target) throws IOException {
    // TODO folder connection
    if (target instanceof final Resource resource) {
      return resource.newOutputStream();
    }
    return null;
  }

  @Override
  public Writer newWriter(final Object target) throws IOException {
    if (target instanceof final Resource resource) {
      return resource.newWriter();
    }
    // TODO folder connection
    return super.newWriter(target);
  }
}

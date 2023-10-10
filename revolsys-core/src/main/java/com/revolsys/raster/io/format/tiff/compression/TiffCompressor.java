package com.revolsys.raster.io.format.tiff.compression;

import java.io.IOException;
import java.io.OutputStream;

import org.jeometry.common.util.BaseCloseable;

public interface TiffCompressor extends BaseCloseable {
  default void endRow() {
  }

  OutputStream getOututStream();

  void writeByte(int value) throws IOException;
}

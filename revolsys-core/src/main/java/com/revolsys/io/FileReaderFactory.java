package com.revolsys.io;

import java.io.File;

import org.jeometry.common.collection.iterator.Reader;

public interface FileReaderFactory<T> {
  Reader<T> newReader(File file);
}

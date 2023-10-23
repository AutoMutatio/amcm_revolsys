package com.revolsys.io;

import java.io.File;

import com.revolsys.collection.iterator.Reader;

public interface FileReaderFactory<T> {
  Reader<T> newReader(File file);
}

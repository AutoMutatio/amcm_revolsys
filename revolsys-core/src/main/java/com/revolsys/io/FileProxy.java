package com.revolsys.io;

import java.io.File;

public interface FileProxy extends FileNameProxy {

  File getFile();

  default File getOrDownloadFile() {
    return getFile();
  }
}

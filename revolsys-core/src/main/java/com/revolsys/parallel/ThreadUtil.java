package com.revolsys.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ThreadUtil {

  public static ExecutorService newVirtualThreadPerTaskExecutor(final String prefix,
    final long start) {
    final ThreadFactory factory = Thread.ofVirtual()
      .name(prefix, start)
      .factory();
    return Executors.newThreadPerTaskExecutor(factory);
  }

}

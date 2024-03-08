package com.revolsys.parallel;

import java.util.concurrent.locks.ReentrantLock;

import com.revolsys.util.BaseCloseable;

public class ThreadUtil {

  public static boolean isInterrupted() {
    return Thread.currentThread()
      .isInterrupted();
  }

  public static BaseCloseable lock(final ReentrantLock lock) {
    lock.lock();
    return lock::unlock;
  }

}

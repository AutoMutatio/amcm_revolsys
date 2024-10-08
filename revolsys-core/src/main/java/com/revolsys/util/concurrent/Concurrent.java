package com.revolsys.util.concurrent;

import java.lang.Thread.Builder.OfPlatform;
import java.lang.Thread.Builder.OfVirtual;
import java.util.function.Consumer;

public class Concurrent {

  public static ClassLoader contextClassLoader() {
    return Thread.currentThread()
      .getContextClassLoader();
  }

  public static ThreadFactoryEx platform() {
    final var factory = Thread.ofPlatform()
      .factory();
    return new ThreadFactoryEx(factory);
  }

  public static ThreadFactoryEx platform(final Consumer<OfPlatform> configurer) {
    final var builder = Thread.ofPlatform();
    if (configurer != null) {
      configurer.accept(builder);
    }
    return ThreadFactoryEx.builder(builder);
  }

  public static ThreadFactoryEx platform(final String prefix) {
    final var factory = Thread.ofPlatform()
      .name(prefix, 0)
      .factory();
    return new ThreadFactoryEx(prefix, factory);
  }

  public static String threadName() {
    return Thread.currentThread()
      .getName();
  }

  public static ThreadFactoryEx virtual() {
    final var factory = Thread.ofVirtual()
      .factory();
    return new ThreadFactoryEx(factory);
  }

  public static ThreadFactoryEx virtual(final Consumer<OfVirtual> configurer) {
    final var builder = Thread.ofVirtual();
    if (configurer != null) {
      configurer.accept(builder);
    }
    return ThreadFactoryEx.builder(builder);
  }

  public static ThreadFactoryEx virtual(final String prefix) {
    final var factory = Thread.ofVirtual()
      .name(prefix, 0)
      .factory();
    return new ThreadFactoryEx(prefix, factory);
  }
}

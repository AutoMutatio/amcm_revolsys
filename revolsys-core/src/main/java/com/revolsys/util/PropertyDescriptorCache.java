package com.revolsys.util;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PropertyDescriptorCache {
  private static Map<Class<?>, PropertyDescriptorCacheForClass> cacheForClass = new WeakHashMap<>();

  private static final ReentrantLock lock = new ReentrantLock();

  public static void clearCache() {
    lock.lock();
    try {
      cacheForClass.clear();
    } finally {
      lock.unlock();
    }
  }

  public static PropertyDescriptor getPropertyDescriptor(final Class<?> clazz,
    final String propertyName) {
    final var propertyDescriptors = getPropertyDescriptors(clazz);
    return propertyDescriptors.getPropertyDescriptor(propertyName);
  }

  public static PropertyDescriptor getPropertyDescriptor(final Object object,
    final String propertyName) {
    if (object == null) {
      return null;
    } else {
      final Class<? extends Object> clazz = object.getClass();
      return getPropertyDescriptor(clazz, propertyName);
    }
  }

  private static PropertyDescriptorCacheForClass getPropertyDescriptors(final Class<?> clazz) {
    lock.lock();
    try {
      var propertyDescriptors = cacheForClass.get(clazz);
      if (propertyDescriptors == null) {
        propertyDescriptors = new PropertyDescriptorCacheForClass(clazz);
        cacheForClass.put(clazz, propertyDescriptors);
      }
      return propertyDescriptors;
    } finally {
      lock.unlock();
    }
  }

  protected static Map<String, Method> getWriteMethods(final Class<?> clazz) {
    final var propertyDescriptors = getPropertyDescriptors(clazz);
    return propertyDescriptors.getWriteMethods();
  }

}

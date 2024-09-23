package com.revolsys.util;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.revolsys.exception.Exceptions;

public class PropertyDescriptorCacheForClass {
  private final Map<String, PropertyDescriptor> propertyDescriptorByName = new HashMap<>();

  private final Map<String, Method> propertyWriteMethodByName = new HashMap<>();

  private final CountDownLatch latch = new CountDownLatch(1);

  PropertyDescriptorCacheForClass(final Class<?> clazz) {
    Thread.ofPlatform().start(() -> {
      try {
        final BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
        for (final PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
          final String propertyName = propertyDescriptor.getName();
          this.propertyDescriptorByName.put(propertyName, propertyDescriptor);
          Method writeMethod = propertyDescriptor.getWriteMethod();
          if (writeMethod == null) {
            final String setMethodName = "set" + Character.toUpperCase(propertyName.charAt(0))
                + propertyName.substring(1);
            try {
              final Class<?> propertyType = propertyDescriptor.getPropertyType();
              writeMethod = clazz.getMethod(setMethodName, propertyType);
              propertyDescriptor.setWriteMethod(writeMethod);
            } catch (NoSuchMethodException | SecurityException e) {
            }
          }
          this.propertyWriteMethodByName.put(propertyName, writeMethod);
        }
      } catch (final IntrospectionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      this.latch.countDown();
    });
  }

  public PropertyDescriptor getPropertyDescriptor(final String propertyName) {
    try {
      this.latch.await();
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return this.propertyDescriptorByName.get(propertyName);
  }

  protected Map<String, Method> getWriteMethods() {
    try {
      this.latch.await();
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return this.propertyWriteMethodByName;
  }

}

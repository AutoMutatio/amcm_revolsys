package com.revolsys.swing.listener;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

/**
 * A property change listener that uses the propertyName and newValue from the {@link PropertyChangeEvent}
 * to set the value for that property on the object.
 *
 */
public class BeanPropertyListener implements PropertyChangeListener {
  private final Reference<Object> reference;

  public BeanPropertyListener(final Object object) {
    this.reference = new WeakReference<>(object);
  }

  @Override
  public void propertyChange(final PropertyChangeEvent event) {
    final Object object = this.reference.get();
    if (object != null) {
      final String propertyName = event.getPropertyName();
      final Object value = event.getNewValue();
      com.revolsys.util.Property.setSimple(object, propertyName, value);
    }
  }
}

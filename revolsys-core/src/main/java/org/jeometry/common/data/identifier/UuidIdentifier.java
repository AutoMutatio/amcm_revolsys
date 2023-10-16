package org.jeometry.common.data.identifier;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.jeometry.common.data.type.DataType;

public class UuidIdentifier implements Identifier, Comparable<Object> {
  private final UUID id;

  protected UuidIdentifier(final UUID id) {
    this.id = id;
  }

  @Override
  public int compareTo(final Object object) {
    Object otherValue;
    if (object instanceof Identifier) {
      final Identifier identifier = (Identifier)object;
      if (identifier.isSingle()) {
        otherValue = identifier.getValue(0);
      } else {
        return -1;
      }
    } else {
      otherValue = object;
    }
    if (otherValue instanceof UUID) {
      final UUID otherId = (UUID)otherValue;
      return this.id.compareTo(otherId);
    } else {
      return this.id.toString().compareTo(otherValue.toString());
    }
  }

  @Override
  public boolean equals(final Identifier identifier) {
    if (identifier != null && identifier.isSingle()) {
      final Object otherValue = identifier.getValue(0);
      return DataType.equal(this.id, otherValue);
    } else {
      return false;
    }
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof Identifier) {
      final Identifier identifier = (Identifier)other;
      return equals(identifier);
    } else {
      return DataType.equal(this.id, other);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final int index) {
    if (index == 0) {
      return (V)this.id;
    } else {
      return null;
    }
  }

  @Override
  public List<Object> getValues() {
    return Collections.singletonList(this.id);
  }

  @Override
  public int hashCode() {
    if (this.id == null) {
      return 0;
    } else {
      return this.id.hashCode();
    }
  }

  @Override
  public String toString() {
    return this.id.toString();
  }
}

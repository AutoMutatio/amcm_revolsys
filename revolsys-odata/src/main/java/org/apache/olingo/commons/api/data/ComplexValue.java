/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.olingo.commons.api.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.revolsys.collection.list.ListEx;

/**
 * Represents the value of a complex property.
 */
public class ComplexValue extends Linked implements ODataPropertyMap {

  private final Map<String, ListEx<Annotation>> annotationByName = new HashMap<>();

  private String typeName;

  private final List<Operation> operations = new ArrayList<>();

  // @Override
  // public boolean equals(final Object o) {
  // return super.equals(o) && this.value.equals(((ComplexValue)o).value);
  // }

  @Override
  public ListEx<Annotation> getAnnotations(final String name) {
    return this.annotationByName.getOrDefault(name, ListEx.empty());
  }

  /**
   * Gets operations.
   *
   * @return operations.
   */
  public List<Operation> getOperations() {
    return this.operations;
  }

  /**
   * Get string representation of type (can be null if not set).
   * @return string representation of type (can be null if not set)
   */
  public String getTypeName() {
    return this.typeName;
  }

  @Override
  public <V> V getValue(final String name) {
    return null;
  }

  @Override
  public int hashCode() {
    final int result = super.hashCode();
    // result = 31 * result + this.value.hashCode();
    return result;
  }

  /**
   * Set string representation of type.
   * @param type string representation of type
   */
  public void setTypeName(final String typeName) {
    this.typeName = typeName;
  }

  // @Override
  // public String toString() {
  // return this.value.toString();
  // }
}

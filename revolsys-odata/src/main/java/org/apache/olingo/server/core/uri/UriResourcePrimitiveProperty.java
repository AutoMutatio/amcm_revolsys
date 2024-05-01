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
package org.apache.olingo.server.core.uri;

import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.uri.UriResourceKind;

public class UriResourcePrimitiveProperty extends UriResourceProperty {

  private final EdmProperty property;

  private final String name;

  public UriResourcePrimitiveProperty(final EdmProperty property, final String name) {
    super(UriResourceKind.primitiveProperty);
    this.property = property;
    this.name = name;
  }

  @Override
  public EdmProperty getProperty() {
    return this.property;
  }

  @Override
  public String getSegmentValue() {
    return getPropertyName();
  }

  public String getPropertyName() {
    if (this.property == null) {
      return this.name;
    } else {
      return this.property.getName();
    }
  }

  @Override
  public EdmType getType() {
    if (this.property == null) {
      return null;
    } else {
      return this.property.getType();
    }
  }

  @Override
  public boolean isCollection() {
    if (this.property == null) {
      return false;
    }
    return this.property.isCollection();
  }

  @Override
  public String toString() {
    return getSegmentValue();
  }
}

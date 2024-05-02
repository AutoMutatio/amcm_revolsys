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
package org.apache.olingo.commons.api.edm.provider;

import java.util.List;

import com.revolsys.record.schema.RecordDefinition;

/**
 * The type Csdl entity type.
 */
public class CsdlEntityType extends CsdlStructuralType<CsdlEntityType> {

  private List<CsdlPropertyRef> key;

  private boolean hasStream = false;

  public CsdlEntityType() {
  }

  public CsdlEntityType(final RecordDefinition recordDefinition) {
    setRecordDefinition(recordDefinition);
  }

  /**
   * Gets key.
   *
   * @return the key
   */
  public List<CsdlPropertyRef> getKey() {
    return this.key;
  }

  /**
   * Has stream.
   *
   * @return the boolean
   */
  public boolean hasStream() {
    return this.hasStream;
  }

  /**
   * Has stream.
   * Duplicate getter according to java naming conventions.
   *
   * @return the boolean
   */
  public boolean isHasStream() {
    return this.hasStream;
  }

  /**
   * Sets has stream.
   *
   * @param hasStream the has stream
   * @return the has stream
   */
  public CsdlEntityType setHasStream(final boolean hasStream) {
    this.hasStream = hasStream;
    return this;
  }

  /**
   * Sets key.
   *
   * @param key the key
   * @return the key
   */
  public CsdlEntityType setKey(final List<CsdlPropertyRef> key) {
    this.key = key;
    return this;
  }

}

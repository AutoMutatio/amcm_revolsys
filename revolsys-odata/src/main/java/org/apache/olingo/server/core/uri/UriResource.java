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

import org.apache.olingo.server.api.uri.UriResourceKind;

/**
 * Abstract class for resource-path elements in URI.
 */
public abstract class UriResource {
  private final UriResourceKind kind;

  public UriResource(final UriResourceKind kind) {
    this.kind = kind;
  }

  public UriResourceKind getKind() {
    return this.kind;
  }

  /**
   * In case of an EntitySet this method will return the EntitySet name. In Case of $ref this method will return '$ref"
   * as a String.
   * @return the value of this URI Resource Segment
   */
  public abstract String getSegmentValue();

  @Override
  public String toString() {
    return getSegmentValue();
  }
}

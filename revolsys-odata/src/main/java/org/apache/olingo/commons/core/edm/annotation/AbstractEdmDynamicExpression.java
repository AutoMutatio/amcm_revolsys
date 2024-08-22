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
package org.apache.olingo.commons.core.edm.annotation;

import org.apache.olingo.commons.api.edm.annotation.EdmPropertyValue;
import org.apache.olingo.commons.core.edm.Edm;

public abstract class AbstractEdmDynamicExpression extends AbstractEdmExpression {

  public AbstractEdmDynamicExpression(final Edm edm, final String name) {
    super(edm, name);
  }

  public EdmAnnotationPath asAnnotationPath() {
    return isAnnotationPath() ? (EdmAnnotationPath)this : null;
  }

  public EdmApply asApply() {
    return isApply() ? (EdmApply)this : null;
  }

  public EdmCast asCast() {
    return isCast() ? (EdmCast)this : null;
  }

  public EdmCollection asCollection() {
    return isCollection() ? (EdmCollection)this : null;
  }

  public EdmIf asIf() {
    return isIf() ? (EdmIf)this : null;
  }

  public EdmIsOf asIsOf() {
    return isIsOf() ? (EdmIsOf)this : null;
  }

  public EdmLabeledElement asLabeledElement() {
    return isLabeledElement() ? (EdmLabeledElement)this : null;
  }

  public EdmLabeledElementReference asLabeledElementReference() {
    return isLabeledElementReference() ? (EdmLabeledElementReference)this : null;
  }

  public EdmNavigationPropertyPath asNavigationPropertyPath() {
    return isNavigationPropertyPath() ? (EdmNavigationPropertyPath)this : null;
  }

  public EdmNull asNull() {
    return isNull() ? (EdmNull)this : null;
  }

  public EdmPath asPath() {
    return isPath() ? (EdmPath)this : null;
  }

  public EdmPropertyPath asPropertyPath() {
    return isPropertyPath() ? (EdmPropertyPath)this : null;
  }

  public EdmPropertyValue asPropertyValue() {
    return isPropertyValue() ? (EdmPropertyValue)this : null;
  }

  public EdmRecord asRecord() {
    return isRecord() ? (EdmRecord)this : null;
  }

  public EdmUrlRef asUrlRef() {
    return isUrlRef() ? (EdmUrlRef)this : null;
  }

  public boolean isAnnotationPath() {
    return this instanceof EdmAnnotationPath;
  }

  public boolean isApply() {
    return this instanceof EdmApply;
  }

  public boolean isCast() {
    return this instanceof EdmCast;
  }

  public boolean isCollection() {
    return this instanceof EdmCollection;
  }

  public boolean isIf() {
    return this instanceof EdmIf;
  }

  public boolean isIsOf() {
    return this instanceof EdmIsOf;
  }

  public boolean isLabeledElement() {
    return this instanceof EdmLabeledElement;
  }

  public boolean isLabeledElementReference() {
    return this instanceof EdmLabeledElementReference;
  }

  public boolean isNavigationPropertyPath() {
    return this instanceof EdmNavigationPropertyPath;
  }

  public boolean isNull() {
    return this instanceof EdmNull;
  }

  public boolean isPath() {
    return this instanceof EdmPath;
  }

  public boolean isPropertyPath() {
    return this instanceof EdmPropertyPath;
  }

  public boolean isPropertyValue() {
    return this instanceof EdmPropertyValue;
  }

  public boolean isRecord() {
    return this instanceof EdmRecord;
  }

  public boolean isUrlRef() {
    return this instanceof EdmUrlRef;
  }

}

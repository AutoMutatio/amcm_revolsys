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
package org.apache.olingo.commons.core.edm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmMapping;
import org.apache.olingo.commons.api.edm.provider.CsdlBindingTarget;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationPropertyBinding;

import com.revolsys.collection.value.ValueHolder;

public abstract class EdmBindingTarget extends AbstractEdmNamed {

  private final CsdlBindingTarget target;

  private final EdmEntityContainer container;

  private final ValueHolder<EdmEntityType> entityType = ValueHolder.lazy(this::initEntityType);

  private List<CsdlNavigationPropertyBinding> navigationPropertyBindings;

  public EdmBindingTarget(final Edm edm, final EdmEntityContainer container,
    final CsdlBindingTarget target) {
    super(edm, target.getName(), target);
    this.container = container;
    this.target = target;
  }

  public EdmEntityContainer getEntityContainer() {
    return this.container;
  }

  public EdmEntityType getEntityType() {
    return this.entityType.getValue();
  }

  public EdmEntityType getEntityTypeWithAnnotations() {
    final EdmEntityType entityType = getEdm()
      .getEntityTypeWithAnnotations(this.target.getTypePathName(), true);
    if (entityType == null) {
      throw new EdmException("Can´t find entity type: " + this.target.getTypePathName()
        + " for entity set or singleton: " + getName());
    }
    return entityType;
  }

  public EdmMapping getMapping() {
    return this.target.getMapping();
  }

  public List<CsdlNavigationPropertyBinding> getNavigationPropertyBindings() {
    if (this.navigationPropertyBindings == null) {
      final List<CsdlNavigationPropertyBinding> providerBindings = this.target
        .getNavigationPropertyBindings();
      final List<CsdlNavigationPropertyBinding> navigationPropertyBindingsLocal = new ArrayList<>();
      if (providerBindings != null) {
        for (final var binding : providerBindings) {
          navigationPropertyBindingsLocal.add(binding);
        }
        this.navigationPropertyBindings = Collections
          .unmodifiableList(navigationPropertyBindingsLocal);
      }
    }
    return this.navigationPropertyBindings;
  }

  public EdmBindingTarget getRelatedBindingTarget(final String path) {
    if (path == null) {
      return null;
    }
    EdmBindingTarget bindingTarget = null;
    boolean found = false;
    for (final Iterator<CsdlNavigationPropertyBinding> itor = getNavigationPropertyBindings()
      .iterator(); itor.hasNext() && !found;) {

      final CsdlNavigationPropertyBinding binding = itor.next();
      if (binding.path() == null || binding.target() == null) {
        throw new EdmException("Path or Target in navigation property binding must not be null!");
      }
      if (path.equals(binding.path())) {
        final Target edmTarget = new Target(binding.target(), this.container);

        final EdmEntityContainer entityContainer = getEdm()
          .getEntityContainer(edmTarget.getEntityContainer());
        if (entityContainer == null) {
          throw new EdmException(
            "Cannot find entity container with name: " + edmTarget.getEntityContainer());
        }
        try {
          bindingTarget = entityContainer.getEntitySet(edmTarget.getTargetName());

          if (bindingTarget == null) {
            throw new EdmException("Cannot find EntitySet " + edmTarget.getTargetName());
          }
        } catch (final EdmException e) {
          // try with singletons ...
          bindingTarget = entityContainer.getSingleton(edmTarget.getTargetName());

          if (bindingTarget == null) {
            throw new EdmException("Cannot find Singleton " + edmTarget.getTargetName(), e);
          }
        } finally {
          found = bindingTarget != null;
        }
      }
    }

    return bindingTarget;
  }

  public String getTitle() {
    return this.target.getTitle();
  }

  private EdmEntityType initEntityType() {
    final EdmEntityType entityType = getEdm().getEntityType(this.target.getTypePathName());
    if (entityType == null) {
      throw new EdmException("Can´t find entity type: " + this.target.getTypePathName()
        + " for entity set or singleton: " + getName());
    }
    return entityType;
  }
}

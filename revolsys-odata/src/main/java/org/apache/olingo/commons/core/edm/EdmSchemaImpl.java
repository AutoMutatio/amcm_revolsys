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
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmAction;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmAnnotations;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmFunction;
import org.apache.olingo.commons.api.edm.EdmSchema;
import org.apache.olingo.commons.api.edm.EdmTerm;
import org.apache.olingo.commons.api.edm.EdmTypeDefinition;
import org.apache.olingo.commons.api.edm.provider.CsdlAction;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotations;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlEnumType;
import org.apache.olingo.commons.api.edm.provider.CsdlFunction;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edm.provider.CsdlTerm;
import org.apache.olingo.commons.api.edm.provider.CsdlTypeDefinition;

import com.revolsys.io.PathName;

public class EdmSchemaImpl extends AbstractEdmAnnotatable implements EdmSchema {

  private final CsdlSchema schema;

  private final Edm edm;

  private final CsdlEdmProvider provider;

  protected final PathName namespace;

  private final PathName alias;

  private final List<EdmEnumType> enumTypes;

  private final List<EdmEntityType> entityTypes;

  private final List<EdmComplexType> complexTypes;

  private final List<EdmAction> actions;

  private final List<EdmFunction> functions;

  private final List<EdmTypeDefinition> typeDefinitions;

  private final List<EdmTerm> terms;

  private final List<EdmAnnotations> annotationGroups;

  private final List<EdmAnnotation> annotations;

  private final EdmEntityContainer entityContainer;

  public EdmSchemaImpl(final Edm edm, final CsdlEdmProvider provider, final CsdlSchema schema) {
    super(edm, schema);
    this.edm = edm;
    this.provider = provider;
    this.schema = schema;
    this.namespace = schema.getNamespace();
    this.alias = schema.getAlias();

    if (this.alias != null) {
      edm.cacheAliasNamespaceInfo(this.alias, this.namespace);
    }

    this.enumTypes = createEnumTypes();
    this.typeDefinitions = createTypeDefinitions();
    this.entityTypes = createEntityTypes();
    this.complexTypes = createComplexTypes();
    this.actions = createActions();
    this.functions = createFunctions();
    this.entityContainer = createEntityContainer();
    this.annotationGroups = createAnnotationGroups();
    this.annotations = createAnnotations();
    this.terms = createTerms();
  }

  protected List<EdmAction> createActions() {
    final List<EdmAction> edmActions = new ArrayList<>();
    final List<CsdlAction> providerActions = this.schema.getActions();
    if (providerActions != null) {
      for (final CsdlAction action : providerActions) {
        final var actionName = this.namespace.newChild(action.getName());
        this.edm.addOperationsAnnotations(action, actionName);
        final EdmActionImpl edmActionImpl = new EdmActionImpl(this.edm, actionName, action);
        edmActions.add(edmActionImpl);
        this.edm.cacheAction(actionName, edmActionImpl);
      }
    }
    return edmActions;
  }

  protected List<EdmAnnotations> createAnnotationGroups() {
    final List<EdmAnnotations> edmAnnotationGroups = new ArrayList<>();
    final List<CsdlAnnotations> providerAnnotations = this.schema.getAnnotationGroups();
    if (providerAnnotations != null) {
      for (final CsdlAnnotations annotationGroup : providerAnnotations) {
        PathName targetName;
        if (annotationGroup.getTarget()
          .contains(".")) {
          targetName = PathName.fromDotSeparated(annotationGroup.getTarget());
        } else {
          targetName = this.namespace.newChild(annotationGroup.getTarget());
        }
        final EdmAnnotationsImpl annotationsImpl = new EdmAnnotationsImpl(this.edm,
          annotationGroup);
        edmAnnotationGroups.add(annotationsImpl);
        this.edm.cacheAnnotationGroup(targetName, annotationsImpl);
      }
    }
    return edmAnnotationGroups;
  }

  protected List<EdmAnnotation> createAnnotations() {
    final List<EdmAnnotation> edmAnnotations = new ArrayList<>();
    final List<CsdlAnnotation> providerAnnotations = this.schema.getAnnotations();
    if (providerAnnotations != null) {
      for (final CsdlAnnotation annotation : providerAnnotations) {
        final EdmAnnotationImpl annotationImpl = new EdmAnnotationImpl(this.edm, annotation);
        edmAnnotations.add(annotationImpl);
      }
    }
    return edmAnnotations;
  }

  protected List<EdmComplexType> createComplexTypes() {
    final List<EdmComplexType> edmComplexTypes = new ArrayList<>();
    final List<CsdlComplexType> providerComplexTypes = this.schema.getComplexTypes();
    if (providerComplexTypes != null) {
      for (final CsdlComplexType complexType : providerComplexTypes) {
        final var comlexTypeName = this.namespace.newChild(complexType.getName());
        this.edm.addStructuralTypeAnnotations(complexType, comlexTypeName,
          this.schema.getEntityContainer());
        final EdmComplexTypeImpl complexTypeImpl = new EdmComplexTypeImpl(this.edm, comlexTypeName,
          complexType);
        edmComplexTypes.add(complexTypeImpl);
        this.edm.cacheComplexType(comlexTypeName, complexTypeImpl);
      }
    }
    return edmComplexTypes;
  }

  protected EdmEntityContainer createEntityContainer() {
    final var entityContainer = this.schema.getEntityContainer();
    if (entityContainer != null) {
      final var containerFQN = this.namespace.newChild(entityContainer.getName());
      this.edm.addEntityContainerAnnotations(entityContainer, containerFQN);
      final var impl = new EdmEntityContainer(this.edm, this.provider, containerFQN,
        entityContainer);
      this.edm.cacheEntityContainer(containerFQN, impl);
      return impl;
    }
    return null;
  }

  protected List<EdmEntityType> createEntityTypes() {
    final List<EdmEntityType> edmEntityTypes = new ArrayList<>();
    final List<CsdlEntityType> providerEntityTypes = this.schema.getEntityTypes();
    if (providerEntityTypes != null) {
      for (final CsdlEntityType entityType : providerEntityTypes) {
        final var entityTypeName = this.namespace.newChild(entityType.getName());
        this.edm.addStructuralTypeAnnotations(entityType, entityTypeName,
          this.schema.getEntityContainer());
        final var entityTypeImpl = new EdmEntityType(this.edm, entityTypeName, entityType);
        edmEntityTypes.add(entityTypeImpl);
        this.edm.cacheEntityType(entityTypeName, entityTypeImpl);
      }
    }
    return edmEntityTypes;
  }

  protected List<EdmEnumType> createEnumTypes() {
    final List<EdmEnumType> enumTyps = new ArrayList<>();
    final List<CsdlEnumType> providerEnumTypes = this.schema.getEnumTypes();
    if (providerEnumTypes != null) {
      for (final CsdlEnumType enumType : providerEnumTypes) {
        final var enumName = this.namespace.newChild(enumType.getName());
        this.edm.addEnumTypeAnnotations(enumType, enumName);
        final EdmEnumType enumTypeImpl = new EdmEnumType(this.edm, enumName, enumType);
        enumTyps.add(enumTypeImpl);
        this.edm.cacheEnumType(enumName, enumTypeImpl);
      }
    }
    return enumTyps;
  }

  protected List<EdmFunction> createFunctions() {
    final List<EdmFunction> edmFunctions = new ArrayList<>();
    final List<CsdlFunction> providerFunctions = this.schema.getFunctions();
    if (providerFunctions != null) {
      for (final CsdlFunction function : providerFunctions) {
        final var functionName = this.namespace.newChild(function.getName());
        this.edm.addOperationsAnnotations(function, functionName);
        final EdmFunction functionImpl = new EdmFunction(this.edm, functionName, function);
        edmFunctions.add(functionImpl);
        this.edm.cacheFunction(functionName, functionImpl);
      }
    }
    return edmFunctions;
  }

  protected List<EdmTerm> createTerms() {
    final List<EdmTerm> edmTerms = new ArrayList<>();
    final List<CsdlTerm> providerTerms = this.schema.getTerms();
    if (providerTerms != null) {
      for (final CsdlTerm term : providerTerms) {
        final var termName = this.namespace.newChild(term.getName());
        final EdmTermImpl termImpl = new EdmTermImpl(this.edm, getNamespace(), term);
        edmTerms.add(termImpl);
        this.edm.cacheTerm(termName, termImpl);
      }
    }
    return edmTerms;
  }

  protected List<EdmTypeDefinition> createTypeDefinitions() {
    final List<EdmTypeDefinition> typeDefns = new ArrayList<>();
    final List<CsdlTypeDefinition> providerTypeDefinitions = this.schema.getTypeDefinitions();
    if (providerTypeDefinitions != null) {
      for (final CsdlTypeDefinition def : providerTypeDefinitions) {
        final var typeDefName = this.namespace.newChild(def.getName());
        this.edm.addTypeDefnAnnotations(def, typeDefName);
        final var typeDefImpl = new EdmTypeDefinition(this.edm, typeDefName, def);
        typeDefns.add(typeDefImpl);
        this.edm.cacheTypeDefinition(typeDefName, typeDefImpl);
      }
    }
    return typeDefns;
  }

  @Override
  public List<EdmAction> getActions() {
    return Collections.unmodifiableList(this.actions);
  }

  @Override
  public PathName getAlias() {
    return this.alias;
  }

  @Override
  public List<EdmAnnotations> getAnnotationGroups() {
    return Collections.unmodifiableList(this.annotationGroups);
  }

  @Override
  public List<EdmAnnotation> getAnnotations() {
    return Collections.unmodifiableList(this.annotations);
  }

  @Override
  public List<EdmComplexType> getComplexTypes() {
    return Collections.unmodifiableList(this.complexTypes);
  }

  @Override
  public EdmEntityContainer getEntityContainer() {
    return this.entityContainer;
  }

  @Override
  public List<EdmEntityType> getEntityTypes() {
    return Collections.unmodifiableList(this.entityTypes);
  }

  @Override
  public List<EdmEnumType> getEnumTypes() {
    return Collections.unmodifiableList(this.enumTypes);
  }

  @Override
  public List<EdmFunction> getFunctions() {
    return Collections.unmodifiableList(this.functions);
  }

  @Override
  public PathName getNamespace() {
    return this.namespace;
  }

  @Override
  public List<EdmTerm> getTerms() {
    return Collections.unmodifiableList(this.terms);
  }

  @Override
  public List<EdmTypeDefinition> getTypeDefinitions() {
    return Collections.unmodifiableList(this.typeDefinitions);
  }
}

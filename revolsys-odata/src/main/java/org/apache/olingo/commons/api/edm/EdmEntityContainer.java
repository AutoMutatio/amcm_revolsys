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
package org.apache.olingo.commons.api.edm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.provider.CsdlActionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlAliasInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlFunctionImport;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlOperationImport;
import org.apache.olingo.commons.api.edm.provider.CsdlSingleton;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.commons.core.edm.AbstractEdmNamed;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.commons.core.edm.EdmActionImportImpl;
import org.apache.olingo.commons.core.edm.EdmSingleton;

import com.revolsys.collection.map.Maps;
import com.revolsys.collection.value.ValueHolder;
import com.revolsys.io.PathName;
import com.revolsys.record.schema.FieldDefinition;

/**
 * A CSDL EntityContainer element.
 *
 * <br/>
 * EdmEntityContainer hold the information of EntitySets, Singletons, ActionImports and FunctionImports contained
 */
public class EdmEntityContainer extends AbstractEdmNamed implements EdmNamed, EdmAnnotatable {

  private final CsdlEdmProvider provider;

  private CsdlEntityContainer container;

  private final PathName entityContainerName;

  private final PathName parentContainerName;

  private List<EdmSingleton> singletons;

  private final Map<String, EdmSingleton> singletonCache = Collections
    .synchronizedMap(new LinkedHashMap<>());

  private List<EdmEntitySet> entitySets;

  private final Map<String, EdmEntitySet> entitySetCache = Collections
    .synchronizedMap(new LinkedHashMap<>());

  private List<EdmActionImport> actionImports;

  private final Map<String, EdmActionImport> actionImportCache = Collections
    .synchronizedMap(new LinkedHashMap<>());

  private final ValueHolder<List<EdmFunctionImport>> functionImports = ValueHolder
    .lazy(this::initFunctionImports);

  private final Map<String, EdmFunctionImport> functionImportByName = Maps
    .lazy(this::createFunctionImport);

  private boolean isAnnotationsIncluded = false;

  private final Map<String, EdmEntitySet> entitySetWithAnnotationsCache = Collections
    .synchronizedMap(new LinkedHashMap<>());

  private final Map<String, EdmSingleton> singletonWithAnnotationsCache = Collections
    .synchronizedMap(new LinkedHashMap<>());

  private boolean isSingletonAnnotationsIncluded = false;

  public EdmEntityContainer(final Edm edm, final CsdlEdmProvider provider,
    final CsdlEntityContainerInfo entityContainerInfo) {
    super(
      edm,
        entityContainerInfo.getContainerName()
          .getName(),
        null);
    this.provider = provider;
    this.entityContainerName = entityContainerInfo.getContainerName();
    this.parentContainerName = entityContainerInfo.getExtendsContainer();
  }

  public EdmEntityContainer(final Edm edm, final CsdlEdmProvider provider,
    final PathName containerFQN, final CsdlEntityContainer entityContainer) {
    super(edm, containerFQN.getName(), entityContainer);
    this.provider = provider;
    this.container = entityContainer;
    this.entityContainerName = containerFQN;
    this.parentContainerName = entityContainer == null ? null
      : entityContainer.getExtendsContainerPathName();
  }

  /**
   * Adds annotations on complex type navigation properties
   * @param complexType
   * @param complexNavProperty
   * @param annotations
   */
  private void addAnnotationsOnComplexTypeNavProperties(final CsdlComplexType complexType,
    final CsdlNavigationProperty complexNavProperty, final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      this.isAnnotationsIncluded = true;
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(complexType.getNavigationProperty(complexNavProperty.getName())
          .getAnnotations(), annotation)) {
          complexType.getNavigationProperty(complexNavProperty.getName())
            .getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * Adds annotations on complex type properties
   * @param complexType
   * @param complexProperty
   * @param annotations
   */
  private void addAnnotationsOnComplexTypeProperties(final CsdlComplexType complexType,
    final FieldDefinition complexProperty, final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      this.isAnnotationsIncluded = true;
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(complexType.getFieldAnnotations(complexProperty.getName()),
          annotation)) {
          complexType.getFieldAnnotations(complexProperty.getName())
            .add(annotation);
        }
      }
    }
  }

  /**
   * Adds annotations on entity sets
   * @param entitySet
   * @param annotations
   */
  private void addAnnotationsOnEntitySet(final CsdlEntitySet entitySet,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      this.isAnnotationsIncluded = true;
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(entitySet.getAnnotations(), annotation)) {
          entitySet.getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * @param entityType
   * @param navProperty
   * @param annotations
   */
  private void addAnnotationsOnETNavProperties(final CsdlEntityType entityType,
    final CsdlNavigationProperty navProperty, final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      this.isAnnotationsIncluded = true;
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(entityType.getNavigationProperty(navProperty.getName())
          .getAnnotations(), annotation)) {
          entityType.getNavigationProperty(navProperty.getName())
            .getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * Adds annotations to Entity type Properties derived from entity set
   * @param entityType
   * @param property
   * @param annotations
   */
  private void addAnnotationsOnETProperties(final CsdlEntityType entityType, final String name,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      this.isAnnotationsIncluded = true;
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(entityType.getFieldAnnotations(name), annotation)) {
          entityType.getFieldAnnotations(name)
            .add(annotation);
        }
      }
    }
  }

  /**
   * Adds annotations on action import
   * @param operationImport
   * @param annotations
   */
  private void addAnnotationsOnOperationImport(final CsdlOperationImport operationImport,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(operationImport.getAnnotations(), annotation)) {
          operationImport.getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * Adds annotations on singleton
   * @param singleton
   * @param annotations
   */
  private void addAnnotationsOnSingleton(final CsdlSingleton singleton,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      this.isSingletonAnnotationsIncluded = true;
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(singleton.getAnnotations(), annotation)) {
          singleton.getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * @param entitySet
   * @param entityContainerName
   * @param complexProperty
   * @param complexType
   * @return
   */
  private void addAnnotationsToComplexTypeIncludedFromES(final CsdlEntitySet entitySet,
    final PathName entityContainerName, final String name, final CsdlComplexType complexType) {
    final var aliasName = getAliasInfo(entityContainerName.getParent());
    for (final var complexPropertyName : complexType.getFields()) {
      removeAnnotationAddedToPropertiesOfComplexType(complexType, complexPropertyName,
        entityContainerName);

      final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
        .get(entityContainerName + "/" + entitySet.getName() + "/" + name + "/"
          + complexPropertyName.getName());
      addAnnotationsOnComplexTypeProperties(complexType, complexPropertyName, annotations);

      final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
        .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "/"
          + entitySet.getName() + "/" + name + "/" + complexPropertyName.getName());
      addAnnotationsOnComplexTypeProperties(complexType, complexPropertyName, annotationsOnAlias);
    }
    for (final CsdlNavigationProperty complexNavProperty : complexType.getNavigationProperties()) {
      checkAnnotationAddedToNavPropertiesOfComplexType(complexType, complexNavProperty,
        entityContainerName);

      final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
        .get(entityContainerName + "/" + entitySet.getName() + "/" + name + "/"
          + complexNavProperty.getName());
      addAnnotationsOnComplexTypeNavProperties(complexType, complexNavProperty, annotations);

      final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
        .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "/"
          + entitySet.getName() + "/" + name + "/" + complexNavProperty.getName());
      addAnnotationsOnComplexTypeNavProperties(complexType, complexNavProperty, annotationsOnAlias);
    }
  }

  /**
   *
   * @param singleton
   * @param entityContainerName2
   * @param annotationGrp
   * @param property
   * @param isComplexNavPropAnnotationsCleared
   * @param complexType
   */
  private void addAnnotationsToComplexTypeIncludedFromSingleton(final CsdlSingleton singleton,
    final String name, final CsdlComplexType complexType) {
    final var aliasName = getAliasInfo(this.entityContainerName.getParent());
    for (final var complexPropertyName : complexType.getFields()) {
      removeAnnotationAddedToPropertiesOfComplexType(complexType, complexPropertyName,
        this.entityContainerName);

      final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
        .get(this.entityContainerName + "/" + singleton.getName() + "/" + name + "/"
          + complexPropertyName.getName());
      addAnnotationsOnComplexTypeProperties(complexType, complexPropertyName, annotations);
      final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
        .get(aliasName.toDotSeparated() + "." + this.entityContainerName.getName() + "/"
          + singleton.getName() + "/" + name + "/" + complexPropertyName.getName());
      addAnnotationsOnComplexTypeProperties(complexType, complexPropertyName, annotationsOnAlias);
    }
    for (final CsdlNavigationProperty complexNavPropertyName : complexType
      .getNavigationProperties()) {
      checkAnnotationAddedToNavPropertiesOfComplexType(complexType, complexNavPropertyName,
        this.entityContainerName);

      final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
        .get(this.entityContainerName + "/" + singleton.getName() + "/" + name + "/"
          + complexNavPropertyName.getName());
      addAnnotationsOnComplexTypeNavProperties(complexType, complexNavPropertyName, annotations);
      final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
        .get(aliasName.toDotSeparated() + "." + this.entityContainerName.getName() + "/"
          + singleton.getName() + "/" + name + "/" + complexNavPropertyName.getName());
      addAnnotationsOnComplexTypeNavProperties(complexType, complexNavPropertyName,
        annotationsOnAlias);
    }
  }

  /**
   * Adds annotations to Entity type Navigation Properties derived from entity set
   * @param entitySet
   * @param entityContainerName
   * @param entityType
   * @param navProperty
   */
  private void addAnnotationsToETNavProperties(final CsdlEntitySet entitySet,
    final PathName entityContainerName, final CsdlEntityType entityType,
    final CsdlNavigationProperty navProperty) {
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName.toDotSeparated() + "/" + entitySet.getName() + "/"
        + navProperty.getName());
    addAnnotationsOnETNavProperties(entityType, navProperty, annotations);

    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "/"
        + entitySet.getName() + "/" + navProperty.getName());
    addAnnotationsOnETNavProperties(entityType, navProperty, annotationsOnAlias);
  }

  /**
   * @param entitySet
   * @param entityContainerName
   * @param entityType
   * @param property
   */
  private void addAnnotationsToETProperties(final CsdlEntitySet entitySet,
    final PathName entityContainerName, final CsdlEntityType entityType, final String name) {
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName.toDotSeparated() + "/" + entitySet.getName() + "/" + name);
    addAnnotationsOnETProperties(entityType, name, annotations);

    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "/"
        + entitySet.getName() + "/" + name);
    addAnnotationsOnETProperties(entityType, name, annotationsOnAlias);
  }

  /** adds annotations to entity type properties derived from singleton
   * E.g of target paths
   * MySchema.MyEntityContainer/MySingleton/MyComplexProperty/MyNavigationProperty
   * @param singleton
   * @param isPropAnnotationsCleared
   * @param isNavPropAnnotationsCleared
   * @param entityType
   * @param entityContainerName
   * @param annotationGrp
   */
  private void addAnnotationsToPropertiesDerivedFromSingleton(final CsdlSingleton singleton,
    final CsdlEntityType entityType, final PathName entityContainerName) {
    String entitySetName = null;
    PathName schemaName = null;
    String containerName = null;
    try {
      final List<CsdlEntitySet> entitySets = this.provider.getEntityContainer() != null
        ? this.provider.getEntityContainer()
          .getEntitySets()
        : new ArrayList<>();
      for (final CsdlEntitySet entitySet : entitySets) {
        entitySetName = entitySet.getName();
        final String entityTypeName = entitySet.getTypePathName()
          .toString();
        if (null != entityTypeName && entityTypeName.equalsIgnoreCase(entitySet.getTypePathName()
          .getParent() + "." + entityType.getName())) {
          containerName = this.provider.getEntityContainer()
            .getName();
          schemaName = entitySet.getTypePathName()
            .getParent();
          for (final var property : entityType.getFields()) {
            final var name = property.getName();
            if (isPropertyComplex(property)) {
              final CsdlComplexType complexType = getComplexTypeFromProperty(property);
              addAnnotationsToComplexTypeIncludedFromSingleton(singleton, name, complexType);
            }
            removeAnnotationsAddedToPropertiesOfEntityType(entityType, property,
              entityContainerName);
            removeAnnotationsAddedToPropertiesViaEntitySet(entityType, name, schemaName,
              containerName, entitySetName);
          }
        }
      }
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  /** Adds annotations to Entity type Properties derived from entity set
   * E.g of target paths
   * MySchema.MyEntityContainer/MyEntitySet/MyProperty
   * MySchema.MyEntityContainer/MyEntitySet/MyNavigationProperty
   * MySchema.MyEntityContainer/MyEntitySet/MyComplexProperty/MyProperty
   * MySchema.MyEntityContainer/MyEntitySet/MyComplexProperty/MyNavigationProperty
   * @param entitySet
   * @param entityContainerName
   * @param entityType
   * @return
   */
  private void addAnnotationsToPropertiesIncludedFromES(final CsdlEntitySet entitySet,
    final PathName entityContainerName, final CsdlEntityType entityType) {
    for (final var property : entityType.getFields()) {
      final var name = property.getName();
      removeAnnotationsAddedToPropertiesOfEntityType(entityType, property, entityContainerName);
      if (isPropertyComplex(property)) {
        final CsdlComplexType complexType = getComplexTypeFromProperty(property);
        addAnnotationsToComplexTypeIncludedFromES(entitySet, entityContainerName, name,
          complexType);
      } else {
        addAnnotationsToETProperties(entitySet, entityContainerName, entityType, name);
      }
    }
    for (final CsdlNavigationProperty navProperty : entityType.getNavigationProperties()) {
      removeAnnotationAddedToNavProperties(entityType, navProperty, entityContainerName);
      addAnnotationsToETNavProperties(entitySet, entityContainerName, entityType, navProperty);
    }
  }

  private void addEntitySetAnnotations(final CsdlEntitySet entitySet,
    final PathName entityContainerName) {
    final CsdlEntityType entityType = getCsdlEntityTypeFromEntitySet(entitySet);
    if (entityType == null) {
      return;
    }

    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName.toDotSeparated() + "/" + entitySet.getName());
    addAnnotationsOnEntitySet(entitySet, annotations);
    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "/"
        + entitySet.getName());
    addAnnotationsOnEntitySet(entitySet, annotationsOnAlias);
    addAnnotationsToPropertiesIncludedFromES(entitySet, entityContainerName, entityType);
  }

  private void addOperationImportAnnotations(final CsdlOperationImport operationImport,
    final PathName entityContainerName) {
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName.toDotSeparated() + "/" + operationImport.getName());
    addAnnotationsOnOperationImport(operationImport, annotations);

    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "/"
        + operationImport.getName());
    addAnnotationsOnOperationImport(operationImport, annotationsOnAlias);
  }

  private void addSingletonAnnotations(final CsdlSingleton singleton,
    final PathName entityContainerName) {
    final CsdlEntityType entityType = fetchEntityTypeFromSingleton(singleton);
    if (entityType == null) {
      return;
    }
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName + "/" + singleton.getName());
    addAnnotationsOnSingleton(singleton, annotations);
    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "/"
        + singleton.getName());
    addAnnotationsOnSingleton(singleton, annotationsOnAlias);
    addAnnotationsToPropertiesDerivedFromSingleton(singleton, entityType, entityContainerName);
  }

  private void checkAnnotationAddedToNavPropertiesOfComplexType(final CsdlComplexType complexType,
    final CsdlNavigationProperty complexNavProperty, final PathName entityContainerName) {
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName.getParent() + "." + complexType.getName() + "/"
        + complexNavProperty.getName());
    removeAnnotationsOnNavProperties(complexNavProperty, annotations);

    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + complexType.getName() + "/"
        + complexNavProperty.getName());
    removeAnnotationsOnNavProperties(complexNavProperty, annotationsOnAlias);
  }

  private boolean compareAnnotations(final List<CsdlAnnotation> annotations,
    final CsdlAnnotation annotation) {
    for (final CsdlAnnotation annot : annotations) {
      if (annot.equals(annotation)) {
        return true;
      }
    }
    return false;
  }

  protected EdmActionImport createActionImport(final String actionImportName) {
    EdmActionImport actionImport = null;

    try {
      final CsdlActionImport providerImport = this.provider
        .getActionImport(this.entityContainerName, actionImportName);
      if (providerImport != null) {
        addOperationImportAnnotations(providerImport, this.entityContainerName);
        actionImport = new EdmActionImportImpl(getEdm(), this, providerImport);
      }
    } catch (final ODataException e) {
      throw new EdmException(e);
    }

    return actionImport;
  }

  protected EdmEntitySet createEntitySet(final String entitySetName) {
    final var providerEntitySet = this.provider.getEntitySet(this.entityContainerName,
      entitySetName);
    if (providerEntitySet != null) {
      addEntitySetAnnotations(providerEntitySet, this.entityContainerName);
      final var entitySet = new EdmEntitySet(getEdm(), this, providerEntitySet);
      if (this.isAnnotationsIncluded) {
        this.entitySetWithAnnotationsCache.put(entitySetName, entitySet);
      } else {
        this.entitySetCache.put(entitySetName, entitySet);
      }
      return entitySet;
    }
    return null;
  }

  protected EdmFunctionImport createFunctionImport(final String functionImportName) {
    final var providerImport = this.provider.getFunctionImport(this.entityContainerName,
      functionImportName);
    if (providerImport != null) {
      addOperationImportAnnotations(providerImport, this.entityContainerName);
      return new EdmFunctionImport(getEdm(), this, providerImport);
    }
    return null;
  }

  protected EdmSingleton createSingleton(final String singletonName) {
    final CsdlSingleton providerSingleton = this.provider.getSingleton(this.entityContainerName,
      singletonName);
    if (providerSingleton != null) {
      addSingletonAnnotations(providerSingleton, this.entityContainerName);
      return new EdmSingleton(getEdm(), this, providerSingleton);
    }
    return null;
  }

  /**
   * @param singleton
   * @return
   */
  private CsdlEntityType fetchEntityTypeFromSingleton(final CsdlSingleton singleton) {
    return singleton.getTypePathName() != null
      ? this.provider.getEntityType(PathName.fromDotSeparated(singleton.getTypePathName()
        .toString()))
      : null;
  }

  public EdmActionImport getActionImport(final String actionImportName) {
    EdmActionImport actionImport = this.actionImportCache.get(actionImportName);
    if (actionImport == null) {
      actionImport = createActionImport(actionImportName);
      if (actionImport != null) {
        this.actionImportCache.put(actionImportName, actionImport);
      }
    }
    return actionImport;
  }

  public List<EdmActionImport> getActionImports() {
    if (this.actionImports == null) {
      loadAllActionImports();
    }
    return Collections.unmodifiableList(this.actionImports);
  }

  /**
   * Get alias name given the namespace from the alias info
   * @param namespace
   * @return
   */
  private PathName getAliasInfo(final PathName namespace) {
    for (final CsdlAliasInfo aliasInfo : this.provider.getAliasInfos()) {
      if (null != aliasInfo.getParent() && aliasInfo.getParent()
        .equalsIgnoreCase(namespace)) {
        return aliasInfo.getAlias();
      }
    }
    return PathName.ROOT;
  }

  /**
   * @param  field
   * @return
   */
  private CsdlComplexType getComplexTypeFromProperty(final FieldDefinition field) {
    CsdlComplexType complexType;
    try {
      complexType = this.provider.getComplexType(Edm.getTypeName(field));
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
    return complexType;
  }

  /**
   * @param entitySet
   * @return
   */
  private CsdlEntityType getCsdlEntityTypeFromEntitySet(final CsdlEntitySet entitySet) {
    CsdlEntityType entityType;
    try {
      entityType = entitySet.getTypePathName() != null
        ? this.provider.getEntityType(PathName.fromDotSeparated(entitySet.getTypePathName()
          .toDotSeparated()))
        : null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
    return entityType;
  }

  public EdmEntitySet getEntitySet(final String entitySetName) {
    EdmEntitySet entitySet = this.entitySetWithAnnotationsCache.get(entitySetName);
    if (entitySet == null) {
      entitySet = this.entitySetCache.get(entitySetName);
      if (entitySet == null) {
        entitySet = createEntitySet(entitySetName);

      }
    }
    getEdm().setIsPreviousES(true);
    return entitySet;
  }

  public List<EdmEntitySet> getEntitySets() {
    if (this.entitySets == null) {
      loadAllEntitySets();
    }
    return Collections.unmodifiableList(this.entitySets);
  }

  public List<EdmEntitySet> getEntitySetsWithAnnotations() {
    loadAllEntitySets();
    return Collections.unmodifiableList(this.entitySets);
  }

  public EdmFunctionImport getFunctionImport(final String functionImportName) {
    return this.functionImportByName.get(functionImportName);
  }

  public List<EdmFunctionImport> getFunctionImports() {
    return this.functionImports.getValue();
  }

  public PathName getNamespace() {
    return this.entityContainerName.getParent();
  }

  public PathName getParentContainerName() {
    return this.parentContainerName;
  }

  public PathName getPathName() {
    return this.entityContainerName;
  }

  public EdmSingleton getSingleton(final String singletonName) {
    EdmSingleton singleton = this.singletonWithAnnotationsCache.get(singletonName);
    if (singleton == null) {
      singleton = this.singletonCache.get(singletonName);
      if (singleton == null) {
        singleton = createSingleton(singletonName);
        if (singleton != null) {
          if (this.isSingletonAnnotationsIncluded) {
            this.singletonWithAnnotationsCache.put(singletonName, singleton);
          } else {
            this.singletonCache.put(singletonName, singleton);
          }
        }
      }
    }
    return singleton;
  }

  public List<EdmSingleton> getSingletons() {
    if (this.singletons == null) {
      loadAllSingletons();
    }
    return Collections.unmodifiableList(this.singletons);
  }

  protected List<EdmFunctionImport> initFunctionImports() {
    loadContainer();
    final List<CsdlFunctionImport> providerFunctionImports = this.container.getFunctionImports();
    final List<EdmFunctionImport> functionImports = new ArrayList<>();

    if (providerFunctionImports != null) {
      for (final var functionImport : providerFunctionImports) {
        addOperationImportAnnotations(functionImport, this.entityContainerName);
        final var impl = new EdmFunctionImport(getEdm(), this, functionImport);
        this.functionImportByName.put(impl.getName(), impl);
        functionImports.add(impl);
      }
    }
    return Collections.unmodifiableList(functionImports);
  }

  private boolean isPropertyComplex(final FieldDefinition field) {
    try {
      return this.provider.getComplexType(Edm.getTypeName(field)) != null ? true : false;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  protected void loadAllActionImports() {
    loadContainer();
    final List<CsdlActionImport> providerActionImports = this.container.getActionImports();
    final List<EdmActionImport> actionImportsLocal = new ArrayList<>();

    if (providerActionImports != null) {
      for (final CsdlActionImport actionImport : providerActionImports) {
        addOperationImportAnnotations(actionImport, this.entityContainerName);
        final EdmActionImportImpl impl = new EdmActionImportImpl(getEdm(), this, actionImport);
        this.actionImportCache.put(actionImport.getName(), impl);
        actionImportsLocal.add(impl);
      }
      this.actionImports = actionImportsLocal;
    }

  }

  protected void loadAllEntitySets() {
    loadContainer();
    final List<CsdlEntitySet> providerEntitySets = this.container.getEntitySets();
    final List<EdmEntitySet> entitySetsLocal = new ArrayList<>();

    if (providerEntitySets != null) {
      for (final CsdlEntitySet entitySet : providerEntitySets) {
        addEntitySetAnnotations(entitySet, this.entityContainerName);
        final var impl = new EdmEntitySet(getEdm(), this, entitySet);
        if (this.isAnnotationsIncluded) {
          this.entitySetWithAnnotationsCache.put(impl.getName(), impl);
        } else {
          this.entitySetCache.put(impl.getName(), impl);
        }
        entitySetsLocal.add(impl);
      }
      this.entitySets = entitySetsLocal;
      getEdm().setIsPreviousES(true);
    }
  }

  protected void loadAllSingletons() {
    loadContainer();
    final List<CsdlSingleton> providerSingletons = this.container.getSingletons();
    final List<EdmSingleton> singletonsLocal = new ArrayList<>();

    if (providerSingletons != null) {
      for (final CsdlSingleton singleton : providerSingletons) {
        addSingletonAnnotations(singleton, this.entityContainerName);
        final EdmSingleton impl = new EdmSingleton(getEdm(), this, singleton);
        this.singletonCache.put(singleton.getName(), impl);
        singletonsLocal.add(impl);
      }
      this.singletons = singletonsLocal;
    }
  }

  private void loadContainer() {
    if (this.container == null) {
      try {
        var containerLocal = this.provider.getEntityContainer();
        if (containerLocal == null) {
          containerLocal = new CsdlEntityContainer().setName(getName());
        }
        getEdm().addEntityContainerAnnotations(containerLocal, this.entityContainerName);
        this.container = containerLocal;
      } catch (final ODataException e) {
        throw new EdmException(e);
      }
    }
  }

  private void removeAnnotationAddedToNavProperties(final CsdlEntityType entityType,
    final CsdlNavigationProperty navProperty, final PathName entityContainerName) {
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(
        entityContainerName.getParent() + "." + entityType.getName() + "/" + navProperty.getName());
    removeAnnotationsOnNavProperties(navProperty, annotations);

    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "."
        + entityType.getName() + "/" + navProperty.getName());
    removeAnnotationsOnNavProperties(navProperty, annotationsOnAlias);
  }

  private void removeAnnotationAddedToPropertiesOfComplexType(final CsdlComplexType complexType,
    final FieldDefinition complexPropertyName, final PathName entityContainerName) {
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName.getParent() + "." + complexType.getName() + "/"
        + complexPropertyName.getName());
    removeAnnotationsOnETProperties(complexPropertyName, annotations);

    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "."
        + complexType.getName() + "/" + complexPropertyName.getName());
    removeAnnotationsOnETProperties(complexPropertyName, annotationsOnAlias);
  }

  /**
   * If annotations are added to properties via entity type path, then remove it
   * @param type
   * @param property
   * @param entityContainerName
   */
  private void removeAnnotationsAddedToPropertiesOfEntityType(final CsdlEntityType type,
    final FieldDefinition property, final PathName entityContainerName) {
    final List<CsdlAnnotation> annotations = getEdm().getAnnotationsMap()
      .get(entityContainerName.getParent() + "." + type.getName() + "/" + property.getName());
    removeAnnotationsOnETProperties(property, annotations);

    final var aliasName = getAliasInfo(entityContainerName.getParent());
    final List<CsdlAnnotation> annotationsOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + entityContainerName.getName() + "." + type.getName()
        + "/" + property.getName());
    removeAnnotationsOnETProperties(property, annotationsOnAlias);
  }

  /**
   * If annotations are added to properties via Entity set then remove them
   * @param entityType
   * @param property
   * @param schemaName
   * @param containerName
   * @param entitySetName
   */
  private void removeAnnotationsAddedToPropertiesViaEntitySet(final CsdlEntityType entityType,
    final String name, final PathName schemaName, final String containerName,
    final String entitySetName) {
    final List<CsdlAnnotation> annotPropDerivedFromES = getEdm().getAnnotationsMap()
      .get(schemaName + "." + containerName + "/" + entitySetName + "/" + name);
    removeAnnotationsOnPropertiesDerivedFromES(entityType, name, annotPropDerivedFromES);
    final var aliasName = getAliasInfo(schemaName);
    final List<CsdlAnnotation> annotPropDerivedFromESOnAlias = getEdm().getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + containerName + "/" + entitySetName + "/" + name);
    removeAnnotationsOnPropertiesDerivedFromES(entityType, name, annotPropDerivedFromESOnAlias);
  }

  /**
   * Removes the annotations added on Entity type
   * properties when there is a target path on entity type
   * @param field
   * @param annotations
   */
  private void removeAnnotationsOnETProperties(final FieldDefinition field,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      for (final CsdlAnnotation annotation : annotations) {
        Edm.getAnnotations(field)
          .remove(annotation);
      }
    }
  }

  /**
   * Removes the annotations added on Entity type
   * navigation properties when there is a target path on entity type
   * @param property
   * @param annotations
   */
  private void removeAnnotationsOnNavProperties(final CsdlNavigationProperty property,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      for (final CsdlAnnotation annotation : annotations) {
        property.getAnnotations()
          .remove(annotation);
      }
    }
  }

  /**
   * Removes the annotations added on properties via Entity Set in case of singleton flow
   * @param entityType
   * @param property
   * @param annotPropDerivedFromES
   */
  private void removeAnnotationsOnPropertiesDerivedFromES(final CsdlEntityType entityType,
    final String name, final List<CsdlAnnotation> annotPropDerivedFromES) {
    if (null != annotPropDerivedFromES && !annotPropDerivedFromES.isEmpty()) {
      for (final CsdlAnnotation annotation : annotPropDerivedFromES) {
        entityType.getFieldAnnotations(name)
          .remove(annotation);
      }
    }
  }
}

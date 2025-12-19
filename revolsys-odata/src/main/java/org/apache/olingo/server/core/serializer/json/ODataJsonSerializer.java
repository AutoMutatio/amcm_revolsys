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
package org.apache.olingo.server.core.serializer.json;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.IConstants;
import org.apache.olingo.commons.api.constants.Constantsv00;
import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.ODataEntity;
import org.apache.olingo.commons.api.data.ODataLinked;
import org.apache.olingo.commons.api.data.ODataPropertyMap;
import org.apache.olingo.commons.api.data.Operation;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.etag.ServiceMetadataETagSupport;
import org.apache.olingo.server.api.serializer.ComplexSerializerOptions;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.EntitySerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.PrimitiveSerializerOptions;
import org.apache.olingo.server.api.serializer.ReferenceCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ReferenceSerializerOptions;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.serializer.SerializerStreamResult;
import org.apache.olingo.server.api.uri.UriHelper;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.LevelsExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.core.ODataWritableContent;
import org.apache.olingo.server.core.serializer.SerializerResultImpl;
import org.apache.olingo.server.core.serializer.utils.CircleStreamBuffer;
import org.apache.olingo.server.core.serializer.utils.ContentTypeHelper;
import org.apache.olingo.server.core.serializer.utils.ExpandSelectHelper;
import org.apache.olingo.server.core.uri.queryoption.ExpandOptionImpl;

import com.revolsys.collection.json.JsonWriter;
import com.revolsys.collection.list.ListEx;
import com.revolsys.data.type.DataType;
import com.revolsys.exception.Exceptions;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryCollection;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.MultiLineString;
import com.revolsys.geometry.model.MultiPoint;
import com.revolsys.geometry.model.MultiPolygon;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Polygon;
import com.revolsys.geometry.model.impl.GeometryCollectionImpl;
import com.revolsys.io.PathName;
import com.revolsys.odata.model.ODataEntityIterator;

public class ODataJsonSerializer implements ODataSerializer {

  private final boolean isODataMetadataNone;

  private final boolean isODataMetadataFull;

  private final IConstants constants;

  private final ODataJsonInstanceAnnotationSerializer instanceAnnotSerializer;

  public ODataJsonSerializer(final ContentType contentType) {
    this.isODataMetadataNone = ContentTypeHelper.isODataMetadataNone(contentType);
    this.isODataMetadataFull = ContentTypeHelper.isODataMetadataFull(contentType);
    this.constants = new Constantsv00();
    this.instanceAnnotSerializer = new ODataJsonInstanceAnnotationSerializer(contentType,
      this.constants);
  }

  public ODataJsonSerializer(final ContentType contentType, final IConstants constants) {
    this.isODataMetadataNone = ContentTypeHelper.isODataMetadataNone(contentType);
    this.isODataMetadataFull = ContentTypeHelper.isODataMetadataFull(contentType);
    this.constants = constants;
    this.instanceAnnotSerializer = new ODataJsonInstanceAnnotationSerializer(contentType,
      constants);
  }

  private void addKeyPropertiesToSelected(final Set<String> selected,
    final EdmStructuredType type) {
    if (!selected.isEmpty() && type instanceof EdmEntityType) {
      final List<String> keyNames = ((EdmEntityType)type).getKeyPredicateNames();
      for (final String key : keyNames) {
        if (!selected.contains(key)) {
          selected.add(key);
        }
      }
    }
  }

  private boolean areKeyPredicateNamesSelected(final SelectOption select,
    final EdmEntityType type) {
    if (select == null || ExpandSelectHelper.isAll(select)) {
      return true;
    }
    final Set<String> selected = ExpandSelectHelper
      .getSelectedPropertyNames(select.getSelectItems());
    for (final String key : type.getKeyPredicateNames()) {
      if (!selected.contains(key)) {
        return false;
      }
    }
    return true;
  }

  ContextURL checkContextURL(final ContextURL contextURL) throws SerializerException {
    if (this.isODataMetadataNone) {
      return null;
    } else if (contextURL == null) {
      throw new SerializerException("ContextURL null!",
        SerializerException.MessageKeys.NO_CONTEXT_URL);
    }
    return contextURL;
  }

  @Override
  public SerializerResult complex(final ServiceMetadata metadata, final EdmComplexType type,
    final String propertyName, final Object value, final ComplexSerializerOptions options)
    throws SerializerException {
    final var complex = (ComplexValue)value;
    final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
    final String name = contextURL == null ? null : contextURL.getEntitySetOrSingletonOrType();
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      final JsonWriter json = new JsonWriter(outputStream, true);) {
      json.startObject();
      writeContextURL(json, contextURL);
      writeMetadataETag(json, metadata);
      EdmComplexType resolvedType = null;
      if (!type.getPathName()
        .toString()
        .equals(complex.getTypeName())) {
        if (type.getBaseType() != null && type.getBaseType()
          .getPathName()
          .toString()
          .equals(complex.getTypeName())) {
          resolvedType = resolveComplexType(metadata, type.getBaseType(), type.getPathName()
            .toString());
        } else {
          resolvedType = resolveComplexType(metadata, type, complex.getTypeName());
        }
      } else {
        resolvedType = resolveComplexType(metadata, type, complex.getTypeName());
      }
      if (!this.isODataMetadataNone && !resolvedType.equals(type) || this.isODataMetadataFull) {
        json.labelValue(this.constants.getType(), "#" + resolvedType.getPathName()
          .toString());
      }
      // writeOperations(complex.getOperations(), json);
      final var properties = complex;
      writeProperties(metadata, type, properties,
        options == null ? null : options == null ? null : options.getSelect(), json, complex,
        options == null ? null : options.getExpand());
      writeNavigationProperties(metadata, type, complex,
        options == null ? null : options.getExpand(), null, null, name, json);
      json.endObject();

    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerResult complexCollection(final ServiceMetadata metadata, final String name,
    final EdmComplexType type, final Object value, final ComplexSerializerOptions options)
    throws SerializerException {

    final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      JsonWriter json = new JsonWriter(outputStream, true)) {
      json.startObject();
      writeContextURL(json, contextURL);
      writeMetadataETag(json, metadata);
      if (this.isODataMetadataFull) {
        json.labelValue(this.constants.getType(), "#Collection(" + type.getPathName()
          .toString() + ")");
      }
      // writeOperations(value.getOperations(), json);
      json.label(Constants.VALUE);
      Set<List<String>> selectedPaths = null;
      if (null != options && null != options.getSelect()) {
        final boolean all = ExpandSelectHelper.isAll(options.getSelect());
        selectedPaths = all ? null
          : ExpandSelectHelper.getSelectedPaths(options.getSelect()
            .getSelectItems());
      }
      Set<List<String>> expandPaths = null;
      if (null != options && null != options.getExpand()) {
        expandPaths = ExpandSelectHelper.getExpandedItemsPath(options.getExpand());
      }
      writeComplexCollection(metadata, type, (Iterable<?>)value, name, selectedPaths, json,
        expandPaths, null, options == null ? null : options.getExpand());
      json.endObject();
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerResult entity(final ServiceMetadata metadata, final EdmEntityType entityType,
    final ODataEntity entity, final EntitySerializerOptions options) throws SerializerException {
    final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      var json = new JsonWriter(outputStream, true)) {
      final String name = contextURL == null ? null : contextURL.getEntitySetOrSingletonOrType();
      writeEntity(metadata, entityType, entity, contextURL,
        options == null ? null : options.getExpand(), null,
        options == null ? null : options.getSelect(),
        options == null ? false : options.getWriteOnlyReferences(), null, name, json);

    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerResult entityCollection(final ServiceMetadata metadata,
    final EdmEntityType entityType, final AbstractEntityCollection entitySet,
    final EntityCollectionSerializerOptions options) throws SerializerException {
    final boolean pagination = false;

    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      JsonWriter json = new JsonWriter(outputStream, true)) {
      json.startObject();

      final ContextURL contextURL = checkContextURL(
        options == null ? null : options.getContextURL());
      final String name = contextURL == null ? null : contextURL.getEntitySetOrSingletonOrType();
      writeContextURL(json, contextURL);

      writeMetadataETag(json, metadata);

      if (options != null && options.isCount()) {
        writeInlineCount(json, "", entitySet.getCount());
      }
      writeOperations(entitySet.getOperations(), json);
      json.label(Constants.VALUE);
      if (options == null) {
        writeEntitySet(metadata, entityType, entitySet, null, null, null, false, null, name, json);
      } else {
        writeEntitySet(metadata, entityType, entitySet, options.getExpand(), null,
          options.getSelect(), options.getWriteOnlyReferences(), null, name, json);
      }
      writeNextLink(entitySet, json, pagination);
      writeDeltaLink(entitySet, json, pagination);

    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerStreamResult entityCollectionStreamed(final ServiceMetadata metadata,
    final EdmEntityType entityType, final ODataEntityIterator entities,
    final EntityCollectionSerializerOptions options) throws SerializerException {

    return ODataWritableContent.with(entities, entityType, this, metadata, options)
      .build();
  }

  @Override
  public SerializerResult error(final ODataServerError error) {
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      var json = new JsonWriter(outputStream, true)) {
      new ODataErrorSerializer().writeErrorDocument(json, error);
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  /**
   * Get the ascii representation of the entity id
   * or thrown an {@link SerializerException} if id is <code>null</code>.
   *
   * @param entity the entity
   * @param entityType
   * @param name
   * @return ascii representation of the entity id
   */
  private String getEntityId(final ODataEntity entity, final EdmEntityType entityType,
    final String name) throws SerializerException {
    if (entity != null && entity.getId() == null) {
      if (entityType == null || entityType.getKeyPredicateNames() == null || name == null) {
        throw new SerializerException("Entity id is null.",
          SerializerException.MessageKeys.MISSING_ID);
      } else {
        entity
          .setId(URI.create(name + '(' + UriHelper.buildKeyPredicate(entityType, entity) + ')'));
      }
    }
    return entity.getId()
      .toASCIIString();
  }

  private boolean isStreamProperty(final EdmProperty edmProperty) {
    final EdmType type = edmProperty.getType();
    return edmProperty.isPrimitive() && type == EdmPrimitiveTypeKind.Stream;
  }

  @Override
  public SerializerResult metadataDocument(final ServiceMetadata serviceMetadata)
    throws SerializerException {
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      var json = new JsonWriter(outputStream, true)) {
      new MetadataDocumentJsonSerializer(serviceMetadata).writeMetadataDocument(json);
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerResult primitive(final ServiceMetadata metadata, final String name,
    final EdmPrimitiveType type, final PrimitiveSerializerOptions options, final Object value)
    throws SerializerException {
    final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      var json = new JsonWriter(outputStream, true)) {
      json.startObject();
      writeContextURL(json, contextURL);
      writeMetadataETag(json, metadata);
      // writeOperations(property.getOperations(), json);
      if (value == null && options != null && options.isNullable() != null
        && !options.isNullable()) {
        throw new SerializerException("Property value can not be null.",
          SerializerException.MessageKeys.NULL_INPUT);
      } else {
        json.label(Constants.VALUE);
        writePropertyValue(json, type.getDataType(), value);
      }
      json.endObject();

    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerResult primitiveCollection(final ServiceMetadata metadata,
    final EdmPrimitiveType type, final String name, final PrimitiveSerializerOptions options,
    final Object value) throws SerializerException {

    final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
    final CircleStreamBuffer buffer = new CircleStreamBuffer();

    try (
      var outputStream = buffer.getOutputStream();
      JsonWriter json = new JsonWriter(outputStream, true)) {
      json.startObject();
      writeContextURL(json, contextURL);
      writeMetadataETag(json, metadata);
      if (this.isODataMetadataFull) {
        json.labelValue(this.constants.getType(), "#Collection(" + type.getPathName()
          .getName() + ")");
      }
      // writeOperations(property.getOperations(), json);
      json.label(Constants.VALUE);
      writePrimitiveCollection(json, type, name, value);
      json.endObject();
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerResult reference(final ServiceMetadata metadata, final EdmEntitySet edmEntitySet,
    final ODataEntity entity, final ReferenceSerializerOptions options) throws SerializerException {

    final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      final JsonWriter json = new JsonWriter(outputStream, true)) {

      json.startObject();
      writeContextURL(json, contextURL);
      json.labelValue(this.constants.getId(), UriHelper.buildCanonicalURL(edmEntitySet, entity));
      json.endObject();
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  @Override
  public SerializerResult referenceCollection(final ServiceMetadata metadata,
    final EdmEntitySet edmEntitySet, final AbstractEntityCollection entityCollection,
    final ReferenceCollectionSerializerOptions options) throws SerializerException {
    final boolean pagination = false;
    final ContextURL contextURL = checkContextURL(options == null ? null : options.getContextURL());
    final CircleStreamBuffer buffer = new CircleStreamBuffer();
    try (
      var outputStream = buffer.getOutputStream();
      final JsonWriter json = new JsonWriter(outputStream, true)) {
      json.startObject();

      writeContextURL(json, contextURL);
      if (options != null && options.getCount() != null && options.getCount()
        .getValue()) {
        writeInlineCount(json, "", entityCollection.getCount());
      }

      json.label(Constants.VALUE);
      json.startList();
      for (final ODataEntity entity : entityCollection) {
        json.startObject();
        json.labelValue(this.constants.getId(), UriHelper.buildCanonicalURL(edmEntitySet, entity));
        json.endObject();
      }
      json.endList();

      writeNextLink(entityCollection, json, pagination);

      json.endObject();
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  protected EdmComplexType resolveComplexType(final ServiceMetadata metadata,
    final EdmComplexType baseType, final String derivedTypeName) throws SerializerException {

    final String fullQualifiedName = baseType.getPathName()
      .toString();
    if (derivedTypeName == null || fullQualifiedName.equals(derivedTypeName)) {
      return baseType;
    }
    final EdmComplexType derivedType = metadata.getEdm()
      .getComplexType(PathName.fromDotSeparated(derivedTypeName));
    if (derivedType == null) {
      throw new SerializerException("Complex Type not found",
        SerializerException.MessageKeys.UNKNOWN_TYPE, derivedTypeName);
    }
    EdmComplexType type = derivedType.getBaseType();
    while (type != null) {
      if (type.getPathName()
        .equals(baseType.getPathName())) {
        return derivedType;
      }
      type = type.getBaseType();
    }
    throw new SerializerException("Wrong base type",
      SerializerException.MessageKeys.WRONG_BASE_TYPE, derivedTypeName, baseType.getPathName()
        .toString());
  }

  protected EdmEntityType resolveEntityType(final ServiceMetadata metadata,
    final EdmEntityType baseType, final String derivedTypeName) throws SerializerException {
    if (derivedTypeName == null || baseType.getPathName()
      .toString()
      .equals(derivedTypeName)) {
      return baseType;
    }
    final EdmEntityType derivedType = metadata.getEdm()
      .getEntityType(PathName.fromDotSeparated(derivedTypeName));
    if (derivedType == null) {
      throw new SerializerException("EntityType not found",
        SerializerException.MessageKeys.UNKNOWN_TYPE, derivedTypeName);
    }
    EdmEntityType type = derivedType.getBaseType();
    while (type != null) {
      if (type.getPathName()
        .equals(baseType.getPathName())) {
        return derivedType;
      }
      type = type.getBaseType();
    }
    throw new SerializerException("Wrong base type",
      SerializerException.MessageKeys.WRONG_BASE_TYPE, derivedTypeName, baseType.getPathName()
        .toString());
  }

  @Override
  public SerializerResult serviceDocument(final ServiceMetadata metadata, final String serviceRoot)
    throws SerializerException {
    final CircleStreamBuffer buffer = new CircleStreamBuffer();

    try (
      var outputStream = buffer.getOutputStream();
      JsonWriter json = new JsonWriter(outputStream, true)) {
      new ServiceDocumentJsonSerializer(metadata, serviceRoot, this.isODataMetadataNone)
        .writeServiceDocument(json);

    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return SerializerResultImpl.with()
      .content(buffer.getInputStream())
      .build();
  }

  private void srid(final JsonWriter json, final int srid) {
    json.startObject();
    json.label(Constants.JSON_CRS);
    json.labelValue(Constants.ATTR_TYPE, Constants.JSON_NAME);
    json.startObject();
    json.label(Constants.PROPERTIES);
    json.labelValue(Constants.JSON_NAME, "EPSG:" + srid);
    json.endObject();
    json.endObject();
  }

  private void writeComplex(final ServiceMetadata metadata, final EdmComplexType type,
    final String name, final Set<List<String>> selectedPaths, final JsonWriter json,
    Set<List<String>> expandedPaths, ODataLinked linked, final ExpandOption expand,
    final ComplexValue complex) {
    json.startObject();
    final String derivedName = complex.getTypeName();
    EdmComplexType resolvedType = null;
    if (!type.getPathName()
      .toString()
      .equals(derivedName)) {
      if (type.getBaseType() != null && type.getBaseType()
        .getPathName()
        .toString()
        .equals(derivedName)) {
        resolvedType = resolveComplexType(metadata, type.getBaseType(), type.getPathName()
          .toString());
      } else {
        resolvedType = resolveComplexType(metadata, type, derivedName);
      }
    } else {
      resolvedType = resolveComplexType(metadata, type, derivedName);
    }
    if (!this.isODataMetadataNone && !resolvedType.equals(type) || this.isODataMetadataFull) {
      json.labelValue(this.constants.getType(), "#" + resolvedType.getPathName()
        .toString());
    }

    if (null != linked) {
      if (linked instanceof final ODataPropertyMap entity) {
        linked = entity.getValue(name);
      }
      expandedPaths = expandedPaths == null || expandedPaths.isEmpty() ? null
        : ExpandSelectHelper.getReducedExpandItemsPaths(expandedPaths, name);
    }

    writeComplexValue(metadata, resolvedType, complex, selectedPaths, json, expandedPaths, linked,
      expand, name);
    json.endObject();
  }

  private void writeComplexCollection(final ServiceMetadata metadata, final EdmComplexType type,
    final Iterable<?> collection, final String name, final Set<List<String>> selectedPaths,
    final JsonWriter json, Set<List<String>> expandedPaths, final ODataLinked linked,
    final ExpandOption expand) {
    json.startList();
    EdmComplexType derivedType = type;
    final Set<List<String>> expandedPaths1 = expandedPaths != null && !expandedPaths.isEmpty()
      ? expandedPaths
      : ExpandSelectHelper.getExpandedItemsPath(expand);
    for (final Object value : collection) {
      expandedPaths = expandedPaths1;
      derivedType = ((ComplexValue)value).getTypeName() != null ? metadata.getEdm()
        .getComplexType(PathName.fromDotSeparated(((ComplexValue)value).getTypeName())) : type;
      json.startObject();
      if (this.isODataMetadataFull || !this.isODataMetadataNone && !derivedType.equals(type)) {
        json.labelValue(this.constants.getType(), "#" + derivedType.getPathName()
          .toString());
      }
      expandedPaths = expandedPaths == null || expandedPaths.isEmpty() ? null
        : ExpandSelectHelper.getReducedExpandItemsPaths(expandedPaths, name);
      writeComplexValue(metadata, derivedType, (ComplexValue)value, selectedPaths, json,
        expandedPaths, (ComplexValue)value, expand, name);
      json.endObject();
    }
    json.endList();
  }

  protected void writeComplexValue(final ServiceMetadata metadata, final EdmComplexType type,
    final ODataPropertyMap properties, final Set<List<String>> selectedPaths, final JsonWriter json,
    Set<List<String>> expandedPaths, final ODataLinked linked, final ExpandOption expand,
    final String complexPropName) {

    if (null != expandedPaths) {
      for (final List<String> paths : expandedPaths) {
        if (!paths.isEmpty() && paths.size() == 1) {
          expandedPaths = ExpandSelectHelper.getReducedExpandItemsPaths(expandedPaths,
            paths.get(0));
        }
      }
    }

    for (final String propertyName : type.getPropertyNames()) {
      final var value = properties.getValue(propertyName);
      if (selectedPaths == null || ExpandSelectHelper.isSelected(selectedPaths, propertyName)) {
        writeProperty(json, metadata, (EdmProperty)type.getProperty(propertyName), propertyName,
          selectedPaths == null ? null
            : ExpandSelectHelper.getReducedSelectedPaths(selectedPaths, propertyName),
          expandedPaths, linked, expand, value);
      }
    }
    writeNavigationProperties(metadata, type, linked, expand, null, null, complexPropName, json);
  }

  void writeContextURL(final JsonWriter json, final ContextURL contextUrl) {
    if (!this.isODataMetadataNone && contextUrl != null) {
      final String string = contextUrl.toUriString();
      json.labelValue(this.constants.getContext(), string);
    }
  }

  void writeDeltaLink(final AbstractEntityCollection entitySet, final JsonWriter json,
    final boolean pagination) {
    if (entitySet.getDeltaLink() != null && !pagination) {
      json.labelValue(this.constants.getDeltaLink(), entitySet.getDeltaLink()
        .toASCIIString());
    }
  }

  protected void writeEntity(final ServiceMetadata metadata, final EdmEntityType entityType,
    final ODataEntity entity, final ContextURL contextURL, final ExpandOption expand,
    final Integer toDepth, final SelectOption select, final boolean onlyReference,
    Set<String> ancestors, final String name, final JsonWriter json) {
    boolean cycle = false;
    if (expand != null) {
      if (ancestors == null) {
        ancestors = new HashSet<>();
      }
      cycle = !ancestors.add(getEntityId(entity, entityType, name));
    }
    try {
      json.startObject();
      if (!this.isODataMetadataNone) {
        // top-level entity
        if (contextURL != null) {
          writeContextURL(json, contextURL);
          writeMetadataETag(json, metadata);
        }
        if (entity.getETag() != null) {
          json.labelValue(this.constants.getEtag(), entity.getETag());
        }
        if (entityType.hasStream()) {
          if (entity.getMediaETag() != null) {
            json.labelValue(this.constants.getMediaEtag(), entity.getMediaETag());
          }
          if (entity.getMediaContentType() != null) {
            json.labelValue(this.constants.getMediaContentType(), entity.getMediaContentType());
          }
          if (entity.getMediaContentSource() != null) {
            json.labelValue(this.constants.getMediaReadLink(), entity.getMediaContentSource()
              .toString());
          }
          if (entity.getMediaEditLinks() != null && !entity.getMediaEditLinks()
            .isEmpty()) {
            json.labelValue(this.constants.getMediaEditLink(), entity.getMediaEditLinks()
              .get(0)
              .getHref());
          }
        }
      }
      if (cycle || onlyReference) {
        json.labelValue(this.constants.getId(), getEntityId(entity, entityType, name));
      } else {
        final EdmEntityType resolvedType = resolveEntityType(metadata, entityType,
          entity.getType());
        if (!this.isODataMetadataNone && !resolvedType.equals(entityType)
          || this.isODataMetadataFull) {
          json.labelValue(this.constants.getType(), "#" + entity.getType());
        }
        if (!this.isODataMetadataNone && !areKeyPredicateNamesSelected(select, resolvedType)
          || this.isODataMetadataFull) {
          json.labelValue(this.constants.getId(), getEntityId(entity, resolvedType, name));
        }

        if (this.isODataMetadataFull) {
          if (entity.getSelfLink() != null) {
            json.labelValue(this.constants.getReadLink(), entity.getSelfLink()
              .getHref());
          }
          if (entity.getEditLink() != null) {
            json.labelValue(this.constants.getEditLink(), entity.getEditLink()
              .getHref());
          }
        }
        this.instanceAnnotSerializer.writeInstanceAnnotationsOnEntity(entity.getAnnotations(),
          json);
        writeProperties(metadata, resolvedType, entity, select, json, entity, expand);
        writeNavigationProperties(metadata, resolvedType, entity, expand, toDepth, ancestors, name,
          json);
        writeOperations(entity.getOperations(), json);
      }
      json.endObject();
    } finally {
      if (expand != null && !cycle && ancestors != null) {
        ancestors.remove(getEntityId(entity, entityType, name));
      }
    }
  }

  protected void writeEntitySet(final ServiceMetadata metadata, final EdmEntityType entityType,
    final AbstractEntityCollection entitySet, final ExpandOption expand, final Integer toDepth,
    final SelectOption select, final boolean onlyReference, final Set<String> ancestors,
    final String name, final JsonWriter json) {

    json.startList();
    for (final var entity : entitySet) {
      if (onlyReference) {
        json.startObject();
        json.labelValue(this.constants.getId(), getEntityId(entity, entityType, name));
        json.endObject();
      } else {
        writeEntity(metadata, entityType, entity, null, expand, toDepth, select, false, ancestors,
          name, json);
      }
    }
    json.endList();
  }

  protected void writeExpandedNavigationProperty(final ServiceMetadata metadata,
    final EdmNavigationProperty property, final Link navigationLink, final ExpandOption innerExpand,
    final Integer toDepth, final SelectOption innerSelect, final CountOption innerCount,
    final boolean writeOnlyCount, final boolean writeOnlyRef, final Set<String> ancestors,
    final String name, final JsonWriter json) {

    if (property.isCollection()) {
      if (writeOnlyCount) {
        if (navigationLink == null || navigationLink.getInlineEntitySet() == null) {
          writeInlineCount(json, property.getName(), 0);
        } else {
          writeInlineCount(json, property.getName(), navigationLink.getInlineEntitySet()
            .getCount());
        }
      } else {
        if (navigationLink == null || navigationLink.getInlineEntitySet() == null) {
          if (innerCount != null && innerCount.getValue()) {
            writeInlineCount(json, property.getName(), 0);
          }
          json.label(property.getName());
          json.startList();
          json.endList();
        } else {
          if (innerCount != null && innerCount.getValue()) {
            writeInlineCount(json, property.getName(), navigationLink.getInlineEntitySet()
              .getCount());
          }
          json.label(property.getName());
          writeEntitySet(metadata, property.getType(), navigationLink.getInlineEntitySet(),
            innerExpand, toDepth, innerSelect, writeOnlyRef, ancestors, name, json);
        }
      }
    } else {
      json.label(property.getName());
      if (navigationLink == null || navigationLink.getInlineEntity() == null) {
        json.writeNull();
      } else {
        writeEntity(metadata, property.getType(), navigationLink.getInlineEntity(), null,
          innerExpand, toDepth, innerSelect, writeOnlyRef, ancestors, name, json);
      }
    }
  }

  private void writeGeoLineStringCoordinates(final JsonWriter json, final LineString line) {
    for (int i = 0; i < line.getVertexCount(); i++) {
      json.startList();
      json.value(line.getX(i));
      json.value(line.getY(i));
      final var z = line.getZ(i);
      if (Double.isFinite(0) && z != 0) {
        json.value(z);
      }
      json.endList();
    }
  }

  private void writeGeoMultiPointCoordinates(final JsonWriter json, final MultiPoint points) {
    for (int i = 0; i < points.getGeometryCount(); i++) {
      json.startList();
      json.value(points.getX(i));
      json.value(points.getY(i));
      final var z = points.getZ(i);
      if (Double.isFinite(0) && z != 0) {
        json.value(z);
      }
      json.endList();
    }
  }

  private void writeGeoPointCoordinates(final JsonWriter json, final Point point) {
    json.value(point.getX());
    json.value(point.getY());
    if (point.getZ() != 0) {
      json.value(point.getZ());
    }
  }

  // TODO: There could be a more strict verification that the lines describe
  // boundaries
  // and have the correct winding order.
  // But arguably the better place for this is the constructor of the Polygon
  // object.
  private void writeGeoPolygonCoordinates(final JsonWriter json, final Polygon polygon) {
    json.startList();
    writeGeoLineStringCoordinates(json, polygon.getShell());
    json.endList();
    for (final var ring : polygon.rings()) {
      json.startList();
      writeGeoLineStringCoordinates(json, ring);
      json.endList();
    }
  }

  /** Writes a geospatial value following the GeoJSON specification defined in RFC 7946. */
  protected void writeGeoValue(final JsonWriter json, final Geometry geoValue,
    final Integer parentSrid) {
    if (geoValue == null) {
      json.writeNull();
    } else {
      json.startObject();
      json.labelValue(Constants.ATTR_TYPE, geoValue.getDataType()
        .getName());
      json.label(geoValue instanceof GeometryCollectionImpl ? Constants.JSON_GEOMETRIES
        : Constants.JSON_COORDINATES);
      json.startList();
      if (geoValue instanceof final Point point) {
        writeGeoPointCoordinates(json, point);
      } else if (geoValue instanceof final LineString line) {
        writeGeoLineStringCoordinates(json, line);
      } else if (geoValue instanceof final Polygon polygon) {
        writeGeoPolygonCoordinates(json, polygon);
      } else if (geoValue instanceof final MultiPoint points) {
        writeGeoMultiPointCoordinates(json, points);
      } else if (geoValue instanceof final MultiLineString lines) {
        for (final var lineString : lines.getLineStrings()) {
          json.startList();
          writeGeoLineStringCoordinates(json, lineString);
          json.endList();
        }
      } else if (geoValue instanceof final MultiPolygon polygons) {
        for (final Polygon polygon : polygons.getPolygons()) {
          json.startList();
          writeGeoPolygonCoordinates(json, polygon);
          json.endList();
        }
      } else if (geoValue instanceof final GeometryCollection geometries) {
        for (final var element : geometries.geometries()) {
          writeGeoValue(json, element, geoValue.getCoordinateSystemId());
        }
      }

      json.endList();

      if (parentSrid == null || !parentSrid.equals(geoValue.getCoordinateSystemId())) {
        srid(json, geoValue.getCoordinateSystemId());
      }
      json.endObject();
    }
  }

  void writeInlineCount(final JsonWriter json, final String propertyName, final Integer count) {
    if (count != null) {
      final var s = count.toString();
      String name = this.constants.getCount();
      if (propertyName.length() > 0) {
        name = propertyName + name;
      }
      json.labelValue(name, s);
    }
  }

  private void writeMetadataETag(final JsonWriter json, final ServiceMetadata metadata) {
    if (!this.isODataMetadataNone && metadata != null) {
      final ServiceMetadataETagSupport eTagSupport = metadata.getServiceMetadataETagSupport();
      if (eTagSupport != null) {
        final String eTag = eTagSupport.getMetadataETag();
        if (eTag != null) {
          json.labelValue(this.constants.getMetadataEtag(), eTag);
        }
      }
    }
  }

  protected void writeNavigationProperties(final ServiceMetadata metadata,
    final EdmStructuredType type, final ODataLinked linked, final ExpandOption expand,
    final Integer toDepth, final Set<String> ancestors, final String name, final JsonWriter json) {
    if (this.isODataMetadataFull) {
      for (final String propertyName : type.getNavigationPropertyNames()) {
        final Link navigationLink = linked.getNavigationLink(propertyName);
        if (navigationLink != null) {
          json.labelValue(propertyName + this.constants.getNavigationLink(),
            navigationLink.getHref());
        }
        final Link associationLink = linked.getAssociationLink(propertyName);
        if (associationLink != null) {
          json.labelValue(propertyName + this.constants.getAssociationLink(),
            associationLink.getHref());
        }
      }
    }
    if (toDepth != null && toDepth > 1 || toDepth == null && ExpandSelectHelper.hasExpand(expand)) {
      final ExpandItem expandAll = ExpandSelectHelper.getExpandAll(expand);
      for (final String propertyName : type.getNavigationPropertyNames()) {
        final ExpandItem innerOptions = ExpandSelectHelper
          .getExpandItemBasedOnType(expand.getExpandItems(), propertyName, type, name);
        if (innerOptions != null || expandAll != null || toDepth != null) {
          Integer levels = null;
          final EdmNavigationProperty property = type.getNavigationProperty(propertyName);
          final Link navigationLink = linked.getNavigationLink(property.getName());
          ExpandOption childExpand = null;
          LevelsExpandOption levelsOption = null;
          if (innerOptions != null) {
            levelsOption = innerOptions.getLevelsOption();
            childExpand = levelsOption == null ? innerOptions.getExpandOption()
              : new ExpandOptionImpl().addExpandItem(innerOptions);
          } else if (expandAll != null) {
            levels = 1;
            levelsOption = expandAll.getLevelsOption();
            childExpand = new ExpandOptionImpl().addExpandItem(expandAll);
          }

          if (levelsOption != null) {
            levels = levelsOption.isMax() ? Integer.MAX_VALUE : levelsOption.getValue();
          }
          if (toDepth != null) {
            levels = toDepth - 1;
            childExpand = expand;
          }

          writeExpandedNavigationProperty(metadata, property, navigationLink, childExpand, levels,
            innerOptions == null ? null : innerOptions.getSelectOption(),
            innerOptions == null ? null : innerOptions.getCountOption(),
            innerOptions == null ? false : innerOptions.hasCountPath(),
            innerOptions == null ? false : innerOptions.isRef(), ancestors, name, json);
        }
      }
    }
  }

  void writeNextLink(final AbstractEntityCollection entitySet, final JsonWriter json,
    boolean pagination) {
    if (entitySet.getNext() != null) {
      pagination = true;
      json.labelValue(this.constants.getNextLink(), entitySet.getNext()
        .toASCIIString());
    } else {
      pagination = false;
    }
  }

  private void writeOperations(final List<Operation> operations, final JsonWriter json) {
    if (this.isODataMetadataFull) {
      for (final Operation operation : operations) {
        json.startObject();
        json.label(operation.getMetadataAnchor());
        json.labelValue(Constants.ATTR_TITLE, operation.getTitle());
        json.labelValue(Constants.ATTR_TARGET, operation.getTarget()
          .toASCIIString());
        json.endObject();
      }
    }
  }

  private void writePrimitiveCollection(final JsonWriter json, final EdmPrimitiveType type,
    final String name, final Object propertyValue) {
    json.startList();
    for (final Object value : (ListEx<?>)propertyValue) {
      try {
        writePrimitiveValue(json, name, type, value);
      } catch (final EdmPrimitiveTypeException e) {
        throw new SerializerException("Wrong value for property!", e,
          SerializerException.MessageKeys.WRONG_PROPERTY_VALUE, name, propertyValue.toString());
      }
    }
    json.endList();
  }

  protected void writePrimitiveValue(final JsonWriter json, final String name,
    final EdmPrimitiveType type, final Object value) {
    if (value == null) {
      json.writeNull();
    } else if (type == EdmPrimitiveTypeKind.Stream) {
      if (value instanceof final Link stream) {
        if (!this.isODataMetadataNone) {
          if (stream.getMediaETag() != null) {
            json.labelValue(name + this.constants.getMediaEtag(), stream.getMediaETag());
          }
          if (stream.getType() != null) {
            json.labelValue(name + this.constants.getMediaContentType(), stream.getType());
          }
        }
        if (this.isODataMetadataFull) {
          if (stream.getRel() != null && stream.getRel()
            .equals(Constants.NS_MEDIA_READ_LINK_REL)) {
            json.labelValue(name + this.constants.getMediaReadLink(), stream.getHref());
          }
          if (stream.getRel() == null || stream.getRel()
            .equals(Constants.NS_MEDIA_EDIT_LINK_REL)) {
            json.labelValue(name + this.constants.getMediaEditLink(), stream.getHref());
          }
        }
      }
    } else {
      final var string = type.valueToString(value);
      json.value(string);
    }
  }

  protected void writeProperties(final ServiceMetadata metadata, final EdmStructuredType type,
    final ODataPropertyMap properties, final SelectOption select, final JsonWriter json,
    final ODataLinked linked, final ExpandOption expand) {
    final boolean all = ExpandSelectHelper.isAll(select);
    final Set<String> selected = all ? new HashSet<>()
      : ExpandSelectHelper.getSelectedPropertyNames(select.getSelectItems());
    addKeyPropertiesToSelected(selected, type);
    final Set<List<String>> expandedPaths = ExpandSelectHelper.getExpandedItemsPath(expand);
    for (final String propertyName : type.getPropertyNames()) {
      if (all || selected.contains(propertyName)) {
        final EdmProperty edmProperty = type.getStructuralProperty(propertyName);
        Object value = null;
        if (properties != null) {
          value = properties.getValue(propertyName);
        }
        final Set<List<String>> selectedPaths = all || edmProperty.isPrimitive() ? null
          : ExpandSelectHelper.getSelectedPaths(select.getSelectItems(), propertyName);
        final var annotations = properties.getAnnotations(propertyName);
        this.instanceAnnotSerializer.writeInstanceAnnotationsOnProperties(json, propertyName,
          annotations);
        writeProperty(json, metadata, edmProperty, propertyName, selectedPaths, expandedPaths,
          linked, expand, value);
      }
    }
  }

  protected void writeProperty(final JsonWriter json, final ServiceMetadata metadata,
    final EdmProperty edmProperty, final String name, final Set<List<String>> selectedPaths,
    final Set<List<String>> expandedPaths, final ODataLinked linked, final ExpandOption expand,
    final Object value) {
    final boolean isStreamProperty = isStreamProperty(edmProperty);
    writePropertyType(edmProperty, json);
    if (!isStreamProperty) {
      json.label(edmProperty.getName());
    }
    writePropertyValue(metadata, edmProperty, name, selectedPaths, json, expandedPaths, linked,
      expand, value, isStreamProperty);
  }

  private void writePropertyType(final EdmProperty edmProperty, final JsonWriter json) {
    if (!this.isODataMetadataFull) {
      return;
    }
    final String typeName = edmProperty.getName() + this.constants.getType();
    final EdmType type = edmProperty.getType();
    if (type.getKind() == EdmTypeKind.ENUM || type.getKind() == EdmTypeKind.DEFINITION) {
      if (edmProperty.isCollection()) {
        json.labelValue(typeName, "#Collection(" + type.getPathName()
          .toString() + ")");
      } else {
        json.labelValue(typeName, "#" + type.getPathName()
          .toString());
      }
    } else if (edmProperty.isPrimitive()) {
      if (edmProperty.isCollection()) {
        json.labelValue(typeName, "#Collection(" + type.getPathName()
          .getName() + ")");
      } else {
        // exclude the properties that can be heuristically determined
        if (type != EdmPrimitiveTypeKind.Boolean && type != EdmPrimitiveTypeKind.Double
          && type != EdmPrimitiveTypeKind.String) {
          json.labelValue(typeName, "#" + type.getPathName()
            .getName());
        }
      }
    } else if (type.getKind() == EdmTypeKind.COMPLEX) {
      // non-collection case written in writeComplex method directly.
      if (edmProperty.isCollection()) {
        json.labelValue(typeName, "#Collection(" + type.getPathName()
          .toString() + ")");
      }
    } else {
      throw new SerializerException("Property type not yet supported!",
        SerializerException.MessageKeys.UNSUPPORTED_PROPERTY_TYPE, edmProperty.getName());
    }
  }

  private void writePropertyValue(final JsonWriter json, final DataType dataType,
    final Object value) {
    if (value == null) {
      json.writeNull();
    } else if (value instanceof final Iterable<?> collection) {
      json.startList();
      for (final var item : collection) {
        writePropertyValue(json, dataType, item);
      }
      json.endList();
    } else if (value instanceof final Geometry geometry) {
      writeGeoValue(json, geometry, null);
    } else if (value instanceof ComplexValue) {
      throw new IllegalArgumentException("Complex not supported");
    } else {
      final var string = dataType.toString(value);
      json.value(string);
    }
    //
    // final EdmType type = edmProperty.getType();
    // try {
    // if (edmProperty.isPrimitive() || type.getKind() == EdmTypeKind.ENUM
    // || type.getKind() == EdmTypeKind.DEFINITION) {
    // writePrimitive(json, name, (EdmPrimitiveType)type, value);
    // // If there is expand on a stream property
    // if (isStreamProperty(edmProperty) && null != expand) {
    // final ExpandItem expandAll = ExpandSelectHelper.getExpandAll(expand);
    // writeExpandedStreamProperty(expand, name, edmProperty, linked, expandAll,
    // json);
    // }
    // }
    // } else if (property.isComplex()) {
    // if (edmProperty.isCollection()) {
    // writeComplexCollection(metadata, (EdmComplexType)type, property, name,
    // selectedPaths,
    // json, expandedPaths, linked, expand);
    // } else {
    // writeComplex(metadata, (EdmComplexType)type, property, name,
    // selectedPaths, json,
    // expandedPaths, linked, expand, (ComplexValue)value);
    // }
    // } else {
    // }
    // }
  }

  private void writePropertyValue(final ServiceMetadata metadata, final EdmProperty edmProperty,
    final String name, final Set<List<String>> selectedPaths, final JsonWriter json,
    final Set<List<String>> expandedPaths, final ODataLinked linked, final ExpandOption expand,
    final Object value, final boolean isStreamProperty) {
    if (value == null) {
      if (!isStreamProperty) {
        if (edmProperty.isCollection()) {
          json.startList();
          json.endList();
        } else {
          json.writeNull();
        }
      }
    } else if (value instanceof final Iterable<?> collection) {
      json.startList();
      for (final var item : collection) {
        writePropertyValue(metadata, edmProperty, name, selectedPaths, json, expandedPaths, linked,
          expand, item, isStreamProperty);
      }
      json.endList();
    } else if (value instanceof final ComplexValue complex) {
      final EdmType type = edmProperty.getType();
      writeComplex(metadata, (EdmComplexType)type, name, selectedPaths, json, expandedPaths, linked,
        expand, complex);
    } else {
      writePropertyValue(json, edmProperty.getDataType(), value);
    }
  }

  public void writeRecords(final ServiceMetadata metadata, final EdmEntityType entityType,
    final ODataEntityIterator records, final EntityCollectionSerializerOptions options,
    final OutputStream outputStream) throws SerializerException {
    final boolean pagination = false;
    try (
      JsonWriter json = new JsonWriter(outputStream)) {
      json.startObject();
      ContextURL contextUrl = null;
      if (options != null) {
        contextUrl = options.getContextURL();
      }
      checkContextURL(contextUrl);
      writeContextURL(json, contextUrl);

      writeMetadataETag(json, metadata);

      if (options != null && options.isCount()) {
        writeInlineCount(json, "", records.getCount());
      }
      json.label(Constants.VALUE);
      final String name = contextUrl == null ? null : contextUrl.getEntitySetOrSingletonOrType();
      if (options == null) {
        writeEntitySet(metadata, entityType, records, null, null, null, false, null, name, json);
      } else {
        writeEntitySet(metadata, entityType, records, options.getExpand(), null,
          options.getSelect(), options.getWriteOnlyReferences(), null, name, json);
      }
      writeNextLink(records, json, pagination);

      json.endObject();
    }
  }

}

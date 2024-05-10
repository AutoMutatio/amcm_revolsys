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
package org.apache.olingo.server.api.uri;

import java.util.List;

import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.ODataEntity;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmKeyPropertyRef;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.core.Encoder;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.deserializer.DeserializerException;
import org.apache.olingo.server.api.deserializer.DeserializerException.MessageKeys;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.core.serializer.utils.ContextURLHelper;
import org.apache.olingo.server.core.uri.UriResource;
import org.apache.olingo.server.core.uri.UriResourceEntitySet;

/**
 * Used for URI-related tasks.
 */
public class UriHelper {

  public static String buildCanonicalURL(final EdmEntitySet edmEntitySet, final ODataEntity entity)
    throws SerializerException {
    return edmEntitySet.getName() + '(' + buildKeyPredicate(edmEntitySet.getEntityType(), entity)
      + ')';
  }

  public static String buildContextURLKeyPredicate(final List<UriParameter> keys)
    throws SerializerException {
    return ContextURLHelper.buildKeyPredicate(keys);
  }

  public static String buildContextURLSelectList(final EdmStructuredType type,
    final ExpandOption expand, final SelectOption select) throws SerializerException {
    return ContextURLHelper.buildSelectList(type, expand, select);
  }

  public static String buildKeyPredicate(final EdmEntityType edmEntityType,
    final ODataEntity entity) throws SerializerException {
    final StringBuilder result = new StringBuilder();
    final List<String> keyNames = edmEntityType.getKeyPredicateNames();
    boolean first = true;
    for (final String keyName : keyNames) {
      final EdmKeyPropertyRef refType = edmEntityType.getKeyPropertyRef(keyName);
      if (first) {
        first = false;
      } else {
        result.append(',');
      }
      if (keyNames.size() > 1) {
        result.append(Encoder.encode(keyName))
          .append('=');
      }
      final EdmProperty edmProperty = refType.getProperty();
      if (edmProperty == null) {
        throw new SerializerException("Property not found (possibly an alias): " + keyName,
          SerializerException.MessageKeys.MISSING_PROPERTY, keyName);
      }
      final EdmPrimitiveType type = (EdmPrimitiveType)edmProperty.getType();
      final Object propertyValue = findPropertyRefValue(entity, refType);
      try {
        final String value = type.toUriLiteral(type.valueToString(propertyValue));
        result.append(Encoder.encode(value));
      } catch (final EdmPrimitiveTypeException e) {
        throw new SerializerException("Wrong key value!", e,
          SerializerException.MessageKeys.WRONG_PROPERTY_VALUE, edmProperty.getName(),
          propertyValue != null ? propertyValue.toString() : null);
      }
    }
    return result.toString();
  }

  private static Object findPropertyRefValue(final ODataEntity entity,
    final EdmKeyPropertyRef refType) throws SerializerException {
    final int INDEX_ERROR_CODE = -1;
    final String propertyPath = refType.getName();
    String tmpPropertyName;
    int lastIndex;
    int index = propertyPath.indexOf('/');
    if (index == INDEX_ERROR_CODE) {
      index = propertyPath.length();
    }
    tmpPropertyName = propertyPath.substring(0, index);
    // get first property
    var prop = entity.getValue(tmpPropertyName);
    // get following properties
    while (index < propertyPath.length()) {
      lastIndex = ++index;
      index = propertyPath.indexOf('/', index + 1);
      if (index == INDEX_ERROR_CODE) {
        index = propertyPath.length();
      }
      tmpPropertyName = propertyPath.substring(lastIndex, index);
      prop = ((ComplexValue)prop).getValue(tmpPropertyName);
    }
    if (prop == null) {
      throw new SerializerException("Key Value Cannot be null for property: " + propertyPath,
        SerializerException.MessageKeys.NULL_PROPERTY, propertyPath);
    }
    return prop;
  }

  public static UriResourceEntitySet parseEntityId(final Edm edm, final String entityId,
    final String rawServiceRoot) throws DeserializerException {

    String oDataPath = entityId;
    if (rawServiceRoot != null && entityId.startsWith(rawServiceRoot)) {
      oDataPath = entityId.substring(rawServiceRoot.length());
    }
    oDataPath = oDataPath.startsWith("/") ? oDataPath : "/" + oDataPath;

    try {
      final List<UriResource> uriResourceParts = edm.getParser()
        .parseUri(oDataPath, null, null, rawServiceRoot)
        .getUriResourceParts();
      if (uriResourceParts.size() == 1 && uriResourceParts.get(0)
        .getKind() == UriResourceKind.entitySet) {
        final UriResourceEntitySet entityUriResource = (UriResourceEntitySet)uriResourceParts
          .get(0);

        return entityUriResource;
      }

      throw new DeserializerException("Invalid entity binding link",
        MessageKeys.INVALID_ENTITY_BINDING_LINK, entityId);
    } catch (final ODataLibraryException e) {
      throw new DeserializerException("Invalid entity binding link", e,
        MessageKeys.INVALID_ENTITY_BINDING_LINK, entityId);
    }
  }
}

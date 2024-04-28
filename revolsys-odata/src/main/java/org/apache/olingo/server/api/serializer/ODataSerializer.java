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
package org.apache.olingo.server.api.serializer;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.constants.Constantsv00;
import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.ODataEntity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.core.serializer.json.ODataJsonSerializer;
import org.apache.olingo.server.core.serializer.xml.ODataXmlSerializer;

import com.revolsys.odata.model.ODataEntityIterator;

/** OData serializer */
public interface ODataSerializer {

  public static final String IO_EXCEPTION_TEXT = "An I/O exception occurred.";

  /** The default character set is UTF-8. */
  public static final String DEFAULT_CHARSET = Constants.UTF8;

  public static void closeCircleStreamBufferOutput(final OutputStream outputStream,
    final SerializerException cachedException) throws SerializerException {
    if (outputStream != null) {
      try {
        outputStream.close();
      } catch (final IOException e) {
        if (cachedException != null) {
          throw cachedException;
        } else {
          throw new SerializerException(IO_EXCEPTION_TEXT, e,
            SerializerException.MessageKeys.IO_EXCEPTION);
        }
      }
    }
  }

  // TODO CSV/TSV
  static ODataSerializer createSerializer(final ContentType contentType) {
    ODataSerializer serializer = null;

    if (contentType != null && contentType.isCompatible(ContentType.APPLICATION_JSON)) {
      final String metadata = contentType.getParameter(ContentType.PARAMETER_ODATA_METADATA);
      if (metadata == null || ContentType.VALUE_ODATA_METADATA_MINIMAL.equalsIgnoreCase(metadata)
        || ContentType.VALUE_ODATA_METADATA_NONE.equalsIgnoreCase(metadata)
        || ContentType.VALUE_ODATA_METADATA_FULL.equalsIgnoreCase(metadata)) {
        serializer = new ODataJsonSerializer(contentType, new Constantsv00());
      }
    } else if (contentType != null && (contentType.isCompatible(ContentType.APPLICATION_XML)
      || contentType.isCompatible(ContentType.APPLICATION_ATOM_XML))) {
      serializer = new ODataXmlSerializer();
    }

    if (serializer == null) {
      throw new SerializerException(
        "Unsupported format: " + (contentType != null ? contentType.toContentTypeString() : null),
        SerializerException.MessageKeys.UNSUPPORTED_FORMAT,
        contentType != null ? contentType.toContentTypeString() : null);
    } else {
      return serializer;
    }
  }

  /**
   * Writes complex-type instance data into an InputStream.
   * @param metadata metadata for the service
   * @param type complex type
   * @param property property value
   * @param options options for the serializer
   */
  SerializerResult complex(ServiceMetadata metadata, EdmComplexType type, Property property,
    ComplexSerializerOptions options) throws SerializerException;

  /**
   * Writes data of a collection of complex-type instances into an InputStream.
   * @param metadata metadata for the service
   * @param type complex type
   * @param property property value
   * @param options options for the serializer
   */
  SerializerResult complexCollection(ServiceMetadata metadata, EdmComplexType type,
    Property property, String name, ComplexSerializerOptions options) throws SerializerException;

  /**
   * Writes entity data into an InputStream.
   * @param metadata metadata for the service
   * @param entityType the {@link EdmEntityType}
   * @param entity the data of the entity
   * @param options options for the serializer
   */
  SerializerResult entity(ServiceMetadata metadata, EdmEntityType entityType, ODataEntity entity,
    EntitySerializerOptions options) throws SerializerException;

  /**
   * Writes entity-collection data into an InputStream.
   * @param metadata metadata for the service
   * @param entityType the {@link EdmEntityType}
   * @param entitySet the data of the entity set
   * @param options options for the serializer
   */
  SerializerResult entityCollection(ServiceMetadata metadata, EdmEntityType entityType,
    AbstractEntityCollection entitySet, EntityCollectionSerializerOptions options)
    throws SerializerException;

  /**
   * Writes entity-collection data into an InputStream.
   * @param metadata metadata for the service
   * @param entityType the {@link EdmEntityType}
   * @param entities the data of the entity set
   * @param options options for the serializer
   */
  SerializerStreamResult entityCollectionStreamed(ServiceMetadata metadata,
    EdmEntityType entityType, ODataEntityIterator entities,
    EntityCollectionSerializerOptions options) throws SerializerException;

  /**
   * Writes an ODataError into an InputStream.
   * @param error the main error
   */
  SerializerResult error(ODataServerError error) throws SerializerException;

  /**
   * Writes the metadata document into an InputStream.
   * @param serviceMetadata the metadata information for the service
   */
  SerializerResult metadataDocument(ServiceMetadata serviceMetadata) throws SerializerException;

  /**
   * Writes primitive-type instance data into an InputStream.
   * @param metadata metadata for the service
   * @param type primitive type
   * @param property property value
   * @param options options for the serializer
   * @param value TODO
   */
  SerializerResult primitive(ServiceMetadata metadata, EdmPrimitiveType type, Property property,
    String name, PrimitiveSerializerOptions options, Object value) throws SerializerException;

  /**
   * Writes data of a collection of primitive-type instances into an InputStream.
   * @param metadata metadata for the service
   * @param type primitive type
   * @param property property value
   * @param options options for the serializer
   */
  SerializerResult primitiveCollection(ServiceMetadata metadata, EdmPrimitiveType type,
    Property property, String name, PrimitiveSerializerOptions options, Object value)
    throws SerializerException;

  /**
   * Writes a single entity reference into an InputStream.
   * @param metadata metadata for the service
   * @param edmEntitySet {@link EdmEntitySet}
   * @param entity data of the entity
   * @param options {@link ReferenceSerializerOptions}
   */
  SerializerResult reference(ServiceMetadata metadata, EdmEntitySet edmEntitySet,
    ODataEntity entity, ReferenceSerializerOptions options) throws SerializerException;

  /**
   * Writes entity-collection references into an InputStream.
   * @param metadata metadata for the service
   * @param edmEntitySet {@link EdmEntitySet}
   * @param entityCollection data of the entity collection
   * @param options {@link ReferenceCollectionSerializerOptions}
   */
  SerializerResult referenceCollection(ServiceMetadata metadata, EdmEntitySet edmEntitySet,
    AbstractEntityCollection entityCollection, ReferenceCollectionSerializerOptions options)
    throws SerializerException;

  /**
   * Writes the service document into an InputStream.
   * @param serviceMetadata the metadata information for the service
   * @param serviceRoot the service-root URI of this OData service
   */
  SerializerResult serviceDocument(ServiceMetadata serviceMetadata, String serviceRoot)
    throws SerializerException;
}

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
import java.util.List;

import org.apache.commons.codec.DecoderException;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.IConstants;
import org.apache.olingo.commons.api.data.Annotation;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Valuable;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.core.serializer.utils.ContentTypeHelper;

import com.revolsys.collection.json.JsonWriter;

public class ODataJsonInstanceAnnotationSerializer {

  private final boolean isODataMetadataNone;

  private final boolean isODataMetadataFull;

  private final IConstants constants;

  public ODataJsonInstanceAnnotationSerializer(final ContentType contentType,
    final IConstants constants) {
    this.isODataMetadataNone = ContentTypeHelper.isODataMetadataNone(contentType);
    this.isODataMetadataFull = ContentTypeHelper.isODataMetadataFull(contentType);
    this.constants = constants;
  }

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  private void writeInstanceAnnotation(final JsonWriter json, final Annotation annotation,
    final String name) {
    try {
      switch (annotation.getValueType()) {
        case PRIMITIVE:
          if (this.isODataMetadataFull && name.length() > 0) {
            json.labelValue(name + this.constants.getType(), "#" + annotation.getType());
          }
          if (name.length() > 0) {
            json.label(name);
          }
          writeInstanceAnnotOnPrimitiveProperty(json, annotation, annotation.getValue());
        break;
        case COLLECTION_PRIMITIVE:
          if (this.isODataMetadataFull && name.length() > 0) {
            json.labelValue(name + this.constants.getType(),
              "#Collection(" + annotation.getType() + ")");
          }
          if (name.length() > 0) {
            json.label(name);
          }
          json.startList();
          final List list = annotation.asCollection();
          for (final Object value : list) {
            writeInstanceAnnotOnPrimitiveProperty(json, annotation, value);
          }
          json.endList();
        break;
        case COMPLEX:
          if (this.isODataMetadataFull && name.length() > 0) {
            json.labelValue(name + this.constants.getType(), "#" + annotation.getType());
          }
          if (name.length() > 0) {
            json.label(name);
          }
          final ComplexValue complexValue = annotation.asComplex();
          writeInstanceAnnotOnComplexProperty(json, annotation, complexValue);
        break;
        case COLLECTION_COMPLEX:
          if (this.isODataMetadataFull && name.length() > 0) {
            json.labelValue(name + this.constants.getType(),
              "#Collection(" + annotation.getType() + ")");
          }
          if (name.length() > 0) {
            json.label(name);
          }
          json.startList();
          final List<ComplexValue> complexValues = (List<ComplexValue>)annotation.asCollection();
          for (final ComplexValue complxValue : complexValues) {
            writeInstanceAnnotOnComplexProperty(json, annotation, complxValue);
          }
          json.endList();
        break;
        default:
      }
    } catch (final EdmPrimitiveTypeException e) {
      throw new SerializerException("Wrong value for instance annotation!", e,
        SerializerException.MessageKeys.WRONG_PROPERTY_VALUE, annotation.getTerm(),
        annotation.getValue()
          .toString());
    }
  }

  /**
   * Write the instance annotation of an entity
   * @param annotations List of annotations
   * @param json JsonWriter
   * @throws IOException
   * @throws SerializerException
   * @throws DecoderException
   */
  public void writeInstanceAnnotationsOnEntity(final List<Annotation> annotations,
    final JsonWriter json) {
    for (final Annotation annotation : annotations) {
      if (this.isODataMetadataFull) {
        json.labelValue(this.constants.getType(), "#" + annotation.getType());
      }
      json.label("@" + annotation.getTerm());
      writeInstanceAnnotation(json, annotation, "");
    }
  }

  public void writeInstanceAnnotationsOnProperties(final JsonWriter json, final String name,
    final Iterable<Annotation> annotations) {
    if (annotations != null) {
      for (final Annotation annotation : annotations) {
        json.label(name + "@" + annotation.getTerm());
        writeInstanceAnnotation(json, annotation, "");
      }
    }
  }

  private void writeInstanceAnnotOnComplexProperty(final JsonWriter json, final Valuable annotation,
    final ComplexValue complexValue) {
    throw new IllegalArgumentException("Complex annotations not supported");
    // json.startObject();
    // if (this.isODataMetadataFull) {
    // json.labelValue(this.constants.getType(), "#" +
    // complexValue.getTypeName());
    // }
    // for (final var name : complexValue.getFieldNames()) {
    // // final var value = complexValue.getValue(name);
    // // writeInstanceAnnotation(json, property, name);
    // }
    // json.endObject();
  }

  private void writeInstanceAnnotOnPrimitiveProperty(final JsonWriter json,
    final Valuable annotation, final Object value) {
    writePrimitiveValue(json, "", EdmPrimitiveTypeKind.getByName(annotation.getType()), value);
  }

  protected void writePrimitiveValue(final JsonWriter json, final String name,
    final EdmPrimitiveType type, final Object primitiveValue) {
    if (primitiveValue == null) {
      json.writeNull();
    } else if (type == EdmPrimitiveTypeKind.Stream) {
      if (primitiveValue instanceof final Link stream) {
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
      final var string = type.valueToString(primitiveValue);
      json.value(string);
    }
  }
}

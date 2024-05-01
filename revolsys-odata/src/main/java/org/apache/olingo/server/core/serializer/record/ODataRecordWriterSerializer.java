package org.apache.olingo.server.core.serializer.record;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.ODataEntity;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
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
import org.apache.olingo.server.core.ODataWritableContent;

import com.revolsys.io.IoFactory;
import com.revolsys.odata.model.ODataEntityIterator;
import com.revolsys.record.io.RecordWriterFactory;

public class ODataRecordWriterSerializer implements ODataSerializer {

  private final RecordWriterFactory writerFactory;

  public ODataRecordWriterSerializer(final ContentType contentType) {
    this.writerFactory = IoFactory.factoryByMediaType(RecordWriterFactory.class,
      contentType.toString());
  }

  @Override
  public SerializerResult complex(final ServiceMetadata metadata, final EdmComplexType type,
    final String name, final Object value, final ComplexSerializerOptions options)
    throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult complexCollection(final ServiceMetadata metadata, final String name,
    final EdmComplexType type, final Object value, final ComplexSerializerOptions options)
    throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult entity(final ServiceMetadata metadata, final EdmEntityType entityType,
    final ODataEntity entity, final EntitySerializerOptions options) throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult entityCollection(final ServiceMetadata metadata,
    final EdmEntityType entityType, final AbstractEntityCollection entitySet,
    final EntityCollectionSerializerOptions options) throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerStreamResult entityCollectionStreamed(final ServiceMetadata metadata,
    final EdmEntityType entityType, final ODataEntityIterator entities,
    final EntityCollectionSerializerOptions options) throws SerializerException {
    return ODataWritableContent.with(entities, entityType, this, metadata, options)
      .build();
  }

  @Override
  public SerializerResult error(final ODataServerError error) throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult metadataDocument(final ServiceMetadata serviceMetadata)
    throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult primitive(final ServiceMetadata metadata, final String name,
    final EdmPrimitiveType type, final PrimitiveSerializerOptions options, final Object value)
    throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult primitiveCollection(final ServiceMetadata metadata,
    final EdmPrimitiveType type, final String name, final PrimitiveSerializerOptions options,
    final Object value) throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult reference(final ServiceMetadata metadata, final EdmEntitySet edmEntitySet,
    final ODataEntity entity, final ReferenceSerializerOptions options) throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult referenceCollection(final ServiceMetadata metadata,
    final EdmEntitySet edmEntitySet, final AbstractEntityCollection entityCollection,
    final ReferenceCollectionSerializerOptions options) throws SerializerException {
    throw new UnsupportedOperationException();

  }

  @Override
  public SerializerResult serviceDocument(final ServiceMetadata serviceMetadata,
    final String serviceRoot) throws SerializerException {
    throw new UnsupportedOperationException();

  }

  public void writeRecords(final ServiceMetadata metadata, final EdmEntityType entityType,
    final ODataEntityIterator records, final EntityCollectionSerializerOptions options,
    final OutputStream outputStream) {
    records.transactionRun(() -> {
      try (
        final var writer = this.writerFactory.newRecordWriter(entityType.getName(), entityType,
          outputStream, StandardCharsets.UTF_8)) {
        writer.writeAll(records.records());
      }
    });
  }

}

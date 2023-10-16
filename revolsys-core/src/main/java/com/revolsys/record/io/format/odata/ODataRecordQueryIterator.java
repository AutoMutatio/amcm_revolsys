package com.revolsys.record.io.format.odata;

import java.util.Map;
import java.util.function.Function;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.http.HttpRequestBuilderFactory;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.RecordState;
import com.revolsys.record.io.RecordIterator;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;

public class ODataRecordQueryIterator extends ODataJsonQueryIterator<Record>
  implements RecordReader, RecordIterator {

  private static Function<JsonObject, Record> newConverter(final ODataRecordStore recordStore,
    final Query query) {
    RecordFactory<Record> recordFactory = query.getRecordFactory();
    if (recordFactory == null) {
      recordFactory = recordStore.getRecordFactory();
    }
    final Function<JsonObject, Record> recordFactoryConverter = recordFactoryConverter(
      query.getRecordDefinition(), recordFactory);
    return recordFactoryConverter;
  }

  private static Function<JsonObject, Record> recordFactoryConverter(
    final RecordDefinition recordDefinition, final RecordFactory<Record> recordFactory) {
    return recordJson -> {
      final Record record = recordFactory.newRecord(recordDefinition);
      if (record != null) {
        record.setState(RecordState.INITIALIZING);
        for (final FieldDefinition field : record.getRecordDefinition().getFields()) {
          final String name = field.getName();
          final Object value = recordJson.getValue(name);
          record.setValue(field, value);
        }
        record.setState(RecordState.PERSISTED);
      }
      return record;
    };
  }

  private final RecordDefinition recordDefinition;

  public ODataRecordQueryIterator(final HttpRequestBuilderFactory requestFactory,
    final HttpRequestBuilder request, final Function<JsonObject, Record> recordConverter,
    final RecordDefinition recordDefinition) {
    super(requestFactory, request, recordConverter, null, Integer.MAX_VALUE);
    this.recordDefinition = recordDefinition;
  }

  public ODataRecordQueryIterator(final HttpRequestBuilderFactory requestFactory,
    final HttpRequestBuilder request, final RecordFactory<Record> recordFactory,
    final RecordDefinition recordDefinition) {
    super(requestFactory, request, recordFactoryConverter(recordDefinition, recordFactory), null,
      Integer.MAX_VALUE);
    this.recordDefinition = recordDefinition;
  }

  public ODataRecordQueryIterator(final ODataRecordStore recordStore,
    final HttpRequestBuilderFactory requestFactory, final Query query,
    final Map<String, Object> properties) {
    super(requestFactory, recordStore.newRequest(query), newConverter(recordStore, query),
      query.toString(), Integer.MAX_VALUE);

    this.recordDefinition = query.getRecordDefinition();
    setProperties(properties);
    this.limit = query.getLimit();
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }

}

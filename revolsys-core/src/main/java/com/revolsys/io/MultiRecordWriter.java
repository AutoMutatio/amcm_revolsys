package com.revolsys.io;

import java.util.Arrays;

import com.revolsys.record.Record;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinitionProxy;

public class MultiRecordWriter extends AbstractRecordWriter {

  private final Iterable<RecordWriter> writers;

  public MultiRecordWriter(final RecordDefinitionProxy recordDefinition,
    final Iterable<RecordWriter> writers) {
    super(recordDefinition);
    this.writers = writers;
  }

  public MultiRecordWriter(final RecordDefinitionProxy recordDefinition,
    final RecordWriter... writers) {
    super(recordDefinition);
    this.writers = Arrays.asList(writers);
  }

  @Override
  public void write(final Record record) {
    for (final RecordWriter writer : this.writers) {
      writer.write(record);
    }
  }

}

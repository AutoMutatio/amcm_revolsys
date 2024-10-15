package com.revolsys.record.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.revolsys.collection.iterator.AbstractIterator;
import com.revolsys.collection.list.ListEx;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.Transaction.SavedBuilder;
import com.revolsys.util.BaseCloseable;

public class QueryPagingIterator extends AbstractIterator<Record> {

  private final ListEx<String> fieldNames;

  private Identifier lastIdentifier;

  private Iterator<Record> iterator = Collections.emptyIterator();

  private final SavedBuilder transaction;

  private final int batchSize;

  private int pageRecordCount = -1;

  private int totalCount = 0;

  private RecordReader reader;

  private final Supplier<Query> queryFactory;

  public QueryPagingIterator(final Supplier<Query> queryFactory, final ListEx<String> fieldNames,
    final int batchSize) {
    this.queryFactory = queryFactory;
    this.fieldNames = fieldNames;
    this.transaction = Transaction.save();
    this.batchSize = batchSize;
  }

  @Override
  protected void closeDo() {
    BaseCloseable.closeValue(this.reader);
  }

  @Override
  protected Record getNext() throws NoSuchElementException {
    final var resultRecord = this.transaction.call(() -> {
      while (!this.iterator.hasNext()) {
        BaseCloseable.closeValue(this.reader);
        if (this.pageRecordCount == 0) {
          this.reader = null;
          this.iterator = null;
          return null;
        } else {
          this.reader = nextRecordReader();
          this.iterator = this.reader.iterator();
          this.pageRecordCount = 0;
        }
      }
      final Record record = this.iterator.next();
      this.pageRecordCount++;
      this.totalCount++;
      this.lastIdentifier = record.getIdentifier(this.fieldNames);
      return record;
    });
    if (resultRecord == null) {
      throw new NoSuchElementException();
    } else {
      return resultRecord;
    }
  }

  private RecordReader nextRecordReader() {
    this.pageRecordCount = 0;
    final Query query = this.queryFactory.get()
      .setOrderByFieldNames(this.fieldNames)
      .fetchSize(5000)
      .setLimit(this.batchSize);
    if (this.lastIdentifier != null) {
      int i = 0;

      final And equalConditions = Q.and();
      final Or or = new Or();

      for (final var fieldName : this.fieldNames) {
        final Object value = this.lastIdentifier.getValue(i);
        final var greaterThan = query.newCondition(fieldName, Q.GREATER_THAN, value);
        if (i == 0) {
          or.addCondition(greaterThan);
        } else {
          final var condition = equalConditions.clone()
            .addCondition(greaterThan);
          or.addCondition(condition);
        }
        if (i < this.fieldNames.size() - 1) {
          equalConditions.addCondition(query.newCondition(fieldName, Q.EQUAL, value));
        }
        i++;
      }
      query.and(or);
    }
    return query.getRecordReader();
  }

}

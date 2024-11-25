package com.revolsys.jdbc.io;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import org.springframework.dao.DataAccessException;

import com.revolsys.collection.iterator.AbstractIterator;
import com.revolsys.collection.list.ListEx;
import com.revolsys.exception.Exceptions;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.transaction.Transaction;

public class JdbcResultSetIterator extends AbstractIterator<Record> {

  private final ListEx<? extends QueryValue> selectExpressions;

  private final RecordDefinition recordDefinition;

  private final RecordFactory<Record> recordFactory;

  private final AbstractJdbcRecordStore recordStore;

  private final ResultSet resultSet;

  JdbcResultSetIterator(final AbstractJdbcRecordStore recordStore, final PathName typeName,
    final ListEx<QueryValue> selectExpressions, final ResultSet resultSet) throws SQLException {
    Transaction.assertInTransaction();

    this.recordStore = recordStore;
    this.selectExpressions = selectExpressions;
    this.resultSet = resultSet;
    this.recordFactory = recordStore.getRecordFactory();

    final var resultSetMetaData = this.resultSet.getMetaData();
    // TODO this needs to be looked at
    this.recordDefinition = this.recordStore.getRecordDefinition(typeName, resultSetMetaData,
      typeName.getName());
  }

  JdbcResultSetIterator(final AbstractJdbcRecordStore recordStore,
    final RecordDefinition recordDefinition, final ListEx<? extends QueryValue> selectExpressions,
    final ResultSet resultSet) {
    this.recordStore = recordStore;
    this.recordDefinition = recordDefinition;
    this.selectExpressions = selectExpressions;
    this.resultSet = resultSet;
    this.recordFactory = recordStore.getRecordFactory();
  }

  @Override
  public synchronized void closeDo() {
    JdbcUtils.close(this.resultSet);
  }

  @Override
  protected Record getNext() throws NoSuchElementException {
    try {
      if (this.resultSet != null && this.resultSet.next()) {
        final Record record = this.recordStore.getNextRecord(this.recordDefinition,
          this.selectExpressions, this.recordFactory, this.resultSet);
        return record;
      } else {
        close();
        throw new NoSuchElementException();
      }
    } catch (final SQLException e) {
      if (Exceptions.isInterruptException(e)) {
        throw Exceptions.toRuntimeException(e);
      }
      final DataAccessException e2 = this.recordStore.getException("Get Next", "", e);
      close();
      throw e2;
    } catch (final RuntimeException | Error e) {
      close();
      throw e;
    }
  }

}

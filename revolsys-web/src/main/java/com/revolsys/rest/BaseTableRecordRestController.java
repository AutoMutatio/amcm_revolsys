package com.revolsys.rest;

import java.io.IOException;
import java.util.function.Consumer;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.io.PathName;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.TableRecordStoreConnection;
import com.revolsys.record.schema.TableRecordStoreFactory;

public class BaseTableRecordRestController extends AbstractTableRecordRestController {

  protected final PathName tablePath;

  protected final String typeName;

  public BaseTableRecordRestController(final PathName tablePath) {
    this.tablePath = tablePath;
    this.typeName = tablePath.getName();
  }

  protected <RS extends AbstractTableRecordStore> RS getTableRecordStore(
    final TableRecordStoreFactory connection) {
    return super.getTableRecordStore(connection, this.tablePath);
  }

  protected void handleGetRecord(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final String fieldName,
    final Object value) throws IOException {
    final Query query = getTableRecordStore(connection, this.typeName).newQuery(connection)//
      .and(fieldName, value);
    handleGetRecord(connection, request, response, query);
  }

  protected void handleInsertRecord(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response) throws IOException {
    handleInsertRecord(connection, request, response, this.tablePath);
  }

  protected Record handleUpdateRecordDo(final TableRecordStoreConnection connection,
    final HttpServletResponse response, final Identifier id, final Consumer<Record> updateAction)
    throws IOException {
    return handleUpdateRecordDo(connection, response, this.tablePath, id, updateAction);
  }

  protected void handleUpdateRecordDo(final TableRecordStoreConnection connection,
    final HttpServletResponse response, final Identifier id, final JsonObject values)
    throws IOException {
    final Record record = connection
      .transactionNewCall(() -> connection.updateRecord(this.tablePath, id, values));
    responseRecordJson(response, record);
  }

  protected Query newQuery(final TableRecordStoreConnection connection,
    final HttpServletRequest request) {
    return super.newQuery(connection, request, this.tablePath);
  }

}

package com.revolsys.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Consumer;
import java.util.function.Supplier;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import com.revolsys.collection.json.JsonList;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.io.PathName;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.format.json.JsonRecordWriter;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.TableRecordStoreConnection;
import com.revolsys.record.schema.TableRecordStoreFactory;
import com.revolsys.web.HttpServletUtils;

public class AbstractTableRecordRestController extends AbstractWebController {

  protected int maxPageSize = Integer.MAX_VALUE;

  public AbstractTableRecordRestController() {
  }

  protected <RS extends AbstractTableRecordStore> RS getTableRecordStore(
    final TableRecordStoreFactory connection, final CharSequence tablePath) {
    final RS tableRecordStore = connection.getTableRecordStore(tablePath);
    if (tableRecordStore == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    }
    return tableRecordStore;
  }

  protected void handleGetRecord(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final Query query)
    throws IOException {
    responseRecordJson(connection, request, response, query);
  }

  public void handleGetRecords(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final Query query)
    throws IOException {
    connection.transactionRun(() -> {
      try (
        final RecordReader records = query.getRecordReader()) {
        Long count = null;
        if (HttpServletUtils.getBooleanParameter(request, "$count")) {
          count = query.getRecordCount();
        }
        responseRecords(connection, request, response, query, records, count);
      }
    });
  }

  protected void handleInsertRecord(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    final CharSequence tablePath) throws IOException {
    final JsonObject json = readJsonBody(request);
    final Record record = connection.newRecord(tablePath, json);
    final Record savedRecord = connection.transactionNewCall(() -> connection.insertRecord(record));
    responseRecordJson(response, savedRecord);
  }

  protected Record handleUpdateRecordDo(final TableRecordStoreConnection connection,
    final HttpServletResponse response, final CharSequence tablePath, final Identifier id,
    final Consumer<Record> updateAction) throws IOException {
    return handleUpdateRecordDo(connection, response,
      () -> connection.updateRecord(tablePath, id, updateAction));
  }

  protected Record handleUpdateRecordDo(final TableRecordStoreConnection connection,
    final HttpServletResponse response, final CharSequence tablePath, final Identifier id,
    final JsonObject values) throws IOException {
    return handleUpdateRecordDo(connection, response,
      () -> connection.updateRecord(tablePath, id, values));
  }

  protected Record handleUpdateRecordDo(final TableRecordStoreConnection connection,
    final HttpServletResponse response, final Supplier<Record> action) throws IOException {
    final Record record = connection.transactionNewCall(() -> action.get());
    responseRecordJson(response, record);
    return record;
  }

  protected Record insertRecord(final TableRecordStoreConnection connection,
    final PathName tablePath, final JsonObject values) {
    final Record record = connection.newRecord(tablePath, values);
    return connection.insertRecord(record);
  }

  protected boolean isUpdateable(final TableRecordStoreConnection connection, final Identifier id) {
    return true;
  }

  protected Query newQuery(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final CharSequence tablePath) {
    final AbstractTableRecordStore recordStore = getTableRecordStore(connection, tablePath);
    return recordStore.newQuery(connection, request, Integer.MAX_VALUE);
  }

  protected void responseRecordJson(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final Query query)
    throws IOException {
    final Record record = query.getRecord();
    responseRecordJson(response, record);
  }

  public void responseRecords(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final Query query,
    final Long count) throws IOException {
    if (query == null) {
      responseJson(response, JsonObject.hash("value", JsonList.array()));
    }
    connection.transaction()
      .requiresNew()
      .readOnly()
      .run(() -> {
        try (
          final RecordReader records = query.getRecordReader()) {
          responseRecords(connection, request, response, query, records, count);
        }
      });
  }

  protected void responseRecords(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final Query query,
    final RecordReader reader, final Long count) throws IOException {
    if ("csv".equals(request.getParameter("format"))) {
      responseRecordsCsv(response, reader);
    } else if ("xlsx".equals(request.getParameter("format"))) {
      responseRecords(response, reader, "Export", "xlsx");
    } else {
      responseRecordsJson(connection, request, response, reader, count, null, query.getOffset(),
        query.getLimit());
    }
  }

  public void responseRecordsJson(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final Query query,
    final Long count, final JsonObject extraData) throws IOException {
    connection.transaction()
      .requiresNew()
      .readOnly()
      .run(() -> {
        try (
          final RecordReader records = query.getRecordReader()) {
          responseRecordsJson(connection, request, response, records, count, extraData, query.getOffset(),
            query.getLimit());
        }
      });
  }

  protected void responseRecordsJson(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response, final RecordReader reader,
    final Long count, final JsonObject extraData, final int offset, final int limit) throws IOException {
    reader.open();
    setContentTypeJson(response);
    response.setStatus(200);
    try (
      PrintWriter writer = response.getWriter();
      JsonRecordWriter jsonWriter = new JsonRecordWriter(reader, writer);) {
      final JsonObject header = JsonObject.hash();
      jsonWriter.setHeader(header);
      if (count != null) {
        header.addValue("@odata.count", count);
      }
      if (extraData != null) {
        header.addValues(extraData);
      }
      jsonWriter.setItemsPropertyName("value");
      final int writeCount = jsonWriter.writeAll(reader);
      final int nextSkip = offset + writeCount;
      boolean writeNext = false;
      if (writeCount != 0) {
        if (count == null) {
          if (writeCount >= limit) {
            writeNext = true;
          }
        } else if (offset + writeCount < count) {
          writeNext = true;
        }
      }

      if (writeNext) {
        final String nextLink = HttpServletUtils.getFullRequestUriBuilder(request)
          .setParameter("$skip", nextSkip)
          .buildString();
        jsonWriter.setFooter(JsonObject.hash("@odata.nextLink", nextLink));
      }
    }
  }
}

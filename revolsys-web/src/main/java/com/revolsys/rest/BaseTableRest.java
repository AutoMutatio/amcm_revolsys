package com.revolsys.rest;

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.ResponseBody;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.TableRecordStoreConnection;

public class BaseTableRest extends AbstractTableRecordRestController {

  public BaseTableRest() {
    super();
  }

  @GetMapping("/app/api/{tableName}({id:[0-9]+})")
  public void getRecordIntegral(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    @PathVariable final String tableName, @PathVariable() final String id) throws IOException {
    getRecordString(connection, request, response, tableName, id);
  }

  @GetMapping("/app/api/{tableName}('{id}')")
  public void getRecordString(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    @PathVariable final String tableName, @PathVariable() final String id) throws IOException {
    final Query query = getTableRecordStore(connection, tableName).newQuery(connection)//
      .andEqualId(id);
    handleGetRecord(connection, request, response, query);
  }

  @GetMapping("/app/api/{tableName:[A-Za-z0-9_\\.]+}/$schema")
  public void getSchema(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    @PathVariable final String tableName) throws IOException {
    final AbstractTableRecordStore recordStore = getTableRecordStore(connection, tableName);
    responseSchema(response, recordStore);
  }

  @GetMapping(path = "/app/api/{tableName:[A-Za-z0-9_\\\\.]+}")
  public @ResponseBody Query listRecords(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    @PathVariable final String tableName) {
    return newQuery(connection, request, tableName);
  }

  @PostMapping(path = "/app/api/{tableName:[A-Za-z0-9_\\\\.]+}", consumes = {
    MediaType.APPLICATION_FORM_URLENCODED_VALUE
  })
  public void listRecordsPost(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    @PathVariable final String tableName) throws IOException {
    listRecords(connection, request, response, tableName);
  }

  public void responseSchema(final HttpServletResponse response,
    final AbstractTableRecordStore recordStore) throws IOException {
    final JsonObject jsonSchema = recordStore.schemaToJson();
    responseJson(response, jsonSchema);
  }

}

package com.revolsys.record.io.format.odata;

import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.revolsys.collection.iterator.AbstractIterator;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.http.HttpRequestBuilderFactory;

public class ODataJsonQueryIterator<V> extends AbstractIterator<V> {

  private final String queryLabel;

  private Iterator<JsonObject> results;

  private URI nextURI;

  private final HttpRequestBuilderFactory requestFactory;

  private final Function<JsonObject, V> converter;

  private int readCount;

  private HttpRequestBuilder request;

  protected int limit = Integer.MAX_VALUE;

  private int count = -1;

  private final int pageLimit;

  private int pageCount = 0;

  public ODataJsonQueryIterator(final HttpRequestBuilderFactory requestFactory,
      final HttpRequestBuilder request, final Function<JsonObject, V> converter,
      final String queryLabel, final int pageLimit) {
    this.requestFactory = requestFactory;
    this.converter = converter;
    this.request = request;
    this.queryLabel = queryLabel != null ? queryLabel : request.getUri().toString();
    this.pageLimit = pageLimit;
  }

  void executeRequest() {
    final JsonObject json = this.request.getJson();
    if (json == null) {
      this.nextURI = null;
      this.results = Collections.emptyIterator();
      return;
    } else {
      if (this.count == -1) {
        this.count = json.getInteger("@odata.count", -1);

      }
      if (json.hasValue("@odata.nextLink")) {
        this.nextURI = json.getURI("@odata.nextLink");
      } else if (json.hasValue("nextLink")) {
        this.nextURI = json.getURI("nextLink");
      } else {
        this.nextURI = null;
      }

      this.results = json.<JsonObject>getList("value").iterator();
    }
  }

  public int getCount() {
    if (this.count == -1) {
      init();
    }
    return this.count;
  }

  @Override
  protected V getNext() throws NoSuchElementException {
    if (this.readCount >= this.limit) {
      throw new NoSuchElementException();
    }
    do {
      if (this.results != null && this.results.hasNext()) {
        final JsonObject recordJson = this.results.next();
        this.readCount++;
        return this.converter.apply(recordJson);
      }
      if (this.nextURI == null || ++this.pageCount >= this.pageLimit) {
        throw new NoSuchElementException();
      } else {
        this.request = this.requestFactory.get(this.nextURI);

        executeRequest();
      }
    } while (this.results != null);
    throw new NoSuchElementException();
  }

  @Override
  protected void initDo() {
    super.initDo();
    executeRequest();
  }

  @Override
  public String toString() {
    return this.queryLabel;
  }

}

package com.revolsys.record.io.format.odata;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.collection.iterator.AbstractIterator;
import com.revolsys.collection.iterator.IterableWithCount;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.exception.Exceptions;
import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.http.HttpRequestBuilderFactory;
import com.revolsys.util.Property;
import com.revolsys.util.concurrent.RateLimiter;

public class ODataJsonQueryIterator<V> extends AbstractIterator<V> implements IterableWithCount<V> {

  private final String queryLabel;

  private Iterator<JsonObject> results = Collections.emptyIterator();

  private URI nextURI;

  private final HttpRequestBuilderFactory requestFactory;

  private final Function<JsonObject, V> converter;

  private RateLimiter rateLimiter;

  private int readCount;

  private HttpRequestBuilder request;

  protected int limit = Integer.MAX_VALUE;

  private int count = -1;

  private final int pageLimit;

  private int pageCount = 0;

  private Consumer<URI> deltaLinkCallback;

  private URI deltaLink;

  private Function<Throwable, Boolean> errorHandler;

  private final boolean hadError = false;

  private Duration timeout = Duration.ofMinutes(5);

  public ODataJsonQueryIterator(final HttpRequestBuilderFactory requestFactory,
    final HttpRequestBuilder request, final Function<JsonObject, V> converter,
    final String queryLabel, final int pageLimit) {
    this.requestFactory = requestFactory;
    this.converter = converter;
    this.request = request;
    this.queryLabel = queryLabel != null ? queryLabel
      : request.getUri()
        .toString();
    this.pageLimit = pageLimit;
  }

  private void callbackDeltaLink(final URI deltaLink) {
    if (!this.hadError && this.deltaLinkCallback != null && deltaLink != null) {
      this.deltaLinkCallback.accept(deltaLink);
    }
  }

  @Override
  protected void closeDo() {
    super.closeDo();
    callbackDeltaLink(this.deltaLink);
  }

  public ODataJsonQueryIterator<V> deltaLinkCallback(final Consumer<URI> deltaLinkCallback) {
    this.deltaLinkCallback = deltaLinkCallback;
    return this;
  }

  /**
   * Error handler for iterating through the data. If that method returns true the iterator will terminate and the error will not be propagated.
   *
   * DOES NOT handle the cases where the converter has an error. Those errors will get propagated.
   *
   * @param errorHandler
   * @return
   */
  public ODataJsonQueryIterator<V> errorHandler(final Function<Throwable, Boolean> errorHandler) {
    this.errorHandler = errorHandler;
    return this;
  }

  boolean executeRequest(final boolean first) {
    try {
      callbackDeltaLink(this.nextURI);
      if (!first) {
        this.request = this.requestFactory.get(this.nextURI);
      }
      final var json = this.request.timeout(this.timeout)
        .rateLimiter(this.rateLimiter)
        .responseAsJson();
      if (Property.isEmpty(json)) {
        return false;
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

        if (json.hasValue("@odata.deltaLink")) {
          this.deltaLink = json.getURI("@odata.deltaLink");
        }
        final var results = json.<JsonObject> getList("value");
        if (results.isEmpty()) {
          return false;
        } else {
          this.results = results.iterator();
          return true;
        }
      }
    } catch (final Throwable e) {
      if (this.errorHandler == null || Exceptions.isInterruptException(e)
        || !this.errorHandler.apply(e)) {
        // Rethrow the error if there was no error handler, it was an interrupt
        // or it didn't handle
        throw e;
      } else {
        // Stop processing if the error handler processed the result
        return false;
      }
    }
  }

  @Override
  public long getCount() {
    if (this.count == -1) {
      init();
    }
    return this.count;
  }

  @Override
  protected V getNext() throws NoSuchElementException {
    if (this.readCount < this.limit) {
      boolean hasMore = true;
      while (hasMore) {
        if (this.results.hasNext()) {
          final JsonObject recordJson = this.results.next();
          this.readCount++;
          return this.converter.apply(recordJson);
        }
        if (this.nextURI == null || ++this.pageCount >= this.pageLimit) {
          hasMore = false;
        } else {
          if (!executeRequest(false)) {
            hasMore = false;
          }
        }
      }
    }
    throw new NoSuchElementException();
  }

  @Override
  protected void initDo() {
    super.initDo();
    if (!executeRequest(true)) {
      this.nextURI = null;
      this.results = Collections.emptyIterator();
    }
  }

  public ODataJsonQueryIterator<V> rateLimiter(final RateLimiter rateLimiter) {
    if (rateLimiter == null) {
      this.rateLimiter = RateLimiter.UNLIMITED;
    } else {
      this.rateLimiter = rateLimiter;
    }
    return this;
  }

  public ODataJsonQueryIterator<V> timeout(final Duration timeout) {
    if (timeout == null) {
      this.timeout = Duration.ZERO;
    } else {
      this.timeout = timeout;
    }
    return this;
  }

  @Override
  public String toString() {
    return this.queryLabel;
  }

}

package com.revolsys.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.net.ssl.SSLContext;

import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.Configurable;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.message.HeaderGroup;
import org.apache.http.protocol.HTTP;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.Args;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonParser;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.value.LazyValueHolder;
import com.revolsys.collection.value.Single;
import com.revolsys.collection.value.ValueHolder;
import com.revolsys.exception.ExceptionWithProperties;
import com.revolsys.exception.Exceptions;
import com.revolsys.io.IoUtil;
import com.revolsys.net.http.ApacheEntityInputStream;
import com.revolsys.net.http.ApacheHttpException;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.UriBuilder;
import com.revolsys.util.concurrent.Concurrent;
import com.revolsys.util.concurrent.RateLimiter;

public class HttpRequestBuilder {

  static class InternalEntityEclosingRequest extends HttpEntityEnclosingRequestBase {

    private final String method;

    InternalEntityEclosingRequest(final String method) {
      super();
      this.method = method;
    }

    @Override
    public String getMethod() {
      return this.method;
    }

  }

  static class InternalRequest extends HttpRequestBase {

    private final String method;

    InternalRequest(final String method) {
      super();
      this.method = method;
    }

    @Override
    public String getMethod() {
      return this.method;
    }

  }

  public static final StringEntity EMPTY_ENTITY = new StringEntity("", ContentType.TEXT_PLAIN);

  public static final ContentType XML = ContentType.create("application/xml",
    StandardCharsets.UTF_8);

  public static final ScopedValue<LazyValueHolder<HttpClient>> JDK_CLIENT = ScopedValue
    .newInstance();

  public static final long CONNECT_TIMEOUT = Duration.ofMinutes(5)
    .toMillis();

  public static HttpRequestBuilder copy(final HttpRequest request) {
    Args.notNull(request, "HTTP request");
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setRequest(request);
  }

  public static HttpRequestBuilder create(final String method) {
    Args.notBlank(method, "HTTP method");
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(method);
  }

  public static HttpRequestBuilder delete() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpDelete.METHOD_NAME);
  }

  public static HttpRequestBuilder delete(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpDelete.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder delete(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpDelete.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder get() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpGet.METHOD_NAME);
  }

  public static HttpRequestBuilder get(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpGet.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder get(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpGet.METHOD_NAME)
      .setUri(uri);
  }

  public static JsonObject getJson(final HttpResponse response) {
    final var entity = response.getEntity();
    if (entity == null) {
      return JsonObject.hash();
    }
    try (
      var in = entity.getContent()) {
      return JsonParser.read(in);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public static ListEx<Object> getList(final HttpResponse response) {
    final var entity = response.getEntity();
    try (
      var in = entity.getContent()) {
      return JsonParser.read(in);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public static String getString(final HttpResponse response) {
    final var entity = response.getEntity();
    try (
      var in = entity.getContent()) {
      return IoUtil.getString(in);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public static HttpRequestBuilder head() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpHead.METHOD_NAME);
  }

  public static HttpRequestBuilder head(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpHead.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder head(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpHead.METHOD_NAME)
      .setUri(uri);
  }

  public static boolean isSuccess(final HttpResponse response) {
    final StatusLine statusLine = response.getStatusLine();
    final int statusCode = statusLine.getStatusCode();
    return statusCode >= 200 && statusCode <= 299;
  }

  public static HttpClient jdkClient() {
    return JDK_CLIENT.get()
      .getValue();
  }

  public static void jdkClient(final Runnable action) {
    try (
      var client = ValueHolder.lazy(HttpRequestBuilder::newHttpClient)) {
      ScopedValue.where(JDK_CLIENT, client)
        .run(action);
    }
  }

  public static HttpClient newHttpClient() {
    return HttpClient.newBuilder()
      .connectTimeout(Duration.ofMillis(CONNECT_TIMEOUT))
      .followRedirects(Redirect.NORMAL)
      .executor(Concurrent.virtual("HttpClient")
        .newThreadPerTaskExecutor())
      .build();
  }

  public static HttpRequestBuilder options() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpOptions.METHOD_NAME);
  }

  public static HttpRequestBuilder options(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpOptions.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder options(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpOptions.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder patch() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPatch.METHOD_NAME);
  }

  public static HttpRequestBuilder patch(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPatch.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder patch(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPatch.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder post() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPost.METHOD_NAME);
  }

  public static HttpRequestBuilder post(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPost.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder post(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPost.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder put() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPut.METHOD_NAME);
  }

  public static HttpRequestBuilder put(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPut.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder put(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpPut.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder trace() {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpTrace.METHOD_NAME);
  }

  public static HttpRequestBuilder trace(final String uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpTrace.METHOD_NAME)
      .setUri(uri);
  }

  public static HttpRequestBuilder trace(final URI uri) {
    return HttpRequestBuilderFactory.FACTORY.newRequestBuilder()
      .setMethod(HttpTrace.METHOD_NAME)
      .setUri(uri);
  }

  private String method;

  private Charset charset = Consts.UTF_8;

  private ProtocolVersion version;

  private RateLimiter rateLimiter = RateLimiter.UNLIMITED;

  private URI uri;

  private HeaderGroup headerGroup;

  private HttpEntity entity;

  private List<NameValuePair> parameters;

  private RequestConfig config;

  private final Set<String> headerNames = new TreeSet<>();

  private final HttpRequestBuilderFactory factory;

  private Duration timeout = Duration.ZERO;

  public HttpRequestBuilder(final HttpRequestBuilderFactory factory) {
    this.factory = factory;
  }

  public HttpRequestBuilder addHeader(final Header header) {
    this.headerNames.add(header.getName());
    if (this.headerGroup == null) {
      this.headerGroup = new HeaderGroup();
    }
    this.headerGroup.addHeader(header);
    return this;
  }

  public HttpRequestBuilder addHeader(final String name, final String value) {
    final BasicHeader header = new BasicHeader(name, value);
    return addHeader(header);
  }

  public HttpRequestBuilder addHeaders(final Header[] headers) {
    for (final Header header : headers) {
      addHeader(header);
    }
    return this;
  }

  public HttpRequestBuilder addParameter(final NameValuePair parameter) {
    if (parameter != null) {
      if (this.parameters == null) {
        this.parameters = new LinkedList<>();
      }
      this.parameters.add(parameter);
    }
    return this;
  }

  public HttpRequestBuilder addParameter(final String name, final Object value) {
    String string;
    if (value == null) {
      string = null;
    } else {
      string = value.toString();
    }
    final BasicNameValuePair parameter = new BasicNameValuePair(name, string);
    return addParameter(parameter);
  }

  public HttpRequestBuilder addParameterNotNull(final String name, final Object value) {
    if (value != null) {
      final String string = value.toString();
      final BasicNameValuePair parameter = new BasicNameValuePair(name, string);
      return addParameter(parameter);
    }
    return this;
  }

  public HttpRequestBuilder addParameters(final Iterable<NameValuePair> parameters) {
    for (final NameValuePair parameter : parameters) {
      addParameter(parameter);
    }
    return this;
  }

  public HttpRequestBuilder addParameters(final NameValuePair... nvps) {
    for (final NameValuePair nvp : nvps) {
      addParameter(nvp);
    }
    return this;
  }

  public HttpRequestBuilder apply(final Consumer<HttpRequestBuilder> action) {
    action.accept(this);
    return this;
  }

  public HttpUriRequest build() {
    getFactory().preBuild(this);
    setConfig(RequestConfig.custom()
      .setConnectTimeout((int)CONNECT_TIMEOUT)
      .build());
    final HttpRequestBase result;
    URI uri = this.uri;
    if (uri == null) {
      uri = URI.create("/");
    }
    HttpEntity entityCopy = this.entity;
    if (this.parameters != null && !this.parameters.isEmpty()) {
      if (entityCopy == null && (HttpPost.METHOD_NAME.equalsIgnoreCase(this.method)
        || HttpPut.METHOD_NAME.equalsIgnoreCase(this.method))) {
        entityCopy = new UrlEncodedFormEntity(this.parameters,
          this.charset != null ? this.charset : HTTP.DEF_CONTENT_CHARSET);
      } else {
        uri = new UriBuilder(uri).setCharset(this.charset)
          .addParameters(this.parameters)
          .build();
      }
    }
    if (entityCopy == null) {
      result = new InternalRequest(this.method);
    } else {
      final InternalEntityEclosingRequest request = new InternalEntityEclosingRequest(this.method);
      request.setEntity(entityCopy);
      result = request;
    }
    result.setProtocolVersion(this.version);
    result.setURI(uri);
    if (this.headerGroup != null) {
      result.setHeaders(this.headerGroup.getAllHeaders());
    }
    result.setConfig(this.config);
    return result;
  }

  protected void configureClient(final HttpClientBuilder builder) {
    if (this.factory != null) {
      this.factory.configureClient(builder);
    }
  }

  public void execute() {
    final Consumer<HttpResponse> noop = response -> {
    };
    execute(noop);
  }

  public void execute(final BiConsumer<HttpUriRequest, HttpResponse> action) {
    execute(action, this.timeout);
  }

  public void execute(final BiConsumer<HttpUriRequest, HttpResponse> action,
    final Duration timeout) {
    final HttpUriRequest request = build();
    try (
      final var httpClient = newClient()) {
      final var response = getResponse(httpClient, request, timeout);
      final var isSuccess = isSuccess(response);
      if (isSuccess) {
        action.accept(request, response);
      } else {
        throw ApacheHttpException.create(request, response);
      }
    } catch (final RuntimeException | IOException e) {
      throw wrapException(request, e);
    }
  }

  public <V> V execute(final BiFunction<HttpUriRequest, HttpResponse, V> action) {
    return execute(action, this.timeout);
  }

  public <V> V execute(final BiFunction<HttpUriRequest, HttpResponse, V> action,
    final Duration timeout) {
    final Instant expireTime = toExpireTime(timeout);
    while (true) {
      final HttpUriRequest request = build();
      try (
        final CloseableHttpClient httpClient = newClient()) {
        this.rateLimiter.aquire();
        return httpClient.execute(request, response -> {
          final var isSuccess = isSuccess(response);
          if (isSuccess) {
            return action.apply(request, response);
          } else {
            throw ApacheHttpException.create(request, response);
          }
        });
      } catch (final ApacheHttpException e) {
        rateLimitApacheException(e, expireTime);
      } catch (final RuntimeException | IOException e) {
        throw wrapException(request, e);
      }
    }
  }

  public void execute(final Consumer<HttpResponse> action) {
    execute(action, this.timeout);
  }

  public void execute(final Consumer<HttpResponse> action, final Duration timeout) {
    final HttpUriRequest request = build();
    try (
      final var httpClient = newClient()) {
      final var response = getResponse(httpClient, request, timeout);
      final var isSuccess = isSuccess(response);
      if (isSuccess) {
        action.accept(response);
      } else {
        throw ApacheHttpException.create(request, response);
      }
    } catch (final RuntimeException | IOException e) {
      throw wrapException(request, e);
    }
  }

  public <V> V execute(final Function<HttpResponse, V> action) {
    return execute(action, this.timeout);
  }

  public <V> V execute(final Function<HttpResponse, V> action, final Duration timeout) {
    final Instant expireTime = toExpireTime(timeout);
    while (true) {
      final HttpUriRequest request = build();
      try (
        final CloseableHttpClient httpClient = newClient()) {
        this.rateLimiter.aquire();
        return httpClient.execute(request, response -> {
          final var isSuccess = isSuccess(response);
          if (isSuccess) {
            return action.apply(response);
          } else {
            throw ApacheHttpException.create(request, response);
          }
        });
      } catch (final ApacheHttpException e) {
        rateLimitApacheException(e, expireTime);
      } catch (final RuntimeException | IOException e) {
        throw wrapException(request, e);
      }
    }
  }

  /**
   * @since 4.4
   */
  public Charset getCharset() {
    return this.charset;
  }

  public RequestConfig getConfig() {
    return this.config;
  }

  public HttpEntity getEntity() {
    return this.entity;
  }

  public HttpRequestBuilderFactory getFactory() {
    return this.factory;
  }

  public Header getFirstHeader(final String name) {
    return this.headerGroup != null ? this.headerGroup.getFirstHeader(name) : null;
  }

  public Set<String> getHeaderNames() {
    return Collections.unmodifiableSet(this.headerNames);
  }

  public Header[] getHeaders(final String name) {
    return this.headerGroup != null ? this.headerGroup.getHeaders(name) : null;
  }

  public Header getLastHeader(final String name) {
    return this.headerGroup != null ? this.headerGroup.getLastHeader(name) : null;
  }

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  public <V> ListEx<V> getList() {
    return (ListEx)responseAsList();
  }

  public String getMethod() {
    return this.method;
  }

  public List<NameValuePair> getParameters() {
    return this.parameters != null ? new ArrayList<>(this.parameters) : new ArrayList<>();
  }

  public HttpResponse getResponse(final CloseableHttpClient httpClient,
    final HttpUriRequest request, final Duration timeout) {
    final Instant expireTime = toExpireTime(timeout);
    while (true) {
      this.rateLimiter.aquire();
      // Debug.printlnTime(request.getURI());
      try {
        final var response = httpClient.execute(request);
        if (isSuccess(response)) {
          return response;
        } else {
          throw ApacheHttpException.create(request, response);
        }
      } catch (final ApacheHttpException e) {
        rateLimitApacheException(e, expireTime);
      } catch (final RuntimeException | IOException e) {
        throw wrapException(request, e);
      }
    }
  }

  public URI getUri() {
    return this.uri;
  }

  public ProtocolVersion getVersion() {
    return this.version;
  }

  public boolean hasHeader(final String name) {
    return this.headerNames.contains(name);
  }

  public Single<JsonObject> jsonObject() {
    try {
      final JsonObject json = responseAsJson();
      return Single.ofNullable(json);
    } catch (final ApacheHttpException e) {
      final int code = e.getStatusCode();
      if (code == HttpStatus.SC_NOT_FOUND || code == HttpStatus.SC_GONE) {
        return Single.empty();
      } else {
        throw e;
      }
    }
  }

  public CloseableHttpClient newClient() {
    try {
      final SSLContext sslContext = SSLContextBuilder.create()
        .loadTrustMaterial(new TrustSelfSignedStrategy())
        .build();
      final SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory(
        sslContext, (hostname, session) -> true);
      final HttpClientBuilder builder = HttpClientBuilder//
        .create()
        .setSSLSocketFactory(connectionFactory);
      configureClient(builder);
      return builder.build();
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private void rateLimitApacheException(final ApacheHttpException e, final Instant timeoutIime) {
    if (e.getStatusCode() == 429) {
      // HTTP 429 Too Many Requests and the timeout hasn't expired
      // Wait until the duration specified in Retry-After
      final String retryAfter = e.getHeader("Retry-After");
      if (this.rateLimiter.pauseHttpRetryAfter(retryAfter, timeoutIime)) {
        // DON'T THROW THE EXCEPTION so it can retry
        return;
      }
    }
    throw e;
  }

  public HttpRequestBuilder rateLimiter(final RateLimiter rateLimiter) {
    if (rateLimiter == null) {
      this.rateLimiter = RateLimiter.UNLIMITED;
    } else {
      this.rateLimiter = rateLimiter;
    }
    return this;
  }

  public HttpRequestBuilder removeHeader(final Header header) {
    if (this.headerGroup != null) {
      this.headerGroup.removeHeader(header);
    }
    return this;
  }

  public HttpRequestBuilder removeHeaders(final String name) {
    if (name != null && this.headerGroup != null) {
      this.headerNames.remove(name);
      for (final HeaderIterator i = this.headerGroup.iterator(); i.hasNext();) {
        final Header header = i.nextHeader();
        if (name.equalsIgnoreCase(header.getName())) {
          i.remove();
        }
      }
    }
    return this;
  }

  public HttpRequestBuilder removeParameters(final String name) {
    if (name != null && this.parameters != null) {
      for (final Iterator<NameValuePair> i = this.parameters.iterator(); i.hasNext();) {
        final NameValuePair parameter = i.next();
        if (name.equalsIgnoreCase(parameter.getName())) {
          i.remove();
        }
      }
    }
    return this;
  }

  public ApacheEntityInputStream responseAsInputStream() {
    final HttpUriRequest request = build();
    final var httpClient = newClient();
    try {
      final var response = getResponse(httpClient, request, this.timeout);
      final var entity = response.getEntity();
      if (entity == null) {
        return null;
      } else {
        return new ApacheEntityInputStream(httpClient, entity);
      }
    } catch (final RuntimeException | IOException e) {
      BaseCloseable.closeSilent(httpClient);
      throw wrapException(request, e);
    }
  }

  public JsonObject responseAsJson() {
    return responseAsJson(this.timeout);
  }

  public JsonObject responseAsJson(final Duration timeout) {
    if (!this.headerNames.contains("Accept")) {
      setHeader("Accept", "application/json");
    }
    final Function<HttpResponse, JsonObject> function = HttpRequestBuilder::getJson;
    return execute(function, timeout);

  }

  public ListEx<Object> responseAsList() {
    if (!this.headerNames.contains("Accept")) {
      setHeader("Accept", "application/json");
    }
    final Function<HttpResponse, ListEx<Object>> function = HttpRequestBuilder::getList;
    return execute(function);
  }

  public String responseAsString() {
    final Function<HttpResponse, String> function = HttpRequestBuilder::getString;
    return execute(function);
  }

  public HttpRequestBuilder setCharset(final Charset charset) {
    this.charset = charset;
    return this;
  }

  public HttpRequestBuilder setConfig(final RequestConfig config) {
    this.config = config;
    return this;
  }

  public HttpRequestBuilder setContentLength(final long contentLength) {
    return setHeader("Content-Length", Long.toString(contentLength));
  }

  public HttpRequestBuilder setContentType(final String contentType) {
    return setHeader("Content-Type", contentType);
  }

  public HttpRequestBuilder setEmptyEntity() {
    setEntity(EMPTY_ENTITY);
    return this;
  }

  public HttpRequestBuilder setEntity(final HttpEntity entity) {
    this.entity = entity;
    return this;
  }

  public HttpRequestBuilder setHeader(final Header header) {
    if (this.headerGroup == null) {
      this.headerGroup = new HeaderGroup();
    }
    this.headerGroup.updateHeader(header);
    return this;
  }

  public HttpRequestBuilder setHeader(final String name, final String value) {
    final BasicHeader header = new BasicHeader(name, value);
    return setHeader(header);
  }

  public HttpRequestBuilder setJsonEntity(final JsonObject value) {
    final String jsonString = value.toJsonString();
    final StringEntity entity = new StringEntity(jsonString, ContentType.APPLICATION_JSON);
    setEntity(entity);
    return this;
  }

  HttpRequestBuilder setMethod(final HttpMethod method) {
    this.method = method.name();
    return this;
  }

  HttpRequestBuilder setMethod(final String method) {
    this.method = method;
    return this;
  }

  public HttpRequestBuilder setParameter(final NameValuePair parameter) {
    removeParameters(parameter.getName());
    return addParameter(parameter);
  }

  public HttpRequestBuilder setParameter(final String name, final Object value) {
    removeParameters(name);
    return addParameter(name, value);
  }

  HttpRequestBuilder setRequest(final HttpRequest request) {
    this.method = request.getRequestLine()
      .getMethod();
    this.version = request.getRequestLine()
      .getProtocolVersion();

    if (this.headerGroup == null) {
      this.headerGroup = new HeaderGroup();
    }
    this.headerGroup.clear();
    this.headerGroup.setHeaders(request.getAllHeaders());

    this.parameters = null;
    this.entity = null;

    if (request instanceof HttpEntityEnclosingRequest) {
      final HttpEntity originalEntity = ((HttpEntityEnclosingRequest)request).getEntity();
      final ContentType contentType = ContentType.get(originalEntity);
      if (contentType != null && contentType.getMimeType()
        .equals(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
        try {
          final List<NameValuePair> formParams = URLEncodedUtils.parse(originalEntity);
          if (!formParams.isEmpty()) {
            this.parameters = formParams;
          }
        } catch (final IOException ignore) {
        }
      } else {
        this.entity = originalEntity;
      }
    }

    if (request instanceof HttpUriRequest) {
      this.uri = ((HttpUriRequest)request).getURI();
    } else {
      this.uri = URI.create(request.getRequestLine()
        .getUri());
    }

    if (request instanceof Configurable) {
      this.config = ((Configurable)request).getConfig();
    } else {
      this.config = null;
    }
    return this;
  }

  HttpRequestBuilder setUri(final String uri) {
    if (uri == null) {
      this.uri = null;
    } else {
      this.uri = URI.create(uri);
    }
    return this;
  }

  HttpRequestBuilder setUri(final URI uri) {
    this.uri = uri;
    return this;
  }

  public HttpRequestBuilder setVersion(final ProtocolVersion version) {
    this.version = version;
    return this;
  }

  public Duration timeout() {
    return this.timeout;
  }

  public HttpRequestBuilder timeout(final Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  private Instant toExpireTime(final Duration timeout) {
    Instant expireTime;
    if (timeout.isNegative()) {
      expireTime = Instant.MAX;
    } else {
      expireTime = Instant.now()
        .plus(timeout);
    }
    return expireTime;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(this.method);
    builder.append(' ');
    builder.append(this.uri);
    builder.append(' ');
    builder.append(this.parameters);

    builder.append(", charset=");
    builder.append(this.charset);
    builder.append(", version=");
    builder.append(this.version);
    builder.append(", headerGroup=");
    builder.append(this.headerGroup);
    builder.append(", entity=");
    builder.append(this.entity);
    builder.append(", config=");
    builder.append(this.config);
    return builder.toString();
  }

  private ExceptionWithProperties wrapException(final HttpUriRequest request, final Throwable e) {
    final var uri = request.getURI();
    if (e instanceof final ApacheHttpException apacheE) {
      return apacheE;
    } else if (e instanceof final ExceptionWithProperties eprops) {
      return eprops.property("uri", uri);
    } else {
      return Exceptions.toWrapped(e)
        .property("uri", uri);
    }
  }

}

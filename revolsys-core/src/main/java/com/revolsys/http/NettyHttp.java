package com.revolsys.http;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.revolsys.net.TrustAllX509TrustManager;
import com.revolsys.reactive.chars.ByteBufFluxProcessor;
import com.revolsys.reactive.chars.ByteBufs;
import com.revolsys.record.io.format.json.FluxSinkValueProcessor;
import com.revolsys.record.io.format.json.JsonChararacterProcessor;
import com.revolsys.record.io.format.json.JsonList;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.io.format.json.JsonType;
import com.revolsys.record.io.format.json.ToJsonProcessor;
import com.revolsys.util.Debug;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.Http2SslContextSpec;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClient.ResponseReceiver;

public class NettyHttp {
  private static Http2SslContextSpec defaultSslSpec = Http2SslContextSpec.forClient()
    .configure(s -> s.trustManager(TrustAllX509TrustManager.INSTANCE));

  public static final Consumer<? super HttpHeaders> ACCEPT_JSON_HEADERS = addHeaderConsumer(
    "Accept", "application/json");

  public static final Consumer<? super HttpHeaders> CONTENT_TYPE_URL_ENCODED_HEADERS = addHeaderConsumer(
    "Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");

  public static final Consumer<? super HttpHeaders> CONTENT_TYPE_JSON_HEADERS = addHeaderConsumer(
    "Content-Type", "application/json; charset=UTF-8");

  public static Consumer<? super HttpHeaders> addHeaderConsumer(final String name,
    final String value) {
    return headers -> {
      headers.add(name, value);
    };
  }

  public static HttpClient createHttpClient() {
    return HttpClient.create().secure(s -> s.sslContext(defaultSslSpec));
  }

  public static Flux<ByteBuf> getByteBuf(final HttpClient client, final String uri) {
    final var receiver = client//
      .get()
      .uri(uri);
    return receiveByteBuf(receiver, b -> b);
  }

  public static <O> Flux<O> getBytes(final HttpClient client, final String uri,
    final ByteBufFluxProcessor processor) {
    Debug.log(uri);
    final ResponseReceiver<?> receiver = client//
      .get()
      .uri(uri);
    return NettyHttp.receiveByteBuf(receiver, bytes -> processor.process(bytes));
  }

  public static Http2SslContextSpec getDefaultSslSpec() {
    return defaultSslSpec;
  }

  public static Mono<InputStream> getInputStream(final HttpClient client, final String uri) {
    final var receiver = client//
      .get()
      .uri(uri);
    return receiveByteBuf(receiver, ByteBufs::inputStream$).singleOrEmpty();
  }

  private static <J extends JsonType> Mono<J> getJson(final HttpClient client, final String uri) {
    Debug.log(uri);
    final ResponseReceiver<?> receiver = client//
      .headers(headers -> {
        headers.add("Accept", "application/json");
      })
      .get()
      .uri(uri);
    return NettyHttp.<J> receiveJson(receiver).singleOrEmpty();
  }

  public static Mono<JsonObject> getJsonList(final HttpClient client, final String uri) {
    return getJson(client, uri);
  }

  public static Mono<JsonList> getJsonList(final HttpClient client, final String uri,
    final ByteBufFluxProcessor processor) {
    final Flux<JsonList> json$ = getBytes(client, uri, processor);
    return json$.singleOrEmpty();
  }

  public static Mono<JsonObject> getJsonObject(final HttpClient client, final String uri) {
    return getJson(client, uri);
  }

  public static Mono<JsonObject> getJsonObject(final HttpClient client, final String uri,
    final ByteBufFluxProcessor processor) {
    final Flux<JsonObject> json$ = getBytes(client, uri, processor);
    return json$.singleOrEmpty();
  }

  public static Mono<JsonList> jsonList(final ByteBufFlux bytes) {
    return singleValue(bytes);
  }

  public static Mono<JsonObject> jsonObject(final ByteBufFlux bytes) {
    return singleValue(bytes);
  }

  public static Cookie newCookie(final String name, final String value) {
    return new DefaultCookie(name, value);
  }

  public static Mono<JsonObject> postAcceptJson(final HttpClient client, final String uri,
    final FormUrlEncodedString body) {
    return NettyHttp
      .<JsonObject, JsonObject> postAcceptJson(client, uri,
        "application/x-www-form-urlencoded; charset=UTF-8", body, null)
      .single();
  }

  public static <O, J extends JsonType> Flux<O> postAcceptJson(final HttpClient client,
    final String uri, final FormUrlEncodedString body,
    final Function<Mono<J>, ? extends Publisher<O>> action) {
    return postAcceptJson(client, uri, "application/x-www-form-urlencoded; charset=UTF-8", body,
      action);
  }

  public static <O, J extends JsonType> Flux<O> postAcceptJson(final HttpClient client,
    final String uri, final String contentType, final FormUrlEncodedString body,
    final Function<Mono<J>, ? extends Publisher<O>> action) {
    Debug.log(uri);
    final ResponseReceiver<?> receiver = client//
      .headers(headers -> {
        headers.add("Content-Type", contentType).add("Accept", "application/json");
      })
      .post()
      .uri(uri)
      .send(ByteBufs.toByteBufFlux(body.toString()));
    return NettyHttp.receiveJson(receiver, action);
  }

  public static <O> Flux<O> postAcceptJsonList(final HttpClient client, final String uri,
    final FormUrlEncodedString body,
    final Function<Mono<JsonList>, ? extends Publisher<O>> action) {
    return postAcceptJson(client, uri, "application/x-www-form-urlencoded; charset=UTF-8", body,
      action);
  }

  public static <O> Flux<O> postAcceptJsonObject(final HttpClient client, final String uri,
    final FormUrlEncodedString body,
    final Function<Mono<JsonObject>, ? extends Publisher<O>> action) {
    return postAcceptJson(client, uri, "application/x-www-form-urlencoded; charset=UTF-8", body,
      action);
  }

  public static <O> Flux<O> receiveByteBuf(final ResponseReceiver<?> receiver,
    final Function<ByteBufFlux, ? extends Publisher<O>> handler) {
    return receiver.response((response, body) -> {
      final HttpResponseStatus status = response.status();
      if (status.equals(HttpResponseStatus.OK)) {
        return handler.apply(body);
      } else {
        return statusError$(status);
      }
    });
  }

  public static <J extends JsonType> Flux<J> receiveJson(final ResponseReceiver<?> receiver) {
    return receiveByteBuf(receiver, NettyHttp::singleValue);
  }

  public static <O, J extends JsonType> Flux<O> receiveJson(final ResponseReceiver<?> receiver,
    final Function<Mono<J>, ? extends Publisher<O>> handler) {
    return receiveByteBuf(receiver, bytes -> {
      final Mono<J> jsonObject$ = singleValue(bytes);
      return handler.apply(jsonObject$);
    });
  }

  public static void setDefaultSslSpec(final Http2SslContextSpec defaultSslSpec) {
    NettyHttp.defaultSslSpec = defaultSslSpec;
  }

  private static <V> Mono<V> singleValue(final ByteBufFlux bytes) {
    return Flux.create((final FluxSink<V> s) -> {
      final JsonChararacterProcessor toJson = new JsonChararacterProcessor(
        new ToJsonProcessor<V>(new FluxSinkValueProcessor<V>(s)));
      final Disposable subscription = ByteBufs.asCharBuffer(bytes, StandardCharsets.UTF_8, 8192)
        .doOnNext(buffer -> toJson.process(buffer))
        .doOnComplete(toJson::onComplete)
        .subscribe();
      s.onCancel(subscription);
      s.onDispose(subscription);
    }).publishOn(Schedulers.boundedElastic()).single();
  }

  public static <V> Mono<V> statusError$(final HttpResponseStatus status) {
    return Mono.error(new HttpException(status.code(), status.reasonPhrase()));
  }
}
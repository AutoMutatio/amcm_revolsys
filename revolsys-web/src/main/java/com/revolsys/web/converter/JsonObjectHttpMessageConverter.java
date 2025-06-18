package com.revolsys.web.converter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.lang.Nullable;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonParser;

public class JsonObjectHttpMessageConverter extends AbstractHttpMessageConverter<JsonObject> {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.ISO_8859_1;

  public static Charset getCharset(final HttpMessage message) {
    final var headers = message.getHeaders();
    final var contentType = headers.getContentType();
    if (contentType != null) {
      final var charset = contentType.getCharset();
      if (charset != null) {
        return charset;
      }
    }
    return StandardCharsets.UTF_8;
  }

  public JsonObjectHttpMessageConverter() {
    super(StandardCharsets.UTF_8, MediaType.APPLICATION_JSON);
  }

  @Override
  protected void addDefaultHeaders(final HttpHeaders headers, final JsonObject value,
    @Nullable final MediaType type) throws IOException {
    if (headers.getContentType() == null) {
      headers.setContentType(MediaType.APPLICATION_JSON);
    }
    super.addDefaultHeaders(headers, value, type);
  }

  @Override
  protected JsonObject readInternal(final Class<? extends JsonObject> clazz,
    final HttpInputMessage inputMessage) throws IOException {
    final Charset charset = getCharset(inputMessage);
    try (
      var body = inputMessage.getBody();
      var reader = new InputStreamReader(body, charset)) {
      return JsonParser.read(reader);
    }
  }

  @Override
  public boolean supports(final Class<?> clazz) {
    return JsonObject.class.isAssignableFrom(clazz);
  }

  @Override
  protected boolean supportsRepeatableWrites(final JsonObject value) {
    return true;
  }

  @Override
  protected void writeInternal(final JsonObject value, final HttpOutputMessage outputMessage)
    throws IOException {
    final Charset charset = getCharset(outputMessage);
    try (
      var out = outputMessage.getBody();
      var writer = new OutputStreamWriter(out, charset)) {
      if (value == null) {
        writer.write("null");
      } else {
        value.appendJson(writer);
      }
    }
  }

}

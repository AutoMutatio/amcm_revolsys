package com.revolsys.web.converter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;

import com.revolsys.record.Record;
import com.revolsys.record.io.BufferedWriterEx;

public class RecordHttpMessageConverter extends AbstractHttpMessageConverter<Record> {

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

  public RecordHttpMessageConverter() {
    super(StandardCharsets.UTF_8, MediaType.APPLICATION_JSON);
  }

  @Override
  protected void addDefaultHeaders(final HttpHeaders headers, final Record value,
    final MediaType type) throws IOException {
    if (headers.getContentType() == null) {
      headers.setContentType(MediaType.APPLICATION_JSON);
    }
    super.addDefaultHeaders(headers, value, type);
  }

  @Override
  protected boolean canRead(final MediaType mediaType) {
    return false;
  }

  @Override
  protected Record readInternal(final Class<? extends Record> clazz, final HttpInputMessage inputMessage)
    throws IOException, HttpMessageNotReadableException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supports(final Class<?> clazz) {
    return Record.class.isAssignableFrom(clazz);
  }

  @Override
  protected boolean supportsRepeatableWrites(final Record value) {
    return true;
  }

  @Override
  protected void writeInternal(final Record value, final HttpOutputMessage outputMessage)
    throws IOException {
    final Charset charset = getCharset(outputMessage);
    try (
      var out = outputMessage.getBody();
      var writer = BufferedWriterEx.forStream(out, charset)) {
      if (value == null) {
        writer.write("null");
      } else {
        value.appendJson(writer);
      }
    }
  }

}

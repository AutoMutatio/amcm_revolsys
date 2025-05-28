package com.revolsys.web.converter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.format.json.JsonRecordWriter;
import com.revolsys.record.query.Query;
import com.revolsys.web.HttpServletUtils;

public class QueryHttpMessageConverter extends AbstractHttpMessageConverter<Query> {

  public QueryHttpMessageConverter() {
    super(StandardCharsets.UTF_8, MediaType.APPLICATION_JSON);
  }

  @Override
  protected void addDefaultHeaders(final HttpHeaders headers, final Query value,
    @Nullable final MediaType type) throws IOException {
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
  protected Query readInternal(final Class<? extends Query> clazz,
    final HttpInputMessage inputMessage) throws IOException {
    throw new UnsupportedOperationException("Cannot read query objects");
  }

  @Override
  public boolean supports(final Class<?> clazz) {
    return Query.class.isAssignableFrom(clazz);
  }

  @Override
  protected void writeInternal(final Query query, final HttpOutputMessage outputMessage)
    throws IOException {
    query.transaction()
      .requiresNew()
      .readOnly()
      .run(() -> {
        final var offset = query.getOffset();
        final var limit = query.getLimit();
        try (
          final RecordReader reader = query.getRecordReader()) {
          reader.open();
          try (
            var out = outputMessage.getBody();
            var writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            var jsonWriter = new JsonRecordWriter(reader, writer);) {
            final JsonObject header = JsonObject.hash();
            jsonWriter.setHeader(header);
            Long count = null;
            if (query.isReturnCount()) {
              count = query.getRecordCount();
            }
            if (count != null) {
              header.addValue("@odata.count", count);
            }
            final var resultHeaders = query.getResultHeaders();
            if (resultHeaders != null) {
              header.addValues(resultHeaders);
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
              final var request = ((ServletRequestAttributes)RequestContextHolder
                .currentRequestAttributes()).getRequest();
              final String nextLink = HttpServletUtils.getFullRequestUriBuilder(request)
                .setParameter("$skip", nextSkip)
                .buildString();
              jsonWriter.setFooter(JsonObject.hash("@odata.nextLink", nextLink));
            }
          }
        }
      });
  }

}

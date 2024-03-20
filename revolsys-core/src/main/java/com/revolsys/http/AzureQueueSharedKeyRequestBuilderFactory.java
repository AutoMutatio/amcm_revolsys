package com.revolsys.http;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.exception.Exceptions;

public class AzureQueueSharedKeyRequestBuilderFactory extends HttpRequestBuilderFactory {

  private static final char NEWLINE = '\n';

  private static final char COLON = ':';

  private static final char SLASH = '/';

  private static final char COMMA = '/';

  private static final List<String> HEADERS = Arrays.asList("Content-MD5", "Content-Type", "Date");

  public static AzureQueueSharedKeyRequestBuilderFactory forConnectionString(
    final JsonObject connectionParameters) {
    final String accountName = connectionParameters.getString("AccountName");
    final String accountKey = connectionParameters.getString("AccountKey");
    final AzureQueueSharedKeyRequestBuilderFactory requestBuilderFactory = new AzureQueueSharedKeyRequestBuilderFactory(
      accountName, accountKey);
    return requestBuilderFactory;
  }

  private final String accountName;

  private final byte[] accountKeyBytes;

  private final SecretKeySpec secretKey;

  public AzureQueueSharedKeyRequestBuilderFactory(final String accountName,
    final String accountKey) {
    this.accountName = accountName;
    this.accountKeyBytes = Base64.getDecoder()
      .decode(accountKey);
    this.secretKey = new SecretKeySpec(this.accountKeyBytes, "HmacSHA256");

  }

  public String getAccountName() {
    return this.accountName;
  }

  String getSharedKeyAuthorization(final StringBuilder data)
    throws NoSuchAlgorithmException, InvalidKeyException {
    final String signature = sign(data);
    return String.format("SharedKey %s:%s", this.accountName, signature);
  }

  @Override
  public void preBuild(final HttpRequestBuilder requestBuilder) {
    final String accountName = getAccountName();
    try {
      final String date = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneOffset.UTC)
        .format(Instant.now());
      requestBuilder.setHeader("Date", date);

      final StringBuilder data = new StringBuilder();
      final String method = requestBuilder.getMethod();
      data.append(method);
      data.append(NEWLINE);

      for (final String name : HEADERS) {
        Header header = requestBuilder.getFirstHeader(name);
        if (header == null) {
          if ("Content-Type".equals(name)) {
            final HttpEntity entity = requestBuilder.getEntity();
            if (entity != null) {
              header = entity.getContentType();
            }
          }
        }
        if (header != null) {
          final String value = header.getValue();
          if (value != null) {
            data.append(value);
          }
        }
        data.append(NEWLINE);
      }
      for (final String name : requestBuilder.getHeaderNames()) {
        if (name.startsWith("x-ms-")) {
          data.append(name);
          data.append(COLON);
          final Header header = requestBuilder.getFirstHeader(name);
          data.append(header.getValue());
          data.append(NEWLINE);
        }
      }
      data.append(SLASH);
      data.append(accountName);
      final String path = requestBuilder.getUri()
        .getRawPath();
      data.append(path);
      final Map<String, Set<String>> parameters = new TreeMap<>();
      for (final NameValuePair parameter : requestBuilder.getParameters()) {
        final String name = parameter.getName()
          .toLowerCase();
        final String value = parameter.getValue();
        Set<String> values = parameters.get(name);
        if (values == null) {
          values = new TreeSet<>();
          parameters.put(path, values);
        }
        values.add(value);
      }
      for (final String name : parameters.keySet()) {
        data.append(NEWLINE);
        data.append(name);
        data.append(COLON);
        final Set<String> values = parameters.get(name);
        boolean first = true;
        for (final String value : values) {
          if (first) {
            first = false;
          } else {
            data.append(COMMA);
          }
          data.append(value);
        }
      }
      final String authorization = getSharedKeyAuthorization(data);
      requestBuilder.setHeader("Authorization", authorization);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private String sign(final StringBuilder data)
    throws NoSuchAlgorithmException, InvalidKeyException {
    final Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(this.secretKey);
    final byte[] signatureBytes = mac.doFinal(data.toString()
      .getBytes(StandardCharsets.UTF_8));
    return Base64.getEncoder()
      .encodeToString(signatureBytes);
  }

  @Override
  public String toString() {
    return this.accountName.toString();
  }

}

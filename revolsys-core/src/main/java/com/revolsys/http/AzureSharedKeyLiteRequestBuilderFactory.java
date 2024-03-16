package com.revolsys.http;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
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
import com.revolsys.io.map.ObjectFactoryConfig;

public class AzureSharedKeyLiteRequestBuilderFactory extends HttpRequestBuilderFactory {

  private static final char NEWLINE = '\n';

  private static final char COLON = ':';

  private static final char SLASH = '/';

  private static final char COMMA = '/';

  private static final List<String> STANDARD_HEADERS = Arrays.asList("Content-MD5", "Content-Type",
    "Date");

  public static AzureSharedKeyLiteRequestBuilderFactory forConnectionString(
    final JsonObject connectionParameters) {
    final String accountName = connectionParameters.getString("AccountName");
    final String accountKey = connectionParameters.getString("AccountKey");
    return fromAccountNameAndKey(accountName, accountKey);
  }

  public static AzureSharedKeyLiteRequestBuilderFactory fromAccountNameAndKey(
    final String accountName, final String accountKey) {
    if (accountName != null && accountKey != null) {
      return new AzureSharedKeyLiteRequestBuilderFactory(accountName, accountKey);
    }
    return null;
  }

  public static AzureSharedKeyLiteRequestBuilderFactory fromConfig(
    final ObjectFactoryConfig factoryConfig, final JsonObject config) {
    final String accountName = config.getString("accountName");
    final String secretId = config.getString("secretId");
    final String accountKey = SecretStore.getSecretValue(factoryConfig, secretId, "accountKey");
    return fromAccountNameAndKey(accountName, accountKey);
  }

  private final String accountName;

  private final SecretKeySpec secretKey;

  public AzureSharedKeyLiteRequestBuilderFactory(final String accountName,
    final SecretKeySpec secretKey) {
    this.accountName = accountName;
    this.secretKey = secretKey;
  }

  public AzureSharedKeyLiteRequestBuilderFactory(final String accountName,
    final String accountKey) {
    this.accountName = accountName;
    final byte[] accountKeyBytes = Base64.getDecoder()
      .decode(accountKey);
    this.secretKey = new SecretKeySpec(accountKeyBytes, "HmacSHA256");
  }

  public String getAccountName() {
    return this.accountName;
  }

  public SecretKeySpec getSecretKey() {
    return this.secretKey;
  }

  String getSharedKeyAuthorization(final StringBuilder data)
    throws NoSuchAlgorithmException, InvalidKeyException {
    final String signature = sign(data);
    return String.format("SharedKey %s:%s", this.accountName, signature);
  }

  String getSharedKeyLiteAuthorization(final StringBuilder data)
    throws NoSuchAlgorithmException, InvalidKeyException {
    final String signature = sign(data);
    return String.format("SharedKeyLite %s:%s", this.accountName, signature);
  }

  @Override
  public void preBuild(final HttpRequestBuilder requestBuilder) {
    final String accountName = getAccountName();
    try {
      final Instant now = Instant.now();
      final String date = AzureSharedKeyRequestBuilderFactory.DATE_FORMAT.withZone(ZoneOffset.UTC)
        .format(now);
      requestBuilder.setHeader("Date", date);

      final StringBuilder data = new StringBuilder();
      final String method = requestBuilder.getMethod();
      data.append(method);
      data.append(NEWLINE);

      for (final String name : STANDARD_HEADERS) {
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
          parameters.put(name, values);
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
      final String authorization = getSharedKeyLiteAuthorization(data);
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

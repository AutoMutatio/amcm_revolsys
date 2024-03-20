package com.revolsys.http;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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

public class AzureSharedKeyRequestBuilderFactory extends HttpRequestBuilderFactory {

  public static final DateTimeFormatter DATE_FORMAT;

  static {
    // manually code maps to ensure correct data always used
    // (locale data can be changed by application code)
    final Map<Long, String> dow = new HashMap<>();
    dow.put(1L, "Mon");
    dow.put(2L, "Tue");
    dow.put(3L, "Wed");
    dow.put(4L, "Thu");
    dow.put(5L, "Fri");
    dow.put(6L, "Sat");
    dow.put(7L, "Sun");
    final Map<Long, String> moy = new HashMap<>();
    moy.put(1L, "Jan");
    moy.put(2L, "Feb");
    moy.put(3L, "Mar");
    moy.put(4L, "Apr");
    moy.put(5L, "May");
    moy.put(6L, "Jun");
    moy.put(7L, "Jul");
    moy.put(8L, "Aug");
    moy.put(9L, "Sep");
    moy.put(10L, "Oct");
    moy.put(11L, "Nov");
    moy.put(12L, "Dec");
    DATE_FORMAT = new DateTimeFormatterBuilder().parseCaseInsensitive()
      .parseLenient()
      .optionalStart()
      .appendText(DAY_OF_WEEK, dow)
      .appendLiteral(", ")
      .optionalEnd()
      .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
      .appendLiteral(' ')
      .appendText(MONTH_OF_YEAR, moy)
      .appendLiteral(' ')
      .appendValue(YEAR, 4) // 2 digit year not handled
      .appendLiteral(' ')
      .appendValue(HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(MINUTE_OF_HOUR, 2)
      .optionalStart()
      .appendLiteral(':')
      .appendValue(SECOND_OF_MINUTE, 2)
      .optionalEnd()
      .appendLiteral(' ')
      .appendOffset("+HHMM", "GMT") // should handle
                                    // UT/Z/EST/EDT/CST/CDT/MST/MDT/PST/MDT
      .toFormatter(Locale.getDefault(Locale.Category.FORMAT));
  }

  private static final char NEWLINE = '\n';

  private static final char COLON = ':';

  private static final char SLASH = '/';

  private static final char COMMA = '/';

  private static final List<String> SHARED_KEY_HEADERS = Arrays.asList("Content-Encoding",
    "Content-Language", "Content-Length", "Content-MD5", "Content-Type", "Date",
    "If-Modified-Since", "If-Match", "If-None-Match", "If-Unmodified-Since", "Range");

  public static AzureSharedKeyRequestBuilderFactory forConnectionString(
    final JsonObject connectionParameters) {
    final String accountName = connectionParameters.getString("AccountName");
    final String accountKey = connectionParameters.getString("AccountKey");
    return fromAccountNameAndKey(accountName, accountKey);
  }

  public static AzureSharedKeyRequestBuilderFactory fromAccountNameAndKey(final String accountName,
    final String accountKey) {
    if (accountName != null && accountKey != null) {
      return new AzureSharedKeyRequestBuilderFactory(accountName, accountKey);
    }
    return null;
  }

  public static AzureSharedKeyRequestBuilderFactory fromConfig(
    final ObjectFactoryConfig factoryConfig, final JsonObject config) {
    final String accountName = config.getString("accountName");
    final String secretId = config.getString("secretId");
    final String accountKey = SecretStore.getSecretValue(factoryConfig, secretId, "accountKey");
    return fromAccountNameAndKey(accountName, accountKey);
  }

  private final String accountName;

  private final byte[] accountKeyBytes;

  private final SecretKeySpec secretKey;

  public AzureSharedKeyRequestBuilderFactory(final String accountName, final String accountKey) {
    this.accountName = accountName;
    this.accountKeyBytes = Base64.getDecoder()
      .decode(accountKey);
    this.secretKey = new SecretKeySpec(this.accountKeyBytes, "HmacSHA256");

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
      final String date = DATE_FORMAT.withZone(ZoneOffset.UTC)
        .format(Instant.now());
      requestBuilder.setHeader("Date", date);

      final StringBuilder data = new StringBuilder();
      final String method = requestBuilder.getMethod();
      data.append(method);
      data.append(NEWLINE);

      for (final String name : SHARED_KEY_HEADERS) {
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
          String value = header.getValue();
          if (value != null) {
            if ("Content-Type".equals(name)) {
              if ("0".equals(value)) {
                value = "";
              }
            }
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

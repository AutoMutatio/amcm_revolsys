package com.revolsys.http;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import com.revolsys.net.oauth.BearerToken;
import com.revolsys.record.io.format.json.JsonObject;

public class AzureCliBearerToken extends BearerToken {
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
      .withZone(ZoneId.of("UTC"));

  public AzureCliBearerToken(final JsonObject config) {
    super(config.getString("accessToken"));
    final String expiresOn = config.getString("expiresOn");
    final TemporalAccessor expireTime = TIMESTAMP_FORMATTER.parse(expiresOn);
    long expireMillis = Instant.from(expireTime).toEpochMilli();
    setExpireTime(expireMillis);
  }

}
package com.revolsys.net.oauth;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Instant;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.regex.Pattern;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonParser;

public class JsonWebToken implements Principal {

  public static JsonObject decodeJson(final String base64) {
    final byte[] decoded = Base64.getDecoder()
      .decode(base64);
    final String string = new String(decoded, StandardCharsets.UTF_8);
    return JsonParser.read(string);
  }

  private final JsonObject header;

  private final JsonObject payload;

  private final byte[] signatureBytes;

  private final String token;

  private final byte[] payloadBytes;

  private final byte[] headerBytes;

  private final String headerText;

  private final String payloadText;

  public JsonWebToken(final String token) {
    this.token = token;
    final int firstDot = token.indexOf('.');
    final int secondDot = token.indexOf('.', firstDot + 1);
    final String headerBase64 = token.substring(0, firstDot);
    final Decoder decoder = Base64.getUrlDecoder();
    if ("0".equals(headerBase64)) {
      this.headerBytes = new byte[] {
        0
      };
      this.headerText = "0";
      this.header = JsonObject.EMPTY;
    } else {
      this.headerBytes = decoder.decode(headerBase64);
      this.headerText = new String(this.headerBytes, StandardCharsets.UTF_8);
      this.header = JsonParser.read(this.headerText);
    }
    final String payloadBase64 = token.substring(firstDot + 1, secondDot);
    this.payloadBytes = decoder.decode(payloadBase64);
    this.payloadText = new String(this.payloadBytes, StandardCharsets.UTF_8);
    this.payload = JsonParser.read(this.payloadText);

    final String signatureText = token.substring(secondDot + 1);
    this.signatureBytes = decoder.decode(signatureText);
  }

  public boolean equalsSubject(final String subject) {
    return this.payload.equalValue("subject", subject);
  }

  public String getAudience() {
    return getString("aud");
  }

  public Instant getExpiry() {
    return getTime("exp");
  }

  public JsonObject getHeader() {
    return this.header;
  }

  public String getId() {
    return getString("jti");
  }

  public Instant getIssuedAt() {
    return getTime("iat");
  }

  public String getIssuer() {
    return getString("iss");
  }

  @Override
  public String getName() {
    var name = getString("oid");
    if (name == null) {
      name = getString("sub");
    }
    return name;
  }

  public Instant getNotBefore() {
    return getTime("nbf");
  }

  public JsonObject getPayload() {
    return this.payload;
  }

  public byte[] getSignature() {
    return this.signatureBytes;
  }

  public String getString(final String name) {
    return this.payload.getString(name);
  }

  public String getSubject() {
    return getString("sub");
  }

  public Instant getTime(final String name) {
    final Integer seconds = this.payload.getInteger(name);
    if (seconds == null) {
      return null;
    } else {
      return Instant.ofEpochSecond(seconds);
    }
  }

  public String getToken() {
    return this.token;
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  public boolean isValid() {
    try {
      final var issuer = getIssuer();
      if (!this.header.equalValue("typ", "JWT") && this.header.hasValue("typ")
        || !this.header.equalValue("alg", "RS256") || !this.header.hasValue("kid")) {
        return false;
      }
      final Instant now = Instant.now();
      final Instant notBefore = getNotBefore();
      if (notBefore != null) {
        if (now.isBefore(notBefore)) {
          return false;
        }
      }

      final Instant expiry = getExpiry();
      if (expiry != null) {
        if (now.isAfter(expiry)) {
          return false;
        }
      }
      var issuerConfigUrl = issuer;
      if (!issuer.endsWith("/")) {
        issuerConfigUrl += "/";
      }
      issuerConfigUrl += ".well-known/openid-configuration";

      final URI openidConfigUri = URI.create(issuerConfigUrl);
      final var openIdConfig = OpenIdConfiguration.getConfiguration(openidConfigUri);
      if (openIdConfig == null) {
        return false;
      }
      final String tokenToSign = this.token.substring(0, this.token.lastIndexOf('.'));
      final String keyId = this.header.getString("kid");
      return openIdConfig.getJwtKeySet()
        .getKey(keyId)
        .isValid(tokenToSign, this.signatureBytes);
    } catch (final Exception e) {
      return false;
    }
  }

  public boolean isValid(final Iterable<String> issuers) {
    for (final String issuer : issuers) {
      if (isValid(issuer)) {
        return true;
      }
    }
    return false;
  }

  public boolean isValid(final Pattern pattern) {
    final String issuer = getIssuer();
    if (!pattern.matcher(issuer)
      .matches()) {
      return false;
    }
    return isValid();
  }

  public boolean isValid(final String issuer) {
    try {
      final String iss = getIssuer();
      if (!issuer.equals(iss)
        || !this.header.equalValue("typ", "JWT") && this.header.hasValue("typ")
        || !this.header.equalValue("alg", "RS256")) {
        return false;
      }
      return isValid();
    } catch (final Exception e) {
      return false;
    }
  }

  @Override
  public String toString() {
    return this.header + "\n" + this.payload;
  }

  public String toStringDump() {
    return this.headerText + "\n" + this.payloadText;
  }
}

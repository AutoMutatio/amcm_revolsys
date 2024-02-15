package com.revolsys.net.oauth;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.exception.Exceptions;

public class JwtKey {

  private final PublicKey publicKey;

  public JwtKey(final JsonObject data) {
    try {
      if ("RSA".equalsIgnoreCase(data.getString("kty"))) {
        final String n = data.getString("n");
        final String e = data.getString("e");
        final byte modulusB[] = Base64.getUrlDecoder()
          .decode(n);
        final byte exponentB[] = Base64.getUrlDecoder()
          .decode(e);
        final BigInteger bigModulus = new BigInteger(1, modulusB);
        final BigInteger bigExponent = new BigInteger(1, exponentB);
        this.publicKey = KeyFactory.getInstance("RSA")
          .generatePublic(new RSAPublicKeySpec(bigModulus, bigExponent));
      } else {
        throw new IllegalArgumentException("Only RSA is supported");
      }
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public boolean isValid(final String tokenToSign, final byte[] signatureBytes) {
    try {
      final Signature s = Signature.getInstance("SHA256withRSA");
      s.initVerify(this.publicKey);
      s.update(tokenToSign.getBytes(StandardCharsets.UTF_8));
      final boolean verify = s.verify(signatureBytes);
      return verify;
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

}

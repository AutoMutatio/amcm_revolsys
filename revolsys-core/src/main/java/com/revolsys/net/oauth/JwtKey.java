package com.revolsys.net.oauth;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.exception.Exceptions;

public class JwtKey {

  private final ListEx<PublicKey> publicKeys = Lists.newArray();

  public JwtKey(final JsonObject data) {
    try {
      if ("RSA".equalsIgnoreCase(data.getString("kty"))) {
        final String n = data.getString("n");
        final String e = data.getString("e");
        final byte modulusB[] = Base64.getUrlDecoder().decode(n);
        final byte exponentB[] = Base64.getUrlDecoder().decode(e);
        final BigInteger bigModulus = new BigInteger(1, modulusB);
        final BigInteger bigExponent = new BigInteger(1, exponentB);
        this.publicKeys.addValue(KeyFactory.getInstance("RSA")
          .generatePublic(new RSAPublicKeySpec(bigModulus, bigExponent)));
      } else {
        throw new IllegalArgumentException("Only RSA is supported");
      }
      // TODO check if this gets called
      for (final String certBase64 : data.<String> getList("x5c")) {
        final byte[] cert = Base64.getDecoder().decode(certBase64);
        final CertificateFactory factory = CertificateFactory.getInstance("X.509");
        final X509Certificate x509 = (X509Certificate)factory
          .generateCertificate(new ByteArrayInputStream(cert));
        this.publicKeys.addValue(x509.getPublicKey());
      }
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public boolean isValid(final String tokenToSign, final byte[] signatureBytes) {
    try {
      for (final var publicKey : this.publicKeys) {
        final Signature s = Signature.getInstance("SHA256withRSA");
        s.initVerify(publicKey);
        s.update(tokenToSign.getBytes(StandardCharsets.UTF_8));
        if (s.verify(signatureBytes)) {
          return true;
        }
      }
      return false;
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

};

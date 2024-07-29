package com.revolsys.net.oauth;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonParser;
import com.revolsys.exception.Exceptions;
import com.revolsys.net.http.ApacheHttpException;
import com.revolsys.net.http.exception.AuthenticationException;
import com.revolsys.parallel.ReentrantLockEx;

public class DeviceCodeResponse {

  private final OpenIdConnectClient client;

  private final String deviceCode;

  private final String expiresIn;

  private final int interval;

  private final String message;

  private final String userCode;

  private final String verificationUri;

  private final String scope;

  private final ReentrantLockEx lock = new ReentrantLockEx();

  private final Condition lockCondition = this.lock.newCondition();

  public DeviceCodeResponse(final OpenIdConnectClient client, final JsonObject response,
    final String scope) {
    this.client = client;
    this.deviceCode = response.getString("device_code");
    this.userCode = response.getString("user_code");
    this.verificationUri = response.getString("verification_uri");
    this.expiresIn = response.getString("expires_in");
    this.interval = response.getInteger("interval");
    this.message = response.getString("message");
    this.scope = scope;
  }

  public String getDeviceCode() {
    return this.deviceCode;
  }

  public String getExpiresIn() {
    return this.expiresIn;
  }

  public int getInterval() {
    return this.interval;
  }

  public String getMessage() {
    return this.message;
  }

  public OpenIdBearerToken getToken() {
    while (true) {
      try (
        var l = this.lock.lockX()) {
        try {
          this.lockCondition.await(this.interval, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          throw Exceptions.toRuntimeException(e);
        }
      }
      try {
        return this.client.tokenDeviceCode(this.deviceCode, this.scope);
      } catch (final AuthenticationException e) {
        if (e.getMessage()
          .startsWith("AADSTS70016")) {
          // wait and try again
        }
      } catch (final ApacheHttpException e) {
        final String errorText = e.getContent();
        try {
          final JsonObject json = JsonParser.read(errorText);
          final String error = json.getString("error");
          if ("authorization_pending".equals(error)) {
            // wait and try again
          } else if ("authorization_declined".equals(error)) {
            throw e;
          } else if ("bad_verification_code".equals(error)) {
            System.err.println(this.message);
          } else if ("expired_token".equals(error)) {
            throw e;
          } else {
            throw e;
          }
        } catch (final Exception e1) {
          throw e;
        }
      }
    }
  }

  public String getUserCode() {
    return this.userCode;
  }

  public String getVerificationUri() {
    return this.verificationUri;
  }

  @Override
  public String toString() {
    return this.deviceCode;
  }
}

package com.revolsys.http;

import org.jeometry.common.json.JsonObject;

import com.revolsys.net.oauth.BearerToken;

public class AzureManagedIdentityBearerToken extends BearerToken {

  public AzureManagedIdentityBearerToken(final JsonObject config, final String resource) {
    super(config, resource);
    final long expiresOn = config.getLong("expires_on");
    final long expireTime = expiresOn * 1000;
    setExpireTime(expireTime);
    final String returnedResource = config.getString("resource");
    setScope(resource, returnedResource);
  }

}


package com.revolsys.net.oauth;

import java.util.function.Supplier;

import org.springframework.dao.TransientDataAccessResourceException;

import com.revolsys.collection.value.ValueHolder;

public class AccessTokenSupplier implements Supplier<String> {

  private final ValueHolder<BearerToken> token;

  public AccessTokenSupplier(final BearerTokenRefresher tokenRefresh) {
    this.token = ValueHolder.lazy(tokenRefresh::refreshBearerToken, t -> t != null && !t.isExpired());
  }

  @Override
  public String get() {
    final var token = this.token.getValue();
    if (token == null) {
      throw new TransientDataAccessResourceException("Unable to acquire access token");
    } else {
      return token.getAccessToken();
    }
  }
}

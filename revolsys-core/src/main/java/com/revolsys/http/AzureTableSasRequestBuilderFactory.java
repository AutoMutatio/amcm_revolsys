package com.revolsys.http;

import java.util.function.Supplier;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.io.map.ObjectFactoryConfig;

public class AzureTableSasRequestBuilderFactory extends HttpRequestBuilderFactory {
  public static AzureTableSasRequestBuilderFactory fromConfig(
    final ObjectFactoryConfig factoryConfig, final JsonObject config) {
    return new AzureTableSasRequestBuilderFactory(() -> {
      final String sas = SecretStore.getSecretValue(factoryConfig, "secretId", "sas");
      if (sas == null) {
        return null;
      } else {
        return new SasToken(sas);
      }
    });
  }

  private final Supplier<SasToken> tokenRefresh;

  private SasToken token;

  public AzureTableSasRequestBuilderFactory(final Supplier<SasToken> tokenRefresh) {
    super();
    this.tokenRefresh = tokenRefresh;
  }

  @Override
  public void preBuild(final HttpRequestBuilder requestBuilder) {
    if (this.token == null || this.token.isExpired()) {
      this.token = this.tokenRefresh.get();
    }
    if (this.token != null) {
      this.token.applyTo(requestBuilder);
    }
  }
}

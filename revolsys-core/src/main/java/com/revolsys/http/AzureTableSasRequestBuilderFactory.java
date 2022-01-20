package com.revolsys.http;

import java.util.function.Supplier;

import org.apache.http.client.methods.RequestBuilder;

import com.revolsys.io.map.ObjectFactoryConfig;
import com.revolsys.record.io.format.json.JsonObject;

public class AzureTableSasRequestBuilderFactory extends ApacheHttpRequestBuilderFactory {
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

  private final Supplier<SasToken> tokenRefesh;

  private SasToken token;

  public AzureTableSasRequestBuilderFactory(final Supplier<SasToken> tokenRefesh) {
    super();
    this.tokenRefesh = tokenRefesh;
  }

  void applyToken(final RequestBuilder builder) {
    if (this.token == null || this.token.isExpired()) {
      this.token = this.tokenRefesh.get();
    }
    if (this.token != null) {
      this.token.applyTo(builder);
    }
  }

  @Override
  protected ApacheHttpRequestBuilder newRequestBuilder(final RequestBuilder requestBuilder) {
    return new AzureTableSasRequestBuilder(this, requestBuilder);
  }
}
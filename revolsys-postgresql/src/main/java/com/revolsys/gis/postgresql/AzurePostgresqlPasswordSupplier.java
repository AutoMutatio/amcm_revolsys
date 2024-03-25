
package com.revolsys.gis.postgresql;

import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.dao.TransientDataAccessResourceException;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.http.AzureManagedIdentityRequestBuilderFactory;
import com.revolsys.net.oauth.BearerToken;

/**
 * The authentication password supplier that enables authentication with Microsoft Entra
 * ID.
 */
public class AzurePostgresqlPasswordSupplier implements Supplier<String> {

  private static final String RESOURCE = "https://ossrdbms-aad.database.windows.net";

  private static final String SCOPE = "https://ossrdbms-aad.database.windows.net/.default";

  private final Function<BearerToken, BearerToken> tokenRefresh;

  private final ValueHolder<BearerToken> token;

  /**
   * Constructor with properties.
   *
   * @param properties the properties.
   */
  public AzurePostgresqlPasswordSupplier(final Properties properties) {
    if ("true".equals(properties.getProperty("azure.managedId", "false"))) {
      this.tokenRefresh = AzureManagedIdentityRequestBuilderFactory.tokenRefesh(RESOURCE);
    } else {
      throw new IllegalArgumentException("Invalid properties");
    }
    this.token = ValueHolder.lazy(this.tokenRefresh, t -> t != null && !t.isExpired());
  }

  @Override
  public String get() {
    final var token = this.token.getValue();
    if (token == null) {
      throw new TransientDataAccessResourceException("Unable to acquire access token");
    } else {
      final var accessToken = token.getAccessToken();
      return accessToken;
    }
  }
}

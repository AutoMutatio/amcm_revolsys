
package com.revolsys.gis.postgresql;

import static org.postgresql.util.PSQLState.INVALID_PASSWORD;

import java.util.Properties;
import java.util.function.Function;

import org.postgresql.plugin.AuthenticationPlugin;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.http.AzureManagedIdentityRequestBuilderFactory;
import com.revolsys.net.oauth.BearerToken;

/**
 * The authentication plugin that enables authentication with Microsoft Entra
 * ID.
 */
public class AzurePostgresqlAuthenticationPlugin implements AuthenticationPlugin {

  private static final String RESOURCE = "https://ossrdbms-aad.database.windows.net";

  private static final String SCOPE = "https://ossrdbms-aad.database.windows.net/.default";

  private final Function<BearerToken, BearerToken> tokenRefresh;

  private final ValueHolder<BearerToken> token;

  /**
   * Constructor with properties.
   *
   * @param properties the properties.
   */
  public AzurePostgresqlAuthenticationPlugin(final Properties properties) {
    if ("true".equals(properties.getProperty("azure.managedId", "false"))) {
      this.tokenRefresh = AzureManagedIdentityRequestBuilderFactory
          .tokenRefesh(RESOURCE);
    } else {
      throw new IllegalArgumentException("Invalid properties");
    }
    this.token = ValueHolder.lazy(this.tokenRefresh, t -> t != null && !t.isExpired());
  }

  /**
   * Callback method to provide the password to use for authentication.
   *
   * @param type The authentication method that the server is requesting.
   *
   *             <p>
   *             AzurePostgresqlAuthenticationPlugin is used as an extension to
   *             perform authentication with Microsoft Entra ID, the value here is
   *             CLEARTEXT_PASSWORD.
   *             </p>
   *
   *             When PostgreSQL client trying to connect with PostgreSQL server:
   *             1. Client will send startup packet to server, the server will
   *             return the AuthenticationRequestType it accepts,
   *             If the username is used to perform Microsoft Entra
   *             authentication, the server will return CLEARTEXT_PASSWORD.
   *             2. Client will do authentication (until AuthenticationOk).
   *
   * @return The password to use.
   * @throws PSQLException It will return a PSQLException if the password is null.
   */
  @Override
  public char[] getPassword(final AuthenticationRequestType type) throws PSQLException {
    final var token = this.token.getValue();
    if (token == null) {
      throw new PSQLException("Unable to acquire access token", INVALID_PASSWORD);
    } else {
      final var accessToken = token.getAccessToken();
      return accessToken.toCharArray();
    }
  }
}

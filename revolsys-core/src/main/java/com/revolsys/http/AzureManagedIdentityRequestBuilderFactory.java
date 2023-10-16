package com.revolsys.http;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;

import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.message.BasicHeader;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.net.http.ApacheHttp;
import com.revolsys.net.http.SimpleNameValuePair;
import com.revolsys.net.oauth.BearerToken;

public class AzureManagedIdentityRequestBuilderFactory extends BearerTokenRequestBuilderFactory {
  public static final String ENDPOINT_URL;

  public static final Header IDENTITY_HEADER;

  private static final NameValuePair API_VERSION;

  private static boolean AVAILABLE;

  static {
    final String apiVersion = "2019-08-01";
    String url = System.getenv("IDENTITY_ENDPOINT");
    String headerName = "X-IDENTITY-HEADER";
    String headerValue = System.getenv("IDENTITY_HEADER");
    boolean available = false;
    if (url != null && headerValue != null) {
      available = true;
    } else {
      url = "http://169.254.169.254/metadata/identity/oauth2/token";
      headerName = "Metadata";
      headerValue = "true";
      available = Files.exists(Paths.get("C:\\WindowsAzure"));
    }

    ENDPOINT_URL = url;
    API_VERSION = new SimpleNameValuePair("api-version", apiVersion);
    IDENTITY_HEADER = new BasicHeader(headerName, headerValue);
    AVAILABLE = available;
  }

  public static RequestBuilder createTokenRequestBuilder(final String resource) {
    return RequestBuilder//
      .get(ENDPOINT_URL)
      .addHeader(IDENTITY_HEADER)
      .addParameter(API_VERSION)
      .addParameter("resource", resource);
  }

  public static boolean isAvailable() {
    return AVAILABLE;
  }

  public static final Function<BearerToken, BearerToken> tokenRefesh(final String resource) {
    return token -> {
      if (isAvailable()) {
        final RequestBuilder requestBuilder = createTokenRequestBuilder(resource);
        final JsonObject response = ApacheHttp.getJson(requestBuilder);
        return new AzureManagedIdentityBearerToken(response, resource);
      } else {
        return null;
      }
    };
  }

  public AzureManagedIdentityRequestBuilderFactory(final String resource) {
    super(tokenRefesh(resource));
  }

}

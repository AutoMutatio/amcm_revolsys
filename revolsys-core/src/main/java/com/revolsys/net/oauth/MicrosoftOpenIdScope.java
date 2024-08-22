package com.revolsys.net.oauth;

public class MicrosoftOpenIdScope extends OpenIdScope {

  public static MicrosoftOpenIdScope fromResource(final String resource) {
    final var scope = resource + "/.default";
    return new MicrosoftOpenIdScope(scope, resource);
  }

  private final String resource;

  private final OpenIdScope resourceScope;

  public MicrosoftOpenIdScope(final String scope, final String resource) {
    super(scope);
    this.resource = resource;
    this.resourceScope = new OpenIdScope(resource);
  }

  public String getResource() {
    return this.resource;
  }

  public OpenIdScope getResourceScope() {
    return this.resourceScope;
  }
}

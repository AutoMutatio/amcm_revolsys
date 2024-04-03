package com.revolsys.net.oauth;

public class OpenIdScope implements CharSequence {

  private final String scope;

  public OpenIdScope(final String scope) {
    this.scope = scope;
  }

  @Override
  public char charAt(final int index) {
    return this.scope.charAt(index);
  }

  public String getScope() {
    return this.scope;
  }

  @Override
  public int length() {
    return this.scope.length();
  }

  @Override
  public CharSequence subSequence(final int start, final int end) {
    return this.scope.subSequence(start, end);
  }

  @Override
  public String toString() {
    return this.scope;
  }
}

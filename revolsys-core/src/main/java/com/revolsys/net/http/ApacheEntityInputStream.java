package com.revolsys.net.http;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import com.revolsys.io.DelegatingInputStream;
import com.revolsys.util.BaseCloseable;

public class ApacheEntityInputStream extends DelegatingInputStream {

  private final CloseableHttpClient client;

  public ApacheEntityInputStream(final CloseableHttpClient client, final HttpEntity entity)
    throws IOException {
    super(entity.getContent());
    this.client = client;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      if (this.client != null) {
        BaseCloseable.closeSilent(this.client);
      }
    }
  }
}

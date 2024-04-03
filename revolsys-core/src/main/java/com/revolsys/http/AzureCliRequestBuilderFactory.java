package com.revolsys.http;

import java.io.File;
import java.util.List;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonParser;
import com.revolsys.collection.list.Lists;
import com.revolsys.exception.Exceptions;
import com.revolsys.io.FileUtil;
import com.revolsys.io.IoUtil;
import com.revolsys.logging.Logs;
import com.revolsys.net.oauth.BearerToken;
import com.revolsys.net.oauth.OpenIdScope;

public class AzureCliRequestBuilderFactory extends BearerTokenRequestBuilderFactory {

  public static BearerToken newToken(final String scope) {
    final List<String> command = Lists.newArray("cmd", "/c", "az", "account", "get-access-token",
        "--scope", scope);
    final ProcessBuilder builder = new ProcessBuilder(command);
    final File logFile = FileUtil.newTempFile("file", "json");
    builder.redirectErrorStream(true);
    builder.redirectOutput(logFile);
    try {
      final Process process = builder.start();
      if (process.waitFor() == 0) {
        final String commandOutput = IoUtil.getString(logFile)
            .strip();
        final JsonObject result = JsonParser.read(commandOutput);
        return new AzureCliBearerToken(result);
      } else {
        final String commandOutput = IoUtil.getString(logFile)
            .strip();
        try {
          throw new RuntimeException(commandOutput);
        } catch (final Exception e) {
          Logs.error(AzureCliRequestBuilderFactory.class,
              "Unknown error getting token\n" + e.getMessage(), e);
        }
      }
    } catch (final InterruptedException e) {
      throw Exceptions.toRuntimeException(e);
    } catch (final Throwable e) {
      Logs.error(AzureCliRequestBuilderFactory.class, "Error getting token", e);
    } finally {
      logFile.delete();
    }
    return null;
  }

  public AzureCliRequestBuilderFactory(final OpenIdScope scope) {
    this(scope.getScope());
  }

  public AzureCliRequestBuilderFactory(final String resource) {
    super(token -> newToken(resource));
  }
}

package com.revolsys.mssqlserver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.value.ValueHolder;
import com.revolsys.jdbc.io.AbstractJdbcDatabaseFactory;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.net.oauth.BearerToken;
import com.revolsys.net.oauth.BearerTokenFactory;
import com.revolsys.net.oauth.MicrosoftOpenIdScope;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;
import com.revolsys.util.UrlUtil;

public class MicrosoftSqlServer extends AbstractJdbcDatabaseFactory {
  private static final String REGEX_NAME = "\\p{IsAlphabetic}[\\p{IsAlphabetic}0-9_\\$]*";

  private static final Pattern PATTERN_URL = Pattern.compile("jdbc:sqlserver://(?:"
    + "([a-zA-Z-0-9][a-zA-Z-0-9\\.\\-]*" + "(?:\\\\([a-zA-Z-0-9][a-zA-Z-0-9\\\\.\\\\-]*))?" // Host
    + "(?::(\\d+))?" // Optional port
    + ")?" + ")?(?:;(" + REGEX_NAME + "=.*))?" // Parameters
  );

  private static final List<FieldDefinition> CONNECTION_FIELD_DEFINITIONS = Arrays.asList();

  private static final MicrosoftOpenIdScope SCOPE = MicrosoftOpenIdScope
    .fromResource("https://database.windows.net");

  public static ValueHolder<String> activeDirectoryAccessToken(final BearerTokenFactory refresher) {
    return refresher.lazyValue(SCOPE)
      .then(BearerToken::getAccessToken);
  }

  public MicrosoftSqlServer() {
  }

  @Override
  public List<FieldDefinition> getConnectionFieldDefinitions() {
    return CONNECTION_FIELD_DEFINITIONS;
  }

  @Override
  public String getDriverClassName() {
    return SQLServerDriver.class.getName();
  }

  @Override
  public String getName() {
    return "Microsoft SqlServer Database";
  }

  @Override
  public String getProductName() {
    // TODO is this correct
    return "Microsoft JDBC Driver";
  }

  @Override
  public List<String> getRecordStoreFileExtensions() {
    return Collections.emptyList();
  }

  @Override
  public Class<? extends RecordStore> getRecordStoreInterfaceClass(
    final Map<String, ? extends Object> connectionProperties) {
    return JdbcRecordStore.class;
  }

  @Override
  public String getVendorName() {
    return "sqlserver";
  }

  @Override
  public JdbcRecordStore newRecordStore(final DataSource dataSource) {
    return new MicrosoftSqlServerRecordStore(dataSource);
  }

  @Override
  public JdbcRecordStore newRecordStore(final MapEx connectionProperties) {
    return new MicrosoftSqlServerRecordStore(this, connectionProperties);
  }

  @Override
  public Map<String, Object> parseUrl(final String url) {
    if (url != null && url.startsWith("jdbc:sqlserver")) {
      final Matcher hostMatcher = PATTERN_URL.matcher(url);
      final Map<String, Object> parameters = new LinkedHashMap<>();
      if (hostMatcher.matches()) {
        parameters.put("recordStoreType", getName());
        final Map<String, Object> urlParameters = UrlUtil.getQueryStringMap(hostMatcher.group(4));
        parameters.putAll(urlParameters);

        final String host = hostMatcher.group(1);
        parameters.put("host", Strings.lowerCase(host));
        final String database = hostMatcher.group(2);
        parameters.put("database", database);
        final String port = hostMatcher.group(1);
        parameters.put("port", port);
        parameters.put("namedConnection", null);
        return parameters;
      }
    }
    return Collections.emptyMap();
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public String toUrl(final Map<String, Object> urlParameters) {
    final StringBuilder url = new StringBuilder("jdbc:sqlserver:");
    final String host = Maps.getString(urlParameters, "host");
    final Integer port = Maps.getInteger(urlParameters, "port");
    final String database = Maps.getString(urlParameters, "database");

    final boolean hasHost = Property.hasValue(host);
    final boolean hasPort = port != null;
    if (hasHost || hasPort) {
      url.append("//");
      if (hasHost) {
        url.append(host);
      }
      if (Property.hasValue(database)) {
        url.append('\\');
        url.append(database);
      }
      if (hasPort) {
        url.append(':');
        url.append(port);
      }
    }
    return url.toString();
  }

  @Override
  public DataAccessException translateException(final String task, final String sql,
    final SQLException exception) {
    return translateSqlStateException(task, sql, exception);
  }
}

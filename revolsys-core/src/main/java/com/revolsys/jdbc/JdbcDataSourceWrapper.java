package com.revolsys.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class JdbcDataSourceWrapper extends JdbcDataSource {

  private final DataSource dataSource;

  public JdbcDataSourceWrapper(final DataSource dataSource) {
    super();
    this.dataSource = dataSource;
    this.exceptionTranslator.setDataSource(dataSource);
  }

  @Override
  public Connection getConnection(final String username, final String password)
      throws SQLException {
    throw new UnsupportedOperationException("Username/password connections are not supported");
  }

  @Override
  protected Connection getConnectionInternal() throws SQLException {
    return this.dataSource.getConnection();
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return this.dataSource.getLoginTimeout();
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.dataSource.getLogWriter();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return this.dataSource.getParentLogger();
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return this.dataSource.isWrapperFor(iface);
  }

  @Override
  public void setLoginTimeout(final int seconds) throws SQLException {
    this.dataSource.setLoginTimeout(seconds);
  }

  @Override
  public void setLogWriter(final PrintWriter out) throws SQLException {
    this.dataSource.setLogWriter(out);
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return this.dataSource.unwrap(iface);

  }

}

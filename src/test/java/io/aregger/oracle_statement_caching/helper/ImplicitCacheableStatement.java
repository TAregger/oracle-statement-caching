package io.aregger.oracle_statement_caching.helper;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

import java.sql.SQLException;

public class ImplicitCacheableStatement implements CacheableStatement {

  private final String sql;

  public ImplicitCacheableStatement(String sql) {
    this.sql = sql;
  }

  @Override
  public String getSql() {
    return sql;
  }

  @Override
  public void closePreparedStatement(OraclePreparedStatement statement) throws SQLException {
    statement.close();
  }

  @Override
  public OraclePreparedStatement getCachedStatement(OracleConnection connection) throws SQLException {
    return PreparedStatementHelper.prepare(connection, sql);
  }
}

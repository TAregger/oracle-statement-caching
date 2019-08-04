package io.aregger.oracle_statement_caching.helper;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

import java.sql.SQLException;

public class ExplicitCacheableStatement implements CacheableStatement {

  private final String sql;
  private final String cacheKey;

  public ExplicitCacheableStatement(String sql, String cacheKey) {
    this.sql = sql;
    this.cacheKey = cacheKey;
  }

  @Override
  public String getSql() {
    return sql;
  }

  @Override
  public void closePreparedStatement(OraclePreparedStatement statement) throws SQLException {
    statement.closeWithKey(cacheKey);
  }

  @Override
  public OraclePreparedStatement getCachedStatement(OracleConnection connection) throws SQLException {
    return (OraclePreparedStatement) connection.getStatementWithKey(cacheKey);
  }
}

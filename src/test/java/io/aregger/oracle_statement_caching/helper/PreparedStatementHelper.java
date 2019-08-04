package io.aregger.oracle_statement_caching.helper;

import oracle.jdbc.OraclePreparedStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class PreparedStatementHelper {

  private PreparedStatementHelper() {
    throw new AssertionError("non-instantiable class");
  }

  public static OraclePreparedStatement prepare(Connection connection, String sql) throws SQLException {
    return (OraclePreparedStatement) connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
  }
}

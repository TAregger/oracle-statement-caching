package io.aregger.oracle_statement_caching.helper;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

import java.sql.SQLException;

public interface CacheableStatement {

  String getSql();

  void closePreparedStatement(OraclePreparedStatement statement) throws SQLException;

  OraclePreparedStatement getCachedStatement(OracleConnection connection) throws SQLException;

}

package io.aregger.oracle_statement_caching;

import io.aregger.oracle_statement_caching.helper.CacheableStatement;
import io.aregger.oracle_statement_caching.helper.ImplicitCacheableStatement;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class ImplicitStatementCacheTest extends StatementCacheTest {

  @Override
  OracleConnection getConnection() throws SQLException {
    OracleConnection connection = (OracleConnection) DriverManager.getConnection(CONNECTION_STRING);
    connection.setImplicitCachingEnabled(true);
    connection.setStatementCacheSize(20);
    return connection;
  }

  @Override
  @Test
  public void testExecuteCachedStatementWithoutBindDirectBinding() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertThrows(SQLException.class, statement::executeQuery);
  }

  @Override
  @Test
  public void testExecuteCachedStatementWithoutBindStreamBinding() throws SQLException {
    CacheableStatement cacheableStatement = new ImplicitCacheableStatement("insert into t1 (c3) values (?)");
    super.testExecuteCachedStatementWithoutBindStreamBinding(cacheableStatement, statement -> assertThrows(SQLException.class, statement::executeUpdate));
  }

  @Override
  @Test
  public void testBatch() throws SQLException {
    CacheableStatement cacheableStatement = new ImplicitCacheableStatement("insert into t1(c2) values (?)");
    super.testBatch(cacheableStatement, 2);
  }

  @Override
  @Test
  public void testGeneratedKeys() throws SQLException {
    CacheableStatement cacheableStatement = new ImplicitCacheableStatement("insert into t1(c2) values (?)") {

      // prepare a statement with the same result set type as the one used in this test
      @Override
      public OraclePreparedStatement getCachedStatement(OracleConnection connection) throws SQLException {
        return (OraclePreparedStatement) connection.prepareStatement(getSql(), new String[]{"C1"});
      }
    };

    super.testGeneratedKeys(cacheableStatement, generatedKeys -> {
      try {
        assertFalse(generatedKeys.next());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  @Test
  public void testUpdateCount() throws SQLException {
    CacheableStatement cacheableStatement = new ImplicitCacheableStatement("insert into t1(c2) values (?)");
    super.testUpdateCount(cacheableStatement, false, 0);
  }

  @Override
  @Test
  public void testUpdateCountWithPrecedingGetUpdateCountCall() throws SQLException {
    CacheableStatement cacheableStatement = new ImplicitCacheableStatement("insert into t1(c2) values (?)");
    super.testUpdateCount(cacheableStatement, true, 0);
  }

  @Override
  @Test
  public void testExecuteCachedStatementWithoutBindLOBBinding() throws SQLException {
    OracleConnection connection = getConnection();
    dropTableIfExists(connection);
    createTable(connection);

    connection.createStatement().execute("create or replace procedure insertLob(pi_data clob) is\n" +
        "begin\n" +
        "execute immediate 'insert into t1 (c3) values (:pi_data)' using pi_data;\n" +
        "end;");

    String sql = "begin insertLob(pi_data => ?); end;";
    OracleCallableStatement statement = (OracleCallableStatement) connection.prepareCall(sql);

    char[] chars = new char[LOBSIZE];
    Arrays.fill(chars, 'b');
    String string = String.valueOf(chars);

    statement.setString(1, string);
    statement.execute();
    statement.close();

    statement = (OracleCallableStatement) connection.prepareCall(sql);
    assertThrows(SQLException.class, statement::execute);

    dropTableIfExists(connection);
    connection.createStatement().execute("drop procedure insertLob");
  }

  @Override
  @Test
  public void testParameterMetadata() throws SQLException {
    CacheableStatement cacheableStatement = new ImplicitCacheableStatement(QUERY);
    super.testParameterMetadata(cacheableStatement);
  }

  @Override
  CacheableStatement getDefaultStatement() {
    return new ImplicitCacheableStatement(QUERY);
  }

  @Override
  int getExpectedResultSetType() {
    return ResultSet.TYPE_SCROLL_INSENSITIVE;
  }

  @Override
  int getExpectedResultSetConcurrency() {
    return ResultSet.CONCUR_UPDATABLE;
  }

  @Override
  int getExpectedPrefetchSize() {
    return PREFETCH_SIZE_DEFAULT;
  }

  @Override
  int getExpectedLobPrefetchSize() {
    return LOB_PREFETCH_SIZE_DEFAULT;
  }

  @Override
  int getExpectedMaxFieldSize() {
    return MAX_FIELD_SIZE_DEFAULT;
  }

  @Override
  int getExpectedQueryTimeout() {
    return QUERY_TIMEOUT_DEFAULT;
  }
}

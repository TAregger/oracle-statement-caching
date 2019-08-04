package io.aregger.oracle_statement_caching;

import io.aregger.oracle_statement_caching.helper.CacheableStatement;
import io.aregger.oracle_statement_caching.helper.ExplicitCacheableStatement;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExplicitStatementCacheTest extends StatementCacheTest {

  private static final String CACHE_KEY = "junit-test";

  @Override
  OracleConnection getConnection() throws SQLException {
    OracleConnection connection = (OracleConnection) DriverManager.getConnection(CONNECTION_STRING);
    connection.setExplicitCachingEnabled(true);
    connection.setStatementCacheSize(20);
    return connection;
  }

  @Override
  @Test
  public void testExecuteCachedStatementWithoutBindDirectBinding() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    statement.executeQuery();
  }

  @Override
  @Test
  public void testExecuteCachedStatementWithoutBindStreamBinding() throws SQLException {
    CacheableStatement cacheableStatement = new ExplicitCacheableStatement("insert into t1 (c3) values (?)", CACHE_KEY);
    super.testExecuteCachedStatementWithoutBindStreamBinding(cacheableStatement, statement -> {
      try {
        statement.executeUpdate();

        assertRows();

      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
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
    statement.closeWithKey("test123");

    statement = (OracleCallableStatement) connection.getCallWithKey("test123");
    statement.execute();

    assertRows();

    dropTableIfExists(connection);
    connection.createStatement().execute("drop procedure insertLob");
  }

  private void assertRows() throws SQLException {
    ResultSet resultSet = getConnection().createStatement().executeQuery("select count(*) cnt, sum(length(c3)) c3length from t1");
    resultSet.next();
    assertEquals(2, resultSet.getInt(1), "Expected 2 rows");
    assertEquals(LOBSIZE * 2, resultSet.getInt(2), "Sum of column length c3 not expected");
  }

  @Override
  @Test
  public void testBatch() throws SQLException {
    CacheableStatement cacheableStatement = new ExplicitCacheableStatement("insert into t1(c2) values (?)", CACHE_KEY);
    super.testBatch(cacheableStatement, 4);

  }

  @Override
  @Test
  public void testUpdateCount() throws SQLException {
    CacheableStatement cacheableStatement = new ExplicitCacheableStatement("insert into t1(c2) values (?)", CACHE_KEY);
    super.testUpdateCount(cacheableStatement, false, 1);
  }

  @Override
  @Test
  public void testUpdateCountWithPrecedingGetUpdateCountCall() throws SQLException {
    CacheableStatement cacheableStatement = new ExplicitCacheableStatement("insert into t1(c2) values (?)", CACHE_KEY);
    super.testUpdateCount(cacheableStatement, true, -1);
  }


  @Override
  @Test
  public void testParameterMetadata() throws SQLException {
    CacheableStatement cacheableStatement = new ExplicitCacheableStatement(QUERY, CACHE_KEY);
    super.testParameterMetadata(cacheableStatement);
  }

  @Override
  @Test
  public void testGeneratedKeys() throws SQLException {
    CacheableStatement cacheableStatement = new ExplicitCacheableStatement("insert into t1(c2) values (?)", CACHE_KEY);
    super.testGeneratedKeys(cacheableStatement, generatedKeys -> {
      try {
        assertTrue(generatedKeys.next());
        assertEquals(1, generatedKeys.getLong(1));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  CacheableStatement getDefaultStatement() {
    return new ExplicitCacheableStatement(QUERY, CACHE_KEY);
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
    return PREFETCH_SIZE_MODIFIED;
  }

  @Override
  int getExpectedLobPrefetchSize() {
    return LOB_PREFETCH_SIZE_MODIFIED;
  }

  @Override
  int getExpectedMaxFieldSize() {
    return MAX_FIELD_SIZE_MODIFIED;
  }

  @Override
  int getExpectedQueryTimeout() {
    return QUERY_TIMEOUT_MODIFIED;
  }
}

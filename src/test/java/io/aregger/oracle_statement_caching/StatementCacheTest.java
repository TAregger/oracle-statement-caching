package io.aregger.oracle_statement_caching;

import io.aregger.oracle_statement_caching.helper.CacheableStatement;
import io.aregger.oracle_statement_caching.helper.ImplicitCacheableStatement;
import io.aregger.oracle_statement_caching.helper.PreparedStatementHelper;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import org.junit.jupiter.api.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class StatementCacheTest {

  static final String CONNECTION_STRING = "jdbc:oracle:thin:thomas/thomas@oracle-database-183:1521/orcl1803";

  static final String QUERY = "select * from dual where dummy = ? connect by rownum <= 10";

  static final int LOBSIZE = 100_000_000;

  static final int PREFETCH_SIZE_DEFAULT = 10;
  static final int PREFETCH_SIZE_MODIFIED = 1000;

  static final int LOB_PREFETCH_SIZE_DEFAULT = 4000;
  static final int LOB_PREFETCH_SIZE_MODIFIED = 8000;

  static final int MAX_FIELD_SIZE_DEFAULT = 0;
  static final int MAX_FIELD_SIZE_MODIFIED = 100;


  static final int QUERY_TIMEOUT_DEFAULT = 0;
  static final int QUERY_TIMEOUT_MODIFIED = 100;

  @Test
  public void testExpectedResultSetType() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertEquals(getExpectedResultSetType(), statement.getResultSetType());
  }

  @Test
  public void testExpectedResultSetConcurrency() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertEquals(getExpectedResultSetConcurrency(), statement.getResultSetConcurrency());
  }

  @Test
  public void testExpectedPrefetchSize() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertEquals(getExpectedPrefetchSize(), statement.getRowPrefetch());
  }

  @Test
  public void testExpectedLobPrefetchSize() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertEquals(getExpectedLobPrefetchSize(), statement.getLobPrefetchSize());
  }

  @Test
  public void testExpectedMaxFieldSize() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertEquals(getExpectedMaxFieldSize(), statement.getMaxFieldSize());
  }

  @Test
  public void testExpectedQueryTimeout() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertEquals(getExpectedQueryTimeout(), statement.getQueryTimeout());
  }

  @Test
  public void testResultSet() throws SQLException {
    OraclePreparedStatement statement = executeAndGetCachedStatement(getConnection());
    assertThrows(SQLException.class, () -> statement.getResultSet().getString(1), "ResultSet was not re-initialized");
  }

  /**
   * Does not assert anything. Trace file on db server has to analyzed to check whether the metadata was fetched from the server or was cached on the client
   */
  @Test
  public void testResultSetMetadata() throws SQLException {
    OracleConnection connection = getConnection();
    CallableStatement call = connection.prepareCall("begin dbms_monitor.session_trace_enable(binds=>true,waits=>true); end;");
    call.execute();

    PreparedStatement statement = executeAndGetCachedStatement(connection);

    // check trace file whether this method call results in a database call
    statement.getMetaData().getColumnLabel(1);
  }

  /**
   * Does not assert anything. Trace file on db server has to analyzed to check whether the metadata was fetched from the server or was cached on the client
   */
  public void testParameterMetadata(CacheableStatement cacheableStatement) throws SQLException {
    OracleConnection connection = getConnection();
    CallableStatement call = connection.prepareCall("begin dbms_monitor.session_trace_enable(binds=>true,waits=>true); end;");
    call.execute();
    OraclePreparedStatement statement = PreparedStatementHelper.prepare(connection, cacheableStatement.getSql());
    statement.setString(1, "X");
    statement.executeQuery();
    statement.getParameterMetaData().getParameterTypeName(1);
    cacheableStatement.closePreparedStatement(statement);

    statement = cacheableStatement.getCachedStatement(connection);

    // check trace file whether this method call results in a database call
    statement.getParameterMetaData().getParameterTypeName(1);
  }

  OraclePreparedStatement executeAndGetCachedStatement(OracleConnection connection) throws SQLException {
    CacheableStatement defaultStatement = getDefaultStatement();
    OraclePreparedStatement statement = PreparedStatementHelper.prepare(connection, defaultStatement.getSql());

    statement.setRowPrefetch(PREFETCH_SIZE_MODIFIED);
    statement.setLobPrefetchSize(LOB_PREFETCH_SIZE_MODIFIED);
    statement.setMaxFieldSize(MAX_FIELD_SIZE_MODIFIED);
    statement.setQueryTimeout(QUERY_TIMEOUT_MODIFIED);

    statement.setString(1, "X");
    ResultSet rs = statement.executeQuery();
    rs.next();
    statement.getResultSet().next();
    defaultStatement.closePreparedStatement(statement);

    return defaultStatement.getCachedStatement(connection);
  }

  void dropTableIfExists(Connection connection) throws SQLException {
    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery("select count(*) CNT from user_tables where table_name  = 'T1'");
    resultSet.next();
    int tableCount = resultSet.getInt("CNT");
    if (tableCount > 0) {
      statement.execute("drop table t1");
    }
  }

  void createTable(Connection connection) throws SQLException {
    Statement statement = connection.createStatement();
    statement.execute("create table t1 (c1 NUMBER GENERATED by default on null as IDENTITY, c2 varchar2(10), c3 clob, c4 clob)");
  }

  void testUpdateCount(CacheableStatement cacheableStatement, boolean callGetUpdateCountAfterFirstExecution, int expectedUpdateCount) throws SQLException {
    // create table
    OracleConnection connection = getConnection();
    dropTableIfExists(connection);
    createTable(connection);

    // execute update
    OraclePreparedStatement statement = PreparedStatementHelper.prepare(connection, cacheableStatement.getSql());
    assertEquals(0, statement.getUpdateCount());
    statement.setString(1, "X");

    statement.executeUpdate();
    if (callGetUpdateCountAfterFirstExecution) {
      statement.getUpdateCount();
    }
    cacheableStatement.closePreparedStatement(statement);

    // get statement from cache and check update count
    statement = cacheableStatement.getCachedStatement(connection);
    assertEquals(expectedUpdateCount, statement.getUpdateCount());

    dropTableIfExists(connection);
  }

  void testGeneratedKeys(CacheableStatement cacheableStatement, Consumer<ResultSet> assertions) throws SQLException {
    // create table
    OracleConnection connection = getConnection();
    dropTableIfExists(connection);
    createTable(connection);

    // execute statement
    OraclePreparedStatement statement = (OraclePreparedStatement) connection.prepareStatement(cacheableStatement.getSql(), new String[]{"C1"});

    statement.setString(1, "X");
    statement.executeUpdate();

    // check that generated keys returns a value
    ResultSet generatedKeys = statement.getGeneratedKeys();
    generatedKeys.next();
    assertEquals(1, generatedKeys.getLong(1));
    cacheableStatement.closePreparedStatement(statement);

    // get statement from cache and call OraclePreparedStatement#generatedKeys
    statement = cacheableStatement.getCachedStatement(connection);
    generatedKeys = statement.getGeneratedKeys();

    // check generated keys
    assertions.accept(generatedKeys);

    dropTableIfExists(connection);
  }

  void testBatch(CacheableStatement cacheableStatement, int expectedInsertCount) throws SQLException {
    // create table
    OracleConnection connection = getConnection();
    dropTableIfExists(connection);
    createTable(connection);

    // execute batched statement
    OraclePreparedStatement batchedStatement = PreparedStatementHelper.prepare(connection, cacheableStatement.getSql());
    batchedStatement.setString(1, "X");
    batchedStatement.addBatch();

    batchedStatement.setString(1, "X");
    batchedStatement.addBatch();

    batchedStatement.executeBatch();
    cacheableStatement.closePreparedStatement(batchedStatement);

    // get statement from cache and re-execute
    batchedStatement = cacheableStatement.getCachedStatement(connection);
    batchedStatement.executeBatch();

    // check
    Statement countRows = connection.createStatement();
    ResultSet rs = countRows.executeQuery("select count(*) from t1");
    rs.next();
    int rowCount = rs.getInt(1);
    assertEquals(expectedInsertCount, rowCount);

    dropTableIfExists(connection);
  }

  void testExecuteCachedStatementWithoutBindStreamBinding(CacheableStatement cacheableStatement, Consumer<PreparedStatement> assertions) throws SQLException {
    OracleConnection connection = getConnection();
    dropTableIfExists(connection);
    createTable(connection);
    OraclePreparedStatement statement = PreparedStatementHelper.prepare(connection, cacheableStatement.getSql());

    // bind parameters larger set with setString which are larger than 32766 characters are stream binded
    char[] chars = new char[LOBSIZE];
    Arrays.fill(chars, 'b');
    String string = String.valueOf(chars);

    statement.setString(1, string);
    statement.executeUpdate();
    cacheableStatement.closePreparedStatement(statement);

    statement = cacheableStatement.getCachedStatement(connection);

    assertions.accept(statement);

    dropTableIfExists(connection);
  }

  abstract OracleConnection getConnection() throws SQLException;

  abstract CacheableStatement getDefaultStatement();

  /*
   * Test methods
   */

  /**
   * Tests whether batches are cached.
   */
  abstract void testBatch() throws SQLException;

  /**
   * Tests whether bind data is cached in the case of direct binding.
   */
  abstract void testExecuteCachedStatementWithoutBindDirectBinding() throws SQLException;

  /**
   * Tests whether bind data is cached in the case of stream binding.
   */
  abstract void testExecuteCachedStatementWithoutBindStreamBinding() throws SQLException;

  /**
   * Tests whether bind data is cached in the case of LOB binding.
   */
  abstract void testExecuteCachedStatementWithoutBindLOBBinding() throws SQLException;

  /**
   * Tests whether generated keys are cached.
   */
  abstract void testGeneratedKeys() throws SQLException;

  /**
   * Tests whether the update count is reset after getting the statement from the cache.
   */
  abstract void testUpdateCount() throws SQLException;

  /**
   * Tests whether the update count is reset after getting the statement from the cache.
   */
  abstract void testUpdateCountWithPrecedingGetUpdateCountCall() throws SQLException;

  /**
   * Tests whether parameter metadata is cached.
   */
  abstract void testParameterMetadata() throws SQLException;

  /*
   * Assert methods
   */
  abstract int getExpectedResultSetType();

  abstract int getExpectedResultSetConcurrency();

  abstract int getExpectedPrefetchSize();

  abstract int getExpectedLobPrefetchSize();

  abstract int getExpectedMaxFieldSize();

  abstract int getExpectedQueryTimeout();

}

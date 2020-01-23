package io.tokern.lineage.catalog.util.strategies;

/**
 * Uses a {@link BasicSqlNameStrategy} and fallbacks to
 * {@link DefaultNameStrategy#CONSTANT_SQL_RAW}.
 */
public class SmartNameStrategy extends DelegatingStatementNameStrategy {

  /**
   * A strategy to choose a smart name for a SQL query using the caller and SQL.
   */
  public SmartNameStrategy() {
    super(DefaultNameStrategy.CHECK_EMPTY,
                DefaultNameStrategy.SQL_OBJECT,
                DefaultNameStrategy.CONSTANT_SQL_RAW);
  }
}

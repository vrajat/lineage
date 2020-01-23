package io.tokern.lineage.catalog.util.strategies;

import com.codahale.metrics.MetricRegistry;

import org.jdbi.v3.core.extension.ExtensionMethod;
import org.jdbi.v3.core.statement.StatementContext;

/**
 * Default strategies which build a basis of more complex strategies.
 */
public enum DefaultNameStrategy implements StatementNameStrategy {

  /**
   * If no SQL in the context, returns `query.empty`, otherwise falls through
   */
  CHECK_EMPTY {
    @Override
    public String getStatementName(StatementContext statementContext) {
      final String rawSql = statementContext.getRawSql();
      if (rawSql == null || rawSql.length() == 0) {
        return "inviscid.query.empty";
      }
      return null;
    }
  },

  /**
   * If there is an SQL object attached to the context, returns the name package,
   * the class and the method on which SQL is declared. If not SQL object is attached,
   * falls through
   */
  SQL_OBJECT {
    @Override
    public String getStatementName(StatementContext statementContext) {
      ExtensionMethod extensionMethod = statementContext.getExtensionMethod();
      if (extensionMethod != null) {
        return MetricRegistry.name(extensionMethod.getType(),
            extensionMethod.getMethod().getName());
      }
      return null;
    }
  },

  /**
   * Returns a raw SQL in the context (even if it's not exist).
   */
  NAIVE_NAME {
    @Override
    public String getStatementName(StatementContext statementContext) {
      return statementContext.getRawSql();
    }
  },

  /**
   * Returns the `query.raw` constant
   */
  CONSTANT_SQL_RAW {
    @Override
    public String getStatementName(StatementContext statementContext) {
      return "inviscid.query.raw";
    }
  }
}

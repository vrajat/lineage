package io.tokern.lineage.sqlplanner.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;

public class IndexImplementor extends RelToSqlConverter {
  IndexImplementor(SqlDialect dialect) {
    super(dialect);
  }

  /**
   * Convert an index scan to a WHERE and FROM clause.
   * @param scan The IndexTableScan object
   * @return A {@link org.apache.calcite.rel.rel2sql.SqlImplementor.Result} with SqlNodes.
   */
  public Result visit(LogicalIndexTableScan scan) {
    final SqlIdentifier identifier =
        new SqlIdentifier(scan.getTable().getQualifiedName(), SqlParserPos.ZERO);
    Result result = result(identifier, Arrays.asList(Clause.FROM), scan, null);

    final Builder builder = result.builder(scan, Clause.WHERE);
    builder.setWhere(builder.context.toSql(null, scan.condition));
    return builder.result();
  }

  Result implement(RelNode node) {
    return dispatch(node);
  }
}

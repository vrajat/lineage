package io.tokern.lineage.sqlplanner.planner;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;

public class SqlMartLiteral extends SqlLiteral {
  protected SqlMartLiteral(SqlLiteral literal) {
    super(literal.getValue(), literal.getTypeName(), literal.getParserPosition());
  }

  public static SqlLiteral createLiteral(SqlLiteral literal) {
    return new SqlMartLiteral(literal);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.literal("?");
  }
}

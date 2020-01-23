package io.tokern.lineage.redshift;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class CustomSqlOperatorTable extends ReflectiveSqlOperatorTable {
  private static CustomSqlOperatorTable instance;

  public static final SqlBinaryOperator DOUBLE_COLON_CAST =
      new SqlBinaryOperator(
          "DOUBLE_COLON_CAST",
          SqlKind.CAST,
          24,
          true,
          ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN_BOOLEAN);
}

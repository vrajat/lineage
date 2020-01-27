package io.tokern.lineage.sqlplanner.visitors;

import io.tokern.lineage.redshift.SqlCopy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;

public class CopyVisitor extends DmlVisitor {
  private SqlLiteral s3Location = null;

  public CopyVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlCopy) {
      SqlCopy copy = (SqlCopy) sqlCall;
      this.targetTable = copy.getTable();
      this.s3Location = copy.getDataSource().s3Loc;
      this.passed = true;
    }
    return null;
  }

  public String getS3Location() {
    return s3Location.toValue();
  }
}

package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;

public class InsertVisitor extends DmlVisitor {

  public InsertVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlInsert) {
      SqlInsert insert = (SqlInsert) sqlCall;
      if (insert.getTargetTable() instanceof SqlIdentifier) {
        targetTable = (SqlIdentifier) insert.getTargetTable();
        TableVisitor tableVisitor = new TableVisitor();
        insert.getSource().accept(tableVisitor);
        sources = tableVisitor.getSources();
        this.passed = true;
      }
      return null;
    }

    return super.visit(sqlCall);
  }
}

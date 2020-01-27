package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.List;

public class CtasVisitor extends DmlVisitor {
  public CtasVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlCreateTable) {
      SqlCreateTable create = (SqlCreateTable) sqlCall;
      List<SqlNode> operands = create.getOperandList();
      targetTable = (SqlIdentifier) operands.get(0);
      SqlNode query = operands.get(2);
      if (query != null && query instanceof SqlSelect) {
        TableVisitor tableVisitor = new TableVisitor();
        query.accept(tableVisitor);
        sources = tableVisitor.getSources();
        this.passed = true;
      }
      return null;
    }
    return super.visit(sqlCall);
  }
}

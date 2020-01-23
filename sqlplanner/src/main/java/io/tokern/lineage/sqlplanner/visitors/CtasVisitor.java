package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CtasVisitor extends ClassifyingVisitor {
  private SqlIdentifier targetTable;
  private List<SqlIdentifier> sources = new ArrayList<>();

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

  public String getTargetTable() {
    return targetTable.toString();
  }

  public List<String> getSources() {
    return sources.stream().map(SqlIdentifier::toString).collect(Collectors.toList());
  }
}

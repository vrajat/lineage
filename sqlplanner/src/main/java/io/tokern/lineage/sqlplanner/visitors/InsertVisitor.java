package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InsertVisitor extends ClassifyingVisitor {
  private SqlIdentifier targetTable;
  private List<SqlIdentifier> sources = new ArrayList<>();

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

  public String getTargetTable() {
    return targetTable.toString();
  }

  public List<String> getSources() {
    return sources.stream().map(SqlIdentifier::toString).collect(Collectors.toList());
  }
}

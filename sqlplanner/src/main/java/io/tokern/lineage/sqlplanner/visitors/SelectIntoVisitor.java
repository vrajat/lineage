package io.tokern.lineage.sqlplanner.visitors;

import io.tokern.lineage.redshift.SqlSelectInto;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SelectIntoVisitor extends ClassifyingVisitor {
  private SqlIdentifier targetTable;
  private List<SqlIdentifier> sources = new ArrayList<>();

  public SelectIntoVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlSelectInto) {
      SqlSelectInto selectInto = (SqlSelectInto) sqlCall;
      SqlNode intoTableRef = selectInto.getIntoTableRef();
      if (intoTableRef != null && intoTableRef instanceof SqlIdentifier) {
        targetTable = (SqlIdentifier) intoTableRef;
        TableVisitor tableVisitor = new TableVisitor();
        selectInto.accept(tableVisitor);
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

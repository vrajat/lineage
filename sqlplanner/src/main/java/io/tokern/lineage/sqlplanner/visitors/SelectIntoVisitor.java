package io.tokern.lineage.sqlplanner.visitors;

import io.tokern.lineage.redshift.SqlSelectInto;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

public class SelectIntoVisitor extends DmlVisitor {
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
}

package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

public class TableVisitor extends SqlBasicVisitor<Void> {
  private List<SqlIdentifier> sources = new ArrayList<>();

  @Override
  public Void visit(SqlIdentifier identifier) {
    sources.add(identifier);
    return super.visit(identifier);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) sqlCall;
      join.getLeft().accept(this);
      join.getRight().accept(this);
    } else if (sqlCall instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) sqlCall;
      if (select.getFrom() != null) {
        select.getFrom().accept(this);
      }
    } else if (sqlCall instanceof SqlBasicCall) {
      SqlBasicCall basicCall = (SqlBasicCall) sqlCall;
      if (basicCall.getOperator() instanceof SqlAsOperator) {
        basicCall.operand(0).accept(this);
      } else {
        return super.visit(sqlCall);
      }
    } else {
      return super.visit(sqlCall);
    }

    return null;
  }

  List<SqlIdentifier> getSources() {
    return sources;
  }
}

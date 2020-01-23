package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;

public class TooManyJoinsVisitor extends ClassifyingVisitor {
  int numJoins = 0;
  public final int limit;

  public TooManyJoinsVisitor() {
    this(10);
  }

  public TooManyJoinsVisitor(int limit) {
    super(true);
    this.limit = limit;
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlJoin) {
      numJoins++;
    }
    return super.visit(sqlCall);
  }

  @Override
  public boolean isPassed() {
    return numJoins > limit;
  }
}

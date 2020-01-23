package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.util.SqlBasicVisitor;

/**
 * Created by rvenkatesh on 9/9/18.
 */
public abstract class ClassifyingVisitor extends SqlBasicVisitor<Void> {
  protected boolean passed;

  protected ClassifyingVisitor(boolean passed) {
    this.passed = passed;
  }

  public boolean isPassed() {
    return passed;
  }
}

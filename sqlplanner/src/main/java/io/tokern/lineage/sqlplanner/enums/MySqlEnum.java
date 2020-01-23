package io.tokern.lineage.sqlplanner.enums;

import io.tokern.lineage.sqlplanner.visitors.IndexVisitor;
import org.apache.calcite.rel.RelNode;

public enum MySqlEnum implements QueryType {
  BAD_NOINDEX {
    @Override
    public boolean isPassed(RelNode relNode, EnumContext context) {
      IndexVisitor visitor = new IndexVisitor();
      MySqlEnumContext enumContext = (MySqlEnumContext) context;
      visitor.go(relNode);
      enumContext.setIndices(visitor.getIndices());
      return visitor.hasLessIndexScans();
    }
  }
}

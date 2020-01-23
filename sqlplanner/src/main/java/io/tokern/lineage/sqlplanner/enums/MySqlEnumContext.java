package io.tokern.lineage.sqlplanner.enums;

import io.tokern.lineage.sqlplanner.visitors.IndexVisitor;

import java.util.Set;

public class MySqlEnumContext extends EnumContext {
  Set<IndexVisitor.Index> indices;

  public Set<IndexVisitor.Index> getIndices() {
    return indices;
  }

  public void setIndices(Set<IndexVisitor.Index> indices) {
    this.indices = indices;
  }
}

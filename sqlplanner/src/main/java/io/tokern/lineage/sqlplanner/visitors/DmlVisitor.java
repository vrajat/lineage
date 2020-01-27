package io.tokern.lineage.sqlplanner.visitors;

import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class DmlVisitor extends ClassifyingVisitor {
  protected SqlIdentifier targetTable;
  protected List<SqlIdentifier> sources = new ArrayList<>();

  protected DmlVisitor(boolean passed) {
    super(passed);
  }

  public String getTargetTable() {
    return targetTable.toString();
  }

  public List<String> getSources() {
    return sources.stream().map(SqlIdentifier::toString).collect(Collectors.toList());
  }
}

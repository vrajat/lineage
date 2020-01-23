package io.tokern.lineage.sqlplanner.redshift;

import io.tokern.lineage.sqlplanner.visitors.ClassifyingVisitor;

public class MaintenanceVisitor extends ClassifyingVisitor {
  MaintenanceVisitor() {
    super(false);
  }

  void visit(String query) {
    if (query.startsWith("padb_fetch_sample:")
        || query.startsWith("Vacuum") ) {
      this.passed = true;
    }
  }
}

package io.tokern.lineage.sqlplanner.redshift;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MaintenanceVisitorTest {

  @Test
  void successTest() {
    MaintenanceVisitor visitor = new MaintenanceVisitor();
    visitor.visit("padb_fetch_sample: select * from results");
    assertTrue(visitor.isPassed());
  }

  @Test
  void vacuumSuccessTest() {
    MaintenanceVisitor visitor = new MaintenanceVisitor();
    visitor.visit("Vacuum select * from results");
    assertTrue(visitor.isPassed());
  }

  @Test
  void failTest() {
    MaintenanceVisitor visitor = new MaintenanceVisitor();
    visitor.visit("select * from results");
    assertFalse(visitor.isPassed());
  }
}
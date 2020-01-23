package io.tokern.lineage.sqlplanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import io.tokern.lineage.sqlplanner.redshift.QueryClasses;
import io.tokern.lineage.sqlplanner.redshift.RedshiftClassifier;
import io.tokern.lineage.sqlplanner.visitors.CopyVisitor;
import io.tokern.lineage.sqlplanner.visitors.CtasVisitor;
import io.tokern.lineage.sqlplanner.visitors.InsertVisitor;
import io.tokern.lineage.sqlplanner.visitors.SelectIntoVisitor;
import io.tokern.lineage.sqlplanner.visitors.UnloadVisitor;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class RedshiftClassifierTest {
  @Test
  void selectQuery() throws SqlParseException {
    RedshiftClassifier redshiftClassifier = new RedshiftClassifier();

    QueryClasses classes = redshiftClassifier.classify("select x from "
        + "a1 join a2 on a1.i1 = a2.i "
        + "join a3 on a1.i2 = a3.i "
        + "join a4 on a1.i3 = a4.i "
        + "join a5 on a1.i4 = a5.i "
        + "join a6 on a1.i5 = a6.i "
        + "join a7 on a1.i6 = a7.i "
        + "join a8 on a1.i7 = a8.i "
        + "join a9 on a1.i8 = a9.i "
        + "join a10 on a1.i9 = a10.i "
        + "join a11 on a1.i10 = a11.i "
        + "join a12 on a1.i11 = a12.i "
        + "join a13 on a1.i12 = a13.i");
    assertFalse(classes.insertContext.isPassed());
  }

  @Test
  void insertTest() throws SqlParseException {
    RedshiftClassifier redshiftClassifier = new RedshiftClassifier();

    QueryClasses classes = redshiftClassifier.classify(
        "insert into c select * from a join b on a.id = b.id");
    InsertVisitor visitor = classes.insertContext;
    assertTrue(visitor.isPassed());
    assertEquals(visitor.getTargetTable(), "C");

    List<String> expectedSources = new ArrayList<>();
    expectedSources.add("A");
    expectedSources.add("B");

    assertIterableEquals(expectedSources, visitor.getSources());

    assertFalse(classes.ctasContext.isPassed());
    assertFalse(classes.maintenanceContext.isPassed());
    assertFalse(classes.ctasContext.isPassed());
    assertFalse(classes.unloadContext.isPassed());
    assertFalse(classes.selectIntoContext.isPassed());
  }

  @Test
  void ctasTest() throws SqlParseException {
    RedshiftClassifier redshiftClassifier = new RedshiftClassifier();

    QueryClasses classes = redshiftClassifier.classify(
        "create table c as select * from a join b on a.id = b.id");
    CtasVisitor visitor = classes.ctasContext;
    assertTrue(visitor.isPassed());
    assertEquals(visitor.getTargetTable(), "C");

    List<String> expectedSources = new ArrayList<>();
    expectedSources.add("A");
    expectedSources.add("B");

    assertIterableEquals(expectedSources, visitor.getSources());

    assertFalse(classes.insertContext.isPassed());
    assertFalse(classes.maintenanceContext.isPassed());
    assertFalse(classes.copyContext.isPassed());
    assertFalse(classes.unloadContext.isPassed());
    assertFalse(classes.selectIntoContext.isPassed());
  }

  @Test
  void unloadTest() throws SqlParseException {
    RedshiftClassifier classifier = new RedshiftClassifier();

    QueryClasses classes = classifier.classify(
        " unload('select a, b from c join d on c.id = d.id') to 's3://bucket/dir' iam_role ''"
        + "delimiter '^' ALLOWOVERWRITE ESCAPE PARALLEL OFF NULL AS ''");

    UnloadVisitor visitor = classes.unloadContext;
    assertTrue(visitor.isPassed());
    assertEquals("s3://bucket/dir", visitor.getS3Location());

    List<String> expectedSources = new ArrayList<>();
    expectedSources.add("C");
    expectedSources.add("D");

    assertIterableEquals(expectedSources, visitor.getSources());

    assertFalse(classes.insertContext.isPassed());
    assertFalse(classes.maintenanceContext.isPassed());
    assertFalse(classes.ctasContext.isPassed());
    assertFalse(classes.copyContext.isPassed());
    assertFalse(classes.selectIntoContext.isPassed());
  }

  @Test
  void copyTest() throws SqlParseException {
    RedshiftClassifier classifier = new RedshiftClassifier();

    QueryClasses classes = classifier.classify(
        "copy a.b(c, d, e) from 's3://bucket/dir' CREDENTIALS '' delimiter ',' REMOVEQUOTES "
        + "ACCEPTINVCHARS IGNOREHEADER 1");

    CopyVisitor visitor = classes.copyContext;
    assertTrue(visitor.isPassed());
    assertEquals("s3://bucket/dir", visitor.getS3Location());

    assertEquals("A.B", visitor.getTargetTable());

    assertFalse(classes.insertContext.isPassed());
    assertFalse(classes.maintenanceContext.isPassed());
    assertFalse(classes.ctasContext.isPassed());
    assertFalse(classes.unloadContext.isPassed());
    assertFalse(classes.selectIntoContext.isPassed());
  }

  @Test
  void selectIntoTest() throws SqlParseException {
    RedshiftClassifier redshiftClassifier = new RedshiftClassifier();

    QueryClasses classes = redshiftClassifier.classify(
        "select * into c from a join b on a.id = b.id");
    SelectIntoVisitor visitor = classes.selectIntoContext;
    assertTrue(visitor.isPassed());
    assertEquals(visitor.getTargetTable(), "C");

    List<String> expectedSources = new ArrayList<>();
    expectedSources.add("A");
    expectedSources.add("B");

    assertIterableEquals(expectedSources, visitor.getSources());

    assertFalse(classes.insertContext.isPassed());
    assertFalse(classes.maintenanceContext.isPassed());
    assertFalse(classes.copyContext.isPassed());
    assertFalse(classes.ctasContext.isPassed());
    assertFalse(classes.unloadContext.isPassed());
  }
}
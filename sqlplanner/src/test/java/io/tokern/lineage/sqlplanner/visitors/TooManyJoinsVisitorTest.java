package io.tokern.lineage.sqlplanner.visitors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tokern.lineage.sqlplanner.planner.Parser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class TooManyJoinsVisitorTest {
  @Test
  public void elevenJoinTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from "
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
        + "join a12 on a1.i11 = a12.i");
    TooManyJoinsVisitor tooManyJoinsVisitor = new TooManyJoinsVisitor();
    parseTree.accept(tooManyJoinsVisitor);
    assertTrue(tooManyJoinsVisitor.isPassed());
  }

  @Test
  public void tenJoinTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from "
        + "a1 join a2 on a1.i1 = a2.i "
        + "join a3 on a1.i2 = a3.i "
        + "join a4 on a1.i3 = a4.i "
        + "join a5 on a1.i4 = a5.i "
        + "join a6 on a1.i5 = a6.i "
        + "join a7 on a1.i6 = a7.i "
        + "join a8 on a1.i7 = a8.i "
        + "join a9 on a1.i8 = a9.i "
        + "join a10 on a1.i9 = a10.i "
        + "join a11 on a1.i10 = a11.i");
    TooManyJoinsVisitor tooManyJoinsVisitor = new TooManyJoinsVisitor();
    parseTree.accept(tooManyJoinsVisitor);
    assertFalse(tooManyJoinsVisitor.isPassed());
  }

  @Test
  public void twoLimitFailTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from "
        + "a1 join a2 on a1.i1 = a2.i "
        + "join a3 on a1.i2 = a3.i "
        + "join a4 on a1.i3 = a4.i ");

    TooManyJoinsVisitor tooManyJoinsVisitor = new TooManyJoinsVisitor(2);
    parseTree.accept(tooManyJoinsVisitor);
    assertTrue(tooManyJoinsVisitor.isPassed());
  }

  @Test
  public void twoLimitPassTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode parseTree = parser.parse("select x from "
        + "a1 join a2 on a1.i1 = a2.i "
        + "join a3 on a1.i2 = a3.i ");

    TooManyJoinsVisitor tooManyJoinsVisitor = new TooManyJoinsVisitor(2);
    parseTree.accept(tooManyJoinsVisitor);
    assertFalse(tooManyJoinsVisitor.isPassed());
  }
}

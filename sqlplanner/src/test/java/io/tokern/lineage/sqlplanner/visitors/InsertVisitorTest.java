package io.tokern.lineage.sqlplanner.visitors;

import io.tokern.lineage.sqlplanner.planner.Parser;
import io.tokern.lineage.sqlplanner.utils.SqlProvider;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InsertVisitorTest {
  static Logger logger = LoggerFactory.getLogger(InsertVisitorTest.class);

  static Parser parser;
  @BeforeAll
  static void setParser() {
    parser = new Parser();
  }

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SqlProvider.class)
  @Tags({@Tag("/insertSuccess.yaml")})
  void sanityTest(String name, String targetTable,
                  List<String> sourceTables, String query) throws SqlParseException {
    SqlNode node = parser.parse(query);
    InsertVisitor visitor = new InsertVisitor();
    node.accept(visitor);

    logger.debug("Expected:" + sourceTables.size());
    logger.debug("Actual: " + visitor.getSources().size());

    assertTrue(visitor.passed);
    assertEquals(targetTable, visitor.getTargetTable());

    Iterator<String> expected = sourceTables.iterator();
    Iterator<String> actual = visitor.getSources().iterator();
    while (expected.hasNext() && actual.hasNext()) {
      assertEquals(expected.next(), actual.next());
    }
    assertFalse(expected.hasNext());
    assertFalse(actual.hasNext());
  }
}
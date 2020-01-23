package io.tokern.lineage.sqlplanner;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.tokern.lineage.sqlplanner.enums.AnalyticsEnum;
import io.tokern.lineage.sqlplanner.enums.EnumContext;
import io.tokern.lineage.sqlplanner.enums.QueryType;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParseException;

import org.junit.jupiter.api.Test;

class AnalyticsClassifierTest {
  @Test
  public void lookupOnlyTest() throws SqlParseException {
    AnalyticsClassifier analyticsClassifier = new AnalyticsClassifier();
    List<QueryType> queryTypeList = analyticsClassifier.classify("select a from b where c = 10",
        EnumContext.EMPTY_CONTEXT);

    List<QueryType> expected = new ArrayList<>();
    expected.add(AnalyticsEnum.LOOKUP);
    assertIterableEquals(expected, queryTypeList);
  }

}
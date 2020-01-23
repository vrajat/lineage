package io.tokern.lineage.sqlplanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.ArrayList;
import java.util.List;

import io.tokern.lineage.sqlplanner.enums.MySqlEnum;
import io.tokern.lineage.sqlplanner.enums.MySqlEnumContext;
import io.tokern.lineage.sqlplanner.enums.QueryType;
import io.tokern.lineage.sqlplanner.planner.Tpcds;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MySqlClassifierTest {
  static SchemaPlus tpcdsSchemaPlus;

  @BeforeAll
  static void setPlanner() throws QanException {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Tpcds tpcds = new Tpcds("tpcds");
    tpcdsSchemaPlus = rootSchema.add("tpcds", tpcds);
    tpcds.setSchemaPlus(tpcdsSchemaPlus);
    tpcds.addTables();
  }

  @Test
  void indexScanTest() throws SqlParseException, QanException {
    MySqlClassifier classifier = new MySqlClassifier(tpcdsSchemaPlus);
    MySqlEnumContext context = new MySqlEnumContext();
    List<QueryType>  queryTypes = classifier.classify(
        "select i_color from item where i_item_id = 'abc'", context);

    assertEquals(0, queryTypes.size());
    assertEquals(0, context.getIndices().size());
  }

  @Test
  void simpleScanTest() throws SqlParseException, QanException {
    MySqlClassifier classifier = new MySqlClassifier(tpcdsSchemaPlus);
    MySqlEnumContext context = new MySqlEnumContext();
    List<QueryType> queryTypes = classifier.classify(
        "select d_date_id from date_dim where d_year = 2018", context);

    List<QueryType> expected = new ArrayList<>();
    expected.add(MySqlEnum.BAD_NOINDEX);
    assertIterableEquals(expected, queryTypes);
    assertEquals(1, context.getIndices().size());
  }
}

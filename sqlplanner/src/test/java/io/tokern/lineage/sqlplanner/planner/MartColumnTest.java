package io.tokern.lineage.sqlplanner.planner;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.tokern.lineage.sqlplanner.QanException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MartColumnTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "int",
      "int(10)",
      "int(10) unsigned",
      "datetime",
      "varchar(20)",
      "char(20)",
      "date",
      "time",
      "time(6)",
      "decimal(8,2)",
      "datetime(6)",
      "tinyint(1)",
      "tinyint(1) unsigned",
      "tinyint(6)",
      "smallint(1) unsigned",
      "bigint(6)",
      "longtext",
      "text",
      "double"
  })
  void typeTest(String typeStr) throws QanException {
    assertNotNull(new MartColumn("a_name", typeStr));
  }
}

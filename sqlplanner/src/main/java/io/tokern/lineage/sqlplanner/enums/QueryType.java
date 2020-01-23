package io.tokern.lineage.sqlplanner.enums;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

/**
 * Created by rvenkatesh on 9/9/18.
 */
public interface QueryType {
  default boolean isPassed(SqlNode sqlNode) {
    return false;
  }

  default boolean isPassed(RelNode relNode, EnumContext context) {
    return false;
  }
}

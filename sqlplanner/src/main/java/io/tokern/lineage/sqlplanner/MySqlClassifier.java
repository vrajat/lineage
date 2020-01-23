package io.tokern.lineage.sqlplanner;

import io.tokern.lineage.sqlplanner.enums.EnumContext;
import io.tokern.lineage.sqlplanner.enums.MySqlEnum;
import io.tokern.lineage.sqlplanner.enums.QueryType;
import io.tokern.lineage.sqlplanner.planner.Planner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MySqlClassifier extends Classifier {
  private static final Logger logger = LoggerFactory.getLogger(MySqlClassifier.class);
  public final Planner planner;

  public MySqlClassifier(SchemaPlus schemaPlus) {
    planner = new Planner(schemaPlus);
  }

  @Override
  protected SqlParserImplFactory getFactory() {
    return SqlBabelParserImpl.FACTORY;
  }

  @Override
  public List<QueryType> classify(String sql, EnumContext context)
      throws SqlParseException, QanException {
    List<QueryType> queryTypes = new ArrayList<>();
    try {
      RelNode relNode = planner.optimize(sql);

      logger.debug(RelOptUtil.dumpPlan("\n--Logical Plan", relNode, SqlExplainFormat.TEXT,
          SqlExplainLevel.DIGEST_ATTRIBUTES));

      for (MySqlEnum sqlEnum : MySqlEnum.values()) {
        if (sqlEnum.isPassed(relNode, context)) {
          queryTypes.add(sqlEnum);
        }
      }

      return queryTypes;
    } catch (ValidationException | RelConversionException exc) {
      throw new QanException(exc);
    }
  }
}

package io.tokern.lineage.sqlplanner;

import io.tokern.lineage.sqlplanner.enums.AnalyticsEnum;
import io.tokern.lineage.sqlplanner.enums.EnumContext;
import io.tokern.lineage.sqlplanner.enums.QueryType;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;

/**
 * Created by rvenkatesh on 9/9/18.
 */
public class AnalyticsClassifier extends Classifier {
  public AnalyticsClassifier() {
    super();
  }

  @Override
  public List<QueryType> classify(String sql, EnumContext context) throws SqlParseException {
    return classifyImpl(parser.parse(sql));
  }

  @Override
  protected SqlParserImplFactory getFactory() {
    return SqlBabelParserImpl.FACTORY;
  }

  List<QueryType> classifyImpl(SqlNode parseTree) {
    List<QueryType> typeList = new ArrayList<>();
    for (AnalyticsEnum analyticsEnum: AnalyticsEnum.values()) {
      if (analyticsEnum.isPassed(parseTree)) {
        typeList.add(analyticsEnum);
      }
    }

    return typeList;
  }
}

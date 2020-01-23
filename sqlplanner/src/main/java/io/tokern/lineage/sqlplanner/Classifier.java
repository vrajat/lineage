package io.tokern.lineage.sqlplanner;

import io.tokern.lineage.sqlplanner.enums.EnumContext;
import io.tokern.lineage.sqlplanner.enums.QueryType;

import java.util.List;

import io.tokern.lineage.sqlplanner.planner.Parser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;

public abstract class Classifier {
  public final Parser parser;

  Classifier() {
    this.parser = new Parser(this.getFactory());
  }

  public abstract List<QueryType> classify(String sql, EnumContext context)
      throws SqlParseException, QanException;


  protected abstract SqlParserImplFactory getFactory();
}

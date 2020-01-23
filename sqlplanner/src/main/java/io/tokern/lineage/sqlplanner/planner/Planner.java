package io.tokern.lineage.sqlplanner.planner;

import io.tokern.lineage.sqlplanner.visitors.LiteralShuffle;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Planner {
  final Parser parser;
  final org.apache.calcite.tools.Planner planner;

  static final List<RelOptRule> RULE_SET = Arrays.asList(
      ReduceExpressionsRule.FilterReduceExpressionsRule.FILTER_INSTANCE,
      FilterProjectTransposeRule.INSTANCE,
      FilterJoinRule.FILTER_ON_JOIN,
      FilterJoinRule.JOIN,
      FilterAggregateTransposeRule.INSTANCE,
      JoinPushExpressionsRule.INSTANCE,
      JoinPushTransitivePredicatesRule.INSTANCE,
      IndexTableScanRule.INSTANCE
  );

  /**
   * Create a Apache Calcite based planner.
   * @param rootSchema Root Schema for the catalog
   */
  public Planner(SchemaPlus rootSchema) {
    this.parser = new Parser();
    List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelDistributionTraitDef.INSTANCE);
    SqlParser.Config parserConfig =
        SqlParser.configBuilder(SqlParser.Config.DEFAULT)
            .setCaseSensitive(false)
            .setConformance(SqlConformanceEnum.MYSQL_5)
            .setQuoting(Quoting.BACK_TICK)
            .build();

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(rootSchema)
        .traitDefs(traitDefs)
        // define the rules you want to apply
        .ruleSets(
            RuleSets.ofList(AbstractConverter.ExpandConversionRule.INSTANCE))
        .programs(Programs.hep(RULE_SET, true, DefaultRelMetadataProvider.INSTANCE))
        .build();
    this.planner = Frameworks.getPlanner(config);
  }

  private String trim(String sql) {
    sql = sql.trim();
    List<Character> chars = Arrays.asList(';');
    boolean found = true;
    while (found) {
      found = false;
      for (Character character : chars) {
        if (sql.charAt(sql.length() - 1) == character) {
          sql = sql.substring(0, sql.length() - 1);
          found = true;
        }
      }
    }
    return sql;
  }

  private String handleNewLine(String sql) {
    return sql.replaceAll("\\\\n", "\n").replaceAll("\\r", "\r");
  }

  RelNode plan(String sql) throws SqlParseException, ValidationException, RelConversionException {
    planner.close();
    planner.reset();
    SqlNode node = planner.parse(handleNewLine(trim(sql)));
    node = planner.validate(node);
    return planner.rel(node).project();
  }

  public RelNode optimize(String sql) throws SqlParseException, ValidationException,
      RelConversionException {
    return optimize(plan(sql));
  }

  RelNode optimize(RelNode root) throws RelConversionException {
    return this.planner.transform(0, planner.getEmptyTraitSet(), root);
  }

  /**
   * Create a query digest by replacing all constants.
   * @param sql SQL Query
   * @param dialect SQL dialect to use. For e.g. MySQL
   * @return A string containing the query digest
   * @throws SqlParseException If there is SQLParseException
   * @throws ValidationException If the query cannot be validated.
   * @throws RelConversionException If the parsed query cannot be setup for optimization
   */
  public String digest(String sql, SqlDialect dialect) throws SqlParseException,
      ValidationException, RelConversionException {
    RelNode relNode = this.optimize(sql);
    IndexImplementor converter = new IndexImplementor(dialect);
    final SqlNode sqlNode = converter.implement(relNode).asStatement();
    LiteralShuffle shuffle = new LiteralShuffle();
    final SqlNode replaced = sqlNode.accept(shuffle);
    return replaced.toSqlString(dialect).getSql();
  }
}

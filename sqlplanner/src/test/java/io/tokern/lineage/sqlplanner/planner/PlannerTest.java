package io.tokern.lineage.sqlplanner.planner;

import static org.junit.jupiter.api.Assertions.*;

import io.tokern.lineage.sqlplanner.QanException;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PlannerTest {
  static Planner planner;

  @BeforeAll
  static void setPlanner() throws QanException {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Tpcds tpcds = new Tpcds("tpcds");
    SchemaPlus tpcdsSchemaPlus = rootSchema.add("tpcds", tpcds);
    tpcds.setSchemaPlus(tpcdsSchemaPlus);
    tpcds.addTables();

    planner = new Planner(tpcdsSchemaPlus);
  }

  @Test
  void planSelectScan() throws SqlParseException,
      ValidationException, RelConversionException {
    RelNode relNode = planner.plan("select d_date_id from date_dim");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
         + "LogicalProject(D_DATE_ID=[$1])\n"
         + "  LogicalTableScan(table=[[tpcds, DATE_DIM]])\n", explainPlan);

  }

  @Test
  void planSelectFilterScan() throws SqlParseException,
      ValidationException, RelConversionException {
   RelNode relNode = planner.plan("select d_date_id from date_dim where d_year=2018");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
         + "LogicalProject(D_DATE_ID=[$1])\n"
         + "  LogicalFilter(condition=[=(CAST($6):INTEGER, 2018)])\n"
         + "    LogicalTableScan(table=[[tpcds, DATE_DIM]])\n", explainPlan);
  }

  @Test
  void optimizeSelectFilterScan() throws SqlParseException,
      ValidationException, RelConversionException {
   RelNode relNode = planner.optimize("select d_date_id from date_dim where d_year=2018");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
         + "LogicalProject(D_DATE_ID=[$1])\n"
         + "  LogicalFilter(condition=[=(CAST($6):INTEGER, 2018)])\n"
         + "    LogicalTableScan(table=[[tpcds, DATE_DIM]])\n", explainPlan);
  }

  @Test
  void planSelectFilterIndex() throws SqlParseException,
      ValidationException, RelConversionException {
    RelNode relNode = planner.plan(
        "select i_color from item where i_item_id='abc'");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
            + "LogicalProject(I_COLOR=[$17])\n"
            + "  LogicalFilter(condition=[=(CAST($1):VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE "
            + "\"ISO-8859-1$en_US$primary\", 'abc')])\n"
            + "    LogicalTableScan(table=[[tpcds, ITEM]])\n", explainPlan);
  }

  @Test
  void optimizeSelectFilterIndex() throws SqlParseException,
      ValidationException, RelConversionException {
    RelNode relNode = planner.optimize(
        "select i_color from item where i_item_id='abc'");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
            + "LogicalProject(I_COLOR=[$17])\n"
            + "  LogicalIndexTableScan(table=[[tpcds, ITEM]], conditions=[=(CAST($1):"
            + "VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE "
            + "\"ISO-8859-1$en_US$primary\", 'abc')])\n", explainPlan);
  }

  @Test
  void optimizeJoinQuery() throws SqlParseException, ValidationException, RelConversionException {
    RelNode relNode = planner.optimize("SELECT ss_quantity\n"
        + "FROM customer LEFT OUTER JOIN store_sales "
        + "ON c_customer_sk = ss_customer_sk\n"
        + "WHERE (ss_list_price is null OR ss_list_price > 100.0) AND "
        + "(c_birth_year is null OR c_birth_year = 1998)"
    );
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals("--Logical Plan\n"
        + "LogicalProject(SS_QUANTITY=[$28])\n"
        + "  LogicalFilter(condition=[OR(IS NULL($30), >($30, 100.0))])\n"
        + "    LogicalJoin(condition=[=($0, $21)], joinType=[left])\n"
        + "      LogicalFilter(condition=[OR(IS NULL($13), =(CAST($13):INTEGER, 1998))])\n"
        + "        LogicalTableScan(table=[[tpcds, CUSTOMER]])\n"
        + "      LogicalTableScan(table=[[tpcds, STORE_SALES]])\n", explainPlan);
  }

  @Test
  void digestTest() throws SqlParseException, ValidationException, RelConversionException {
    String digest = planner.digest("select i_color from item where i_color = 'abc'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `tpcds`.`ITEM`\n"
        + "WHERE `I_COLOR` = ?", digest);
  }

  @Test
  void digestWithIndexTest() throws SqlParseException, ValidationException, RelConversionException {
    String digest = planner.digest("select i_color from item where i_item_id = 'abc'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `tpcds`.`ITEM`\n"
        + "WHERE `I_ITEM_ID` = ?", digest);
  }

  @Test
  void digestWithMultipleFilters() throws SqlParseException, ValidationException,
      RelConversionException {
    String digest = planner.digest("select i_color from item where i_item_id = 'abc' "
            + "and i_color = 'blue'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `tpcds`.`ITEM`\n"
        + "WHERE `I_ITEM_ID` = ? AND `I_COLOR` = ?", digest);
  }

  @Test
  void digestWithIndexORTest() throws SqlParseException, ValidationException,
      RelConversionException {
    String digest = planner.digest("select i_color from item where i_item_id = 'abc' "
            + "or i_item_id = 'def'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `tpcds`.`ITEM`\n"
        + "WHERE `I_ITEM_ID` = ? OR `I_ITEM_ID` = ?", digest);
  }
}
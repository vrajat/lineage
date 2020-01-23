package io.tokern.lineage.sqlplanner.visitors;

import io.tokern.lineage.sqlplanner.planner.LogicalIndexTableScan;
import io.tokern.lineage.sqlplanner.planner.MartColumn;
import io.tokern.lineage.sqlplanner.planner.MartTable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class IndexVisitor extends RelVisitor {
  private static Logger logger = LoggerFactory.getLogger(IndexVisitor.class);

  private static List<SqlOperator> indexOperators = Arrays.asList(
      SqlStdOperatorTable.EQUALS,
      SqlStdOperatorTable.GREATER_THAN,
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      SqlStdOperatorTable.LESS_THAN,
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      SqlStdOperatorTable.BETWEEN
  );

  public class Index {
    public final MartTable table;
    public final MartColumn column;

    Index(MartTable table, MartColumn column) {
      this.table = table;
      this.column = column;
    }

    @Override
    public String toString() {
      return "{"
          + table
          + ", "
          + column
          + '}';
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Index)) {
        return false;
      }
      Index index = (Index) obj;
      return Objects.equals(table, index.table)
          && Objects.equals(column, index.column);
    }

    @Override
    public int hashCode() {
      return Objects.hash(table, column);
    }
  }


  int numIndexTableScans = 0;
  int numScans = 0;
  double numFullScanRows = 0.0;
  double numIndexScanRows = 0.0;
  Set<Index> indices = new HashSet<>();

  @Override
  public void visit(RelNode relNode, int ordinal, RelNode parent) {
    if (relNode instanceof TableScan) {
      final TableScan scan = (TableScan) relNode;
      final MartTable martTable = scan.getTable().unwrap(MartTable.class);

      double rowCount = scan.getRows();
      numScans++;
      if (relNode instanceof LogicalIndexTableScan) {
        logger.debug(martTable + " has an index");
        numIndexTableScans++;
        numIndexScanRows += rowCount;
      } else {
        logger.debug(martTable + " has no index. Parent type: " + parent.getClass().getName());
        numFullScanRows += rowCount;
        if (parent instanceof Filter) {
          Filter filter = (Filter) parent;
          RexNode condition = filter.getCondition();
          captureInputRefs(condition, martTable);
        }
      }
    }
    super.visit(relNode, ordinal, parent);
  }

  private void captureInputRefs(RexNode condition, MartTable martTable) {
    indexOperators.forEach(sqlOperator -> {
      RexCall op = RexUtil.findOperatorCall(sqlOperator, condition);
      if (op != null) {
        logger.debug(sqlOperator + " found");
        List<RexInputRef> inputRefs = new ArrayList<>();
        RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            logger.debug("Visitor found " + inputRef);
            inputRefs.add(inputRef);
            return null;
          }
        };
        op.accept(visitor);

        logger.debug("Found " + indices.size() + " inputRefs");
        inputRefs.forEach(item -> indices.add(
            new Index(martTable, martTable.getColumn(item.getIndex()))));
      }
    });
  }

  public boolean hasNoIndexScans() {
    return numIndexTableScans == 0;
  }

  public boolean hasLessIndexScans() {
    return numIndexScanRows < numFullScanRows;
  }

  public Set<Index> getIndices() {
    return indices;
  }
}

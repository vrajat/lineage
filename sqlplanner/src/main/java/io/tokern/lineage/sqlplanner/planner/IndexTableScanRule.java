package io.tokern.lineage.sqlplanner.planner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that converts
 * a {@link org.apache.calcite.rel.core.Filter}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * of a {@link MartTable}
 * to a {@link LogicalIndexTableScan}.
 *
 * @see org.apache.calcite.rel.rules.ProjectTableScanRule
 */
public class IndexTableScanRule extends RelOptRule {
  /** Rule that matches Filter on TableScan. */
  public static final IndexTableScanRule INSTANCE =
      new IndexTableScanRule(
          operand(Filter.class,
              operand(TableScan.class,
                  none())),
          RelFactories.LOGICAL_BUILDER,
          "IndexTableScanRule");

  protected IndexTableScanRule(RelOptRuleOperand operand,
                               RelBuilderFactory relBuilderFactory, String description) {
    super(operand, relBuilderFactory, description);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Filter filter = call.rel(0);

    final TableScan scan = call.rel(1);
    final RelOptTable table = scan.getTable();
    final MartTable martTable = table.unwrap(MartTable.class);
    return (scan instanceof LogicalTableScan)
      && martTable != null
      && hasIndexUsage(filter.getCondition(), martTable.getKeyOrdinals());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final TableScan scan = call.rel(1);

    final ImmutableIntList projects = scan.identity();

    final Mapping mapping = Mappings.target(projects,
        scan.getTable().getRowType().getFieldCount());
    final RexNode condition = RexUtil.apply(mapping, filter.getCondition());

    call.transformTo(
        LogicalIndexTableScan.create(scan.getCluster(), scan.getTable(),
            condition, projects));
  }

  private boolean hasIndexUsage(RexNode rexNode, ImmutableIntList indexOrdinals) {
    List<RexCall> equalityOps = new ArrayList<>();
    RexVisitor<Void> rexVisitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            if (call.getOperator().equals(SqlStdOperatorTable.EQUALS)
                || call.getOperator().equals(SqlStdOperatorTable.GREATER_THAN)
                || call.getOperator().equals(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
                || call.getOperator().equals(SqlStdOperatorTable.LESS_THAN)
                || call.getOperator().equals(SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
                || call.getOperator().equals(SqlStdOperatorTable.BETWEEN)
            ) {
              equalityOps.add(call);
            }
            super.visitCall(call);
            return null;
          }
        };
    rexNode.accept(rexVisitor);

    boolean hasIndexAccess = false;
    for (RexCall eop : equalityOps) {
      IndexVisitor inputRefVisitor = new IndexVisitor(indexOrdinals);
      eop.accept(inputRefVisitor);
      hasIndexAccess |= inputRefVisitor.getFoundIndex();
    }
    return hasIndexAccess;
  }

  class IndexVisitor extends RexVisitorImpl<Void> {
    boolean foundIndex = false;
    final ImmutableIntList indexOrdinals;

    IndexVisitor(ImmutableIntList indexOrdinals) {
      super(true);
      this.indexOrdinals = indexOrdinals;
    }

    @Override
    public Void visitInputRef(RexInputRef ref) {
      for (int ordinal : indexOrdinals) {
        if (ordinal == ref.getIndex()) {
          foundIndex = true;
        }
      }
      return null;
    }

    public boolean getFoundIndex() {
      return foundIndex;
    }
  }
}

package io.tokern.lineage.sqlplanner.planner;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;

public class LogicalIndexTableScan extends TableScan {
  public final RexNode condition;
  public final ImmutableIntList projects;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalIndexTableScan.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalIndexTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                               RexNode condition, ImmutableIntList projects) {
    super(cluster, traitSet, table);
    this.condition = condition;
    this.projects = projects;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return this;
  }

  /** Creates a LogicalIndexTableScan.
   *
   * @param cluster Cluster
   * @param relOptTable Table
   */
  public static LogicalIndexTableScan create(RelOptCluster cluster,
                                             final RelOptTable relOptTable,
                                             RexNode condition,
                                             ImmutableIntList projects) {
    final Table table = relOptTable.unwrap(Table.class);
    final RelTraitSet traitSet =
        cluster.traitSetOf(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
              if (table != null) {
                return table.getStatistic().getCollations();
              }
              return new ArrayList<>();
            });
    return new LogicalIndexTableScan(cluster, traitSet, relOptTable, condition, projects);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("conditions", condition, true);
  }
}

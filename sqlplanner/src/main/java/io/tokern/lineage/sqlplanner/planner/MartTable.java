package io.tokern.lineage.sqlplanner.planner;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class MartTable extends AbstractTable
    implements QueryableTable, TranslatableTable {

  protected static final Logger LOG = LoggerFactory.getLogger(MartTable.class);

  protected final MartSchema schema;
  protected final String name;
  protected final List<MartColumn> columns;
  final Integer primaryKey;
  final List<Integer> secondaryIndexes;
  final Double rowCount;

  /**
   * Create a MartTable that stores information about a table.
   * @param schema Schema of the table.
   * @param name Name of the table.
   * @param columns List of columns of type MartColumn
   */
  public MartTable(MartSchema schema, String name, List<MartColumn> columns) {
    this(schema, name, columns, null, null, new ArrayList<>());
  }

  /**
   * Create a MartTable that stores information about a table.
   * @param schema Schema of the table.
   * @param name Name of the table.
   * @param columns List of columns of type MartColumn
   * @param rowCount No. of rows in the table
   */
  public MartTable(MartSchema schema, String name, List<MartColumn> columns, Double rowCount) {
    this(schema, name, columns, rowCount, null, new ArrayList<>());
  }

  /**
   * Create a MartTable that stores information about a table.
   * Also set key/index information.
   * @param schema Schema of the table
   * @param name Name of the table
   * @param columns List of columns of the table
   * @param rowCount No. of rows in the table
   * @param primaryKey Primary key of the table
   */
  public MartTable(MartSchema schema, String name, List<MartColumn> columns, Double rowCount,
                   String primaryKey) {
    this(schema, name, columns, rowCount, primaryKey, new ArrayList<>());
  }

  /**
   * Create a MartTable that stores information about a table.
   * Also set key/index information.
   * @param schema Schema of the table
   * @param name Name of the table
   * @param columns List of columns of the table
   * @param rowCount No. of rows in the table
   * @param primaryKey Primary key of the table
   * @param secondaryIndexes List of secondary indexes
   */
  public MartTable(MartSchema schema, String name, List<MartColumn> columns, Double rowCount,
                   String primaryKey, List<String> secondaryIndexes) {
    this.schema = schema;
    this.name = name;
    this.columns = columns;
    this.rowCount = rowCount;
    this.secondaryIndexes = new ArrayList<>();
    if (primaryKey != null) {
      this.primaryKey = getFieldOrdinal(primaryKey.toUpperCase());
      this.secondaryIndexes.add(this.primaryKey);
    } else {
      this.primaryKey = null;
    }
    for (String column : secondaryIndexes) {
      this.secondaryIndexes.add(getFieldOrdinal(column.toUpperCase()));
    }
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName,
                                  Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return LogicalTableScan.create(context.getCluster(), relOptTable);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final List<String> names = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();
    for (MartColumn col : this.columns) {
      final FieldType fieldType = FieldType.of(col.type);
      if (fieldType == null) {
        LOG.error("Field Type is null for " + col.type);
      }
      final RelDataType type = fieldType.toType((JavaTypeFactory) typeFactory);
      types.add(type);
      names.add(col.name);
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  public MartColumn getColumn(int index) {
    return columns.get(index);
  }

  public ImmutableIntList getKeyOrdinals() {
    return ImmutableIntList.copyOf(this.secondaryIndexes);
  }

  /**
   * Get the ordinal number of column.
   * @param columnName Name of the column
   * @return An integer representing the ordinal number
   */
  public int getFieldOrdinal(String columnName) {
    int count = 0;
    for (MartColumn column : columns) {
      if (columnName.equals(column.name)) {
        return count;
      }
      count++;
    }

    throw new RuntimeException("Column " + columnName + " not found in " + this.toString());
  }

  @Override
  public Statistic getStatistic() {
    if (rowCount != null) {
      return Statistics.of(rowCount, new ArrayList<>());
    } else {
      return Statistics.UNKNOWN;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    MartTable other = (MartTable) obj;
    return schema.equals(other.schema) && name.equals(other.name) && columns.equals(other.columns);
  }

  @Override
  public String toString() {
    return schema + "." + name;
  }

  @Override
  public int hashCode() {
    return schema.hashCode() + name.hashCode() * 31 + columns.hashCode() * 47;
  }
}


package io.tokern.lineage.sqlplanner.planner;

import io.tokern.lineage.sqlplanner.QanException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MartSchema extends AbstractSchema {
  private static final Logger LOG = LoggerFactory.getLogger(MartSchema.class);

  public final String name;
  SchemaPlus schemaPlus;
  Map<String, Table> tableMap;

  /**
   * Represents a DB or Schema.
   * @param name Name of the schema
   */
  public MartSchema(String name) {
    super();
    this.name = name;
    tableMap = new HashMap<>();
  }

  /**
   * Set Calcite Schemaplus of this schema.
   * @param schemaPlus SchemaPlus object
   */
  public void setSchemaPlus(SchemaPlus schemaPlus) {
    this.schemaPlus = schemaPlus;
  }

  /**
   * Add a table to the schema.
   * @param martTable Table object
   * @throws QanException Throw an exception if SchemaPlus is not set
   */
  public void addTable(MartTable martTable) throws QanException {
    if (schemaPlus == null) {
      throw new QanException("Internal Error. SchemaPlus is not set");
    }
    this.schemaPlus.add(martTable.name, martTable);
    tableMap.put(martTable.name, martTable);
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}

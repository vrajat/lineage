package io.tokern.lineage.sqlplanner.redshift;

import io.tokern.lineage.redshift.SqlRedshiftParser;
import io.tokern.lineage.sqlplanner.visitors.CtasVisitor;
import io.tokern.lineage.sqlplanner.visitors.InsertVisitor;
import io.tokern.lineage.sqlplanner.visitors.SelectIntoVisitor;
import io.tokern.lineage.sqlplanner.visitors.UnloadVisitor;
import io.tokern.lineage.sqlplanner.planner.Parser;
import io.tokern.lineage.sqlplanner.visitors.CopyVisitor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class RedshiftClassifier {

  public final Parser parser;

  public RedshiftClassifier() {
    parser = new Parser(SqlRedshiftParser.FACTORY);
  }

  /**
   * Classify whether a query is an insert statement.
   * @param query Query String
   * @return Returns the visitor with additional info about the insert statement
   * @throws SqlParseException Exception thrown if there is a syntax error
   */
  public QueryClasses classify(String query) throws SqlParseException {
    // Maintenance Visitor is before parse as regex is used.

    MaintenanceVisitor maintenanceVisitor = new MaintenanceVisitor();
    InsertVisitor insertVisitor = new InsertVisitor();
    CtasVisitor ctasVisitor = new CtasVisitor();
    UnloadVisitor unloadVisitor = new UnloadVisitor();
    CopyVisitor copyVisitor = new CopyVisitor();
    SelectIntoVisitor selectIntoVisitor = new SelectIntoVisitor();

    maintenanceVisitor.visit(query);

    if (!maintenanceVisitor.isPassed()) {
      SqlNode sqlNode = parser.parse(query);
      sqlNode.accept(insertVisitor);
      sqlNode.accept(ctasVisitor);
      sqlNode.accept(unloadVisitor);
      sqlNode.accept(copyVisitor);
      sqlNode.accept(selectIntoVisitor);
    }

    return new QueryClasses(insertVisitor, maintenanceVisitor, ctasVisitor,
        unloadVisitor, copyVisitor, selectIntoVisitor);
  }
}

package io.tokern.lineage.sqlplanner.redshift;

import io.tokern.lineage.sqlplanner.visitors.CopyVisitor;
import io.tokern.lineage.sqlplanner.visitors.CtasVisitor;
import io.tokern.lineage.sqlplanner.visitors.InsertVisitor;
import io.tokern.lineage.sqlplanner.visitors.SelectIntoVisitor;
import io.tokern.lineage.sqlplanner.visitors.UnloadVisitor;

public class QueryClasses {
  public final InsertVisitor insertContext;
  public final MaintenanceVisitor maintenanceContext;
  public final CtasVisitor ctasContext;
  public final UnloadVisitor unloadContext;
  public final CopyVisitor copyContext;
  public final SelectIntoVisitor selectIntoContext;

  /**
   * Holds context of a query. The valid context depends on the type of query.
   * @param insertContext Context if it is an INSERT query
   * @param maintenanceContext Context if it is a maintenance query
   * @param ctasContext Context if it is a CTAS query
   * @param unloadContext Context if it is an UNLOAD query
   * @param copyContext Context if it is a COPY query
   * @param selectIntoContext Context if it is a SELECT .. INTO query
   */
  public QueryClasses(InsertVisitor insertContext,
                      MaintenanceVisitor maintenanceContext,
                      CtasVisitor ctasContext,
                      UnloadVisitor unloadContext,
                      CopyVisitor copyContext,
                      SelectIntoVisitor selectIntoContext) {
    this.insertContext = insertContext;
    this.maintenanceContext = maintenanceContext;
    this.ctasContext = ctasContext;
    this.unloadContext = unloadContext;
    this.copyContext = copyContext;
    this.selectIntoContext = selectIntoContext;
  }
}

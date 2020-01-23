package io.tokern.lineage.analyses.redshift;

import io.tokern.lineage.catalog.redshift.UserQuery;
import io.tokern.lineage.sqlplanner.redshift.QueryClasses;

class QueryInfo implements Comparable<QueryInfo> {
  public final UserQuery query;
  final QueryClasses classes;

  public QueryInfo(UserQuery query, QueryClasses classes) {
    this.query = query;
    this.classes = classes;
  }

  @Override
  public int compareTo(QueryInfo queryInfo) {
    return this.query.startTime.compareTo(queryInfo.query.startTime);
  }
}

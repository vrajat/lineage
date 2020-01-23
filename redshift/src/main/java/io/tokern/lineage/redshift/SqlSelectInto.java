package io.tokern.lineage.redshift;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlSelectInto extends SqlSelect {

  final SqlNode intoTableRef;

  /**
   * Represents a SELECT or SELECT .. INTO SQL Statement.
   * @param pos Position of cursor
   * @param keywordList Keyword list like DISTINCT
   * @param selectList List of expressions in projection
   * @param from From Clause
   * @param where Where clause
   * @param groupBy Group BY clause
   * @param having Having clause
   * @param windowDecls Windowing functions
   * @param orderBy ORDER BY clause
   * @param offset offset of LIMIT .. OFFSET
   * @param fetch Fetch clause
   * @param intoTableRef Table referenced in INTO clause
   */
  public SqlSelectInto(SqlParserPos pos,
                       SqlNodeList keywordList,
                       SqlNodeList selectList,
                       SqlNode from,
                       SqlNode where,
                       SqlNodeList groupBy,
                       SqlNode having,
                       SqlNodeList windowDecls,
                       SqlNodeList orderBy,
                       SqlNode offset,
                       SqlNode fetch,
                       SqlNode intoTableRef) {
    super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls,
          orderBy, offset, fetch);

    this.intoTableRef = intoTableRef;
  }

  public final SqlNode getIntoTableRef() {
    return intoTableRef;
  }
}

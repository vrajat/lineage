package io.tokern.lineage.sqlplanner.visitors;

import io.tokern.lineage.redshift.SqlRedshiftParser;
import io.tokern.lineage.redshift.SqlUnload;
import io.tokern.lineage.sqlplanner.planner.Parser;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UnloadVisitor extends ClassifyingVisitor {
  private static Logger logger = LoggerFactory.getLogger(UnloadVisitor.class);

  private List<SqlIdentifier> sources = new ArrayList<>();
  private SqlLiteral s3Location = null;

  public UnloadVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlUnload) {
      SqlUnload unload = (SqlUnload) sqlCall;
      this.s3Location = unload.getS3Loc();

      try {
        Parser parser = new Parser(SqlRedshiftParser.FACTORY);
        SqlNode sqlNode = parser.parse(unload.getSqlStmt().toValue());
        TableVisitor visitor = new TableVisitor();
        sqlNode.accept(visitor);

        this.sources = visitor.getSources();
        this.passed = true;
      } catch (SqlParseException parseExc) {
        logger.error(parseExc.getMessage());
      }
    }
    return null;
  }

  public List<String> getSources() {
    return sources.stream().map(SqlIdentifier::toString).collect(Collectors.toList());
  }

  public String getS3Location() {
    return s3Location.toValue();
  }
}

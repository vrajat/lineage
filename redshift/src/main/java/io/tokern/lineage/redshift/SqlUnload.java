package io.tokern.lineage.redshift;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlUnload extends SqlCall {

  public static class Params {
    public boolean manifest = false;
    public boolean encrypt = false;
    public boolean bzip2 = false;
    public boolean gzip = false;
    public boolean addQuotes = false;
    public boolean escape = false;
    public boolean allowOverWrite = false;
    public boolean parallel = false;
  }


  private final SqlSpecialOperator operator;
  private final SqlLiteral sqlStmt;
  private final SqlLiteral s3Loc;
  private final Credentials credentials;
  private final SqlLiteral delim;
  private final SqlLiteral nullAs;
  private final SqlLiteral fixedWidth;
  private final SqlLiteral region;
  private final int maxFileSize;
  private final Params params;

  /**
   * Represents an UNLOAD statement.
   * @param pos Position of the statement (for logs and exceptions)
   * @param sqlStmt Literal that stores the sql statement
   * @param s3Loc S3 Location where results are stored
   * @param delim Delimiter in the o/p
   * @param nullAs NULL is stored as literal
   * @param fixedWidth Parameters for fixedwidth o/p
   * @param region Region of the S3 Bucket
   * @param maxFileSize Max size of o/p file in MB or GB
   * @param params Boolean params for Unload
   * @param credentials Credential information to connect to S3
   */
  public SqlUnload(SqlParserPos pos, SqlNode sqlStmt, SqlNode s3Loc,
                   SqlNode delim, SqlNode nullAs,
                   SqlNode fixedWidth, SqlNode region, int maxFileSize,
                   Params params, Credentials credentials) {
    super(pos);

    this.sqlStmt = (SqlLiteral) sqlStmt;
    this.s3Loc = (SqlLiteral) s3Loc;
    this.delim = (SqlLiteral) delim;
    this.nullAs = (SqlLiteral) nullAs;
    this.fixedWidth = (SqlLiteral) fixedWidth;
    this.region = (SqlLiteral) region;

    this.maxFileSize = maxFileSize;
    this.params = params;
    this.credentials = credentials;

    operator = new SqlSpecialOperator("UNLOAD", SqlKind.OTHER);
  }

  @Override
  public SqlOperator getOperator() {
    return operator;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(sqlStmt, s3Loc, delim, nullAs, fixedWidth, region);
  }

  public SqlLiteral getSqlStmt() {
    return sqlStmt;
  }

  public SqlLiteral getS3Loc() {
    return s3Loc;
  }
}

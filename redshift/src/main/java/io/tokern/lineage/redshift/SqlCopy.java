package io.tokern.lineage.redshift;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlCopy extends SqlCall {

  public static class Format {
    public boolean csv = false;
    public SqlLiteral delimiter = null;
    public SqlLiteral json = null;
  }

  public static class DataSource {
    public SqlLiteral s3Loc = null;
    public SqlNode region = null;
    public boolean manifest = false;
    public boolean encrypted = false;
    public Format format = new Format();
    public Credentials credentials = new Credentials();
  }

  public static class ConversionParams {
    public boolean acceptAnyDate = false;
    public boolean acceptInvChars = false;
    public SqlLiteral acceptInvCharsValue = null;
    public boolean blanksAsNull = false;
    public SqlLiteral dateFormat = null;
    public boolean emptyAsNull = false;
    public boolean encoding = false;
    public boolean escape = false;
    public boolean explicitIds = false;
    public boolean fillRecord = false;
    public boolean ignoreBlankLines = false;
    public int ignoreAsHeaders = 0;
    public SqlLiteral nullAs = null;
    public boolean removeQuotes = false;
    public boolean roundec = false;
    public SqlLiteral timeFormat = null;
    public boolean trimBlanks = false;
    public boolean truncateColumns = false;
    public Format format = new Format();
  }

  public static class LoadParams {
    public int compRows = 0;
    public boolean compUpdate = false;
    public int maxError = 0;
    public boolean noLoad = false;
    public boolean statusUpdate;
  }

  private final SqlSpecialOperator operator;
  private final SqlIdentifier table;
  private final DataSource dataSource;
  private final SqlNodeList columnList;
  private final ConversionParams conversionParams;
  private final LoadParams loadParams;

  /**
   * COPY statement in AWS Redshift.
   * @param pos Position of cursor
   * @param table Table to load
   * @param columnList Optional list of columns to load
   * @param dataSource Details about DataSource (FROM clause)
   * @param conversionParams Parameters on how to transform the data before storing it
   * @param loadParams Parameters for parsing the input files
   */
  public SqlCopy(SqlParserPos pos,
                 SqlNode table,
                 SqlNodeList columnList,
                 DataSource dataSource,
                 ConversionParams conversionParams,
                 LoadParams loadParams) {
    super(pos);

    assert table instanceof SqlIdentifier;

    this.table = (SqlIdentifier) table;
    this.columnList = columnList;
    this.dataSource = dataSource;
    this.conversionParams = conversionParams;
    this.loadParams = loadParams;

    this.operator = new SqlSpecialOperator("COPY", SqlKind.OTHER);
  }

  @Override
  public SqlOperator getOperator() {
    return operator;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(
        table,
        columnList);
  }

  public SqlIdentifier getTable() {
    return table;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public SqlNodeList getColumnList() {
    return columnList;
  }

  public ConversionParams getConversionParams() {
    return conversionParams;
  }

  public LoadParams getLoadParams() {
    return loadParams;
  }
}

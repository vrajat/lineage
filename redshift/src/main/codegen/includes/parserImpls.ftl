<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#--
  Add implementations of additional parser statements here.
  Each implementation should return an object of SqlNode type.

  Example of SqlShowTables() implementation:
  SqlNode SqlShowTables()
  {
    ...local variables...
  }
  {
    <SHOW> <TABLES>
    ...
    {
      return SqlShowTables(...)
    }
  }
-->
boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

boolean tempOpt() :
{
}
{
    <TEMP> { return true; }
|
    { return false;}
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        (
            <NULL> { nullable = true; }
        |
            <NOT> <NULL> { nullable = false; }
        |
            { nullable = true; }
        )
        (
            [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
            (
                <VIRTUAL> { strategy = ColumnStrategy.VIRTUAL; }
            |
                <STORED> { strategy = ColumnStrategy.STORED; }
            |
                { strategy = ColumnStrategy.VIRTUAL; }
            )
        |
            <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                strategy = ColumnStrategy.DEFAULT;
            }
        |
            {
                e = null;
                strategy = nullable ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
            }
        )
        {
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), e, strategy));
        }
    |
        { list.add(id); }
    )
|
    id = SimpleIdentifier() {
        list.add(id);
    }
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN> {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <UNIQUE> { s.add(this); }
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    )
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final boolean temp;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
}
{
    temp = tempOpt() <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    {
        return SqlDdlNodes.createTable(s.end(this), replace, ifNotExists, id,
            tableElementList, query);
    }
}

SqlUnload SqlUnloadStmt() :
{
    final SqlNode selectStmt;
    final SqlNode s3Loc;
    SqlNode delim = null;
    SqlNode nullAs = null;
    SqlNode fixedW = null;
    SqlNode region = null;
    int maxFileSize = RelDataType.PRECISION_NOT_SPECIFIED;
    SqlUnload.Params params = new SqlUnload.Params();
    Credentials credentials = new Credentials();
    final Span s;
}
{
    <UNLOAD>
    {
        s = span();
    }
        <LPAREN>
            selectStmt = StringLiteral()
        <RPAREN>
    <TO>
        s3Loc = StringLiteral()
    Authorization(credentials)
    (
        <MANIFEST> [ <VERBOSE> ] { params.manifest = true; }
    |
        <DELIMITER> [ <AS> ] delim = StringLiteral()
    |
        <FIXEDWIDTH> [ <AS> ] fixedW = StringLiteral()
    |
        <ENCRYPTED> { params.encrypt = true; }
    |
        <BZIP2>  { params.bzip2 = true; }
    |
        <GZIP> { params.gzip = true; }
    |
        <ADDQUOTES> { params.addQuotes = false; }
    |
        <NULL> [ <AS> ] nullAs = StringLiteral()
    |
        <ESCAPE> { params.escape = false; }
    |
        <ALLOWOVERWRITE> { params.allowOverWrite = false; }
    |
        <PARALLEL>
        (
            <ON> {params.parallel = true;}
        |
            <TRUE> {params.parallel = true;}
        |
            <OFF> {params.parallel = false;}
        |
            <FALSE> {params.parallel = false;}
        )
    |
        <MAXFILESIZE> [ <AS> ] maxFileSize = UnsignedIntLiteral()
        (
            <MB>
        |
            <GB>
        )
    |
        <REGION> [ <AS> ] region = StringLiteral()
    )*
    {
        return new SqlUnload(s.end(this),
            selectStmt,
            s3Loc,
            delim,
            nullAs,
            fixedW,
            region,
            maxFileSize,
            params, credentials);
    }
}

void Authorization(Credentials credentials) :
{
    final SqlNode sqlNode;
}
{
    (
        <CREDENTIALS> sqlNode = StringLiteral()
        {
            credentials.credentials = (SqlLiteral) sqlNode;
        }
    |
        <IAM_ROLE> sqlNode = StringLiteral()
        {
            credentials.iamRole = (SqlLiteral) sqlNode;
        }
    )
}

void parseConversionParams(SqlCopy.ConversionParams conversionParams, SqlCopy.LoadParams loadParams) :
{
    SqlNode acceptInvChars = null;
    SqlNode dateFormat = null;
    int ignoreAsHeaders = RelDataType.PRECISION_NOT_SPECIFIED;
    SqlNode nullAs = null;
    SqlNode timeFormat = null;
    int compRows = RelDataType.PRECISION_NOT_SPECIFIED;
    int maxError = RelDataType.PRECISION_NOT_SPECIFIED;
}
{
    (
        <ACCEPTANYDATE> {conversionParams.acceptAnyDate = true;}
    |
        <ACCEPTINVCHARS>
        (
            [ <AS> ] acceptInvChars = StringLiteral()
            {
                conversionParams.acceptInvChars = true;
                conversionParams.acceptInvCharsValue = (SqlLiteral) acceptInvChars;
            }
        |
            {
                conversionParams.acceptInvChars = true;
            }
        )
    |
        <BLANKSASNULL> {conversionParams.blanksAsNull = true;}
    |
        <DATEFORMAT> [ <AS> ] dateFormat = StringLiteral()
        {
            conversionParams.dateFormat = (SqlLiteral) dateFormat;
        }
    |
        <EMPTYASNULL> {conversionParams.emptyAsNull = true;}
    |
        <ESCAPE> {conversionParams.escape = true;}
    |
        <EXPLICIT_IDS> {conversionParams.explicitIds = true;}
    |
        <FILLRECORD> {conversionParams.fillRecord = true;}
    |
        <IGNOREBLANKLINES> {conversionParams.ignoreBlankLines = true;}
    |
        <IGNOREHEADER> [ <AS> ] ignoreAsHeaders = UnsignedIntLiteral()
        {
            conversionParams.ignoreAsHeaders = ignoreAsHeaders;
        }
    |
        <NULL> [ <AS> ] nullAs = StringLiteral()
        {
            conversionParams.nullAs = (SqlLiteral) nullAs;
        }
    |
        <REMOVEQUOTES> {conversionParams.removeQuotes = true;}
    |
        <ROUNDEC> {conversionParams.roundec = true;}
    |
        <TIMEFORMAT> [ <AS> ] timeFormat = StringLiteral()
        {
            conversionParams.timeFormat = (SqlLiteral) timeFormat;
        }
    |
        <TRIMBLANKS> {conversionParams.trimBlanks = true;}
    |
        <TRUNCATECOLUMNS> {conversionParams.truncateColumns = true;}
    |
        <COMPROWS> compRows = UnsignedIntLiteral()
        {
            loadParams.compRows = compRows;
        }
    |
        <COMPUPDATE>
        (
            <ON> {loadParams.compUpdate = true;}
        |
            <TRUE> {loadParams.compUpdate = true;}
        |
            <OFF> {loadParams.compUpdate = false;}
        |
            <FALSE> {loadParams.compUpdate = false;}
        )
    |
        <MAXERROR> maxError = UnsignedIntLiteral()
        {
            loadParams.maxError = maxError;
        }
    |
        <NOLOAD> {loadParams.noLoad = true;}
    |
        <STATUPDATE>
        (
            <ON> {loadParams.compUpdate = true;}
        |
            <TRUE> {loadParams.compUpdate = true;}
        |
            <OFF> {loadParams.compUpdate = false;}
        |
            <FALSE> {loadParams.compUpdate = false;}
        )
    |
        copyFormat(conversionParams.format)
    )*
}

void copyFormat(SqlCopy.Format format) :
{
    final SqlNode delimiter;
    final SqlNode json;
}
{
    [ <FORMAT> [ <AS> ] ]
    (
        <CSV>
        {
            format.csv = true;
        }
    |
        <DELIMITER> [ <AS> ] delimiter = StringLiteral()
        {
            format.delimiter = (SqlLiteral) delimiter;
        }
    |
        <JSON> [ <AS> ] json = StringLiteral()
        {
            format.json = (SqlLiteral) json;
        }
    )
}

void copyDataSource(SqlCopy.DataSource source) :
{
    final SqlNode s3Loc;
    SqlNode region = null;
}
{
    <FROM> s3Loc = StringLiteral()
    {
        source.s3Loc = (SqlLiteral) s3Loc;
    }
    Authorization(source.credentials)
    (
        <MANIFEST> {source.manifest = true;}
    |
        <ENCRYPTED> {source.encrypted = true;}
    |
        <REGION> [ <AS> ] region = StringLiteral()
        {
            source.region = (SqlLiteral) region;
        }
    |
        copyFormat(source.format)
    )*
}

SqlCopy SqlCopyStmt() :
{
    final SqlNode table;
    SqlNodeList columnList = null;
    final Span s;
    SqlCopy.DataSource dataSource = new SqlCopy.DataSource();
    SqlCopy.ConversionParams conversionParams = new SqlCopy.ConversionParams();
    SqlCopy.LoadParams loadParams = new SqlCopy.LoadParams();
}
{
    <COPY>
    {
        s = span();
    }
        table = CompoundIdentifier()
    [
        LOOKAHEAD(2)
        { final Pair<SqlNodeList, SqlNodeList> p; }
        p = ParenthesizedCompoundIdentifierList() {
            if (p.left.size() > 0) {
                columnList = p.left;
            }
        }
    ]
    copyDataSource(dataSource)
    parseConversionParams(conversionParams, loadParams)
    {
        return new SqlCopy(s.end(this), table, columnList,
            dataSource, conversionParams, loadParams);
    }
}
import io.tokern.lineage.redshift.SqlRedshiftParser;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.tokern.lineage.redshift.utils.SqlProvider;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RedshiftParserTest {
  private static Logger logger = LoggerFactory.getLogger(RedshiftParserTest.class);

  Quoting quoting = Quoting.DOUBLE_QUOTE;
  Casing unquotedCasing = Casing.TO_UPPER;
  Casing quotedCasing = Casing.UNCHANGED;
  SqlConformance conformance = SqlConformanceEnum.LENIENT;

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SqlProvider.class)
  @Tags({@Tag("/parseRedshiftSuccess.yaml")})
  public void parseSuccessTest(String name, String targetTable,
                               List<String> sources, String query) throws SqlParseException {
    logger.info(name);
    logger.info(query);
    SqlParser parser = SqlParser.create(query,
        SqlParser.configBuilder()
            .setParserFactory(SqlRedshiftParser.FACTORY)
            .setQuoting(quoting)
            .setUnquotedCasing(unquotedCasing)
            .setQuotedCasing(quotedCasing)
            .setConformance(conformance)
            .setCaseSensitive(false)
            .build());

    try {
      SqlNode sqlNode = parser.parseStmt();
      assertNotNull(sqlNode);
    } catch (SqlParseException parseExc) {
      logger.error(parseExc.getMessage(), parseExc);
      throw parseExc;
    }
  }
}

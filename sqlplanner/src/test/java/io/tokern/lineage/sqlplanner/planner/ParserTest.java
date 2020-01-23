package io.tokern.lineage.sqlplanner.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ParserTest {
  private static Logger logger = LoggerFactory.getLogger(ParserTest.class);

  static class SqlProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws IOException {
      Stream.Builder<Arguments> argumentsBuilder = Stream.builder();
      for (String filename : extensionContext.getTags()) {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(this.getClass().getResourceAsStream(filename)));

        String name = null;
        StringBuilder stringBuilder = new StringBuilder();

        Pattern pattern = Pattern.compile("-- ParserTest:(.+)");
        while (reader.ready()) {
          String line = reader.readLine();
          Matcher matcher = pattern.matcher(line);
          if (matcher.matches()) {
            if (name != null) {
              argumentsBuilder.add(Arguments.of(name, stringBuilder.toString()));
            }
            name = matcher.group(1);
            stringBuilder = new StringBuilder();
          } else if (stringBuilder != null) {
            stringBuilder.append(line);
            stringBuilder.append('\n');
          } else {
            throw new IOException("SQL file does not follow required pattern");
          }
        }
        if (name != null) {
          argumentsBuilder.add(Arguments.of(name, stringBuilder.toString()));
        }
      }
      return argumentsBuilder.build();
    }
  }

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SqlProvider.class)
  @Tags({@Tag("/parseSuccess.sql"), @Tag("/tpcds.sql")})
  public void parseSuccessTest(String name, String sql) throws SqlParseException {
    Parser parser = new Parser();
    logger.info(name);
    SqlNode sqlNode = parser.parse(sql);
    assertNotNull(sqlNode);
  }

  @Test
  public void testError() {
    Parser parser = new Parser();
    Assertions.assertThrows(SqlParseException.class, () -> parser.parse("select from"));
  }

  @Test
  void digestTest() throws SqlParseException {
    Parser parser = new Parser();
    String digest = parser.digest("select i_color from item where i_color = 'abc'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `ITEM`\n"
        + "WHERE `I_COLOR` = ?", digest);
  }

  @Test
  void digestWithMultipleFilters() throws SqlParseException {
    Parser parser = new Parser();
    String digest = parser.digest("select i_color from item where i_item_id = 'abc' "
            + "and i_color = 'blue'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `ITEM`\n"
        + "WHERE `I_ITEM_ID` = ? AND `I_COLOR` = ?", digest);
  }

  @Test
  void prettyTest() throws SqlParseException {
    Parser parser = new Parser();
    String digest = parser.pretty("select i_color from item where i_color = 'abc'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `ITEM`\n"
        + "WHERE `I_COLOR` = 'abc'", digest);
  }
}
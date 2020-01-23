package io.tokern.lineage.sqlplanner.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.params.provider.Arguments;

import java.util.List;

public class TestCase {
  public final String name;
  public final String targetTable;
  public final List<String> sourceTables;
  public final String query;

  @JsonCreator
  public TestCase(
      @JsonProperty("name") String name,
      @JsonProperty("target") String targetTable,
      @JsonProperty("sources") List<String> sourceTables,
      @JsonProperty("query") String query) {
    this.name = name;
    this.targetTable = targetTable;
    this.sourceTables = sourceTables;
    this.query = query;
  }

  Arguments getArgs() {
    return Arguments.of(
        this.name,
        this.targetTable,
        this.sourceTables,
        this.query
      );
    }
}

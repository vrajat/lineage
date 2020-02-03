package io.tokern.lineage.catalog.redshift;

public class AgentConfiguration {
  public static class CsvConfiguration {
    String pathDir;

    public String getPathDir() {
      return pathDir;
    }

    public void setPathDir(String pathDir) {
      this.pathDir = pathDir;
    }
  }

  CsvConfiguration csv;

  public CsvConfiguration getCsv() {
    return csv;
  }

  public void setCsv(CsvConfiguration csv) {
    this.csv = csv;
  }
}

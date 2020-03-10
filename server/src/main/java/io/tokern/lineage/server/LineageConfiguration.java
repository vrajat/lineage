package io.tokern.lineage.server;

import io.dropwizard.Configuration;
import io.tokern.lineage.catalog.redshift.CatalogConfiguration;

public class LineageConfiguration extends Configuration {
  CatalogConfiguration catalog;

  public CatalogConfiguration getCatalog() {
    return catalog;
  }

  public void setCatalog(CatalogConfiguration catalog) {
    this.catalog = catalog;
  }
}

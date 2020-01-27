package io.tokern.lineage.server;

import io.dropwizard.Configuration;
import io.tokern.lineage.catalog.redshift.AgentConfiguration;

public class LineageConfiguration extends Configuration {
  AgentConfiguration agent;

  public AgentConfiguration getAgent() {
    return agent;
  }

  public void setAgent(AgentConfiguration agent) {
    this.agent = agent;
  }
}

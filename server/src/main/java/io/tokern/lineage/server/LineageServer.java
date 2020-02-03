package io.tokern.lineage.server;

import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.tokern.lineage.analyses.redshift.Dag;
import io.tokern.lineage.analyses.redshift.Etl;
import io.tokern.lineage.analyses.redshift.GraphSerializer;
import io.tokern.lineage.catalog.redshift.Agent;
import io.tokern.lineage.catalog.redshift.RedshiftCsv;
import io.tokern.lineage.server.resources.DagResource;

import java.io.FileNotFoundException;

public class LineageServer extends Application<LineageConfiguration> {

  public static void main(final String[] args) throws Exception {
    new LineageServer().run(args);
  }

  @Override
  public String getName() {
    return "Server";
  }

  @Override
  public void initialize(final Bootstrap<LineageConfiguration> bootstrap) {
    SimpleModule module = new SimpleModule();
    module.addSerializer(Dag.Graph.class, new GraphSerializer());
    bootstrap.getObjectMapper().registerModule(module);
  }

  @Override
  public void run(final LineageConfiguration configuration,
                  final Environment environment) throws FileNotFoundException {
    Etl etl = new Etl(environment.metrics());
    Agent agent = null;

    if (configuration.agent.getCsv() != null) {
      agent = new RedshiftCsv(configuration.agent.getCsv().getPathDir(), environment.metrics());
    }

    environment.jersey().register(new DagResource(etl, agent));
  }
}

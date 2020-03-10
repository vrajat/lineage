package io.tokern.lineage.server;

import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
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
    bootstrap.addBundle(new AssetsBundle("/frontend/assets/", "/", "index.html"));
    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
            new EnvironmentVariableSubstitutor(false)
        )
    );
    SimpleModule module = new SimpleModule();
    module.addSerializer(Dag.Graph.class, new GraphSerializer());
    bootstrap.getObjectMapper().registerModule(module);
  }

  @Override
  public void run(final LineageConfiguration configuration,
                  final Environment environment) throws FileNotFoundException {
    Etl etl = new Etl(environment.metrics());
    Agent agent = null;

    if (configuration.catalog.getCsv() != null) {
      agent = new RedshiftCsv(configuration.catalog.getCsv().getPathDir(), environment.metrics());
    }

    environment.jersey().register(new DagResource(etl, agent));
  }
}

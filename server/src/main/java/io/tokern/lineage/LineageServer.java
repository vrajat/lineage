package io.tokern.lineage;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

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
        // TODO: application initialization
    }

    @Override
    public void run(final LineageConfiguration configuration,
                    final Environment environment) {
        // TODO: implement application
    }

}

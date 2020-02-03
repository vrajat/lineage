package io.tokern.lineage.server.resources;

import io.tokern.lineage.analyses.redshift.Dag;
import io.tokern.lineage.analyses.redshift.Etl;
import io.tokern.lineage.catalog.redshift.Agent;
import io.tokern.lineage.catalog.util.MetricAgentException;
import io.tokern.lineage.sqlplanner.QanException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.LocalDateTime;

@Path("/dag")
@Produces(MediaType.APPLICATION_JSON)
public class DagResource {
  private static final Logger logger = LoggerFactory.getLogger(DagResource.class);

  Etl etl;
  Agent agent;

  public DagResource(Etl etl, Agent agent) {
    this.etl = etl;
    this.agent = agent;
  }

  @GET
  public Response getDag(@QueryParam("start") String startStr,
                         @QueryParam("end") String endStr,
                         @QueryParam("node") String node,
                         @QueryParam("predecessors") boolean isPredecessors
                         ) throws MetricAgentException, QanException {

    LocalDateTime start = LocalDateTime.parse(startStr);
    LocalDateTime end = LocalDateTime.parse(endStr);

    Dag.Graph graph = etl.getSubDag(agent.getQueries(start, end), node, isPredecessors);

    return Response.ok().entity(graph).build();
  }
}

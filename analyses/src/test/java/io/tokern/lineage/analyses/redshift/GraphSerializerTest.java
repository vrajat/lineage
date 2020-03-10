package io.tokern.lineage.analyses.redshift;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphSerializerTest {
  @Test
  void simpleGraph() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    SimpleModule module = new SimpleModule();
    module.addSerializer(Dag.Graph.class, new GraphSerializer());
    mapper.registerModule(module);

    MutableGraph<Dag.Node> graph = GraphBuilder.directed().allowsSelfLoops(true).build();
    Dag.Node aNode = new Dag.Node("a");
    aNode.addStartEndTime(LocalDateTime.of(2019, 1, 15, 6, 30),
        LocalDateTime.of(2019, 1, 15, 6, 40));
    aNode.updateExecutionTimes(10);

    Dag.Node bNode = new Dag.Node("b");
    bNode.addStartEndTime(LocalDateTime.of(2019, 1, 15, 7, 30),
        LocalDateTime.of(2019, 1, 15, 7, 40));
    bNode.updateExecutionTimes(10);

    Dag.Node cNode = new Dag.Node("c");
    cNode.addStartEndTime(LocalDateTime.of(2019, 1, 15, 7, 30),
        LocalDateTime.of(2019, 1, 15, 7, 40));
    cNode.updateExecutionTimes(10);

    graph.putEdge(aNode, bNode);
    graph.putEdge(bNode, cNode);
    ImmutableGraph<Dag.Node> immutableGraph = ImmutableGraph.copyOf(graph);
    List<Dag.Phase> phases = Dag.topologicalSort(immutableGraph);
    String serialized = mapper.writeValueAsString(
        new Dag.Graph(ImmutableGraph.copyOf(graph), phases));
    assertEquals("{\"nodes\":[{\"data\":{\"id\":\"a\"}},{\"data\":{\"id\":\"b\"}},{\"data\":{\"id\":\"c\"}}]," +
            "\"edges\":[{\"data\":{\"source\":\"a\",\"target\":\"b\"}},{\"data\":{\"source\":\"b\",\"target\":\"c\"}}]," +
            "\"phases\":[{\"tables\":[\"a\"]},{\"tables\":[\"b\"]},{\"tables\":[\"c\"]}]}"
        , serialized);

  }
}

package io.tokern.lineage.analyses.redshift;

import com.google.common.graph.EndpointPair;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class GraphSerializer extends StdSerializer<Dag.Graph> {
  public GraphSerializer() {
    this(null);
  }

  public GraphSerializer(Class<Dag.Graph> clazz) {
    super(clazz);
  }

  @Override
  public void serialize(
      Dag.Graph graph,
      JsonGenerator jgen, SerializerProvider serializerProvider)
    throws IOException {
    jgen.writeStartObject();

    jgen.writeFieldName("nodes");
    jgen.writeStartArray();
    for (Dag.Node node : graph.dag.nodes()) {
      jgen.writeStartObject();
      jgen.writeFieldName("data");
      jgen.writeStartObject();
      jgen.writeStringField("id", node.getTable());
      jgen.writeEndObject();
      jgen.writeEndObject();
    }
    jgen.writeEndArray();

    jgen.writeFieldName("edges");
    jgen.writeStartArray();
    for (EndpointPair<Dag.Node> pair : graph.dag.edges()) {
      jgen.writeStartObject();
      jgen.writeFieldName("data");
      jgen.writeStartObject();
      jgen.writeStringField("source", pair.source().getTable());
      jgen.writeStringField("target", pair.target().getTable());
      jgen.writeEndObject();
      jgen.writeEndObject();
    }

    jgen.writeEndArray();

    jgen.writeFieldName("phases");
    jgen.writeObject(graph.phases);

    jgen.writeEndObject();
  }
}

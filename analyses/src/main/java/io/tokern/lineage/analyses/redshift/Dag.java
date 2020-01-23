package io.tokern.lineage.analyses.redshift;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.ImmutableGraph;
import com.google.common.graph.MutableGraph;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import io.tokern.lineage.sqlplanner.visitors.CopyVisitor;
import io.tokern.lineage.sqlplanner.visitors.CtasVisitor;
import io.tokern.lineage.sqlplanner.visitors.InsertVisitor;
import io.tokern.lineage.sqlplanner.visitors.SelectIntoVisitor;
import io.tokern.lineage.sqlplanner.visitors.UnloadVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

public class Dag {
  private static Logger logger = LoggerFactory.getLogger(Dag.class);

  static class Node implements Comparable<Node> {
    private final String table;
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("Y-MM-dd HH:mm:ss");
    private Histogram executionTimes;
    private List<LocalDateTime> startTimes;
    private List<LocalDateTime> endTimes;
    int numDegree;

    Node(String table) {
      this.table = table;
      executionTimes = new Histogram(new ExponentiallyDecayingReservoir());
      startTimes = new ArrayList<>();
      endTimes = new ArrayList<>();
      numDegree = 0;
    }

    public void updateExecutionTimes(long seconds) {
      executionTimes.update(seconds);
    }

    public void addStartEndTime(LocalDateTime start, LocalDateTime end) {
      startTimes.add(start);
      endTimes.add(end);
    }

    public String getTable() {
      return table;
    }

    public double getMean() {
      return executionTimes.getSnapshot().getMean();
    }

    public List<String> getStartTimes() {
      return startTimes.stream().map(tm -> tm.format(formatter)).collect(Collectors.toList());
    }

    public List<String> getEndTimes() {
      return endTimes.stream().map(tm -> tm.format(formatter)).collect(Collectors.toList());
    }

    public int getNumDegree() {
      return numDegree;
    }

    public void setNumDegree(int numDegree) {
      this.numDegree = numDegree;
    }

    @Override
    public int compareTo(Node node) {
      return Integer.compare(this.numDegree, node.numDegree);
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      Node node = (Node) object;
      return table.equals(node.table);
    }

    @Override
    public int hashCode() {
      return Objects.hash(table);
    }

    @Override
    public String toString() {
      return "Node{"
          + "table='" + table + '\''
          + ", numDegree=" + numDegree
          + '}';
    }
  }

  static class Graph {
    public final ImmutableGraph<Node> dag;
    public final List<Phase> phases;

    public Graph(ImmutableGraph<Node> dag, List<Phase> phases) {
      this.dag = dag;
      this.phases = phases;
    }
  }

  static class Phase {
    public final Set<String> tables;

    public Phase(Set<String> tables) {
      this.tables = tables;
    }
  }

  /**
   * Generate a di-graph with tables as nodes and insert data movement as dependency.
   * @param infos Query information POJO
   * @return A Guava immutable graph of inserts
   */
  static Graph buildGraph(List<QueryInfo> infos) {
    Map<String, Node> nodeMap = new HashMap<>();

    Node s3Source = new Node("S3 Source");
    Node s3Sink = new Node("S3 Sink");

    infos.forEach((info) -> {
      if (info.classes.insertContext.isPassed()) {
        InsertVisitor visitor = info.classes.insertContext;
        String targetTable = visitor.getTargetTable();
        if (!nodeMap.containsKey(targetTable)) {
          nodeMap.put(targetTable, new Node(targetTable));
        }
        Node node = nodeMap.get(targetTable);
        node.addStartEndTime(info.query.startTime, info.query.endTime);
        node.updateExecutionTimes(info.query.getDuration());
        visitor.getSources().forEach((src) -> {
          if (!nodeMap.containsKey(src)) {
            nodeMap.put(src, new Node(src));
          }
        });
      } else if (info.classes.ctasContext.isPassed()) {
        CtasVisitor visitor = info.classes.ctasContext;
        String targetTable = visitor.getTargetTable();
        if (!nodeMap.containsKey(targetTable)) {
          nodeMap.put(targetTable, new Node(targetTable));
        }
        Node node = nodeMap.get(targetTable);
        node.addStartEndTime(info.query.startTime, info.query.endTime);
        node.updateExecutionTimes(info.query.getDuration());
        visitor.getSources().forEach((src) -> {
          if (!nodeMap.containsKey(src)) {
            nodeMap.put(src, new Node(src));
          }
        });
      } else if (info.classes.copyContext.isPassed()) {
        CopyVisitor visitor = info.classes.copyContext;
        String targetTable = visitor.getTargetTable();
        if (!nodeMap.containsKey(targetTable)) {
          nodeMap.put(targetTable, new Node(targetTable));
        }
        Node node = nodeMap.get(targetTable);
        node.addStartEndTime(info.query.startTime, info.query.endTime);
        node.updateExecutionTimes(info.query.getDuration());
      } else if (info.classes.unloadContext.isPassed()) {
        UnloadVisitor visitor = info.classes.unloadContext;
        visitor.getSources().forEach((src) -> {
          if (!nodeMap.containsKey(src)) {
            nodeMap.put(src, new Node(src));
          }
        });
      } else if (info.classes.selectIntoContext.isPassed()) {
        SelectIntoVisitor visitor = info.classes.selectIntoContext;
        String targetTable = visitor.getTargetTable();
        if (!nodeMap.containsKey(targetTable)) {
          nodeMap.put(targetTable, new Node(targetTable));
        }
        Node node = nodeMap.get(targetTable);
        node.addStartEndTime(info.query.startTime, info.query.endTime);
        node.updateExecutionTimes(info.query.getDuration());
        visitor.getSources().forEach((src) -> {
          if (!nodeMap.containsKey(src)) {
            nodeMap.put(src, new Node(src));
          }
        });
      }
    });

    MutableGraph<Node> dag = GraphBuilder.directed().allowsSelfLoops(true).build();
    infos.forEach((info) -> {
      if (info.classes.insertContext.isPassed()) {
        InsertVisitor visitor = info.classes.insertContext;
        visitor.getSources().forEach(src -> dag.putEdge(nodeMap.get(src),
            nodeMap.get(visitor.getTargetTable())));
      } else if (info.classes.ctasContext.isPassed()) {
        CtasVisitor visitor = info.classes.ctasContext;
        visitor.getSources().forEach(src -> dag.putEdge(nodeMap.get(src),
            nodeMap.get(visitor.getTargetTable())));
      } else if (info.classes.unloadContext.isPassed()) {
        UnloadVisitor visitor = info.classes.unloadContext;
        visitor.getSources().forEach(src -> dag.putEdge(nodeMap.get(src), s3Sink));
      } else if (info.classes.copyContext.isPassed()) {
        CopyVisitor visitor = info.classes.copyContext;
        dag.putEdge(s3Source, nodeMap.get(visitor.getTargetTable()));
      } else if (info.classes.selectIntoContext.isPassed()) {
        SelectIntoVisitor visitor = info.classes.selectIntoContext;
        visitor.getSources().forEach(src -> dag.putEdge(nodeMap.get(src),
            nodeMap.get(visitor.getTargetTable())));
      }
    });

    ImmutableGraph<Node> immutableGraph = ImmutableGraph.copyOf(dag);
    List<Phase> phases = topologicalSort(immutableGraph);
    return new Graph(immutableGraph, phases);
  }

  static List<Phase> topologicalSort(ImmutableGraph<Node> dag) {
    List<Phase> phases = new ArrayList<>();
    PriorityQueue<Node> priorityQueue = new PriorityQueue<>(dag.nodes().size());

    dag.nodes().forEach((node) -> {
      node.setNumDegree(dag.inDegree(node));
      priorityQueue.add(node);
      logger.debug(node.toString());
    });

    Set<Node> processed = new HashSet<>();
    logger.debug("Start Processing");
    while (!priorityQueue.isEmpty()) {
      Set<Node> nodes = new HashSet<>();
      int leastDegree = priorityQueue.peek().getNumDegree();
      logger.debug("Lowest degress is " + leastDegree);
      while (priorityQueue.peek() != null
          && priorityQueue.peek().getNumDegree() == leastDegree) {
        Node node = priorityQueue.poll();
        nodes.add(node);
        processed.add(node);
        logger.debug("Processed " + node);
      }
      logger.info("Removed nodes: " + nodes.size());
      nodes.forEach(node ->
          dag.successors(node).forEach((successor) -> {
            if (!processed.contains(successor)) {
              priorityQueue.remove(successor);
              successor.setNumDegree(successor.getNumDegree() - 1);
              priorityQueue.add(successor);
            }
          })
      );
      logger.info("Updated successors");
      Phase phase = new Phase(
          nodes.stream().map(Node::getTable).collect(Collectors.toSet())
      );
      phases.add(phase);
    }

    return phases;
  }
}
